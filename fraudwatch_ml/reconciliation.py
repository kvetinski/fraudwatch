"""Audited replay and promotion for terminals blocked by late events."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np
import pandas as pd
from psycopg.types.json import Jsonb

from fraudwatch_ml.event_ledger import record_label_event
from fraudwatch_ml.features import (
    FEATURE_NAMES,
    LABEL_DELAY_DAYS,
    TERMINAL_WINDOWS_DAYS,
    base_features_one,
)
from fraudwatch_ml.labels import parse_label
from fraudwatch_ml.locks import lock_terminal, lock_transaction
from fraudwatch_ml.online_features import TerminalHistory
from fraudwatch_ml.quarantine import InvalidLabel, store_quarantined
from fraudwatch_ml.scoring import (
    FEATURE_SCHEMA_VERSION,
    Transaction,
    make_feature_frame,
)
from fraudwatch_ml.transactions import parse_transaction, validate_transaction_key


class ReconciliationError(RuntimeError):
    """A fail-closed reconciliation invariant was not satisfied."""


@dataclass(frozen=True)
class TransactionSource:
    ledger_id: int
    event: Transaction
    occurred_at: datetime
    source_topic: str
    source_partition: int
    source_offset: int
    payload_hash: bytes


@dataclass(frozen=True)
class LabelSource:
    transaction_id: str
    terminal_id: str
    fraud_label: int
    observed_at: datetime
    source_topic: str | None
    source_partition: int | None
    source_offset: int | None
    kafka_key: bytes | None
    kafka_payload: bytes
    payload_hash: bytes
    pending: bool


@dataclass(frozen=True)
class OrderingQuarantine:
    quarantine_id: int
    event_ledger_id: int
    status: str


@dataclass(frozen=True)
class SourceSnapshot:
    terminal_id: str
    transactions: tuple[TransactionSource, ...]
    labels: tuple[LabelSource, ...]
    quarantines: tuple[OrderingQuarantine, ...]
    digest: bytes

    @property
    def event_count(self) -> int:
        return len(self.transactions) + len(self.labels)


@dataclass(frozen=True)
class ReconciliationRun:
    run_id: int
    terminal_id: str
    status: str
    model_reference: str
    feature_schema_version: str
    source_event_count: int
    source_event_sha256: bytes
    candidate_count: int | None = None
    candidate_sha256: bytes | None = None
    parity_verified: bool = False


def _values(row, *names):
    if isinstance(row, dict):
        return tuple(row[name] for name in names)
    return tuple(row)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _timestamp(value: datetime) -> str:
    return _utc(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


def _canonical_label_payload(
    transaction_id: str,
    fraud_label: int,
    observed_at: datetime,
) -> bytes:
    return json.dumps(
        {
            "fraud_label": fraud_label,
            "observed_at": _timestamp(observed_at),
            "transaction_id": transaction_id,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _digest_records(records: list[dict[str, Any]]) -> bytes:
    encoded = json.dumps(
        records,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(encoded).digest()


def _load_transactions(conn, terminal_id: str) -> list[TransactionSource]:
    rows = conn.execute(
        """
        SELECT id, transaction_id, terminal_id, occurred_at,
               source_topic, source_partition, source_offset,
               kafka_key, kafka_payload, source_payload_sha256
        FROM valid_event_ledger
        WHERE event_type = 'transaction' AND terminal_id = %s
        ORDER BY occurred_at, transaction_id
        """,
        (terminal_id,),
    ).fetchall()
    transactions = []
    for row in rows:
        (
            ledger_id,
            transaction_id,
            stored_terminal,
            occurred_at,
            source_topic,
            source_partition,
            source_offset,
            kafka_key,
            kafka_payload,
            payload_hash,
        ) = _values(
            row,
            "id",
            "transaction_id",
            "terminal_id",
            "occurred_at",
            "source_topic",
            "source_partition",
            "source_offset",
            "kafka_key",
            "kafka_payload",
            "source_payload_sha256",
        )
        if kafka_payload is None:
            raise ReconciliationError(
                f"Transaction {transaction_id} has no recoverable source payload"
            )
        payload = bytes(kafka_payload)
        digest = bytes(payload_hash)
        if hashlib.sha256(payload).digest() != digest:
            raise ReconciliationError(
                f"Transaction {transaction_id} source digest does not match"
            )
        event, parsed_at = parse_transaction(payload)
        validate_transaction_key(event, None if kafka_key is None else bytes(kafka_key))
        if (
            event.transaction_id != transaction_id
            or event.terminal_id != stored_terminal
            or parsed_at.to_pydatetime() != occurred_at
        ):
            raise ReconciliationError(
                f"Transaction {transaction_id} ledger metadata does not match its payload"
            )
        if source_topic is None or source_partition is None or source_offset is None:
            raise ReconciliationError(
                f"Transaction {transaction_id} has no Kafka source coordinates"
            )
        transactions.append(
            TransactionSource(
                ledger_id=ledger_id,
                event=event,
                occurred_at=occurred_at,
                source_topic=source_topic,
                source_partition=source_partition,
                source_offset=source_offset,
                payload_hash=digest,
            )
        )
    if not transactions:
        raise ReconciliationError(f"Terminal {terminal_id} has no valid events")
    return transactions


def _load_active_labels(
    conn,
    transactions: list[TransactionSource],
) -> list[LabelSource]:
    ids = [item.event.transaction_id for item in transactions]
    terminals = {item.event.transaction_id: item.event.terminal_id for item in transactions}
    rows = conn.execute(
        """
        SELECT fl.transaction_id, fl.fraud_label, fl.observed_at,
               ledger.terminal_id, ledger.source_topic,
               ledger.source_partition, ledger.source_offset,
               ledger.kafka_key, ledger.kafka_payload,
               ledger.source_payload_sha256
        FROM fraud_labels AS fl
        LEFT JOIN valid_event_ledger AS ledger
          ON ledger.event_type = 'label'
         AND ledger.transaction_id = fl.transaction_id
        WHERE fl.transaction_id = ANY(%s)
        ORDER BY fl.transaction_id
        """,
        (ids,),
    ).fetchall()
    labels = []
    for row in rows:
        (
            transaction_id,
            fraud_label,
            observed_at,
            ledger_terminal,
            source_topic,
            source_partition,
            source_offset,
            kafka_key,
            kafka_payload,
            payload_hash,
        ) = _values(
            row,
            "transaction_id",
            "fraud_label",
            "observed_at",
            "terminal_id",
            "source_topic",
            "source_partition",
            "source_offset",
            "kafka_key",
            "kafka_payload",
            "source_payload_sha256",
        )
        if kafka_payload is None or payload_hash is None:
            raise ReconciliationError(
                f"Active label {transaction_id} is missing its immutable ledger event"
            )
        payload = bytes(kafka_payload)
        digest = bytes(payload_hash)
        if hashlib.sha256(payload).digest() != digest:
            raise ReconciliationError(f"Active label {transaction_id} digest mismatch")
        parsed_id, parsed_label, parsed_at = parse_label(payload)
        if (
            parsed_id != transaction_id
            or parsed_label != fraud_label
            or parsed_at != observed_at
            or ledger_terminal not in (None, terminals[transaction_id])
        ):
            raise ReconciliationError(
                f"Active label {transaction_id} ledger metadata mismatch"
            )
        labels.append(
            LabelSource(
                transaction_id=transaction_id,
                terminal_id=terminals[transaction_id],
                fraud_label=fraud_label,
                observed_at=observed_at,
                source_topic=source_topic,
                source_partition=source_partition,
                source_offset=source_offset,
                kafka_key=None if kafka_key is None else bytes(kafka_key),
                kafka_payload=payload,
                payload_hash=digest,
                pending=False,
            )
        )
    return labels


def _load_pending_labels(
    conn,
    transactions: list[TransactionSource],
    *,
    quarantine_invalid: bool,
) -> list[LabelSource]:
    by_id = {item.event.transaction_id: item for item in transactions}
    rows = conn.execute(
        """
        SELECT transaction_id, fraud_label, observed_at,
               source_topic, source_partition, source_offset,
               kafka_key, kafka_payload, source_payload_sha256
        FROM pending_fraud_labels
        WHERE transaction_id = ANY(%s)
        ORDER BY transaction_id
        """,
        (list(by_id),),
    ).fetchall()
    labels = []
    for row in rows:
        (
            transaction_id,
            fraud_label,
            observed_at,
            source_topic,
            source_partition,
            source_offset,
            kafka_key,
            kafka_payload,
            payload_hash,
        ) = _values(
            row,
            "transaction_id",
            "fraud_label",
            "observed_at",
            "source_topic",
            "source_partition",
            "source_offset",
            "kafka_key",
            "kafka_payload",
            "source_payload_sha256",
        )
        tx = by_id[transaction_id]
        payload = (
            bytes(kafka_payload)
            if kafka_payload is not None
            else _canonical_label_payload(transaction_id, fraud_label, observed_at)
        )
        digest = hashlib.sha256(payload).digest()
        if payload_hash is not None and bytes(payload_hash) != digest:
            raise ReconciliationError(f"Pending label {transaction_id} digest mismatch")
        if kafka_payload is not None:
            parsed_id, parsed_label, parsed_at = parse_label(payload)
            if (
                parsed_id != transaction_id
                or parsed_label != fraud_label
                or parsed_at != observed_at
            ):
                raise ReconciliationError(
                    f"Pending label {transaction_id} metadata mismatch"
                )
        if kafka_key is not None and bytes(kafka_key) != transaction_id.encode():
            raise ReconciliationError(f"Pending label {transaction_id} Kafka key mismatch")

        earliest = tx.occurred_at + timedelta(days=LABEL_DELAY_DAYS)
        if observed_at < earliest:
            error = InvalidLabel(
                "Fraud label violates the seven-day delay",
                reason_code="label.delay_violation",
            )
            if not quarantine_invalid:
                raise ReconciliationError(
                    f"Pending label {transaction_id} violates the label delay"
                )
            if (
                source_topic is None
                or source_partition is None
                or source_offset is None
                or kafka_payload is None
            ):
                raise ReconciliationError(
                    f"Invalid legacy pending label {transaction_id} has no recoverable source"
                )
            store_quarantined(
                conn,
                source_topic=source_topic,
                source_partition=source_partition,
                source_offset=source_offset,
                kafka_key=None if kafka_key is None else bytes(kafka_key),
                kafka_payload=payload,
                error=error,
            )
            conn.execute(
                "DELETE FROM pending_fraud_labels WHERE transaction_id = %s",
                (transaction_id,),
            )
            continue

        labels.append(
            LabelSource(
                transaction_id=transaction_id,
                terminal_id=tx.event.terminal_id,
                fraud_label=fraud_label,
                observed_at=observed_at,
                source_topic=source_topic,
                source_partition=source_partition,
                source_offset=source_offset,
                kafka_key=None if kafka_key is None else bytes(kafka_key),
                kafka_payload=payload,
                payload_hash=digest,
                pending=True,
            )
        )
    return labels


def _load_ordering_quarantines(
    conn,
    terminal_id: str,
    transactions: list[TransactionSource],
) -> list[OrderingQuarantine]:
    rows = conn.execute(
        """
        SELECT q.id, links.event_ledger_id, q.status
        FROM valid_event_quarantine_links AS links
        JOIN valid_event_ledger AS ledger ON ledger.id = links.event_ledger_id
        JOIN event_quarantine AS q ON q.id = links.quarantine_id
        WHERE ledger.terminal_id = %s
          AND q.reason_code IN (
              'transaction.out_of_order', 'transaction.terminal_blocked'
          )
        ORDER BY q.id
        """,
        (terminal_id,),
    ).fetchall()
    result = [
        OrderingQuarantine(*_values(row, "id", "event_ledger_id", "status"))
        for row in rows
    ]
    if not result:
        raise ReconciliationError(
            f"Terminal {terminal_id} has no linked ordering quarantine"
        )
    if any(item.status != "pending" for item in result):
        raise ReconciliationError(
            f"Terminal {terminal_id} has a non-pending ordering quarantine"
        )

    scored = {
        _values(row, "transaction_id")[0]
        for row in conn.execute(
            """
            SELECT transaction_id FROM scored_transactions
            WHERE transaction_id = ANY(%s)
            """,
            ([item.event.transaction_id for item in transactions],),
        ).fetchall()
    }
    linked = {item.event_ledger_id for item in result}
    incomplete = [
        item.event.transaction_id
        for item in transactions
        if item.event.transaction_id not in scored and item.ledger_id not in linked
    ]
    if incomplete:
        raise ReconciliationError(
            "Unscored ledger events are not linked to quarantine: "
            + ", ".join(incomplete)
        )
    return result


def _snapshot(
    conn,
    terminal_id: str,
    *,
    quarantine_invalid_pending: bool = False,
) -> SourceSnapshot:
    transactions = _load_transactions(conn, terminal_id)
    active = _load_active_labels(conn, transactions)
    pending = _load_pending_labels(
        conn,
        transactions,
        quarantine_invalid=quarantine_invalid_pending,
    )
    overlap = {item.transaction_id for item in active} & {
        item.transaction_id for item in pending
    }
    if overlap:
        raise ReconciliationError(
            "Labels are both active and pending: " + ", ".join(sorted(overlap))
        )
    labels = sorted(active + pending, key=lambda item: item.transaction_id)
    quarantines = _load_ordering_quarantines(conn, terminal_id, transactions)

    records: list[dict[str, Any]] = []
    for item in transactions:
        records.append(
            {
                "kind": "transaction",
                "ledger_id": item.ledger_id,
                "transaction_id": item.event.transaction_id,
                "terminal_id": item.event.terminal_id,
                "occurred_at": _timestamp(item.occurred_at),
                "source": [
                    item.source_topic,
                    item.source_partition,
                    item.source_offset,
                ],
                "payload_sha256": item.payload_hash.hex(),
            }
        )
    for item in labels:
        records.append(
            {
                "kind": "pending_label" if item.pending else "label",
                "transaction_id": item.transaction_id,
                "fraud_label": item.fraud_label,
                "observed_at": _timestamp(item.observed_at),
                "source": [
                    item.source_topic,
                    item.source_partition,
                    item.source_offset,
                ],
                "payload_sha256": item.payload_hash.hex(),
            }
        )
    for item in quarantines:
        records.append(
            {
                "kind": "ordering_quarantine",
                "quarantine_id": item.quarantine_id,
                "event_ledger_id": item.event_ledger_id,
                "status": item.status,
            }
        )
    return SourceSnapshot(
        terminal_id=terminal_id,
        transactions=tuple(transactions),
        labels=tuple(labels),
        quarantines=tuple(quarantines),
        digest=_digest_records(records),
    )


def _lock_source(conn, terminal_id: str) -> None:
    """Lock transaction identities before the terminal using the worker order.

    A transaction worker can add one last ledger event while these locks are
    being acquired. Detect that case after obtaining the terminal lock and
    retry from a new database transaction instead of taking locks out of order.
    """
    before = {
        _values(row, "transaction_id")[0]
        for row in conn.execute(
            """
            SELECT transaction_id FROM valid_event_ledger
            WHERE event_type = 'transaction' AND terminal_id = %s
            """,
            (terminal_id,),
        ).fetchall()
    }
    for transaction_id in sorted(before):
        lock_transaction(conn, transaction_id)
    lock_terminal(conn, terminal_id)
    after = {
        _values(row, "transaction_id")[0]
        for row in conn.execute(
            """
            SELECT transaction_id FROM valid_event_ledger
            WHERE event_type = 'transaction' AND terminal_id = %s
            """,
            (terminal_id,),
        ).fetchall()
    }
    if after != before:
        raise ReconciliationError(
            "Terminal source changed while locks were acquired; retry the operation"
        )


def _block(conn, terminal_id: str):
    row = conn.execute(
        """
        SELECT first_quarantine_id, last_scored_at, late_occurred_at
        FROM terminal_order_blocks
        WHERE terminal_id = %s
        FOR UPDATE
        """,
        (terminal_id,),
    ).fetchone()
    if row is None:
        raise ReconciliationError(f"Terminal {terminal_id} is not blocked")
    return _values(row, "first_quarantine_id", "last_scored_at", "late_occurred_at")


def _audit(conn, run_id: int, terminal_id: str, action: str, note: dict) -> None:
    conn.execute(
        """
        INSERT INTO terminal_reconciliation_audit (
            reconciliation_run_id, terminal_id, action, note
        ) VALUES (%s, %s, %s, %s)
        """,
        (run_id, terminal_id, action, Jsonb(note)),
    )


def preview_terminal(
    conn,
    *,
    terminal_id: str,
    model_reference: str,
    requested_by: str,
    note: str,
) -> ReconciliationRun:
    """Freeze and audit the exact source snapshot that a replay will use."""
    if not terminal_id.strip() or not requested_by.strip() or not note.strip():
        raise ValueError("terminal_id, requested_by, and note are required")
    with conn.transaction():
        _lock_source(conn, terminal_id)
        first_quarantine_id, _, _ = _block(conn, terminal_id)
        existing = conn.execute(
            """
            SELECT id FROM terminal_reconciliation_runs
            WHERE terminal_id = %s AND status IN ('previewed', 'verified')
            """,
            (terminal_id,),
        ).fetchone()
        if existing is not None:
            raise ReconciliationError(
                f"Terminal {terminal_id} already has active reconciliation run "
                f"{_values(existing, 'id')[0]}"
            )
        snapshot = _snapshot(
            conn,
            terminal_id,
            quarantine_invalid_pending=True,
        )
        if first_quarantine_id not in {
            item.quarantine_id for item in snapshot.quarantines
        }:
            raise ReconciliationError(
                "The terminal block is not linked to its first quarantine event"
            )
        row = conn.execute(
            """
            INSERT INTO terminal_reconciliation_runs (
                terminal_id, first_quarantine_id, model_reference,
                feature_schema_version, source_event_count,
                source_event_sha256, requested_by, note
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                terminal_id,
                first_quarantine_id,
                model_reference,
                FEATURE_SCHEMA_VERSION,
                snapshot.event_count,
                snapshot.digest,
                requested_by,
                note,
            ),
        ).fetchone()
        run_id = _values(row, "id")[0]
        _audit(
            conn,
            run_id,
            terminal_id,
            "previewed",
            {
                "source_event_count": snapshot.event_count,
                "source_event_sha256": snapshot.digest.hex(),
                "ordering_quarantine_ids": [
                    item.quarantine_id for item in snapshot.quarantines
                ],
            },
        )
    return get_run(conn, run_id)


def _get_run_for_update(conn, run_id: int):
    row = conn.execute(
        """
        SELECT id, terminal_id, status, model_reference,
               feature_schema_version, source_event_count,
               source_event_sha256, candidate_count, candidate_sha256,
               parity_verified
        FROM terminal_reconciliation_runs
        WHERE id = %s
        FOR UPDATE
        """,
        (run_id,),
    ).fetchone()
    if row is None:
        raise ReconciliationError(f"Reconciliation run {run_id} does not exist")
    return _values(
        row,
        "id",
        "terminal_id",
        "status",
        "model_reference",
        "feature_schema_version",
        "source_event_count",
        "source_event_sha256",
        "candidate_count",
        "candidate_sha256",
        "parity_verified",
    )


def _verify_snapshot(
    snapshot: SourceSnapshot,
    source_count: int,
    source_digest: bytes | memoryview,
) -> None:
    if snapshot.event_count != source_count or snapshot.digest != bytes(source_digest):
        raise ReconciliationError(
            "Terminal source changed after preview; create a new reconciliation run"
        )


def _reference_features(
    current: TransactionSource,
    transactions: tuple[TransactionSource, ...],
    labels: dict[str, LabelSource],
) -> np.ndarray:
    at = pd.Timestamp(current.occurred_at)
    base = base_features_one(at, current.event.amount)
    values = []
    for days in TERMINAL_WINDOWS_DAYS:
        lower = at - pd.Timedelta(days=LABEL_DELAY_DAYS + days)
        upper = at - pd.Timedelta(days=LABEL_DELAY_DAYS)
        eligible = [
            item
            for item in transactions
            if lower <= pd.Timestamp(item.occurred_at) <= upper
        ]
        frauds = sum(
            labels[item.event.transaction_id].fraud_label == 1
            and pd.Timestamp(labels[item.event.transaction_id].observed_at) <= at
            for item in eligible
            if item.event.transaction_id in labels
        )
        count = len(eligible)
        values.extend((count, frauds, frauds / count if count else 0.0))
    terminal = np.asarray(values, dtype=np.float32)
    return np.concatenate((base, terminal.astype(np.float64)))


def _candidate_record(
    transaction: TransactionSource,
    version: int,
    score: float,
    model_reference: str,
    features: dict[str, float],
) -> dict[str, Any]:
    return {
        "decision_version": version,
        "event_ledger_id": transaction.ledger_id,
        "feature_schema_version": FEATURE_SCHEMA_VERSION,
        "feature_vector": features,
        "fraud_score": float(score).hex(),
        "model_reference": model_reference,
        "occurred_at": _timestamp(transaction.occurred_at),
        "payload_sha256": transaction.payload_hash.hex(),
        "source": [transaction.source_partition, transaction.source_offset],
        "terminal_id": transaction.event.terminal_id,
        "transaction_id": transaction.event.transaction_id,
    }


def replay_run(conn, scorer, *, run_id: int) -> ReconciliationRun:
    """Replay a preview under a pinned model and persist unpromoted versions."""
    with conn.transaction():
        run = _get_run_for_update(conn, run_id)
        (
            _,
            terminal_id,
            status,
            model_reference,
            schema_version,
            source_count,
            source_digest,
            _,
            _,
            _,
        ) = run
        if status != "previewed":
            raise ReconciliationError(
                f"Run {run_id} must be previewed, not {status}"
            )
        if schema_version != FEATURE_SCHEMA_VERSION:
            raise ReconciliationError("Feature schema version changed after preview")
        if scorer.model_reference != model_reference:
            raise ReconciliationError(
                "Loaded model does not match the model pinned by the preview"
            )
        _lock_source(conn, terminal_id)
        _block(conn, terminal_id)
        snapshot = _snapshot(conn, terminal_id)
        _verify_snapshot(snapshot, source_count, source_digest)

        history = TerminalHistory()
        for item in snapshot.transactions:
            history.record_transaction(
                item.event.transaction_id,
                item.event.terminal_id,
                item.occurred_at,
            )
        labels = {item.transaction_id: item for item in snapshot.labels}
        for item in snapshot.labels:
            history.record_label(
                item.transaction_id,
                item.fraud_label,
                item.observed_at,
            )

        version_rows = conn.execute(
            """
            SELECT transaction_id, max(decision_version)
            FROM fraud_decision_versions
            WHERE transaction_id = ANY(%s)
            GROUP BY transaction_id
            """,
            ([item.event.transaction_id for item in snapshot.transactions],),
        ).fetchall()
        versions = {
            _values(row, "transaction_id", "max")[0]: _values(
                row, "transaction_id", "max"
            )[1]
            for row in version_rows
        }

        candidate_records = []
        for item in snapshot.transactions:
            frame = make_feature_frame(item.event, history)
            actual = frame.iloc[0].to_numpy(dtype=np.float64, copy=False)
            reference = _reference_features(item, snapshot.transactions, labels)
            if not np.allclose(actual, reference, rtol=0.0, atol=1e-6):
                raise ReconciliationError(
                    f"Offline feature parity failed for {item.event.transaction_id}"
                )
            score = scorer.predict_probability(actual)
            if not math.isfinite(score) or not 0.0 <= score <= 1.0:
                raise ReconciliationError(
                    f"Model returned invalid score for {item.event.transaction_id}"
                )
            feature_vector = {
                name: float(value) for name, value in zip(FEATURE_NAMES, actual)
            }
            version = versions.get(item.event.transaction_id, 0) + 1
            conn.execute(
                """
                INSERT INTO fraud_decision_versions (
                    transaction_id, decision_version, event_ledger_id,
                    terminal_id, occurred_at, fraud_score, model_reference,
                    feature_schema_version, feature_vector,
                    source_payload_hash, kafka_partition, kafka_offset,
                    origin, reconciliation_run_id
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    'reconciliation', %s
                )
                """,
                (
                    item.event.transaction_id,
                    version,
                    item.ledger_id,
                    terminal_id,
                    item.occurred_at,
                    score,
                    model_reference,
                    FEATURE_SCHEMA_VERSION,
                    Jsonb(feature_vector),
                    item.payload_hash,
                    item.source_partition,
                    item.source_offset,
                    run_id,
                ),
            )
            candidate_records.append(
                _candidate_record(
                    item,
                    version,
                    score,
                    model_reference,
                    feature_vector,
                )
            )

        candidate_digest = _digest_records(candidate_records)
        conn.execute(
            """
            UPDATE terminal_reconciliation_runs
            SET status = 'verified', candidate_count = %s,
                candidate_sha256 = %s, parity_verified = true,
                verified_at = now()
            WHERE id = %s
            """,
            (len(candidate_records), candidate_digest, run_id),
        )
        _audit(
            conn,
            run_id,
            terminal_id,
            "replayed",
            {"candidate_count": len(candidate_records)},
        )
        _audit(
            conn,
            run_id,
            terminal_id,
            "verified",
            {
                "candidate_sha256": candidate_digest.hex(),
                "offline_feature_parity": True,
            },
        )
    return get_run(conn, run_id)


def _load_candidate_records(conn, run_id: int) -> tuple[list[tuple], list[dict]]:
    rows = conn.execute(
        """
        SELECT versions.transaction_id, versions.decision_version,
               versions.event_ledger_id, versions.terminal_id,
               versions.occurred_at, versions.fraud_score,
               versions.model_reference, versions.feature_schema_version,
               versions.feature_vector, versions.source_payload_hash,
               versions.kafka_partition, versions.kafka_offset
        FROM fraud_decision_versions AS versions
        WHERE versions.reconciliation_run_id = %s
        ORDER BY versions.occurred_at, versions.transaction_id
        """,
        (run_id,),
    ).fetchall()
    unpacked = [
        _values(
            row,
            "transaction_id",
            "decision_version",
            "event_ledger_id",
            "terminal_id",
            "occurred_at",
            "fraud_score",
            "model_reference",
            "feature_schema_version",
            "feature_vector",
            "source_payload_hash",
            "kafka_partition",
            "kafka_offset",
        )
        for row in rows
    ]
    records = []
    for row in unpacked:
        (
            transaction_id,
            version,
            ledger_id,
            terminal_id,
            occurred_at,
            score,
            model_reference,
            schema_version,
            features,
            payload_hash,
            partition,
            offset,
        ) = row
        records.append(
            {
                "decision_version": version,
                "event_ledger_id": ledger_id,
                "feature_schema_version": schema_version,
                "feature_vector": features,
                "fraud_score": float(score).hex(),
                "model_reference": model_reference,
                "occurred_at": _timestamp(occurred_at),
                "payload_sha256": bytes(payload_hash).hex(),
                "source": [partition, offset],
                "terminal_id": terminal_id,
                "transaction_id": transaction_id,
            }
        )
    return unpacked, records


def promote_run(
    conn,
    *,
    run_id: int,
    expected_source_sha256: str,
) -> ReconciliationRun:
    """Atomically promote verified versions, resolve ordering quarantine, unblock."""
    try:
        expected_digest = bytes.fromhex(expected_source_sha256)
    except ValueError as exc:
        raise ValueError("expected_source_sha256 must be hexadecimal") from exc
    if len(expected_digest) != 32:
        raise ValueError("expected_source_sha256 must contain 32 bytes")

    with conn.transaction():
        run = _get_run_for_update(conn, run_id)
        (
            _,
            terminal_id,
            status,
            _,
            _,
            source_count,
            source_digest,
            candidate_count,
            candidate_digest,
            parity_verified,
        ) = run
        if status != "verified" or not parity_verified:
            raise ReconciliationError(f"Run {run_id} is not verified")
        if bytes(source_digest) != expected_digest:
            raise ReconciliationError("Promotion confirmation hash does not match preview")

        _lock_source(conn, terminal_id)
        first_quarantine_id, _, _ = _block(conn, terminal_id)
        snapshot = _snapshot(conn, terminal_id)
        _verify_snapshot(snapshot, source_count, source_digest)
        candidates, candidate_records = _load_candidate_records(conn, run_id)
        if (
            len(candidates) != candidate_count
            or _digest_records(candidate_records) != bytes(candidate_digest)
        ):
            raise ReconciliationError("Reconciliation candidates changed after verification")
        if {row[0] for row in candidates} != {
            item.event.transaction_id for item in snapshot.transactions
        }:
            raise ReconciliationError("Candidate set does not match the source transaction set")

        for item in snapshot.transactions:
            conn.execute(
                """
                INSERT INTO scored_transactions (
                    transaction_id, terminal_id, occurred_at
                ) VALUES (%s, %s, %s)
                ON CONFLICT (transaction_id) DO NOTHING
                """,
                (item.event.transaction_id, terminal_id, item.occurred_at),
            )
            stored = conn.execute(
                """
                SELECT terminal_id, occurred_at FROM scored_transactions
                WHERE transaction_id = %s
                """,
                (item.event.transaction_id,),
            ).fetchone()
            if _values(stored, "terminal_id", "occurred_at") != (
                terminal_id,
                item.occurred_at,
            ):
                raise ReconciliationError(
                    f"Scored transaction conflicts for {item.event.transaction_id}"
                )

        conn.execute(
            """
            INSERT INTO fraud_decisions (
                transaction_id, fraud_score, model_reference,
                feature_schema_version, feature_vector,
                source_payload_hash, kafka_partition, kafka_offset,
                decision_version, scored_at
            )
            SELECT transaction_id, fraud_score, model_reference,
                   feature_schema_version, feature_vector,
                   source_payload_hash, kafka_partition, kafka_offset,
                   decision_version, now()
            FROM fraud_decision_versions
            WHERE reconciliation_run_id = %s
            ON CONFLICT (transaction_id) DO UPDATE SET
                fraud_score = EXCLUDED.fraud_score,
                model_reference = EXCLUDED.model_reference,
                feature_schema_version = EXCLUDED.feature_schema_version,
                feature_vector = EXCLUDED.feature_vector,
                source_payload_hash = EXCLUDED.source_payload_hash,
                kafka_partition = EXCLUDED.kafka_partition,
                kafka_offset = EXCLUDED.kafka_offset,
                decision_version = EXCLUDED.decision_version,
                scored_at = EXCLUDED.scored_at
            """,
            (run_id,),
        )

        for label in snapshot.labels:
            if not label.pending:
                continue
            record_label_event(
                conn,
                transaction_id=label.transaction_id,
                terminal_id=label.terminal_id,
                fraud_label=label.fraud_label,
                observed_at=label.observed_at,
                source_topic=label.source_topic,
                source_partition=label.source_partition,
                source_offset=label.source_offset,
                kafka_key=label.kafka_key,
                kafka_payload=label.kafka_payload,
            )
            conn.execute(
                """
                INSERT INTO fraud_labels (
                    transaction_id, fraud_label, observed_at
                ) VALUES (%s, %s, %s)
                """,
                (label.transaction_id, label.fraud_label, label.observed_at),
            )
            conn.execute(
                "DELETE FROM pending_fraud_labels WHERE transaction_id = %s",
                (label.transaction_id,),
            )

        quarantine_ids = [item.quarantine_id for item in snapshot.quarantines]
        updated = conn.execute(
            """
            UPDATE event_quarantine
            SET status = 'resolved', resolved_at = now()
            WHERE id = ANY(%s) AND status = 'pending'
            """,
            (quarantine_ids,),
        )
        if updated.rowcount != len(quarantine_ids):
            raise ReconciliationError("Not all ordering quarantines remained pending")
        for quarantine_id in quarantine_ids:
            conn.execute(
                """
                INSERT INTO event_quarantine_audit (
                    quarantine_id, action, note
                ) VALUES (%s, 'resolved', %s)
                """,
                (quarantine_id, f"promoted by terminal reconciliation run {run_id}"),
            )

        deleted = conn.execute(
            """
            DELETE FROM terminal_order_blocks
            WHERE terminal_id = %s AND first_quarantine_id = %s
            """,
            (terminal_id, first_quarantine_id),
        )
        if deleted.rowcount != 1:
            raise ReconciliationError("Terminal block changed before promotion")
        conn.execute(
            """
            UPDATE fraud_decision_versions
            SET promoted_at = now()
            WHERE reconciliation_run_id = %s
            """,
            (run_id,),
        )
        conn.execute(
            """
            UPDATE terminal_reconciliation_runs
            SET status = 'promoted', promoted_at = now()
            WHERE id = %s
            """,
            (run_id,),
        )
        _audit(
            conn,
            run_id,
            terminal_id,
            "promoted",
            {
                "candidate_count": candidate_count,
                "candidate_sha256": bytes(candidate_digest).hex(),
                "resolved_quarantine_ids": quarantine_ids,
            },
        )
        _audit(
            conn,
            run_id,
            terminal_id,
            "unblocked",
            {"first_quarantine_id": first_quarantine_id},
        )
    return get_run(conn, run_id)


def abandon_run(
    conn,
    *,
    run_id: int,
    requested_by: str,
    note: str,
) -> ReconciliationRun:
    """Close an unpromoted run so a changed terminal can be previewed again."""
    if not requested_by.strip() or not note.strip():
        raise ValueError("requested_by and note are required")
    with conn.transaction():
        run = _get_run_for_update(conn, run_id)
        terminal_id = run[1]
        status = run[2]
        if status not in ("previewed", "verified"):
            raise ReconciliationError(
                f"Run {run_id} cannot be abandoned from status {status}"
            )
        conn.execute(
            """
            UPDATE terminal_reconciliation_runs
            SET status = 'abandoned', abandoned_at = now()
            WHERE id = %s
            """,
            (run_id,),
        )
        _audit(
            conn,
            run_id,
            terminal_id,
            "abandoned",
            {"requested_by": requested_by, "reason": note},
        )
    return get_run(conn, run_id)


def get_run(conn, run_id: int) -> ReconciliationRun:
    row = conn.execute(
        """
        SELECT id, terminal_id, status, model_reference,
               feature_schema_version, source_event_count,
               source_event_sha256, candidate_count, candidate_sha256,
               parity_verified
        FROM terminal_reconciliation_runs
        WHERE id = %s
        """,
        (run_id,),
    ).fetchone()
    if row is None:
        raise ReconciliationError(f"Reconciliation run {run_id} does not exist")
    values = _values(
        row,
        "id",
        "terminal_id",
        "status",
        "model_reference",
        "feature_schema_version",
        "source_event_count",
        "source_event_sha256",
        "candidate_count",
        "candidate_sha256",
        "parity_verified",
    )
    return ReconciliationRun(
        run_id=values[0],
        terminal_id=values[1],
        status=values[2],
        model_reference=values[3],
        feature_schema_version=values[4],
        source_event_count=values[5],
        source_event_sha256=bytes(values[6]),
        candidate_count=values[7],
        candidate_sha256=(None if values[8] is None else bytes(values[8])),
        parity_verified=values[9],
    )
