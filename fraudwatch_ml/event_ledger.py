"""Append-only records for contract-valid transaction and label events."""

from __future__ import annotations

import hashlib
from datetime import datetime

from fraudwatch_ml.quarantine import InvalidLabel, InvalidTransaction


def _id(row) -> int:
    return row["id"] if isinstance(row, dict) else row[0]


def _values(row, *names):
    if isinstance(row, dict):
        return tuple(row[name] for name in names)
    return tuple(row)


def record_transaction_event(
    conn,
    *,
    transaction_id: str,
    terminal_id: str,
    occurred_at: datetime,
    source_topic: str,
    source_partition: int,
    source_offset: int,
    kafka_key: bytes | None,
    kafka_payload: bytes,
    source_payload_hash: bytes,
) -> int:
    """Insert once and reject a transaction ID that changes meaning."""
    inserted = conn.execute(
        """
        INSERT INTO valid_event_ledger (
            event_type, transaction_id, terminal_id, occurred_at,
            source_topic, source_partition, source_offset,
            kafka_key, kafka_payload, source_payload_sha256
        ) VALUES (
            'transaction', %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (event_type, transaction_id) DO NOTHING
        RETURNING id
        """,
        (
            transaction_id,
            terminal_id,
            occurred_at,
            source_topic,
            source_partition,
            source_offset,
            kafka_key,
            kafka_payload,
            source_payload_hash,
        ),
    ).fetchone()
    if inserted is not None:
        return _id(inserted)

    existing = conn.execute(
        """
        SELECT id, terminal_id, occurred_at, source_payload_sha256,
               kafka_payload
        FROM valid_event_ledger
        WHERE event_type = 'transaction' AND transaction_id = %s
        """,
        (transaction_id,),
    ).fetchone()
    if existing is None:
        raise RuntimeError("Transaction ledger conflict did not return a row")
    (
        ledger_id,
        stored_terminal,
        stored_at,
        stored_hash,
        stored_payload,
    ) = _values(
        existing,
        "id",
        "terminal_id",
        "occurred_at",
        "source_payload_sha256",
        "kafka_payload",
    )
    exact = (
        stored_terminal == terminal_id
        and stored_at == occurred_at
        and bytes(stored_hash) == source_payload_hash
        and (
            stored_payload is None
            or bytes(stored_payload) == kafka_payload
        )
    )
    if not exact:
        raise InvalidTransaction(
            f"Conflicting immutable event for {transaction_id}",
            reason_code="transaction.conflict",
        )
    return ledger_id


def record_label_event(
    conn,
    *,
    transaction_id: str,
    terminal_id: str | None,
    fraud_label: int,
    observed_at: datetime,
    source_topic: str | None,
    source_partition: int | None,
    source_offset: int | None,
    kafka_key: bytes | None,
    kafka_payload: bytes,
) -> int:
    """Append a fully delay-validated label to the immutable ledger."""
    payload_hash = hashlib.sha256(kafka_payload).digest()
    inserted = conn.execute(
        """
        INSERT INTO valid_event_ledger (
            event_type, transaction_id, terminal_id, fraud_label, observed_at,
            source_topic, source_partition, source_offset,
            kafka_key, kafka_payload, source_payload_sha256
        ) VALUES (
            'label', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (event_type, transaction_id) DO NOTHING
        RETURNING id
        """,
        (
            transaction_id,
            terminal_id,
            fraud_label,
            observed_at,
            source_topic,
            source_partition,
            source_offset,
            kafka_key,
            kafka_payload,
            payload_hash,
        ),
    ).fetchone()
    if inserted is not None:
        return _id(inserted)

    existing = conn.execute(
        """
        SELECT id, terminal_id, fraud_label, observed_at,
               source_payload_sha256, kafka_payload
        FROM valid_event_ledger
        WHERE event_type = 'label' AND transaction_id = %s
        """,
        (transaction_id,),
    ).fetchone()
    if existing is None:
        raise RuntimeError("Label ledger conflict did not return a row")
    (
        ledger_id,
        stored_terminal,
        stored_label,
        stored_at,
        stored_hash,
        stored_payload,
    ) = _values(
        existing,
        "id",
        "terminal_id",
        "fraud_label",
        "observed_at",
        "source_payload_sha256",
        "kafka_payload",
    )
    exact = (
        stored_terminal in (None, terminal_id)
        and stored_label == fraud_label
        and stored_at == observed_at
        and bytes(stored_hash) == payload_hash
        and (
            stored_payload is None
            or bytes(stored_payload) == kafka_payload
        )
    )
    if not exact:
        raise InvalidLabel(
            f"Conflicting immutable label for {transaction_id}",
            reason_code="label.conflict",
        )
    return ledger_id
