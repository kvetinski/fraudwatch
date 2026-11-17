
import hashlib
import json
from datetime import datetime, timedelta, timezone

from fraudwatch_ml.contracts import (
    EventContractError,
    LABEL_FIELDS,
    decode_event_object,
    require_rfc3339,
)
from fraudwatch_ml.event_ledger import record_label_event
from fraudwatch_ml.features import LABEL_DELAY_DAYS
from fraudwatch_ml.locks import lock_terminal, lock_transaction
from fraudwatch_ml.quarantine import InvalidLabel, store_quarantined


_KAFKA_KEY_NOT_PROVIDED = object()


def _ledger_payload(
    transaction_id: str,
    label: int,
    observed_at: datetime,
    raw_payload: bytes | memoryview | None,
) -> bytes:
    if raw_payload is not None:
        return bytes(raw_payload)
    timestamp = observed_at.astimezone(timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )
    return json.dumps(
        {
            "transaction_id": transaction_id,
            "fraud_label": label,
            "observed_at": timestamp,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def parse_label(payload: bytes) -> tuple[str, int, datetime]:
    try:
        data = decode_event_object(
            payload,
            expected_fields=LABEL_FIELDS,
            event_name="fraud-label",
        )
    except EventContractError as exc:
        raise InvalidLabel(
            str(exc), reason_code="label.contract_violation"
        ) from exc

    transaction_id = data["transaction_id"]
    label = data["fraud_label"]
    timestamp = data["observed_at"]

    if not isinstance(transaction_id, str) or not transaction_id.strip():
        raise InvalidLabel(
            "Invalid transaction ID", reason_code="label.invalid_transaction_id"
        )

    if type(label) is not int or label not in (0, 1):
        raise InvalidLabel(
            "Fraud label must be 0 or 1", reason_code="label.invalid_value"
        )

    try:
        require_rfc3339(timestamp, field_name="observed_at")
    except EventContractError as exc:
        raise InvalidLabel(
            str(exc), reason_code="label.invalid_timestamp"
        ) from exc

    observed_at = datetime.fromisoformat(
        timestamp.replace("Z", "+00:00")
    )

    return (
        transaction_id,
        label,
        observed_at.astimezone(timezone.utc),
    )


def store_label(
    conn,
    payload: bytes,
    *,
    kafka_key: bytes | None | object = _KAFKA_KEY_NOT_PROVIDED,
    source_topic: str | None = None,
    source_partition: int | None = None,
    source_offset: int | None = None,
) -> bool:
    """
    Store an active label or stage it until its transaction arrives.

    Returns True for a new logical label and False for an identical retry.
    Deterministic contract, delay, and conflict errors raise InvalidLabel.
    Production callers provide all source coordinates so a staged label can be
    quarantined later if the transaction proves its delay invalid.
    """
    transaction_id, label, observed_at = parse_label(payload)
    source_coordinates = (source_topic, source_partition, source_offset)
    if any(value is not None for value in source_coordinates) and not all(
        value is not None for value in source_coordinates
    ):
        raise RuntimeError("Label source coordinates must be provided together")

    if kafka_key is not _KAFKA_KEY_NOT_PROVIDED:
        expected_key = transaction_id.encode("utf-8")
        if kafka_key != expected_key:
            raise InvalidLabel(
                "Kafka key must equal transaction_id",
                reason_code="label.kafka_key_mismatch",
            )

    with conn.transaction():
        lock_transaction(conn, transaction_id)
        transaction = conn.execute(
            """
            SELECT terminal_id, occurred_at
            FROM scored_transactions
            WHERE transaction_id = %s
            """,
            (transaction_id,),
        ).fetchone()

        if transaction is not None:
            terminal_id, occurred_at = transaction
            lock_terminal(conn, terminal_id)
            _validate_delay(observed_at, occurred_at)

        active = conn.execute(
            """
            SELECT fraud_label, observed_at
            FROM fraud_labels
            WHERE transaction_id = %s
            """,
            (transaction_id,),
        ).fetchone()
        pending = conn.execute(
            """
            SELECT fraud_label, observed_at,
                   source_topic, source_partition, source_offset,
                   kafka_key, kafka_payload
            FROM pending_fraud_labels
            WHERE transaction_id = %s
            """,
            (transaction_id,),
        ).fetchone()

        if active is not None and pending is not None:
            raise RuntimeError(
                f"Label is both active and pending: {transaction_id}"
            )

        existing = (
            active
            if active is not None
            else pending[:2] if pending is not None else None
        )
        if existing is not None:
            if existing != (label, observed_at):
                raise InvalidLabel(
                    "Conflicting fraud-label event",
                    reason_code="label.conflict",
                )

            if active is not None or transaction is None:
                return False

            # A legacy/deployment-overlap pending row can be activated here.
            record_label_event(
                conn,
                transaction_id=transaction_id,
                terminal_id=transaction[0],
                fraud_label=label,
                observed_at=observed_at,
                source_topic=pending[2],
                source_partition=pending[3],
                source_offset=pending[4],
                kafka_key=pending[5],
                kafka_payload=_ledger_payload(
                    transaction_id, label, observed_at, pending[6]
                ),
            )
            _insert_active_label(conn, transaction_id, label, observed_at)
            conn.execute(
                "DELETE FROM pending_fraud_labels WHERE transaction_id = %s",
                (transaction_id,),
            )
            return False

        if transaction is None:
            normalized_key = (
                None
                if kafka_key is _KAFKA_KEY_NOT_PROVIDED
                else kafka_key
            )
            conn.execute(
                """
                INSERT INTO pending_fraud_labels (
                    transaction_id, fraud_label, observed_at,
                    source_topic, source_partition, source_offset,
                    kafka_key, kafka_payload, source_payload_sha256
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    transaction_id, label, observed_at,
                    source_topic, source_partition, source_offset,
                    normalized_key, payload, hashlib.sha256(payload).digest(),
                ),
            )
            return True

        record_label_event(
            conn,
            transaction_id=transaction_id,
            terminal_id=transaction[0],
            fraud_label=label,
            observed_at=observed_at,
            source_topic=source_topic,
            source_partition=source_partition,
            source_offset=source_offset,
            kafka_key=(
                None if kafka_key is _KAFKA_KEY_NOT_PROVIDED else kafka_key
            ),
            kafka_payload=payload,
        )
        _insert_active_label(conn, transaction_id, label, observed_at)
        return True


def _validate_delay(observed_at: datetime, occurred_at: datetime) -> None:
    earliest = occurred_at + timedelta(days=LABEL_DELAY_DAYS)
    if observed_at < earliest:
        raise InvalidLabel(
            "Fraud label violates the seven-day delay",
            reason_code="label.delay_violation",
        )


def _insert_active_label(
    conn,
    transaction_id: str,
    label: int,
    observed_at: datetime,
) -> None:
    conn.execute(
        """
        INSERT INTO fraud_labels (
            transaction_id,
            fraud_label,
            observed_at
        )
        VALUES (%s, %s, %s)
        """,
        (transaction_id, label, observed_at),
    )


def resolve_pending_label(
    conn,
    *,
    transaction_id: str,
    terminal_id: str,
    occurred_at: datetime,
) -> str | None:
    """Activate or quarantine one staged label inside the scoring transaction.

    The caller must already hold the transaction-ID lock followed by the
    terminal lock. The pending-row deletion, activation/quarantine, score, and
    decision all commit or roll back together.
    """
    pending = conn.execute(
        """
        SELECT fraud_label, observed_at,
               source_topic, source_partition, source_offset,
               kafka_key, kafka_payload, source_payload_sha256
        FROM pending_fraud_labels
        WHERE transaction_id = %s
        """,
        (transaction_id,),
    ).fetchone()
    if pending is None:
        return None

    (
        label,
        observed_at,
        source_topic,
        source_partition,
        source_offset,
        kafka_key,
        kafka_payload,
        source_payload_sha256,
    ) = pending

    if (
        kafka_payload is not None
        and source_payload_sha256 is not None
        and hashlib.sha256(bytes(kafka_payload)).digest()
        != bytes(source_payload_sha256)
    ):
        raise RuntimeError(
            f"Pending label payload digest mismatch: {transaction_id}"
        )

    try:
        _validate_delay(observed_at, occurred_at)
    except InvalidLabel as error:
        if (
            source_topic is None
            or source_partition is None
            or source_offset is None
            or kafka_payload is None
        ):
            raise RuntimeError(
                "Legacy pending label violates delay but has no recoverable "
                f"Kafka source: {transaction_id}"
            ) from error
        store_quarantined(
            conn,
            source_topic=source_topic,
            source_partition=source_partition,
            source_offset=source_offset,
            kafka_key=kafka_key,
            kafka_payload=_ledger_payload(
                transaction_id, label, observed_at, kafka_payload
            ),
            error=error,
        )
        resolution = "quarantined"
    else:
        record_label_event(
            conn,
            transaction_id=transaction_id,
            terminal_id=terminal_id,
            fraud_label=label,
            observed_at=observed_at,
            source_topic=source_topic,
            source_partition=source_partition,
            source_offset=source_offset,
            kafka_key=kafka_key,
            kafka_payload=_ledger_payload(
                transaction_id, label, observed_at, kafka_payload
            ),
        )
        _insert_active_label(conn, transaction_id, label, observed_at)
        resolution = "activated"

    conn.execute(
        "DELETE FROM pending_fraud_labels WHERE transaction_id = %s",
        (transaction_id,),
    )
    return resolution
