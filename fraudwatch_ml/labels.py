
from datetime import datetime, timedelta, timezone

from fraudwatch_ml.contracts import (
    EventContractError,
    LABEL_FIELDS,
    decode_event_object,
    require_rfc3339,
)
from fraudwatch_ml.features import LABEL_DELAY_DAYS
from fraudwatch_ml.quarantine import InvalidLabel


_KAFKA_KEY_NOT_PROVIDED = object()


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
) -> bool:
    """
    Returns True for a new label, False for an identical retry.
    Raises ValueError for conflicting or invalid events.
    """
    transaction_id, label, observed_at = parse_label(payload)
    if kafka_key is not _KAFKA_KEY_NOT_PROVIDED:
        expected_key = transaction_id.encode("utf-8")
        if kafka_key != expected_key:
            raise InvalidLabel(
                "Kafka key must equal transaction_id",
                reason_code="label.kafka_key_mismatch",
            )

    with conn.transaction():
        transaction = conn.execute(
            """
            SELECT occurred_at
            FROM scored_transactions
            WHERE transaction_id = %s
            """,
            (transaction_id,),
        ).fetchone()

        if transaction is not None:
            earliest = (
                transaction[0]
                + timedelta(days=LABEL_DELAY_DAYS)
            )

            if observed_at < earliest:
                raise InvalidLabel(
                    "Fraud label violates the seven-day delay",
                    reason_code="label.delay_violation",
                )

        inserted = conn.execute(
            """
            INSERT INTO fraud_labels (
                transaction_id,
                fraud_label,
                observed_at
            )
            VALUES (%s, %s, %s)
            ON CONFLICT (transaction_id) DO NOTHING
            RETURNING transaction_id
            """,
            (transaction_id, label, observed_at),
        ).fetchone()

        if inserted is not None:
            return True

        existing = conn.execute(
            """
            SELECT fraud_label, observed_at
            FROM fraud_labels
            WHERE transaction_id = %s
            """,
            (transaction_id,),
        ).fetchone()

        if existing != (label, observed_at):
            raise InvalidLabel(
                "Conflicting fraud-label event",
                reason_code="label.conflict",
            )

        return False
