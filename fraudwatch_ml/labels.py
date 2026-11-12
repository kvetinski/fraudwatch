
from datetime import datetime, timedelta, timezone

from fraudwatch_ml.contracts import (
    LABEL_FIELDS,
    decode_event_object,
    require_rfc3339,
)
from fraudwatch_ml.features import LABEL_DELAY_DAYS


def parse_label(payload: bytes) -> tuple[str, int, datetime]:
    data = decode_event_object(
        payload,
        expected_fields=LABEL_FIELDS,
        event_name="fraud-label",
    )

    transaction_id = data["transaction_id"]
    label = data["fraud_label"]
    timestamp = data["observed_at"]

    if not isinstance(transaction_id, str) or not transaction_id.strip():
        raise ValueError("Invalid transaction ID")

    if type(label) is not int or label not in (0, 1):
        raise ValueError("Fraud label must be 0 or 1")

    require_rfc3339(timestamp, field_name="observed_at")

    observed_at = datetime.fromisoformat(
        timestamp.replace("Z", "+00:00")
    )

    return (
        transaction_id,
        label,
        observed_at.astimezone(timezone.utc),
    )


def store_label(conn, payload: bytes) -> bool:
    """
    Returns True for a new label, False for an identical retry.
    Raises ValueError for conflicting or invalid events.
    """
    transaction_id, label, observed_at = parse_label(payload)

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
                raise ValueError(
                    "Fraud label violates the seven-day delay"
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
            raise ValueError("Conflicting fraud-label event")

        return False
