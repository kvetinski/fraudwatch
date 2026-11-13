"""Durable quarantine for deterministic invalid Kafka event messages.

Never call this for PostgreSQL, model, code, or infrastructure failures: those
require retry/operator intervention without an offset commit.
"""

from __future__ import annotations

import hashlib


class QuarantinableEvent(ValueError):
    """A deterministic event failure that is safe to persist and acknowledge."""

    event_type = "event"
    default_reason_code = "event.invalid"

    def __init__(self, message: str, *, reason_code: str | None = None):
        super().__init__(message)
        self.reason_code = reason_code or self.default_reason_code


class InvalidTransaction(QuarantinableEvent):
    """An invalid Kafka transaction event that can be quarantined safely."""

    event_type = "transaction"
    default_reason_code = "transaction.invalid"


class InvalidLabel(QuarantinableEvent):
    """An invalid Kafka fraud-label event that can be quarantined safely."""

    event_type = "label"
    default_reason_code = "label.invalid"


def store_quarantined(
    conn,
    *,
    source_topic: str,
    source_partition: int,
    source_offset: int,
    kafka_key: bytes | None,
    kafka_payload: bytes | None,
    error: QuarantinableEvent,
) -> bool:
    """Return True if inserted, False if this exact Kafka record is already saved.

    The transaction must commit before the caller commits the Kafka offset.
    A conflict with different source bytes is fatal; never acknowledge it.
    Requires an autocommit psycopg connection, as used by the scoring worker.
    """
    if not isinstance(error, QuarantinableEvent):
        raise TypeError("Only deterministic event errors may be quarantined")

    payload_hash = hashlib.sha256(kafka_payload or b"").digest()

    with conn.transaction():
        inserted = conn.execute(
            """
            INSERT INTO event_quarantine (
                event_type, source_topic, source_partition, source_offset,
                kafka_key, kafka_payload, source_payload_sha256,
                source_payload_was_null,
                reason_code, error_type, error_message
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_topic, source_partition, source_offset)
            DO NOTHING
            RETURNING id
            """,
            (
                error.event_type,
                source_topic, source_partition, source_offset,
                kafka_key, kafka_payload, payload_hash,
                kafka_payload is None, error.reason_code,
                type(error).__name__, str(error)[:2000],
            ),
        ).fetchone()
        if inserted is not None:
            quarantine_id = (
                inserted["id"] if isinstance(inserted, dict) else inserted[0]
            )
            conn.execute(
                """
                INSERT INTO event_quarantine_audit (
                    quarantine_id, action, note
                ) VALUES (%s, 'quarantined', %s)
                """,
                (quarantine_id, f"{error.reason_code}: {str(error)[:1900]}"),
            )
            return True

        matches = conn.execute(
            """
            SELECT
                kafka_key IS NOT DISTINCT FROM %s
                AND source_payload_sha256 = %s
                AND source_payload_was_null = %s
                AND (
                    kafka_payload IS NOT DISTINCT FROM %s
                    OR (payload_purged_at IS NOT NULL
                        AND source_payload_was_null IS FALSE)
                )
                AND event_type = %s AS exact_match
            FROM event_quarantine
            WHERE source_topic = %s
              AND source_partition = %s
              AND source_offset = %s
            """,
            (
                kafka_key, payload_hash, kafka_payload is None, kafka_payload,
                error.event_type,
                source_topic, source_partition, source_offset,
            ),
        ).fetchone()
        exact_match = (
            matches["exact_match"] if isinstance(matches, dict)
            else matches[0] if matches is not None
            else False
        )
        if not exact_match:
            raise RuntimeError("Quarantine Kafka coordinates conflict with different bytes")

        return False
