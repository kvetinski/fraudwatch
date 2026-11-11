"""Durable quarantine for deterministic invalid transaction messages.

Never call this for PostgreSQL failures, model failures, or out-of-order
transactions: those require retry/operator intervention without offset commit.
"""


class InvalidTransaction(ValueError):
    """An invalid Kafka transaction event that can be quarantined safely."""


def store_quarantined(
    conn,
    *,
    source_topic: str,
    source_partition: int,
    source_offset: int,
    kafka_key: bytes | None,
    kafka_payload: bytes | None,
    error: InvalidTransaction,
) -> bool:
    """Return True if inserted, False if this exact Kafka record is already saved.

    The transaction must commit before the caller commits the Kafka offset.
    A conflict with different source bytes is fatal; never acknowledge it.
    Requires an autocommit psycopg connection, as used by the scoring worker.
    """
    if not isinstance(error, InvalidTransaction):
        raise TypeError("Only invalid transaction errors may be quarantined")

    with conn.transaction():
        inserted = conn.execute(
            """
            INSERT INTO scoring_quarantine (
                source_topic, source_partition, source_offset,
                kafka_key, kafka_payload, error_type, error_message
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_topic, source_partition, source_offset)
            DO NOTHING
            RETURNING id
            """,
            (
                source_topic, source_partition, source_offset,
                kafka_key, kafka_payload,
                type(error).__name__, str(error)[:2000],
            ),
        ).fetchone()
        if inserted is not None:
            return True

        matches = conn.execute(
            """
            SELECT
                kafka_key IS NOT DISTINCT FROM %s
                AND kafka_payload IS NOT DISTINCT FROM %s
            FROM scoring_quarantine
            WHERE source_topic = %s
              AND source_partition = %s
              AND source_offset = %s
            """,
            (kafka_key, kafka_payload, source_topic, source_partition, source_offset),
        ).fetchone()
        if matches is None or not matches[0]:
            raise RuntimeError("Quarantine Kafka coordinates conflict with different bytes")

        return False
