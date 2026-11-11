"""Conservative, durable terminal-level event-time ordering policy.

Do not automatically score a late event or unblock a terminal. Scoring an
older transaction after newer decisions have been persisted can change both
historical features and the validity of previous scores.

Call these helpers *inside* the existing PostgreSQL transaction and *after*
acquiring the per-terminal transaction advisory lock. No Kafka offset should be
committed until the outer PostgreSQL transaction has committed successfully.
"""

from dataclasses import dataclass
from datetime import datetime

from fraudwatch_ml.quarantine import InvalidTransaction, store_quarantined


class OutOfOrderTransaction(InvalidTransaction):
    """An older event arrived after a newer terminal event was already scored."""


class TerminalBlocked(InvalidTransaction):
    """An event belongs to a terminal with unresolved history ordering."""


@dataclass(frozen=True)
class TerminalQuarantined:
    terminal_id: str
    reason: str
    new_record: bool


def block_for_late_event(
    conn,
    *,
    terminal_id: str,
    occurred_at: datetime,
    last_scored_at: datetime,
    source_topic: str,
    source_partition: int,
    source_offset: int,
    kafka_key: bytes | None,
    kafka_payload: bytes,
) -> TerminalQuarantined:
    """Atomically save a late event and permanently block this terminal.

    Must be called within the worker's transaction and terminal advisory lock.
    `store_quarantined` opens a nested psycopg transaction (SAVEPOINT), which
    does not commit independently of the outer worker transaction.
    """
    reason = (
        f"Out-of-order transaction for terminal {terminal_id}: "
        f"{occurred_at.isoformat()} < {last_scored_at.isoformat()}"
    )
    inserted = store_quarantined(
        conn,
        source_topic=source_topic,
        source_partition=source_partition,
        source_offset=source_offset,
        kafka_key=kafka_key,
        kafka_payload=kafka_payload,
        error=OutOfOrderTransaction(reason),
    )
    row = conn.execute(
        """
        SELECT id FROM scoring_quarantine
        WHERE source_topic = %s
          AND source_partition = %s
          AND source_offset = %s
        """,
        (source_topic, source_partition, source_offset),
    ).fetchone()
    if row is None:
        raise RuntimeError("Durable late-event quarantine record is missing")

    conn.execute(
        """
        INSERT INTO terminal_order_blocks (
            terminal_id, first_quarantine_id, last_scored_at, late_occurred_at
        ) VALUES (%s, %s, %s, %s)
        ON CONFLICT (terminal_id) DO NOTHING
        """,
        (terminal_id, row[0], last_scored_at, occurred_at),
    )
    return TerminalQuarantined(terminal_id, reason, inserted)


def quarantine_blocked_terminal(
    conn,
    *,
    terminal_id: str,
    source_topic: str,
    source_partition: int,
    source_offset: int,
    kafka_key: bytes | None,
    kafka_payload: bytes,
) -> TerminalQuarantined:
    """Persist subsequent events from a blocked terminal without scoring them."""
    reason = f"Terminal {terminal_id} blocked due to unresolved out-of-order transaction"
    inserted = store_quarantined(
        conn,
        source_topic=source_topic,
        source_partition=source_partition,
        source_offset=source_offset,
        kafka_key=kafka_key,
        kafka_payload=kafka_payload,
        error=TerminalBlocked(reason),
    )
    return TerminalQuarantined(terminal_id, reason, inserted)
