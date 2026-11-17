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

    default_reason_code = "transaction.out_of_order"


class TerminalBlocked(InvalidTransaction):
    """An event belongs to a terminal with unresolved history ordering."""

    default_reason_code = "transaction.terminal_blocked"


@dataclass(frozen=True)
class TerminalQuarantined:
    terminal_id: str
    reason: str
    new_record: bool


def block_for_late_event(
    conn,
    *,
    terminal_id: str,
    event_ledger_id: int,
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
        SELECT id FROM event_quarantine
        WHERE source_topic = %s
          AND source_partition = %s
          AND source_offset = %s
        """,
        (source_topic, source_partition, source_offset),
    ).fetchone()
    if row is None:
        raise RuntimeError("Durable late-event quarantine record is missing")
    quarantine_id = row[0]

    _link_valid_event(conn, event_ledger_id, quarantine_id)

    conn.execute(
        """
        INSERT INTO terminal_order_blocks (
            terminal_id, first_quarantine_id, last_scored_at, late_occurred_at
        ) VALUES (%s, %s, %s, %s)
        ON CONFLICT (terminal_id) DO NOTHING
        """,
        (terminal_id, quarantine_id, last_scored_at, occurred_at),
    )
    return TerminalQuarantined(terminal_id, reason, inserted)


def quarantine_blocked_terminal(
    conn,
    *,
    terminal_id: str,
    event_ledger_id: int,
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
    row = conn.execute(
        """
        SELECT id FROM event_quarantine
        WHERE source_topic = %s
          AND source_partition = %s
          AND source_offset = %s
        """,
        (source_topic, source_partition, source_offset),
    ).fetchone()
    if row is None:
        raise RuntimeError("Durable blocked-event quarantine record is missing")
    _link_valid_event(conn, event_ledger_id, row[0])
    return TerminalQuarantined(terminal_id, reason, inserted)


def _link_valid_event(conn, event_ledger_id: int, quarantine_id: int) -> None:
    linked = conn.execute(
        """
        INSERT INTO valid_event_quarantine_links (
            event_ledger_id, quarantine_id
        ) VALUES (%s, %s)
        ON CONFLICT DO NOTHING
        RETURNING event_ledger_id
        """,
        (event_ledger_id, quarantine_id),
    ).fetchone()
    if linked is not None:
        return
    existing = conn.execute(
        """
        SELECT event_ledger_id, quarantine_id
        FROM valid_event_quarantine_links
        WHERE event_ledger_id = %s OR quarantine_id = %s
        """,
        (event_ledger_id, quarantine_id),
    ).fetchone()
    if isinstance(existing, dict):
        existing = (existing["event_ledger_id"], existing["quarantine_id"])
    if existing != (event_ledger_id, quarantine_id):
        raise RuntimeError("Valid-event quarantine link conflicts")
