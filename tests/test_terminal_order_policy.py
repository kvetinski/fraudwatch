"""Integration tests for fail-closed out-of-order terminal handling.

Run with TEST_DATABASE_URL (make test configures it). The test uses the
existing scoring worker and does not require a running Kafka broker or MLflow.
"""
import json
import os
import uuid
from types import SimpleNamespace

import numpy as np
import psycopg
import pytest

from services.scoring.worker import LoadedScorer, process_and_commit


class FixedModel:
    def predict_proba(self, features):
        assert features.shape == (1, 12)
        return np.array([[0.7, 0.3]], dtype=np.float64)


class Message:
    def __init__(self, transaction_id, terminal_id, at, offset):
        self.transaction_id = transaction_id
        self._terminal_id = terminal_id
        self._offset = offset
        self._payload = json.dumps({
            "transaction_id": transaction_id,
            "terminal_id": terminal_id,
            "amount": 37.5,
            "occurred_at": at,
        }).encode()

    def topic(self):
        return "transactions.v1"

    def partition(self):
        return 1

    def offset(self):
        return self._offset

    def key(self):
        return self._terminal_id.encode()

    def value(self):
        return self._payload


class CheckedCommit:
    def __init__(self, dsn, expected_quarantine_offset=None):
        self.dsn = dsn
        self.calls = 0
        self.expected_quarantine_offset = expected_quarantine_offset

    def commit(self, *, message, asynchronous):
        assert asynchronous is False
        if self.expected_quarantine_offset == message.offset():
            # Verify that quarantine + terminal block committed BEFORE Kafka ack.
            with psycopg.connect(self.dsn) as other:
                saved = other.execute(
                    """
                    SELECT count(*) FROM scoring_quarantine
                    WHERE source_topic = %s AND source_partition = %s
                      AND source_offset = %s
                    """,
                    (message.topic(), message.partition(), message.offset()),
                ).fetchone()[0]
                assert saved == 1
                blocked = other.execute(
                    "SELECT count(*) FROM terminal_order_blocks WHERE terminal_id = %s",
                    (message.key().decode(),),
                ).fetchone()[0]
                assert blocked == 1
        self.calls += 1
        return [SimpleNamespace(error=None)]


def test_late_event_quarantines_and_blocks_only_its_terminal():
    dsn = os.getenv("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TEST_DATABASE_URL not configured")

    salt = uuid.uuid4().hex
    a, b = f"order-a-{salt}", f"order-b-{salt}"
    offsets = [(uuid.uuid4().int % (2**61)) + i for i in range(4)]
    original = Message(f"order-original-{salt}", a, "2018-04-01T02:00:00Z", offsets[0])
    late = Message(f"order-late-{salt}", a, "2018-04-01T01:00:00Z", offsets[1])
    later = Message(f"order-later-{salt}", a, "2018-04-01T03:00:00Z", offsets[2])
    healthy = Message(f"order-healthy-{salt}", b, "2018-04-01T03:00:00Z", offsets[3])
    scorer = LoadedScorer(FixedModel(), "models:/fraudwatch-hgb/order-test")
    consumer = CheckedCommit(dsn, expected_quarantine_offset=late.offset())

    with psycopg.connect(dsn, autocommit=True) as conn:
        try:
            assert process_and_commit(conn, scorer, consumer, original)[:2] == (True, False)
            assert process_and_commit(conn, scorer, consumer, late)[:2] == (False, True)
            assert process_and_commit(conn, scorer, consumer, later)[:2] == (False, True)
            assert process_and_commit(conn, scorer, consumer, healthy)[:2] == (True, False)
            # Simulated Kafka redelivery of the already quarantined late message.
            assert process_and_commit(conn, scorer, consumer, late)[:2] == (False, True)
            # Even after blocking, replaying an earlier SUCCESSFUL event remains idempotent.
            assert process_and_commit(conn, scorer, consumer, original)[:2] == (False, False)
            assert consumer.calls == 6

            with psycopg.connect(dsn) as observer:
                decisions = observer.execute(
                    """
                    SELECT transaction_id FROM fraud_decisions
                    WHERE transaction_id = ANY(%s)
                    """,
                    ([m.transaction_id for m in (original, late, later, healthy)],),
                ).fetchall()
                assert {r[0] for r in decisions} == {
                    original.transaction_id, healthy.transaction_id,
                }
                qrows = observer.execute(
                    """
                    SELECT source_offset, error_type, status
                    FROM scoring_quarantine
                    WHERE source_topic = 'transactions.v1'
                      AND source_partition = 1
                      AND source_offset = ANY(%s)
                    """,
                    ([late.offset(), later.offset()],),
                ).fetchall()
                assert {(r[0], r[1], r[2]) for r in qrows} == {
                    (late.offset(), "OutOfOrderTransaction", "pending"),
                    (later.offset(), "TerminalBlocked", "pending"),
                }
                blocks = observer.execute(
                    """
                    SELECT terminal_id, last_scored_at, late_occurred_at
                    FROM terminal_order_blocks WHERE terminal_id = %s
                    """,
                    (a,),
                ).fetchone()
                assert blocks is not None
                assert blocks[0] == a
                assert blocks[2] < blocks[1]
                assert observer.execute(
                    "SELECT count(*) FROM terminal_order_blocks WHERE terminal_id = %s",
                    (b,),
                ).fetchone()[0] == 0
        finally:
            # Do not delete shared data: all IDs are unique to this test.
            conn.execute("DELETE FROM terminal_order_blocks WHERE terminal_id = %s", (a,))
            conn.execute(
                """
                DELETE FROM scoring_quarantine
                WHERE source_topic = 'transactions.v1'
                  AND source_partition = 1
                  AND source_offset = ANY(%s)
                """,
                ([late.offset(), later.offset()],),
            )
            ids = [m.transaction_id for m in (original, late, later, healthy)]
            conn.execute("DELETE FROM fraud_decisions WHERE transaction_id = ANY(%s)", (ids,))
            conn.execute("DELETE FROM scored_transactions WHERE transaction_id = ANY(%s)", (ids,))


def test_storage_failure_does_not_commit_or_block(monkeypatch):
    """Fail before storing the quarantine, then confirm PG rolls back."""
    dsn = os.getenv("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TEST_DATABASE_URL not configured")

    from fraudwatch_ml import terminal_order

    salt = uuid.uuid4().hex
    terminal = f"order-failure-{salt}"
    t0 = Message(f"original-{salt}", terminal, "2018-04-01T02:00:00Z", uuid.uuid4().int % (2**61))
    t1 = Message(f"late-{salt}", terminal, "2018-04-01T01:00:00Z", uuid.uuid4().int % (2**61))
    scorer = LoadedScorer(FixedModel(), "models:/fraudwatch-hgb/order-test")

    class CountCommits:
        calls = 0
        def commit(self, *, message, asynchronous):
            self.calls += 1
            return [SimpleNamespace(error=None)]

    consumer = CountCommits()
    with psycopg.connect(dsn, autocommit=True) as conn:
        try:
            assert process_and_commit(conn, scorer, consumer, t0)[:2] == (True, False)

            def fail(*args, **kwargs):
                raise RuntimeError("simulated PostgreSQL quarantine failure")

            with monkeypatch.context() as mp:
                mp.setattr(terminal_order, "store_quarantined", fail)
                with pytest.raises(RuntimeError, match="simulated PostgreSQL"):
                    process_and_commit(conn, scorer, consumer, t1)

            assert consumer.calls == 1  # only original was acknowledged
            assert conn.execute(
                "SELECT count(*) FROM terminal_order_blocks WHERE terminal_id = %s",
                (terminal,),
            ).fetchone()[0] == 0
        finally:
            conn.execute("DELETE FROM terminal_order_blocks WHERE terminal_id = %s", (terminal,))
            conn.execute("DELETE FROM fraud_decisions WHERE transaction_id = %s", (t0.transaction_id,))
            conn.execute("DELETE FROM scored_transactions WHERE transaction_id = %s", (t0.transaction_id,))
