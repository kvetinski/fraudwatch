"""Quarantine/commit-order regression tests.

Database integration tests run when TEST_DATABASE_URL is configured by make test.
"""

import json
import os
import uuid

import psycopg
import pytest

from fraudwatch_ml.quarantine import InvalidTransaction, store_quarantined
from services.scoring import worker


class FakeMessage:
    def __init__(self, payload, *, offset=1234, key=b"terminal-q"):
        self.payload = payload
        self._offset = offset
        self._key = key

    def topic(self):
        return "transactions.v1"

    def partition(self):
        return 2

    def offset(self):
        return self._offset

    def key(self):
        return self._key

    def value(self):
        return self.payload


class FakeConsumer:
    def __init__(self, on_commit=None):
        self.commits = 0
        self.on_commit = on_commit

    def commit(self, *, message, asynchronous):
        assert asynchronous is False
        if self.on_commit:
            self.on_commit(message)
        self.commits += 1
        return [type("Partition", (), {"error": None})()]


@pytest.mark.parametrize("payload", [
    b"{broken json",
    b"{}",
    b'{"transaction_id":"x","terminal_id":"t","amount":-1,"occurred_at":"2018-04-01T00:00:00Z"}',
    b'{"transaction_id":"x","terminal_id":"t","amount":1,"occurred_at":"2018-04-01"}',
    b'{"transaction_id":"x","terminal_id":"t","amount":NaN,"occurred_at":"2018-04-01T00:00:00Z"}',
])
def test_invalid_transaction_is_classified(payload):
    with pytest.raises(InvalidTransaction):
        worker.parse_transaction(payload)


def test_invalid_message_committed_only_after_quarantine(monkeypatch):
    events = []

    def fake_store(conn, **kwargs):
        events.append("quarantine_db_committed")
        assert kwargs["kafka_payload"] == b"{bad"
        return True

    monkeypatch.setattr(worker, "store_quarantined", fake_store)
    consumer = FakeConsumer(lambda msg: events.append("kafka_commit"))
    result = worker.process_and_commit(
        None, None, consumer, FakeMessage(b"{bad")
    )
    assert result[0:2] == (False, True)
    assert events == ["quarantine_db_committed", "kafka_commit"]
    assert consumer.commits == 1


def test_tombstone_is_quarantined(monkeypatch):
    stored = []
    monkeypatch.setattr(worker, "store_quarantined", lambda conn, **kw: stored.append(kw) or True)
    consumer = FakeConsumer()
    result = worker.process_and_commit(None, None, consumer, FakeMessage(None))
    assert result[0:2] == (False, True)
    assert stored[0]["kafka_payload"] is None
    assert consumer.commits == 1


def test_mismatched_transaction_key_is_quarantined(monkeypatch):
    payload = json.dumps({
        "transaction_id": "key-mismatch",
        "customer_id": "customer-1",
        "terminal_id": "expected-terminal",
        "amount": 1.0,
        "occurred_at": "2018-04-01T00:00:00Z",
    }).encode()
    stored = []
    monkeypatch.setattr(
        worker,
        "store_quarantined",
        lambda conn, **kwargs: stored.append(kwargs) or True,
    )
    consumer = FakeConsumer()

    result = worker.process_and_commit(
        None,
        None,
        consumer,
        FakeMessage(payload, key=b"wrong-terminal"),
    )

    assert result[0:2] == (False, True)
    assert isinstance(stored[0]["error"], InvalidTransaction)
    assert "Kafka key" in str(stored[0]["error"])
    assert consumer.commits == 1


@pytest.mark.parametrize("failure", [
    RuntimeError("database unavailable"),
    ValueError("out-of-order terminal transaction"),
])
def test_operational_and_order_failures_are_not_committed(monkeypatch, failure):
    def fail(*args, **kwargs):
        raise failure

    monkeypatch.setattr(worker, "process_event", fail)
    monkeypatch.setattr(worker, "store_quarantined", lambda *a, **kw: pytest.fail("not quarantinable"))
    consumer = FakeConsumer()
    msg = FakeMessage(b'{"valid":"does not matter"}')
    with pytest.raises(type(failure)):
        worker.process_and_commit(None, None, consumer, msg)
    assert consumer.commits == 0


def test_postgres_quarantine_is_durable_and_idempotent():
    dsn = os.getenv("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TEST_DATABASE_URL not configured")

    source_offset = uuid.uuid4().int % (2**62)
    kwargs = dict(
        source_topic="transactions.v1",
        source_partition=2,
        source_offset=source_offset,
        kafka_key=b"test-terminal",
        kafka_payload=b"{bad",
        error=InvalidTransaction("Invalid transaction JSON"),
    )

    with psycopg.connect(dsn, autocommit=True) as conn:
        try:
            assert store_quarantined(conn, **kwargs) is True
            # Check from a different connection: the transaction is committed.
            with psycopg.connect(dsn) as other:
                row = other.execute(
                    """SELECT kafka_key, kafka_payload, status
                       FROM scoring_quarantine
                       WHERE source_topic=%s AND source_partition=%s AND source_offset=%s""",
                    ("transactions.v1", 2, source_offset),
                ).fetchone()
                assert row == (b"test-terminal", b"{bad", "pending")

            assert store_quarantined(conn, **kwargs) is False
            with pytest.raises(RuntimeError, match="different bytes"):
                store_quarantined(conn, **{**kwargs, "kafka_payload": b"another"})

            # Failed conflicting insert must not modify the original row.
            count = conn.execute(
                """SELECT count(*) FROM scoring_quarantine
                   WHERE source_topic=%s AND source_partition=%s AND source_offset=%s""",
                ("transactions.v1", 2, source_offset),
            ).fetchone()[0]
            assert count == 1
        finally:
            conn.execute(
                """DELETE FROM scoring_quarantine
                   WHERE source_topic=%s AND source_partition=%s AND source_offset=%s""",
                ("transactions.v1", 2, source_offset),
            )


def test_actual_db_commit_happens_before_kafka_ack():
    dsn = os.getenv("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TEST_DATABASE_URL not configured")

    source_offset = uuid.uuid4().int % (2**62)
    msg = FakeMessage(b"{malformed-json", offset=source_offset)

    with psycopg.connect(dsn, autocommit=True) as conn:
        def assert_committed(message):
            # Use a separate connection to prove the row is visible before ack.
            with psycopg.connect(dsn) as verifier:
                assert verifier.execute(
                    """SELECT count(*) FROM scoring_quarantine
                       WHERE source_topic = %s AND source_partition = %s
                       AND source_offset = %s""",
                    (message.topic(), message.partition(), message.offset()),
                ).fetchone()[0] == 1

        consumer = FakeConsumer(on_commit=assert_committed)
        try:
            result = worker.process_and_commit(conn, None, consumer, msg)
            assert result[0:2] == (False, True)
            assert consumer.commits == 1
        finally:
            conn.execute(
                """DELETE FROM scoring_quarantine
                   WHERE source_topic=%s AND source_partition=%s AND source_offset=%s""",
                (msg.topic(), msg.partition(), msg.offset()),
            )


def test_quarantine_storage_failure_does_not_ack(monkeypatch):
    def fail_storage(conn, **kwargs):
        raise RuntimeError("postgres failed")

    monkeypatch.setattr(worker, "store_quarantined", fail_storage)
    consumer = FakeConsumer()
    with pytest.raises(RuntimeError, match="postgres failed"):
        worker.process_and_commit(None, None, consumer, FakeMessage(b"{bad"))
    assert consumer.commits == 0


def test_offset_commit_failure_can_be_retried_idempotently():
    dsn = os.getenv("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TEST_DATABASE_URL not configured")

    source_offset = uuid.uuid4().int % (2**62)
    msg = FakeMessage(b"{bad", offset=source_offset)

    class FailedCommit:
        def commit(self, *, message, asynchronous):
            assert asynchronous is False
            raise RuntimeError("simulated broker failure after DB commit")

    with psycopg.connect(dsn, autocommit=True) as conn:
        try:
            with pytest.raises(RuntimeError, match="simulated broker failure"):
                worker.process_and_commit(conn, None, FailedCommit(), msg)

            saved = conn.execute(
                """SELECT count(*) FROM scoring_quarantine
                   WHERE source_topic = %s AND source_partition = %s
                     AND source_offset = %s""",
                (msg.topic(), msg.partition(), msg.offset()),
            ).fetchone()[0]
            assert saved == 1

            consumer = FakeConsumer()
            _, quarantined, _, _ = worker.process_and_commit(conn, None, consumer, msg)
            assert quarantined and consumer.commits == 1

            saved_again = conn.execute(
                """SELECT count(*) FROM scoring_quarantine
                   WHERE source_topic = %s AND source_partition = %s
                     AND source_offset = %s""",
                (msg.topic(), msg.partition(), msg.offset()),
            ).fetchone()[0]
            assert saved_again == 1
        finally:
            conn.execute(
                """DELETE FROM scoring_quarantine
                   WHERE source_topic = %s AND source_partition = %s
                     AND source_offset = %s""",
                (msg.topic(), msg.partition(), msg.offset()),
            )
