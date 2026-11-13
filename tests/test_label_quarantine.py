"""Label poison-record quarantine and offset-order regression tests."""

import json
import os
import uuid
from types import SimpleNamespace

import psycopg
import pytest

from fraudwatch_ml.labels import store_label
from fraudwatch_ml.quarantine import InvalidLabel
from services.scoring import label_worker


class Message:
    def __init__(self, payload, *, key=b"label-tx", offset=4567):
        self._payload = payload
        self._key = key
        self._offset = offset

    def topic(self):
        return "fraud-labels.v1"

    def partition(self):
        return 1

    def offset(self):
        return self._offset

    def key(self):
        return self._key

    def value(self):
        return self._payload


class Consumer:
    def __init__(self, on_commit=None):
        self.commits = 0
        self.on_commit = on_commit

    def commit(self, *, message, asynchronous):
        assert asynchronous is False
        if self.on_commit is not None:
            self.on_commit(message)
        self.commits += 1
        return [SimpleNamespace(error=None)]


def test_invalid_label_is_durable_before_offset_commit(monkeypatch):
    events = []

    def invalid(*args, **kwargs):
        raise InvalidLabel("bad label", reason_code="label.invalid_value")

    def quarantine(conn, **kwargs):
        assert kwargs["error"].reason_code == "label.invalid_value"
        events.append("quarantine_durable")
        return True

    monkeypatch.setattr(label_worker, "store_label", invalid)
    monkeypatch.setattr(label_worker, "store_quarantined", quarantine)
    consumer = Consumer(lambda message: events.append("offset_committed"))

    inserted, quarantined, _, _ = label_worker.process_and_commit(
        None, consumer, Message(b'{"bad":true}')
    )

    assert inserted is False
    assert quarantined is True
    assert events == ["quarantine_durable", "offset_committed"]


def test_label_tombstone_is_quarantined(monkeypatch):
    stored = []
    monkeypatch.setattr(
        label_worker,
        "store_quarantined",
        lambda conn, **kwargs: stored.append(kwargs) or True,
    )
    consumer = Consumer()

    inserted, quarantined, _, _ = label_worker.process_and_commit(
        None, consumer, Message(None)
    )

    assert inserted is False and quarantined is True
    assert stored[0]["kafka_payload"] is None
    assert stored[0]["error"].reason_code == "label.tombstone"
    assert consumer.commits == 1


def test_label_infrastructure_failure_does_not_advance_offset(monkeypatch):
    monkeypatch.setattr(
        label_worker,
        "store_label",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            RuntimeError("database unavailable")
        ),
    )
    monkeypatch.setattr(
        label_worker,
        "store_quarantined",
        lambda *args, **kwargs: pytest.fail("operational error was quarantined"),
    )
    consumer = Consumer()

    with pytest.raises(RuntimeError, match="database unavailable"):
        label_worker.process_and_commit(None, consumer, Message(b"payload"))
    assert consumer.commits == 0


def test_poison_label_does_not_prevent_following_record(monkeypatch):
    def store(conn, payload, *, kafka_key):
        if payload == b"poison":
            raise InvalidLabel("poison", reason_code="label.contract_violation")
        return True

    monkeypatch.setattr(label_worker, "store_label", store)
    monkeypatch.setattr(
        label_worker, "store_quarantined", lambda conn, **kwargs: True
    )
    consumer = Consumer()

    assert label_worker.process_and_commit(
        None, consumer, Message(b"poison", offset=10)
    )[:2] == (False, True)
    assert label_worker.process_and_commit(
        None, consumer, Message(b"valid", offset=11)
    )[:2] == (True, False)
    assert consumer.commits == 2


def test_label_kafka_key_must_match_transaction_id():
    payload = json.dumps({
        "transaction_id": "label-key-test",
        "fraud_label": 1,
        "observed_at": "2018-04-08T00:00:00Z",
    }).encode()
    with pytest.raises(InvalidLabel, match="Kafka key") as error:
        store_label(None, payload, kafka_key=b"wrong")
    assert error.value.reason_code == "label.kafka_key_mismatch"


def test_real_label_quarantine_is_visible_before_acknowledgment():
    dsn = os.getenv("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TEST_DATABASE_URL not configured")

    source_offset = uuid.uuid4().int % (2**62)
    message = Message(b"{bad-json", key=b"bad-label", offset=source_offset)

    def verify_durable(committed_message):
        with psycopg.connect(dsn) as verifier:
            row = verifier.execute(
                """
                SELECT id, event_type, reason_code, source_payload_sha256
                FROM event_quarantine
                WHERE source_topic=%s AND source_partition=%s
                  AND source_offset=%s
                """,
                (
                    committed_message.topic(),
                    committed_message.partition(),
                    committed_message.offset(),
                ),
            ).fetchone()
            assert row is not None
            assert row[1] == "label"
            assert row[2] == "label.contract_violation"
            assert len(row[3]) == 32

    with psycopg.connect(dsn, autocommit=True) as conn:
        consumer = Consumer(verify_durable)
        try:
            assert label_worker.process_and_commit(
                conn, consumer, message
            )[:2] == (False, True)
        finally:
            conn.execute(
                """
                DELETE FROM event_quarantine_audit
                WHERE quarantine_id IN (
                    SELECT id FROM event_quarantine
                    WHERE source_topic=%s AND source_partition=%s
                      AND source_offset=%s
                )
                """,
                (message.topic(), message.partition(), message.offset()),
            )
            conn.execute(
                """
                DELETE FROM event_quarantine
                WHERE source_topic=%s AND source_partition=%s
                  AND source_offset=%s
                """,
                (message.topic(), message.partition(), message.offset()),
            )
