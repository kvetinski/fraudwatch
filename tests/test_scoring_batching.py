from contextlib import contextmanager
from types import SimpleNamespace

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier

from fraudwatch_ml.features import FEATURE_NAMES
from services.scoring import worker


class Message:
    def __init__(
        self, partition, offset, *, topic="transactions.v1", payload=b"payload",
        key=b"terminal",
    ):
        self._partition = partition
        self._offset = offset
        self._topic = topic
        self._payload = payload
        self._key = key

    def topic(self):
        return self._topic

    def partition(self):
        return self._partition

    def offset(self):
        return self._offset

    def value(self):
        return self._payload

    def key(self):
        return self._key


def test_partition_batches_are_partitioned_and_offset_ordered():
    messages = [Message(2, 9), Message(0, 4), Message(2, 7), Message(0, 3)]

    batches = worker.partition_batches(messages)

    assert [
        [(message.partition(), message.offset()) for message in batch]
        for batch in batches
    ] == [[(0, 3), (0, 4)], [(2, 7), (2, 9)]]


def test_partition_batch_commits_only_the_next_offset_after_last_durable():
    messages = [Message(1, 20), Message(1, 21), Message(1, 22)]
    results = [
        worker.DurableMessageResult(
            message=message,
            new_decision=True,
            quarantined=False,
            timings=worker.EventTimings(),
            started_ns=1,
            durable_ns=2,
        )
        for message in messages
    ]

    class Consumer:
        def commit(self, *, offsets, asynchronous):
            assert asynchronous is False
            assert len(offsets) == 1
            assert offsets[0].topic == "transactions.v1"
            assert offsets[0].partition == 1
            assert offsets[0].offset == 23
            return [SimpleNamespace(error=None)]

    worker.commit_partition_batch(Consumer(), results)


def test_scoring_batch_locks_identities_in_sorted_sets(monkeypatch):
    messages = [
        Message(1, 1, payload=b"tx-b|term-z", key=b"term-z"),
        Message(1, 2, payload=b"tx-a|term-a", key=b"term-a"),
    ]

    def parse(payload):
        transaction_id, terminal_id = payload.decode().split("|")
        return SimpleNamespace(
            transaction_id=transaction_id, terminal_id=terminal_id
        ), None

    class Connection:
        def __init__(self):
            self.active = False
            self.lock_sets = []

        @contextmanager
        def transaction(self):
            self.active = True
            try:
                yield
            finally:
                self.active = False

        def execute(self, statement, params):
            assert self.active
            self.lock_sets.append(params[0])
            return SimpleNamespace(fetchall=lambda: [])

    def process(conn, scorer, message, **kwargs):
        assert conn.active
        assert kwargs["batch_transaction"] is True
        return worker.DurableMessageResult(
            message=message,
            new_decision=True,
            quarantined=False,
            timings=worker.EventTimings(),
            started_ns=1,
            durable_ns=2,
        )

    monkeypatch.setattr(worker, "parse_transaction", parse)
    monkeypatch.setattr(worker, "validate_transaction_key", lambda event, key: None)
    monkeypatch.setattr(worker, "process_without_kafka_commit", process)
    connection = Connection()

    results = worker.persist_scoring_partition_batch(
        connection, object(), messages
    )

    assert connection.lock_sets == [
        ["tx-a", "tx-b"],
        ["term-a", "term-z"],
    ]
    assert all(result.durable_ns > 2 for result in results)


def test_array_inference_is_exactly_equal_after_named_schema_validation():
    rng = np.random.default_rng(42)
    X = pd.DataFrame(
        rng.normal(size=(200, len(FEATURE_NAMES))),
        columns=list(FEATURE_NAMES),
    )
    y = (X.iloc[:, 0] + X.iloc[:, 6] > 0).astype(int)
    model = HistGradientBoostingClassifier(random_state=42).fit(X, y)
    probe = X.iloc[:20]
    expected = model.predict_proba(probe)

    prepared = worker.prepare_model_for_array_inference(model)
    actual = prepared.predict_proba(probe.to_numpy())

    np.testing.assert_array_equal(actual, expected)
    assert not hasattr(prepared, "feature_names_in_")
