"""Integration test: DB commit succeeds but Kafka offset commit fails.

Run through `make test`, with TEST_DATABASE_URL pointing to fraudwatch_test.
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
    def predict_proba(self, frame):
        assert frame.shape == (1, 12)
        return np.array([[0.75, 0.25]], dtype=np.float64)


class FakeMessage:
    def __init__(self, payload, key):
        self._payload = payload
        self._key = key

    def topic(self):
        return "transactions.v1"

    def partition(self):
        return 0

    def offset(self):
        return 123456789

    def key(self):
        return self._key

    def value(self):
        return self._payload


class FailingCommitConsumer:
    def __init__(self, dsn, transaction_id):
        self.dsn = dsn
        self.transaction_id = transaction_id
        self.calls = 0

    def commit(self, *, message, asynchronous):
        assert asynchronous is False
        self.calls += 1
        # Independent connection proves the DB transaction already committed.
        with psycopg.connect(self.dsn) as observer:
            count = observer.execute(
                "SELECT count(*) FROM fraud_decisions WHERE transaction_id = %s",
                (self.transaction_id,),
            ).fetchone()[0]
        assert count == 1
        raise RuntimeError("simulated Kafka offset commit failure")


class SuccessfulCommitConsumer:
    def __init__(self):
        self.calls = 0

    def commit(self, *, message, asynchronous):
        assert asynchronous is False
        self.calls += 1
        return [SimpleNamespace(error=None)]


def test_scoring_recovers_after_db_commit_but_failed_kafka_ack():
    dsn = os.getenv("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TEST_DATABASE_URL not configured")

    transaction_id = f"recovery-{uuid.uuid4().hex}"
    terminal_id = f"recovery-terminal-{uuid.uuid4().hex}"
    payload = json.dumps(
        {
            "transaction_id": transaction_id,
            "customer_id": f"recovery-customer-{uuid.uuid4().hex}",
            "terminal_id": terminal_id,
            "occurred_at": "2018-04-01T00:00:00Z",
            "amount": 0.0,
        }
    ).encode("utf-8")
    msg = FakeMessage(payload, terminal_id.encode())
    scorer = LoadedScorer(
        model=FixedModel(),
        model_reference="models:/fraudwatch-hgb/recovery-test",
    )

    with psycopg.connect(dsn, autocommit=True) as conn:
        try:
            failed = FailingCommitConsumer(dsn, transaction_id)
            with pytest.raises(RuntimeError, match="simulated Kafka offset commit failure"):
                process_and_commit(conn, scorer, failed, msg)
            assert failed.calls == 1

            # The Kafka offset is uncommitted, but DB state must be durable.
            with psycopg.connect(dsn) as observer:
                assert observer.execute(
                    "SELECT count(*) FROM scored_transactions WHERE transaction_id = %s",
                    (transaction_id,),
                ).fetchone()[0] == 1

            # Simulate redelivery of exactly the same Kafka message.
            recovered = SuccessfulCommitConsumer()
            is_new, quarantined, _, _ = process_and_commit(
                conn, scorer, recovered, msg
            )
            assert is_new is False
            assert quarantined is False
            assert recovered.calls == 1

            assert conn.execute(
                "SELECT count(*) FROM fraud_decisions WHERE transaction_id = %s",
                (transaction_id,),
            ).fetchone()[0] == 1
            assert conn.execute(
                "SELECT count(*) FROM scored_transactions WHERE transaction_id = %s",
                (transaction_id,),
            ).fetchone()[0] == 1
        finally:
            conn.execute(
                "DELETE FROM fraud_decisions WHERE transaction_id = %s",
                (transaction_id,),
            )
            conn.execute(
                "DELETE FROM scored_transactions WHERE transaction_id = %s",
                (transaction_id,),
            )
