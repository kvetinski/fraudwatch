"""Cross-topic label/transaction ordering and locking integration tests."""

import json
import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from types import SimpleNamespace

import numpy as np
import psycopg
import pytest

from fraudwatch_ml.labels import store_label
from services.scoring.worker import LoadedScorer, process_and_commit, process_event


class FixedModel:
    def predict_proba(self, frame):
        assert frame.shape == (1, 12)
        return np.array([[0.8, 0.2]], dtype=np.float64)


class Message:
    def __init__(self, payload, terminal_id, offset):
        self._payload = payload
        self._terminal_id = terminal_id
        self._offset = offset

    def topic(self):
        return "transactions.v1"

    def partition(self):
        return 2

    def offset(self):
        return self._offset

    def key(self):
        return self._terminal_id.encode()

    def value(self):
        return self._payload


class Consumer:
    def __init__(self):
        self.commits = 0

    def commit(self, *, message, asynchronous):
        assert asynchronous is False
        self.commits += 1
        return [SimpleNamespace(error=None)]


def transaction_payload(transaction_id, terminal_id):
    return json.dumps({
        "transaction_id": transaction_id,
        "customer_id": f"customer-{transaction_id}",
        "terminal_id": terminal_id,
        "amount": 42.5,
        "occurred_at": "2018-04-01T00:00:00Z",
    }).encode()


def label_payload(transaction_id, observed_at):
    return json.dumps({
        "transaction_id": transaction_id,
        "fraud_label": 1,
        "observed_at": observed_at,
    }).encode()


def cleanup(conn, transaction_id, terminal_id, label_offset):
    conn.execute(
        "DELETE FROM terminal_order_blocks WHERE terminal_id = %s",
        (terminal_id,),
    )
    conn.execute(
        """
        DELETE FROM event_quarantine_audit
        WHERE quarantine_id IN (
            SELECT id FROM event_quarantine
            WHERE source_topic = 'fraud-labels.v1'
              AND source_partition = 1
              AND source_offset = %s
        )
        """,
        (label_offset,),
    )
    conn.execute(
        """
        DELETE FROM event_quarantine
        WHERE source_topic = 'fraud-labels.v1'
          AND source_partition = 1
          AND source_offset = %s
        """,
        (label_offset,),
    )
    conn.execute(
        "DELETE FROM pending_fraud_labels WHERE transaction_id = %s",
        (transaction_id,),
    )
    conn.execute(
        "DELETE FROM fraud_labels WHERE transaction_id = %s",
        (transaction_id,),
    )
    conn.execute(
        "DELETE FROM fraud_decisions WHERE transaction_id = %s",
        (transaction_id,),
    )
    conn.execute(
        "DELETE FROM scored_transactions WHERE transaction_id = %s",
        (transaction_id,),
    )


def test_pending_delay_violation_is_quarantined_when_transaction_arrives():
    dsn = os.getenv("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TEST_DATABASE_URL not configured")

    salt = uuid.uuid4().hex
    transaction_id = f"pending-invalid-{salt}"
    terminal_id = f"pending-terminal-{salt}"
    label_offset = uuid.uuid4().int % (2**62)
    tx_offset = uuid.uuid4().int % (2**62)
    early_label = label_payload(transaction_id, "2018-04-04T00:00:00Z")
    tx_payload = transaction_payload(transaction_id, terminal_id)
    scorer = LoadedScorer(FixedModel(), "models:/fraudwatch-hgb/consistency-test")

    with psycopg.connect(dsn, autocommit=True) as conn:
        try:
            assert store_label(
                conn,
                early_label,
                kafka_key=transaction_id.encode(),
                source_topic="fraud-labels.v1",
                source_partition=1,
                source_offset=label_offset,
            ) is True

            consumer = Consumer()
            outcome = process_and_commit(
                conn,
                scorer,
                consumer,
                Message(tx_payload, terminal_id, tx_offset),
            )
            assert outcome[:2] == (True, False)
            assert consumer.commits == 1

            assert conn.execute(
                "SELECT count(*) FROM pending_fraud_labels WHERE transaction_id = %s",
                (transaction_id,),
            ).fetchone()[0] == 0
            assert conn.execute(
                "SELECT count(*) FROM fraud_labels WHERE transaction_id = %s",
                (transaction_id,),
            ).fetchone()[0] == 0
            quarantine = conn.execute(
                """
                SELECT event_type, reason_code, kafka_payload
                FROM event_quarantine
                WHERE source_topic = 'fraud-labels.v1'
                  AND source_partition = 1
                  AND source_offset = %s
                """,
                (label_offset,),
            ).fetchone()
            assert quarantine is not None
            assert quarantine[:2] == ("label", "label.delay_violation")
            assert bytes(quarantine[2]) == early_label
        finally:
            cleanup(conn, transaction_id, terminal_id, label_offset)


def test_concurrent_valid_label_and_transaction_converge_to_active_label():
    dsn = os.getenv("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TEST_DATABASE_URL not configured")

    salt = uuid.uuid4().hex
    transaction_id = f"label-race-{salt}"
    terminal_id = f"label-race-terminal-{salt}"
    label_offset = uuid.uuid4().int % (2**62)
    tx_offset = uuid.uuid4().int % (2**62)
    tx_payload = transaction_payload(transaction_id, terminal_id)
    label = label_payload(transaction_id, "2018-04-08T00:00:00Z")
    scorer = LoadedScorer(FixedModel(), "models:/fraudwatch-hgb/consistency-test")
    start = Barrier(2)

    def score():
        with psycopg.connect(dsn, autocommit=True) as conn:
            start.wait()
            return process_event(
                conn,
                scorer,
                tx_payload,
                partition=2,
                offset=tx_offset,
                kafka_key=terminal_id.encode(),
            )

    def label_event():
        with psycopg.connect(dsn, autocommit=True) as conn:
            start.wait()
            return store_label(
                conn,
                label,
                kafka_key=transaction_id.encode(),
                source_topic="fraud-labels.v1",
                source_partition=1,
                source_offset=label_offset,
            )

    with psycopg.connect(dsn, autocommit=True) as conn:
        try:
            with ThreadPoolExecutor(max_workers=2) as executor:
                score_result = executor.submit(score)
                label_result = executor.submit(label_event)
                assert score_result.result(timeout=10) is True
                assert label_result.result(timeout=10) is True

            assert conn.execute(
                """
                SELECT fraud_label, observed_at
                FROM fraud_labels
                WHERE transaction_id = %s
                """,
                (transaction_id,),
            ).fetchone() is not None
            assert conn.execute(
                "SELECT count(*) FROM pending_fraud_labels WHERE transaction_id = %s",
                (transaction_id,),
            ).fetchone()[0] == 0
            assert conn.execute(
                "SELECT count(*) FROM fraud_decisions WHERE transaction_id = %s",
                (transaction_id,),
            ).fetchone()[0] == 1
        finally:
            cleanup(conn, transaction_id, terminal_id, label_offset)


def test_label_after_transaction_is_activated_without_staging():
    dsn = os.getenv("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TEST_DATABASE_URL not configured")

    salt = uuid.uuid4().hex
    transaction_id = f"transaction-first-{salt}"
    terminal_id = f"transaction-first-terminal-{salt}"
    label_offset = uuid.uuid4().int % (2**62)
    tx_offset = uuid.uuid4().int % (2**62)
    tx_payload = transaction_payload(transaction_id, terminal_id)
    label = label_payload(transaction_id, "2018-04-08T00:00:00Z")
    scorer = LoadedScorer(FixedModel(), "models:/fraudwatch-hgb/consistency-test")

    with psycopg.connect(dsn, autocommit=True) as conn:
        try:
            assert process_event(
                conn,
                scorer,
                tx_payload,
                partition=2,
                offset=tx_offset,
                kafka_key=terminal_id.encode(),
            ) is True
            assert store_label(
                conn,
                label,
                kafka_key=transaction_id.encode(),
                source_topic="fraud-labels.v1",
                source_partition=1,
                source_offset=label_offset,
            ) is True
            assert store_label(
                conn,
                label,
                kafka_key=transaction_id.encode(),
                source_topic="fraud-labels.v1",
                source_partition=1,
                source_offset=label_offset,
            ) is False

            assert conn.execute(
                "SELECT count(*) FROM fraud_labels WHERE transaction_id = %s",
                (transaction_id,),
            ).fetchone()[0] == 1
            assert conn.execute(
                "SELECT count(*) FROM pending_fraud_labels WHERE transaction_id = %s",
                (transaction_id,),
            ).fetchone()[0] == 0
        finally:
            cleanup(conn, transaction_id, terminal_id, label_offset)
