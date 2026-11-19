import json
import os
import uuid
from contextlib import contextmanager

import psycopg
import pytest

from services.scoring import label_worker


class FakeConnection:
    def __init__(self):
        self.active = False
        self.commits = 0

    @contextmanager
    def transaction(self):
        assert not self.active
        self.active = True
        try:
            yield
        finally:
            self.active = False
            self.commits += 1


def test_partition_batch_uses_one_database_transaction(monkeypatch):
    connection = FakeConnection()
    messages = [object(), object(), object()]
    processed = []

    def process(conn, batch):
        assert conn.active
        processed.extend(batch)
        return [
            label_worker.DurableLabelResult(
                message=message,
                inserted=True,
                quarantined=False,
                started=0.0,
                durable=-1.0,
            )
            for message in batch
        ]

    monkeypatch.setattr(
        label_worker, "try_persist_new_active_labels", process
    )

    results = label_worker.persist_partition_batch(connection, messages)

    assert processed == messages
    assert connection.commits == 1
    assert all(result.durable >= 0 for result in results)


class Message:
    def __init__(self, transaction_id, offset):
        self.transaction_id = transaction_id
        self._offset = offset
        self._value = json.dumps(
            {
                "transaction_id": transaction_id,
                "fraud_label": offset % 2,
                "observed_at": "2018-04-08T00:00:00Z",
            }
        ).encode()

    def value(self):
        return self._value

    def key(self):
        return self.transaction_id.encode()

    def topic(self):
        return "fraud-labels.v1"

    def partition(self):
        return 1

    def offset(self):
        return self._offset


def test_new_active_labels_use_bulk_path():
    dsn = os.getenv("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TEST_DATABASE_URL not configured")

    salt = uuid.uuid4().hex
    terminal_id = f"label-batch-terminal-{salt}"
    transaction_ids = [f"label-batch-{salt}-{index}" for index in range(3)]
    offset_base = uuid.uuid4().int % (2**61)
    messages = [
        Message(transaction_id, offset_base + index)
        for index, transaction_id in enumerate(transaction_ids)
    ]

    with psycopg.connect(dsn, autocommit=True) as conn:
        try:
            with conn.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO scored_transactions (
                        transaction_id, terminal_id, occurred_at
                    ) VALUES (%s, %s, '2018-04-01T00:00:00Z')
                    """,
                    [(transaction_id, terminal_id) for transaction_id in transaction_ids],
                )

            results = label_worker.persist_partition_batch(conn, messages)

            assert all(result.inserted for result in results)
            assert conn.execute(
                "SELECT count(*) FROM fraud_labels WHERE transaction_id = ANY(%s)",
                (transaction_ids,),
            ).fetchone()[0] == 3
            assert conn.execute(
                """
                SELECT count(*) FROM valid_event_ledger
                WHERE event_type = 'label' AND transaction_id = ANY(%s)
                """,
                (transaction_ids,),
            ).fetchone()[0] == 3
            assert conn.execute(
                """
                SELECT count(*) FROM scored_transactions
                WHERE transaction_id = ANY(%s)
                  AND fraud_label IS NOT NULL
                  AND label_observed_at = '2018-04-08T00:00:00Z'
                """,
                (transaction_ids,),
            ).fetchone()[0] == 3
        finally:
            conn.execute(
                "DELETE FROM valid_event_ledger WHERE transaction_id = ANY(%s)",
                (transaction_ids,),
            )
            conn.execute(
                "DELETE FROM fraud_labels WHERE transaction_id = ANY(%s)",
                (transaction_ids,),
            )
            conn.execute(
                "DELETE FROM scored_transactions WHERE transaction_id = ANY(%s)",
                (transaction_ids,),
            )
