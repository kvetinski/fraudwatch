import json
import os
import uuid

import pandas as pd
import psycopg
import pytest

from fraudwatch_ml.labels import store_label
from fraudwatch_ml.labels import resolve_pending_label
from fraudwatch_ml.locks import lock_terminal, lock_transaction
from fraudwatch_ml.postgres_features import PostgresTerminalHistory


def test_label_before_transaction_and_idempotency():
    dsn = os.getenv("TEST_DATABASE_URL")

    if not dsn:
        pytest.skip("TEST_DATABASE_URL not configured")

    transaction_id = f"label-test-{uuid.uuid4().hex}"
    terminal_id = f"terminal-{uuid.uuid4().hex}"
    source_offset = uuid.uuid4().int % (2**62)

    payload = json.dumps({
        "transaction_id": transaction_id,
        "fraud_label": 1,
        "observed_at": "2018-04-08T00:00:00Z",
    }).encode()

    with psycopg.connect(dsn, autocommit=True) as conn:
        try:
            # Label arrives before the transaction.
            assert store_label(
                conn,
                payload,
                kafka_key=transaction_id.encode(),
                source_topic="fraud-labels.v1",
                source_partition=1,
                source_offset=source_offset,
            ) is True

            assert conn.execute(
                "SELECT count(*) FROM pending_fraud_labels WHERE transaction_id = %s",
                (transaction_id,),
            ).fetchone()[0] == 1
            assert conn.execute(
                "SELECT count(*) FROM fraud_labels WHERE transaction_id = %s",
                (transaction_id,),
            ).fetchone()[0] == 0

            # Identical replay must not create a duplicate.
            assert store_label(
                conn,
                payload,
                kafka_key=transaction_id.encode(),
                source_topic="fraud-labels.v1",
                source_partition=1,
                source_offset=source_offset,
            ) is False

            conflicting = json.dumps({
                "transaction_id": transaction_id,
                "fraud_label": 0,
                "observed_at": "2018-04-08T00:00:00Z",
            }).encode()

            with pytest.raises(ValueError):
                store_label(
                    conn,
                    conflicting,
                    kafka_key=transaction_id.encode(),
                    source_topic="fraud-labels.v1",
                    source_partition=1,
                    source_offset=source_offset + 1,
                )

            with conn.transaction():
                lock_transaction(conn, transaction_id)
                lock_terminal(conn, terminal_id)
                conn.execute(
                    """
                    INSERT INTO scored_transactions (
                        transaction_id,
                        terminal_id,
                        occurred_at
                    )
                    VALUES (%s, %s, %s)
                    """,
                    (
                        transaction_id,
                        terminal_id,
                        "2018-04-01T00:00:00Z",
                    ),
                )
                assert resolve_pending_label(
                    conn,
                    transaction_id=transaction_id,
                    terminal_id=terminal_id,
                    occurred_at=pd.Timestamp(
                        "2018-04-01T00:00:00Z"
                    ).to_pydatetime(),
                ) == "activated"

            history = PostgresTerminalHistory(conn)

            # On April 15, the April 1 transaction
            # is inside both delayed windows.
            features = history.features(
                terminal_id,
                "2018-04-15T00:00:00Z",
            )

            assert features.tolist() == [
                1, 1, 1,
                1, 1, 1,
            ]

        finally:
            conn.execute(
                """
                DELETE FROM scored_transactions
                WHERE transaction_id = %s
                """,
                (transaction_id,),
            )

            conn.execute(
                """
                DELETE FROM fraud_labels
                WHERE transaction_id = %s
                """,
                (transaction_id,),
            )
            conn.execute(
                """
                DELETE FROM pending_fraud_labels
                WHERE transaction_id = %s
                """,
                (transaction_id,),
            )
