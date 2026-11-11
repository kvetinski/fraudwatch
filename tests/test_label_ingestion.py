import json
import os
import uuid

import psycopg
import pytest

from fraudwatch_ml.labels import store_label
from fraudwatch_ml.postgres_features import PostgresTerminalHistory


def test_label_before_transaction_and_idempotency():
    dsn = os.getenv("TEST_DATABASE_URL")

    if not dsn:
        pytest.skip("TEST_DATABASE_URL not configured")

    transaction_id = f"label-test-{uuid.uuid4().hex}"
    terminal_id = f"terminal-{uuid.uuid4().hex}"

    payload = json.dumps({
        "transaction_id": transaction_id,
        "fraud_label": 1,
        "observed_at": "2018-04-08T00:00:00Z",
    }).encode()

    with psycopg.connect(dsn, autocommit=True) as conn:
        try:
            # Label arrives before the transaction.
            assert store_label(conn, payload) is True

            # Identical replay must not create a duplicate.
            assert store_label(conn, payload) is False

            conflicting = json.dumps({
                "transaction_id": transaction_id,
                "fraud_label": 0,
                "observed_at": "2018-04-08T00:00:00Z",
            }).encode()

            with pytest.raises(ValueError):
                store_label(conn, conflicting)

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
