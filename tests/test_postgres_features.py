import os
import uuid
from datetime import timedelta, timezone

import numpy as np
import pandas as pd
import psycopg
import pytest

from fraudwatch_ml.features import terminal_risk_features
from fraudwatch_ml.postgres_features import PostgresTerminalHistory


def test_postgres_matches_offline_features():
    dsn = os.getenv("TEST_DATABASE_URL")

    if not dsn:
        pytest.skip("TEST_DATABASE_URL not configured")

    prefix = uuid.uuid4().hex
    terminal = f"terminal-{prefix}"

    df = pd.DataFrame({
        "TRANSACTION_ID": [
            f"{prefix}-{i}" for i in range(6)
        ],
        "TERMINAL_ID": [terminal] * 6,
        "TX_DATETIME": pd.to_datetime([
            "2018-04-01",
            "2018-04-06",
            "2018-04-08",
            "2018-04-10",
            "2018-04-20",
            "2018-05-12",
        ]),
        "TX_FRAUD": [1, 0, 1, 0, 1, 0],
    })

    expected = terminal_risk_features(df)

    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("SET TIME ZONE 'UTC'")

        try:
            for row in df.itertuples(index=False):
                occurred = row.TX_DATETIME.to_pydatetime().replace(
                    tzinfo=timezone.utc
                )

                observed = occurred + timedelta(days=7)

                conn.execute(
                    """
                    INSERT INTO scored_transactions (
                        transaction_id,
                        terminal_id,
                        occurred_at,
                        fraud_label,
                        label_observed_at
                    )
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        row.TRANSACTION_ID,
                        row.TERMINAL_ID,
                        occurred,
                        int(row.TX_FRAUD),
                        observed,
                    ),
                )

            history = PostgresTerminalHistory(conn)

            for i, row in enumerate(df.itertuples(index=False)):
                at = row.TX_DATETIME.tz_localize("UTC")

                actual = history.features(terminal, at)

                np.testing.assert_allclose(
                    actual,
                    expected[i],
                    rtol=0,
                    atol=1e-6,
                    err_msg=f"Mismatch at transaction {i}",
                )

            # Simulate a label arriving later than seven days.
            conn.execute(
                """
                UPDATE scored_transactions
                SET label_observed_at = %s
                WHERE transaction_id = %s
                """,
                (
                    pd.Timestamp(
                        "2018-04-21", tz="UTC"
                    ).to_pydatetime(),
                    df.iloc[0]["TRANSACTION_ID"],
                ),
            )

            # On April 8, the April 1 transaction
            # contributes to the denominator, but
            # its delayed fraud label is still unknown.
            features = history.features(
                terminal,
                pd.Timestamp("2018-04-08", tz="UTC"),
            )

            np.testing.assert_array_equal(
                features,
                [1, 0, 0, 1, 0, 0],
            )

        finally:
            conn.execute(
                """
                DELETE FROM scored_transactions
                WHERE transaction_id = ANY(%s)
                """,
                (df["TRANSACTION_ID"].tolist(),),
            )
