import numpy as np
import pandas as pd

from fraudwatch_ml.features import (
    LABEL_DELAY_DAYS,
    TERMINAL_WINDOWS_DAYS,
)


class PostgresTerminalHistory:
    def __init__(self, connection):
        self.connection = connection

    def features(self, terminal_id: str, at) -> np.ndarray:
        at = pd.Timestamp(at)

        if at.tzinfo is None:
            # The original simulator uses UTC-naive timestamps.
            at = at.tz_localize("UTC")
        else:
            at = at.tz_convert("UTC")

        # Compute every window in one PostgreSQL statement. The statement uses
        # one MVCC snapshot, so concurrent label activation cannot make the 7d
        # and 30d features observe different database states.
        rows = self.connection.execute(
            """
            WITH parameters AS (
                SELECT %s::timestamptz AS feature_at,
                       %s::integer AS delay_days
            ), windows AS (
                SELECT window_days, ordinal
                FROM unnest(%s::integer[]) WITH ORDINALITY
                    AS configured(window_days, ordinal)
            )
            SELECT
                windows.ordinal,
                count(st.transaction_id) AS tx_count,
                count(st.transaction_id) FILTER (
                    WHERE
                        COALESCE(fl.fraud_label, st.fraud_label) = 1
                        AND COALESCE(
                            fl.observed_at,
                            st.label_observed_at
                        ) <= parameters.feature_at
                ) AS fraud_count
            FROM windows
            CROSS JOIN parameters
            LEFT JOIN scored_transactions AS st
                ON st.terminal_id = %s
               AND st.occurred_at >= parameters.feature_at - make_interval(
                    days => parameters.delay_days + windows.window_days
               )
               AND st.occurred_at <= parameters.feature_at - make_interval(
                    days => parameters.delay_days
               )
            LEFT JOIN fraud_labels AS fl
                ON fl.transaction_id = st.transaction_id
            GROUP BY windows.ordinal
            ORDER BY windows.ordinal
            """,
            (
                at.to_pydatetime(),
                LABEL_DELAY_DAYS,
                list(TERMINAL_WINDOWS_DAYS),
                terminal_id,
            ),
        ).fetchall()

        if len(rows) != len(TERMINAL_WINDOWS_DAYS):
            raise RuntimeError("PostgreSQL returned an incomplete history snapshot")

        values = []
        for _, count, frauds in rows:
            values.extend([
                count,
                frauds,
                frauds / count if count else 0.0,
            ])

        return np.asarray(values, dtype=np.float32)
