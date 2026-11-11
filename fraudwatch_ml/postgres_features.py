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

        values = []

        for window in TERMINAL_WINDOWS_DAYS:
            lower = at - pd.Timedelta(
                days=LABEL_DELAY_DAYS + window
            )
            upper = at - pd.Timedelta(
                days=LABEL_DELAY_DAYS
            )

            row = self.connection.execute(
                """
                SELECT
                    count(*) AS tx_count,
                    count(*) FILTER (
                        WHERE
                            COALESCE(fl.fraud_label, st.fraud_label) = 1
                            AND COALESCE(
                                fl.observed_at,
                                st.label_observed_at
                            ) <= %s
                    ) AS fraud_count
                FROM scored_transactions AS st
                LEFT JOIN fraud_labels AS fl
                    ON fl.transaction_id = st.transaction_id
                WHERE st.terminal_id = %s
                AND st.occurred_at >= %s
                AND st.occurred_at <= %s
                """,
                (
                    at.to_pydatetime(),
                    terminal_id,
                    lower.to_pydatetime(),
                    upper.to_pydatetime(),
                ),
            ).fetchone()

            count, frauds = row

            values.extend([
                count,
                frauds,
                frauds / count if count else 0.0,
            ])

        return np.asarray(values, dtype=np.float32)
