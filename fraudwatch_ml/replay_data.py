import numpy as np
import pandas as pd


def select_day(df: pd.DataFrame, limit: int) -> pd.DataFrame:
    if limit < 1:
        raise ValueError("limit must be positive")

    ordered = df.sort_values(
        ["TX_DATETIME", "TRANSACTION_ID"],
        kind="stable",
    ).reset_index(drop=True)

    count = min(limit, len(ordered))

    if count == 0:
        return ordered

    indices = np.linspace(
        0,
        len(ordered) - 1,
        num=count,
        dtype=np.int64,
    )

    return ordered.iloc[indices].reset_index(drop=True)
