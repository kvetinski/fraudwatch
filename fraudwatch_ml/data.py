from pathlib import Path

import pandas as pd


def load_transactions(
    directory: Path,
    start: str,
    end: str,
) -> pd.DataFrame:
    files = sorted(
        p for p in directory.glob("*.pkl")
        if start <= p.stem < end
    )

    if not files:
        raise ValueError("No transaction files found")

    return pd.concat(
        [pd.read_pickle(p) for p in files],
        ignore_index=True,
    )
