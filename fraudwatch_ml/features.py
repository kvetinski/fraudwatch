"""Canonical offline FraudWatch features plus a parity-tested single-row path."""

import numpy as np
import pandas as pd


LABEL_DELAY_DAYS = 7
TERMINAL_WINDOWS_DAYS = (7, 30)

FEATURE_NAMES = (
    "log_amount",
    "hour_sin",
    "hour_cos",
    "weekday_sin",
    "weekday_cos",
    "is_weekend",
    "terminal_7d_count",
    "terminal_7d_fraud_count",
    "terminal_7d_fraud_rate",
    "terminal_30d_count",
    "terminal_30d_fraud_count",
    "terminal_30d_fraud_rate",
)


def base_features(df: pd.DataFrame) -> np.ndarray:
    """Canonical vectorized feature calculation used for offline training."""
    dt = pd.to_datetime(df["TX_DATETIME"])

    seconds = (
        dt.dt.hour * 3600
        + dt.dt.minute * 60
        + dt.dt.second
    )

    weekday = dt.dt.day_of_week

    hour_angle = 2 * np.pi * seconds / 86400
    weekday_angle = 2 * np.pi * weekday / 7

    return np.column_stack([
        np.log1p(df["TX_AMOUNT"].to_numpy()),
        np.sin(hour_angle),
        np.cos(hour_angle),
        np.sin(weekday_angle),
        np.cos(weekday_angle),
        (weekday >= 5).astype(int),
    ])


def base_features_one(timestamp: pd.Timestamp, amount: float) -> np.ndarray:
    """Calculate one transaction's six base features without a DataFrame.

    The timestamp's timezone is respected, as in base_features(). The Kafka
    worker normalizes event timestamps to UTC before calling this function.
    A pd.Timestamp is expected on this hot path (parsed once by the worker).
    """
    seconds = timestamp.hour * 3600 + timestamp.minute * 60 + timestamp.second
    weekday = timestamp.day_of_week
    hour_angle = 2 * np.pi * seconds / 86400
    weekday_angle = 2 * np.pi * weekday / 7

    return np.asarray([
        np.log1p(amount),
        np.sin(hour_angle),
        np.cos(hour_angle),
        np.sin(weekday_angle),
        np.cos(weekday_angle),
        float(weekday >= 5),
    ], dtype=np.float64)


def terminal_risk_features(df: pd.DataFrame) -> np.ndarray:
    # Positional indexing avoids dependence on the caller's DataFrame index.
    work = df.reset_index(drop=True)

    features = np.zeros(
        (len(work), len(TERMINAL_WINDOWS_DAYS) * 3),
        dtype=np.float32,
    )

    for _, group in work.groupby("TERMINAL_ID", sort=False):
        group = group.sort_values(
            ["TX_DATETIME", "TRANSACTION_ID"],
            kind="stable",
        )

        times = group["TX_DATETIME"].to_numpy(dtype="datetime64[ns]")
        labels = group["TX_FRAUD"].to_numpy(dtype=np.int64)
        indices = group.index.to_numpy()

        prefix_frauds = np.r_[0, np.cumsum(labels, dtype=np.int64)]

        right = np.searchsorted(
            times,
            times - np.timedelta64(LABEL_DELAY_DAYS, "D"),
            side="right",
        )

        for j, window_days in enumerate(TERMINAL_WINDOWS_DAYS):
            left = np.searchsorted(
                times,
                times - np.timedelta64(LABEL_DELAY_DAYS + window_days, "D"),
                side="left",
            )

            counts = right - left
            frauds = prefix_frauds[right] - prefix_frauds[left]

            rates = np.divide(
                frauds,
                counts,
                out=np.zeros(len(counts), dtype=float),
                where=counts > 0,
            )

            offset = j * 3
            features[indices, offset] = counts
            features[indices, offset + 1] = frauds
            features[indices, offset + 2] = rates

    return features


def build_features(df: pd.DataFrame) -> np.ndarray:
    X = np.column_stack([
        base_features(df),
        terminal_risk_features(df),
    ])

    assert X.shape[1] == len(FEATURE_NAMES)
    assert np.isfinite(X).all()
    return X
