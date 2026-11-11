"""Regression checks against the canonical pandas-based feature formulas."""

import numpy as np
import pandas as pd
import pytest

from fraudwatch_ml.features import FEATURE_NAMES, base_features, base_features_one
from fraudwatch_ml.scoring import Transaction, make_feature_frame, make_feature_vector


class FixedHistory:
    def __init__(self, values):
        self.values = np.asarray(values, dtype=np.float32)

    def features(self, terminal_id, timestamp):
        return self.values.copy()


@pytest.mark.parametrize("timestamp, amount", [
    ("2018-04-01T00:00:00Z", 0.0),
    ("2018-04-02T23:59:59Z", 0.01),
    ("2018-04-07T12:04:05Z", 220.0),
    ("2018-04-14T12:04:05Z", 0.0),
    ("2018-05-31T09:30:17Z", 999.99),
    ("2018-04-01T03:00:00+03:00", 100.0),
])
def test_single_base_matches_canonical(timestamp, amount):
    dt = pd.Timestamp(timestamp).tz_convert("UTC")
    offline = base_features(pd.DataFrame({
        "TX_DATETIME": [dt], "TX_AMOUNT": [amount],
    }))[0]
    fast = base_features_one(dt, amount)
    np.testing.assert_allclose(fast, offline, rtol=0, atol=1e-14)
    assert fast.dtype == np.float64


def test_single_base_matches_canonical_many_random_cases():
    rng = np.random.default_rng(42)
    day_seconds = rng.integers(0, 86400, size=400)
    day_offsets = rng.integers(0, 183, size=400)
    amounts = rng.uniform(0, 1000, size=400)
    amounts[0] = 0
    dates = pd.Timestamp("2018-04-01", tz="UTC") + pd.to_timedelta(day_offsets * 86400 + day_seconds, unit="s")

    raw = pd.DataFrame({"TX_DATETIME": dates, "TX_AMOUNT": amounts})
    expected = base_features(raw)
    actual = np.vstack([
        base_features_one(ts, float(amount))
        for ts, amount in zip(dates, amounts)
    ])
    np.testing.assert_allclose(actual, expected, rtol=0, atol=1e-14)


def test_vector_frame_and_float32_history_rounding():
    tx = Transaction(
        transaction_id="tx-zero",
        terminal_id="terminal-42",
        timestamp="2018-04-14T12:04:05Z",
        amount=0.0,
    )
    # The offline risk-feature array is float32. In particular 1/3 is rounded.
    history = FixedHistory([3, 1, 1 / 3, 11, 2, 2 / 11])

    raw = pd.DataFrame({
        "TX_DATETIME": [pd.Timestamp(tx.timestamp)],
        "TX_AMOUNT": [tx.amount],
    })
    expected = np.concatenate([base_features(raw)[0], history.values])

    vector = make_feature_vector(tx, history)
    frame = make_feature_frame(tx, history)

    assert list(frame.columns) == list(FEATURE_NAMES)
    assert vector.dtype == np.float64
    np.testing.assert_allclose(vector, expected, rtol=0, atol=1e-14)
    np.testing.assert_array_equal(frame.to_numpy()[0], vector)


def test_rejects_incorrect_history_size_and_nonfinite_values():
    tx = Transaction("tx", "terminal", "2018-04-01T00:00:00Z", 0.0)
    with pytest.raises(ValueError, match="feature count"):
        make_feature_vector(tx, FixedHistory([0]))
    with pytest.raises(ValueError, match="Non-finite"):
        make_feature_vector(tx, FixedHistory([0, 0, 0, 0, 0, np.nan]))


def test_hgb_probability_parity_with_named_feature_input():
    from sklearn.ensemble import HistGradientBoostingClassifier

    rng = np.random.default_rng(2026)
    training = pd.DataFrame(rng.normal(size=(120, len(FEATURE_NAMES))), columns=FEATURE_NAMES)
    labels = (training["log_amount"] + training["hour_sin"] > 0).astype(int)
    model = HistGradientBoostingClassifier(
        max_iter=8, max_leaf_nodes=5, min_samples_leaf=5, random_state=42,
    ).fit(training, labels)

    tx = Transaction("tx", "terminal", "2018-04-14T12:04:05Z", 99.0)
    history = FixedHistory([3, 1, 1 / 3, 11, 2, 2 / 11])
    old = np.concatenate([base_features(pd.DataFrame({
        "TX_DATETIME": [pd.Timestamp(tx.timestamp)],
        "TX_AMOUNT": [tx.amount],
    }))[0], history.values])
    old_frame = pd.DataFrame([old], columns=list(FEATURE_NAMES))
    fast_frame = make_feature_frame(tx, history)
    np.testing.assert_allclose(
        model.predict_proba(fast_frame),
        model.predict_proba(old_frame),
        rtol=0, atol=0,
    )
