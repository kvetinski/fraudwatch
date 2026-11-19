import numpy as np
import pandas as pd

from fraudwatch_ml.features import build_features
from fraudwatch_ml.historical_evaluation import (
    ParityAccumulator,
    StreamingFeatureBuilder,
    holdout_metrics,
)


def test_streaming_builder_matches_vectorized_offline_features():
    rows = []
    for day in (0, 3, 7, 10, 14, 21, 37, 40):
        for terminal, amount, label in (("a", 10.0 + day, day % 2), ("b", 4.0, 0)):
            rows.append(
                {
                    "TRANSACTION_ID": f"{day}-{terminal}",
                    "TX_DATETIME": pd.Timestamp("2018-04-01") + pd.Timedelta(days=day),
                    "TERMINAL_ID": terminal,
                    "TX_AMOUNT": amount,
                    "TX_FRAUD": label,
                }
            )
    frame = pd.DataFrame(rows).sort_values(
        ["TX_DATETIME", "TRANSACTION_ID"], kind="stable"
    ).reset_index(drop=True)
    expected = build_features(frame)
    builder = StreamingFeatureBuilder()
    actual = np.vstack(
        [
            builder.build(
                terminal_id=row.TERMINAL_ID,
                occurred_at=row.TX_DATETIME,
                amount=row.TX_AMOUNT,
                fraud_label=row.TX_FRAUD,
            )
            for row in frame.itertuples(index=False)
        ]
    )
    np.testing.assert_array_equal(actual, expected)


def test_parity_accumulator_enforces_tolerances():
    accumulator = ParityAccumulator()
    features = np.asarray([[1.0, 2.0]])
    scores = np.asarray([0.25])
    accumulator.observe(features, features.copy(), scores, scores.copy())
    assert accumulator.compared == 1
    assert accumulator.max_feature_absolute_error == 0


def test_holdout_metrics_are_deterministic_and_include_scenarios():
    result = holdout_metrics(
        np.asarray([0, 1, 1, 0]),
        np.asarray([0.1, 0.9, 0.8, 0.2]),
        np.asarray(["a", "b", "c", "d"]),
        np.asarray([0, 1, 2, 0]),
        alert_fraction=0.25,
    )
    assert result["alert_count"] == 1
    assert result["frauds_detected"] == 1
    assert result["precision_at_1pct"] == 1
    assert result["recall_at_1pct"] == 0.5
    assert result["scenario_recall"]["1"]["recall"] == 1
    assert result["scenario_recall"]["2"]["recall"] == 0
