"""Streaming offline feature parity and holdout evaluation primitives."""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass
import numpy as np
import pandas as pd
from sklearn.calibration import calibration_curve
from sklearn.metrics import average_precision_score, brier_score_loss, roc_auc_score

from fraudwatch_ml.features import (
    FEATURE_NAMES,
    LABEL_DELAY_DAYS,
    TERMINAL_WINDOWS_DAYS,
    base_features_one,
)


NANOSECONDS_PER_DAY = 86_400_000_000_000


class StreamingFeatureBuilder:
    """Reproduce offline terminal features with bounded per-terminal history."""

    def __init__(self):
        self._history: dict[str, deque[tuple[int, int]]] = defaultdict(deque)

    def build(
        self,
        *,
        terminal_id: str,
        occurred_at: pd.Timestamp,
        amount: float,
        fraud_label: int,
    ) -> np.ndarray:
        timestamp = pd.Timestamp(occurred_at)
        if timestamp.tzinfo is not None:
            timestamp = timestamp.tz_convert("UTC").tz_localize(None)
        at_ns = timestamp.value
        history = self._history[terminal_id]
        oldest_ns = at_ns - (
            LABEL_DELAY_DAYS + max(TERMINAL_WINDOWS_DAYS)
        ) * NANOSECONDS_PER_DAY
        while history and history[0][0] < oldest_ns:
            history.popleft()

        terminal_values = []
        available_ns = at_ns - LABEL_DELAY_DAYS * NANOSECONDS_PER_DAY
        for window_days in TERMINAL_WINDOWS_DAYS:
            lower_ns = at_ns - (
                LABEL_DELAY_DAYS + window_days
            ) * NANOSECONDS_PER_DAY
            count = 0
            frauds = 0
            for event_ns, label in history:
                if lower_ns <= event_ns <= available_ns:
                    count += 1
                    frauds += label
            terminal_values.extend(
                (count, frauds, frauds / count if count else 0.0)
            )

        history.append((at_ns, int(fraud_label)))
        vector = np.empty(len(FEATURE_NAMES), dtype=np.float64)
        vector[:6] = base_features_one(timestamp, amount)
        # Match terminal_risk_features' deliberate float32 rounding.
        vector[6:] = np.asarray(terminal_values, dtype=np.float32)
        return vector


@dataclass
class ParityAccumulator:
    compared: int = 0
    max_feature_absolute_error: float = 0.0
    max_score_absolute_error: float = 0.0

    def observe(
        self,
        expected_features: np.ndarray,
        actual_features: np.ndarray,
        expected_scores: np.ndarray,
        actual_scores: np.ndarray,
        *,
        feature_atol: float = 1e-6,
        score_rtol: float = 1e-8,
        score_atol: float = 1e-10,
    ) -> None:
        np.testing.assert_allclose(
            actual_features,
            expected_features,
            rtol=0,
            atol=feature_atol,
        )
        np.testing.assert_allclose(
            actual_scores,
            expected_scores,
            rtol=score_rtol,
            atol=score_atol,
        )
        self.compared += len(expected_scores)
        if expected_features.size:
            self.max_feature_absolute_error = max(
                self.max_feature_absolute_error,
                float(np.max(np.abs(actual_features - expected_features))),
            )
        if expected_scores.size:
            self.max_score_absolute_error = max(
                self.max_score_absolute_error,
                float(np.max(np.abs(actual_scores - expected_scores))),
            )


def holdout_metrics(
    labels: np.ndarray,
    scores: np.ndarray,
    transaction_ids: np.ndarray,
    scenarios: np.ndarray,
    *,
    alert_fraction: float = 0.01,
) -> dict:
    if not len(labels) or len({len(labels), len(scores), len(transaction_ids), len(scenarios)}) != 1:
        raise ValueError("Holdout arrays must be nonempty and equal length")
    alert_count = max(1, int(np.ceil(len(scores) * alert_fraction)))
    # Stable tie break is part of the report contract.
    selected = np.lexsort((transaction_ids.astype(str), -scores))[:alert_count]
    selected_mask = np.zeros(len(scores), dtype=bool)
    selected_mask[selected] = True
    fraud_mask = labels == 1
    fraud_count = int(fraud_mask.sum())
    detected = int(labels[selected].sum())

    observed, probability = calibration_curve(
        labels,
        scores,
        n_bins=10,
        strategy="quantile",
    )
    scenario_metrics = {}
    for scenario in sorted(np.unique(scenarios[fraud_mask])):
        mask = fraud_mask & (scenarios == scenario)
        cases = int(mask.sum())
        found = int((mask & selected_mask).sum())
        scenario_metrics[str(int(scenario))] = {
            "fraud_cases": cases,
            "detected": found,
            "recall": found / cases if cases else None,
        }

    return {
        "records": len(labels),
        "fraud_count": fraud_count,
        "fraud_prevalence": float(labels.mean()),
        "average_precision": float(average_precision_score(labels, scores)),
        "roc_auc": float(roc_auc_score(labels, scores)),
        "brier_score": float(brier_score_loss(labels, scores)),
        "alert_fraction": alert_fraction,
        "alert_count": alert_count,
        "frauds_detected": detected,
        "precision_at_1pct": detected / alert_count,
        "recall_at_1pct": detected / fraud_count,
        "calibration_quantile_bins": [
            {
                "mean_predicted_probability": float(predicted),
                "observed_fraud_fraction": float(fraction),
            }
            for predicted, fraction in zip(probability, observed)
        ],
        "scenario_recall": scenario_metrics,
    }
