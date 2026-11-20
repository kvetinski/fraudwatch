"""Reference distributions and drift/quality evaluation for deployed models."""

from __future__ import annotations

from datetime import datetime, timezone
import math
from typing import Mapping, Sequence

import numpy as np
from sklearn.metrics import average_precision_score, roc_auc_score

from fraudwatch_ml.features import FEATURE_NAMES

BASELINE_SCHEMA_VERSION = 1
DEFAULT_PSI_WARNING = 0.20
DEFAULT_PSI_CRITICAL = 0.30
DEFAULT_QUALITY_DEGRADATION = 0.20
DEFAULT_MIN_SAMPLES = 1000
DEFAULT_MIN_POSITIVES = 20


def reference_distribution(values: Sequence[float], *, bins: int = 10) -> dict:
    array = np.asarray(values, dtype=np.float64)
    if array.ndim != 1 or not len(array) or not np.isfinite(array).all():
        raise ValueError("Reference values must be a nonempty finite vector")
    if bins < 2:
        raise ValueError("bins must be at least two")
    cuts = np.unique(np.quantile(array, np.arange(1, bins) / bins))
    indexes = np.searchsorted(cuts, array, side="right")
    counts = np.bincount(indexes, minlength=len(cuts) + 1).astype(np.float64)
    return {
        "cuts": [float(value) for value in cuts],
        "proportions": [float(value) for value in counts / counts.sum()],
    }


def population_stability_index(values: Sequence[float], reference: Mapping) -> float:
    array = np.asarray(values, dtype=np.float64)
    if array.ndim != 1 or not len(array) or not np.isfinite(array).all():
        raise ValueError("Observed values must be a nonempty finite vector")
    cuts = np.asarray(reference["cuts"], dtype=np.float64)
    expected = np.asarray(reference["proportions"], dtype=np.float64)
    if len(expected) != len(cuts) + 1 or not np.isclose(expected.sum(), 1.0):
        raise ValueError("Invalid reference distribution")
    indexes = np.searchsorted(cuts, array, side="right")
    actual = np.bincount(indexes, minlength=len(expected)).astype(np.float64)
    actual /= actual.sum()
    epsilon = 1e-6
    expected = np.clip(expected, epsilon, None)
    actual = np.clip(actual, epsilon, None)
    expected /= expected.sum()
    actual /= actual.sum()
    return float(np.sum((actual - expected) * np.log(actual / expected)))


def quality_metrics(labels: Sequence[int], scores: Sequence[float]) -> dict:
    labels_array = np.asarray(labels, dtype=np.int8)
    scores_array = np.asarray(scores, dtype=np.float64)
    if len(labels_array) != len(scores_array) or not len(labels_array):
        raise ValueError("Labels and scores must be nonempty and equal length")
    if not np.isin(labels_array, (0, 1)).all() or not np.isfinite(scores_array).all():
        raise ValueError("Invalid label or score values")
    positives = int(labels_array.sum())
    alert_count = max(1, int(math.ceil(len(scores_array) * 0.01)))
    # Stable row order is sufficient here because IDs are not loaded into the
    # monitoring service; ties at the boundary are exceptionally unlikely.
    selected = np.argsort(-scores_array, kind="stable")[:alert_count]
    detected = int(labels_array[selected].sum())
    both_classes = 0 < positives < len(labels_array)
    return {
        "fraud_prevalence": float(labels_array.mean()),
        "average_precision": float(average_precision_score(labels_array, scores_array)) if both_classes else None,
        "roc_auc": float(roc_auc_score(labels_array, scores_array)) if both_classes else None,
        "precision_at_1pct": detected / alert_count,
        "recall_at_1pct": detected / positives if positives else None,
    }


def build_monitoring_baseline(
    features: np.ndarray,
    scores: Sequence[float],
    labels: Sequence[int],
    *,
    model_reference: str,
    model_digest: str,
    dataset_fingerprint: str,
    feature_schema_version: str,
    bins: int = 10,
) -> dict:
    matrix = np.asarray(features, dtype=np.float64)
    scores_array = np.asarray(scores, dtype=np.float64)
    labels_array = np.asarray(labels, dtype=np.int8)
    if matrix.shape != (len(scores_array), len(FEATURE_NAMES)):
        raise ValueError("Feature matrix differs from the deployed feature schema")
    if len(labels_array) != len(scores_array):
        raise ValueError("Labels, scores and features must have equal length")
    return {
        "schema_version": BASELINE_SCHEMA_VERSION,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "identity": {
            "model_reference": model_reference,
            "model_digest": model_digest,
            "dataset_fingerprint": dataset_fingerprint,
            "feature_schema_version": feature_schema_version,
        },
        "sample_count": len(scores_array),
        "positive_count": int(labels_array.sum()),
        "thresholds": {
            "psi_warning": DEFAULT_PSI_WARNING,
            "psi_critical": DEFAULT_PSI_CRITICAL,
            "quality_degradation": DEFAULT_QUALITY_DEGRADATION,
            "min_samples": DEFAULT_MIN_SAMPLES,
            "min_positives": DEFAULT_MIN_POSITIVES,
        },
        "reference": {
            "score": reference_distribution(scores_array, bins=bins),
            "features": {
                name: reference_distribution(matrix[:, index], bins=bins)
                for index, name in enumerate(FEATURE_NAMES)
            },
        },
        "validation_metrics": quality_metrics(labels_array, scores_array),
    }


def evaluate_monitoring_window(
    features: np.ndarray,
    scores: Sequence[float],
    labels: Sequence[int | None],
    baseline: Mapping,
) -> dict:
    matrix = np.asarray(features, dtype=np.float64)
    scores_array = np.asarray(scores, dtype=np.float64)
    if len(labels) != len(scores_array):
        raise ValueError("Labels, scores and features must have equal length")
    if matrix.shape != (len(scores_array), len(FEATURE_NAMES)):
        raise ValueError("Monitoring feature matrix differs from the deployed schema")
    if not len(scores_array):
        raise ValueError("Monitoring window is empty")
    identity = baseline.get("identity", {})
    reference = baseline["reference"]
    score_psi = population_stability_index(scores_array, reference["score"])
    feature_psi = {
        name: population_stability_index(matrix[:, index], reference["features"][name])
        for index, name in enumerate(FEATURE_NAMES)
    }
    labeled_indexes = [index for index, label in enumerate(labels) if label is not None]
    labeled = np.asarray([labels[index] for index in labeled_indexes], dtype=np.int8)
    labeled_scores = scores_array[labeled_indexes]
    metrics = quality_metrics(labeled, labeled_scores) if len(labeled) else {
        "fraud_prevalence": None,
        "average_precision": None,
        "roc_auc": None,
        "precision_at_1pct": None,
        "recall_at_1pct": None,
    }
    baseline_metrics = baseline.get("validation_metrics", {})
    degradation = {}
    for name in ("average_precision", "roc_auc", "precision_at_1pct", "recall_at_1pct"):
        expected = baseline_metrics.get(name)
        actual = metrics.get(name)
        degradation[name] = (
            max(0.0, (expected - actual) / expected)
            if expected not in (None, 0) and actual is not None else None
        )
    thresholds = baseline.get("thresholds", {})
    min_samples = int(thresholds.get("min_samples", DEFAULT_MIN_SAMPLES))
    min_positives = int(thresholds.get("min_positives", DEFAULT_MIN_POSITIVES))
    positive_count = int(labeled.sum()) if len(labeled) else 0
    return {
        "identity": identity,
        "sample_count": len(scores_array),
        "labeled_count": len(labeled),
        "positive_count": positive_count,
        "label_coverage": len(labeled) / len(scores_array),
        "score_psi": score_psi,
        "feature_psi": feature_psi,
        **metrics,
        "quality_degradation": degradation,
        "alert_eligible": len(scores_array) >= min_samples and positive_count >= min_positives,
    }
