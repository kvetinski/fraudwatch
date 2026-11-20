import numpy as np
import pytest

from fraudwatch_ml.features import FEATURE_NAMES
from fraudwatch_ml.model_monitoring import (
    build_monitoring_baseline,
    evaluate_monitoring_window,
    population_stability_index,
    reference_distribution,
)


def sample(size=2000):
    rng = np.random.default_rng(42)
    features = rng.normal(size=(size, len(FEATURE_NAMES)))
    scores = 1 / (1 + np.exp(-features[:, 0]))
    labels = (scores > 0.9).astype(np.int8)
    return features, scores, labels


def test_reference_distribution_has_stable_zero_psi():
    values = np.linspace(0, 1, 1000)
    reference = reference_distribution(values)
    assert len(reference["proportions"]) == 10
    assert population_stability_index(values, reference) < 1e-12


def test_shifted_population_has_material_psi():
    values = np.linspace(0, 1, 1000)
    reference = reference_distribution(values)
    assert population_stability_index(values + 0.5, reference) > 0.2


def test_monitoring_window_includes_drift_quality_and_gates():
    features, scores, labels = sample()
    baseline = build_monitoring_baseline(
        features, scores, labels,
        model_reference="models:/fraudwatch-hgb/1",
        model_digest="sha256:" + "a" * 64,
        dataset_fingerprint="sha256:" + "b" * 64,
        feature_schema_version="v1",
    )
    result = evaluate_monitoring_window(
        features.copy(), scores.copy(), labels.tolist(), baseline,
    )
    assert result["sample_count"] == 2000
    assert result["positive_count"] >= 20
    assert result["alert_eligible"] is True
    assert result["score_psi"] < 1e-12
    assert max(result["feature_psi"].values()) < 1e-12
    assert result["quality_degradation"]["average_precision"] == 0


def test_monitoring_alert_gate_requires_labels_and_positives():
    features, scores, labels = sample(100)
    baseline = build_monitoring_baseline(
        features, scores, labels,
        model_reference="models:/fraudwatch-hgb/1",
        model_digest="digest",
        dataset_fingerprint="dataset",
        feature_schema_version="v1",
    )
    result = evaluate_monitoring_window(
        features, scores, [None] * len(scores), baseline,
    )
    assert result["label_coverage"] == 0
    assert result["alert_eligible"] is False
    assert result["average_precision"] is None


def test_monitoring_window_rejects_misaligned_labels():
    features, scores, labels = sample(100)
    baseline = build_monitoring_baseline(
        features, scores, labels,
        model_reference="models:/fraudwatch-hgb/1",
        model_digest="digest",
        dataset_fingerprint="dataset",
        feature_schema_version="v1",
    )
    with pytest.raises(ValueError, match="equal length"):
        evaluate_monitoring_window(features, scores, labels[:-1], baseline)
