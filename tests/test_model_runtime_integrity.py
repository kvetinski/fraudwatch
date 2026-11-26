import json

import pytest

from fraudwatch_ml.model_integrity import sha256_tree, verify_portable_model_digest
from fraudwatch_ml.features import FEATURE_NAMES
from fraudwatch_ml.scoring import FEATURE_SCHEMA_VERSION


def test_portable_model_digest_is_calculated_from_runtime_bytes(tmp_path):
    bundle = tmp_path / "bundle"
    model = bundle / "model"
    model.mkdir(parents=True)
    artifact = model / "model.skops"
    artifact.write_bytes(b"trusted model bytes")
    digest = "sha256:" + sha256_tree(model)
    manifest = {"model_digest": digest}
    (bundle / "manifest.json").write_text(json.dumps(manifest))

    assert verify_portable_model_digest(bundle, manifest) == digest

    artifact.write_bytes(b"modified model bytes")
    with pytest.raises(RuntimeError, match="bytes differ"):
        verify_portable_model_digest(bundle, manifest)


def test_portable_model_digest_rejects_unpinned_metadata(tmp_path):
    model = tmp_path / "model"
    model.mkdir()
    (model / "MLmodel").write_text("model")

    with pytest.raises(RuntimeError, match="invalid model digest"):
        verify_portable_model_digest(tmp_path, {"model_digest": "unknown"})


def test_integrity_failure_prevents_kafka_consumer_creation(monkeypatch):
    from services.scoring import worker

    consumer_created = False

    def fail_model_load():
        raise RuntimeError("Exported model bytes differ from the manifest digest")

    def unexpected_consumer(*_args, **_kwargs):
        nonlocal consumer_created
        consumer_created = True
        raise AssertionError("Kafka consumer must not exist after integrity failure")

    monkeypatch.setenv("FRAUDWATCH_METRICS_ENABLED", "0")
    monkeypatch.setattr(worker, "load_scorer", fail_model_load)
    monkeypatch.setattr(worker, "Consumer", unexpected_consumer)

    with pytest.raises(RuntimeError, match="bytes differ"):
        worker.main()

    assert consumer_created is False


def test_legacy_replay_verifier_loads_only_verified_bundle(tmp_path, monkeypatch):
    from scripts import verify_replay

    bundle = tmp_path / "bundle"
    model = bundle / "model"
    model.mkdir(parents=True)
    artifact = model / "model.skops"
    artifact.write_bytes(b"verified replay model")
    digest = "sha256:" + sha256_tree(model)
    (bundle / "manifest.json").write_text(
        json.dumps(
            {
                "model_reference": "models:/fraudwatch-hgb/9",
                "model_digest": digest,
                "feature_names": list(FEATURE_NAMES),
                "feature_schema_version": FEATURE_SCHEMA_VERSION,
            }
        )
    )
    loaded = object()
    monkeypatch.setattr(
        verify_replay.mlflow.sklearn,
        "load_model",
        lambda path: loaded,
    )

    assert verify_replay.load_verified_model(bundle) == (
        loaded,
        "models:/fraudwatch-hgb/9",
        digest,
    )

    artifact.write_bytes(b"tampered replay model")
    with pytest.raises(RuntimeError, match="bytes differ"):
        verify_replay.load_verified_model(bundle)


def test_model_monitor_rejects_unverified_baseline_identity():
    from scripts.model_monitor import verified_baseline_identity

    baseline = {
        "identity": {
            "model_reference": "models:/fraudwatch-hgb/1",
            "model_digest": "legacy:unverified",
            "feature_schema_version": "v1",
            "dataset_fingerprint": "sha256:" + "b" * 64,
        }
    }
    with pytest.raises(SystemExit, match="invalid model_digest"):
        verified_baseline_identity(baseline)
