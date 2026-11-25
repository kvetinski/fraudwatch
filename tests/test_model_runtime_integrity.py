import json

import pytest

from fraudwatch_ml.model_integrity import sha256_tree, verify_portable_model_digest


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
