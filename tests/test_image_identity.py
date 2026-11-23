import hashlib
import json
import subprocess

import pytest

from scripts.image_identity import image_identity, model_digest, source_identity


def git(root, *args):
    subprocess.run(
        ["git", *args], cwd=root, check=True, capture_output=True
    )


def test_model_digest_rejects_an_invalid_manifest(tmp_path):
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"model_digest": "latest"}))
    with pytest.raises(SystemExit, match="Invalid model_digest"):
        model_digest(manifest)


def test_image_identity_includes_source_and_model_content(tmp_path):
    git(tmp_path, "init", "-q")
    git(tmp_path, "config", "user.email", "test@example.invalid")
    git(tmp_path, "config", "user.name", "FraudWatch test")
    (tmp_path / "scripts").mkdir()
    (tmp_path / "scripts" / "migrate_database.py").write_text("stable\n")
    (tmp_path / "README.md").write_text("stable docs\n")
    (tmp_path / ".gitignore").write_text("artifacts/\n")
    git(
        tmp_path,
        "add",
        "scripts/migrate_database.py",
        "README.md",
        ".gitignore",
    )
    git(tmp_path, "commit", "-qm", "baseline")
    digest = "sha256:" + hashlib.sha256(b"model").hexdigest()
    bundle = tmp_path / "artifacts" / "candidate-model"
    bundle.mkdir(parents=True)
    (bundle / "manifest.json").write_text(json.dumps({"model_digest": digest}))

    clean_revision, clean_tag = source_identity(tmp_path)
    assert clean_tag == clean_revision[:12]

    (tmp_path / "README.md").write_text("deployment docs only\n")
    docs_revision, docs_tag = source_identity(tmp_path)
    assert docs_revision == clean_revision
    assert docs_tag == clean_tag

    (tmp_path / "scripts" / "untracked.py").write_text("deployment helper\n")
    helper_revision, helper_tag = source_identity(tmp_path)
    assert helper_revision == clean_revision
    assert helper_tag == clean_tag

    (tmp_path / "scripts" / "migrate_database.py").write_text("changed\n")
    values = image_identity(tmp_path)
    assert "+dirty." in values["FRAUDWATCH_BUILD_REVISION"]
    assert "-dirty-" in values["FRAUDWATCH_APP_TAG"]
    assert values["FRAUDWATCH_SCORING_TAG"].endswith(
        f"-bundle-{values['FRAUDWATCH_MODEL_BUNDLE_DIGEST'].removeprefix('sha256:')[:12]}"
    )
    assert f"-model-{digest.removeprefix('sha256:')[:12]}-" in values[
        "FRAUDWATCH_SCORING_TAG"
    ]

    (bundle / "monitoring-baseline.json").write_text("{}\n")
    changed_bundle = image_identity(tmp_path)
    assert changed_bundle["FRAUDWATCH_MODEL_DIGEST"] == digest
    assert changed_bundle["FRAUDWATCH_MODEL_BUNDLE_DIGEST"] != values[
        "FRAUDWATCH_MODEL_BUNDLE_DIGEST"
    ]
    assert changed_bundle["FRAUDWATCH_SCORING_TAG"] != values[
        "FRAUDWATCH_SCORING_TAG"
    ]
