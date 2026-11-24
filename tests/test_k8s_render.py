from pathlib import Path

import pytest
import yaml

from scripts.render_k8s import replace_image_tags


ROOT = Path(__file__).resolve().parents[1]


def test_render_replaces_source_and_model_image_identities():
    manifest = """
image: fraudwatch/ingestion:development
image: fraudwatch/scoring:development
image: fraudwatch/model-monitor:development
"""
    rendered = replace_image_tags(
        manifest,
        {
            "FRAUDWATCH_APP_TAG": "abc123",
            "FRAUDWATCH_SCORING_TAG": "abc123-model-def456-bundle-789abc",
        },
    )
    assert "fraudwatch/ingestion:abc123" in rendered
    assert rendered.count(
        "abc123-model-def456-bundle-789abc"
    ) == 2
    assert ":development" not in rendered


def test_render_fails_if_model_identity_was_not_loaded():
    with pytest.raises(RuntimeError, match="FRAUDWATCH_SCORING_TAG"):
        replace_image_tags(
            "image: fraudwatch/scoring:development\n",
            {"FRAUDWATCH_APP_TAG": "abc123"},
        )


def test_ingestion_rollout_keeps_one_ordered_outbox_publisher():
    documents = yaml.safe_load_all(
        (ROOT / "infra/k8s/base/apps/ingestion.yaml").read_text(
            encoding="utf-8"
        )
    )
    deployment = next(
        item for item in documents if item["kind"] == "Deployment"
    )

    assert deployment["spec"]["replicas"] == 1
    assert deployment["spec"]["strategy"] == {"type": "Recreate"}
