import pytest

from scripts.render_k8s import replace_image_tags


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
