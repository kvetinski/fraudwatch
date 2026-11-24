import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

from scripts.deploy_k8s import (
    SECRET_CONSUMERS,
    bind_secret_name,
    fraudwatch_secret_references,
)
from scripts.render_k8s import replace_image_tags


ROOT = Path(__file__).resolve().parents[1]
HAS_KUBECTL = shutil.which("kubectl") is not None


def kustomize(path: str) -> list[dict[str, object]]:
    manifest = subprocess.check_output(
        ["kubectl", "kustomize", str(ROOT / path)],
        text=True,
    )
    return [document for document in yaml.safe_load_all(manifest) if document]


def config_map_references(pod_spec: dict[str, object]) -> set[str]:
    references: set[str] = set()
    for container in pod_spec.get("containers", []):
        for source in container.get("envFrom", []):
            reference = source.get("configMapRef", {})
            if reference.get("name"):
                references.add(reference["name"])
    return references


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


@pytest.mark.skipif(not HAS_KUBECTL, reason="kubectl is required for Kustomize")
def test_application_config_is_content_addressed_in_local_and_ci_overlays():
    local = kustomize("infra/k8s/overlays/local/apps")
    ci = kustomize("infra/k8s/overlays/ci/apps")

    local_config = next(item for item in local if item["kind"] == "ConfigMap")
    ci_config = next(item for item in ci if item["kind"] == "ConfigMap")
    local_name = local_config["metadata"]["name"]
    ci_name = ci_config["metadata"]["name"]

    assert local_name.startswith("fraudwatch-config-")
    assert ci_name.startswith("fraudwatch-config-")
    assert local_name != ci_name
    assert local_config["data"]["FRAUDWATCH_MONITOR_INTERVAL_SECONDS"] == "15"
    assert ci_config["data"]["FRAUDWATCH_MONITOR_INTERVAL_SECONDS"] == "5"

    config_consumers = {"ingestion", "scoring", "labels", "monitor"}
    for documents, expected_name in ((local, local_name), (ci, ci_name)):
        deployments = {
            item["metadata"]["name"]: item
            for item in documents
            if item["kind"] == "Deployment"
        }
        for deployment_name in config_consumers:
            pod_spec = deployments[deployment_name]["spec"]["template"]["spec"]
            assert config_map_references(pod_spec) == {expected_name}


@pytest.mark.skipif(not HAS_KUBECTL, reason="kubectl is required for Kustomize")
def test_every_secret_consumer_binds_to_content_addressed_name():
    paths = (
        "infra/k8s/overlays/local/platform",
        "infra/k8s/overlays/local/apps",
        "infra/observability",
    )
    manifest = "\n---\n".join(
        subprocess.check_output(
            ["kubectl", "kustomize", str(ROOT / path)],
            text=True,
        )
        for path in paths
    )
    expected_name = "fraudwatch-secrets-0123456789abcdef01234567"
    bound = bind_secret_name(manifest, expected_name)
    workloads = {
        (item["kind"], item["metadata"]["name"]): item
        for item in yaml.safe_load_all(bound)
        if item and item["kind"] in {"Deployment", "StatefulSet"}
    }
    actual_consumers = {
        identity
        for identity, workload in workloads.items()
        if fraudwatch_secret_references(workload["spec"]["template"]["spec"])
    }

    assert actual_consumers == set(SECRET_CONSUMERS)
    for identity in SECRET_CONSUMERS:
        workload = workloads[identity]
        pod_spec = workload["spec"]["template"]["spec"]
        assert fraudwatch_secret_references(pod_spec) == {expected_name}
