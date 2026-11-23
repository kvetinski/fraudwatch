import json
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
OBSERVABILITY = ROOT / "infra/observability"


def load_yaml(path: Path):
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def test_kubernetes_prometheus_discovers_every_application_pod():
    config = load_yaml(OBSERVABILITY / "k8s/config/prometheus.yml")
    jobs = {job["job_name"]: job for job in config["scrape_configs"]}

    application = jobs["fraudwatch-pods"]
    assert application["kubernetes_sd_configs"] == [
        {"role": "pod", "namespaces": {"names": ["fraudwatch"]}}
    ]
    replacements = {
        rule.get("target_label"): rule.get("replacement")
        for rule in application["relabel_configs"]
    }
    assert replacements["job"] == "fraudwatch-$1"
    assert jobs["kube-state-metrics"]["static_configs"] == [
        {"targets": ["kube-state-metrics:8080"]}
    ]


def test_kubernetes_workload_alerts_are_in_shared_rule_file():
    rules = load_yaml(OBSERVABILITY / "rules/fraudwatch-alerts.yml")
    names = {
        rule["alert"]
        for group in rules["groups"]
        for rule in group["rules"]
        if "alert" in rule
    }
    assert "FraudWatchKubernetesDeploymentUnavailable" in names
    assert "FraudWatchKubernetesCrashLoop" in names


def test_all_provisioned_grafana_dashboards_are_valid_json():
    kustomization = load_yaml(OBSERVABILITY / "kustomization.yaml")
    dashboard_generator = next(
        generator
        for generator in kustomization["configMapGenerator"]
        if generator["name"] == "grafana-dashboards"
    )
    for relative_path in dashboard_generator["files"]:
        dashboard = json.loads(
            (OBSERVABILITY / relative_path).read_text(encoding="utf-8")
        )
        assert dashboard["uid"].startswith("fraudwatch-")
        assert dashboard["panels"]


def test_observability_workload_images_are_digest_pinned():
    for path in sorted((OBSERVABILITY / "k8s").glob("*.yaml")):
        for document in yaml.safe_load_all(path.read_text(encoding="utf-8")):
            if not document or document.get("kind") != "Deployment":
                continue
            pod_spec = document["spec"]["template"]["spec"]
            for container in pod_spec["containers"]:
                assert "@sha256:" in container["image"]
                security = container["securityContext"]
                assert security["allowPrivilegeEscalation"] is False
                assert security["readOnlyRootFilesystem"] is True
                assert security["capabilities"]["drop"] == ["ALL"]


def test_kube_state_metrics_probes_use_supported_telemetry_endpoint():
    documents = list(
        yaml.safe_load_all(
            (OBSERVABILITY / "k8s/kube-state-metrics.yaml").read_text(
                encoding="utf-8"
            )
        )
    )
    deployment = next(item for item in documents if item["kind"] == "Deployment")
    container = deployment["spec"]["template"]["spec"]["containers"][0]
    for probe_name in ("livenessProbe", "readinessProbe"):
        assert container[probe_name]["httpGet"] == {
            "path": "/metrics",
            "port": "telemetry",
        }
