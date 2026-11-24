import json
from pathlib import Path
from urllib.parse import quote

import pytest

from scripts import deploy_k8s
from scripts.deploy_k8s import (
    SECRET_CONSUMERS,
    content_addressed_secret_name,
    load_env_keys,
    secret_manifest,
    secret_rollout_identity_errors,
    timeout_seconds,
    validate_secret_transition,
)
from scripts.init_k8s_secrets import ensure, initialize


def test_generated_secret_file_is_private_and_valid(tmp_path: Path):
    path = tmp_path / "fraudwatch.env"
    initialize(path)

    values = load_env_keys(path)

    assert path.stat().st_mode & 0o777 == 0o600
    assert values["POSTGRES_PASSWORD"] in values["DATABASE_URL"]
    assert "default_transaction_read_only%3Don" in values["DATABASE_URL_READ_ONLY"]
    assert len(values["GRAFANA_ADMIN_PASSWORD"]) >= 24


def test_secret_initializer_refuses_to_overwrite(tmp_path: Path):
    path = tmp_path / "fraudwatch.env"
    initialize(path)

    with pytest.raises(FileExistsError):
        initialize(path)


def test_secret_ensure_upgrades_without_rotating_database_password(tmp_path: Path):
    path = tmp_path / "fraudwatch.env"
    initialize(path)
    original = path.read_text(encoding="utf-8")
    without_grafana = "\n".join(
        line
        for line in original.splitlines()
        if not line.startswith("GRAFANA_ADMIN_PASSWORD=")
    ) + "\n"
    path.write_text(without_grafana, encoding="utf-8")

    assert ensure(path) is True
    upgraded = load_env_keys(path)
    assert upgraded["POSTGRES_PASSWORD"] in original
    assert len(upgraded["GRAFANA_ADMIN_PASSWORD"]) >= 24
    assert path.stat().st_mode & 0o777 == 0o600
    assert ensure(path) is False


def test_secret_validation_rejects_example_placeholders(tmp_path: Path):
    path = tmp_path / "fraudwatch.env"
    path.write_text(
        "\n".join(
            (
                "POSTGRES_DB=fraudwatch",
                "POSTGRES_USER=fraudwatch",
                "POSTGRES_PASSWORD=replace-with-a-local-password",
                "DATABASE_URL=postgres://example",
                "DATABASE_URL_READ_ONLY=postgres://example?default_transaction_read_only=on",
                "GRAFANA_ADMIN_PASSWORD=replace-with-a-local-password",
            )
        ),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="placeholder"):
        load_env_keys(path)


def test_secret_validation_rejects_database_url_credential_drift(tmp_path: Path):
    path = tmp_path / "fraudwatch.env"
    initialize(path)
    contents = path.read_text(encoding="utf-8")
    path.write_text(
        contents.replace(
            "POSTGRES_PASSWORD=",
            "POSTGRES_PASSWORD=changed-",
            1,
        ),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="DATABASE_URL must match"):
        load_env_keys(path)


def test_secret_validation_accepts_percent_encoded_database_password(tmp_path: Path):
    path = tmp_path / "fraudwatch.env"
    password = "p@ss word:/%"
    encoded = quote(password, safe="")
    database_url = (
        f"postgres://fraudwatch:{encoded}"
        "@postgres:5432/fraudwatch?sslmode=disable"
    )
    path.write_text(
        "\n".join(
            (
                "POSTGRES_DB=fraudwatch",
                "POSTGRES_USER=fraudwatch",
                f"POSTGRES_PASSWORD={password}",
                f"DATABASE_URL={database_url}",
                f"DATABASE_URL_READ_ONLY={database_url}"
                "&options=-cdefault_transaction_read_only%3Don",
                "GRAFANA_ADMIN_PASSWORD=a-long-random-grafana-password",
                "",
            )
        ),
        encoding="utf-8",
    )

    assert load_env_keys(path)["POSTGRES_PASSWORD"] == password


def test_secret_identity_is_canonical_and_content_addressed():
    values = {"B": "second", "A": "first"}

    first = content_addressed_secret_name(values)
    reordered = content_addressed_secret_name({"A": "first", "B": "second"})
    changed = content_addressed_secret_name({"A": "changed", "B": "second"})

    assert first == reordered
    assert first != changed
    assert first.startswith("fraudwatch-secrets-")

    document = json.loads(secret_manifest(first, values))
    assert document["metadata"]["name"] == first
    assert document["immutable"] is True
    assert document["data"] == {"A": "Zmlyc3Q=", "B": "c2Vjb25k"}


def test_secret_change_requires_explicit_approval():
    active = [{"password": "old"}]
    desired = {"password": "new"}

    with pytest.raises(RuntimeError, match="Refusing to change"):
        validate_secret_transition(active, desired, allow_change=False)

    validate_secret_transition(active, desired, allow_change=True)
    validate_secret_transition([desired], desired, allow_change=False)
    validate_secret_transition([], desired, allow_change=False)


def test_apply_secret_creates_immutable_revision_without_argv_secrets(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    path = tmp_path / "fraudwatch.env"
    initialize(path)
    values = load_env_keys(path)
    expected_name = content_addressed_secret_name(values)
    calls: list[tuple[list[str], str | None]] = []

    monkeypatch.setattr(
        deploy_k8s,
        "active_secret_names",
        lambda context, executable="kubectl": {"fraudwatch-secrets"},
    )

    def fake_get_secret_values(context, name, executable="kubectl"):
        if name == expected_name:
            return None
        if name == "fraudwatch-secrets":
            return values
        raise AssertionError(name)

    def fake_run(args, *, input_text=None, capture_output=False):
        calls.append((args, input_text))
        return None

    monkeypatch.setattr(deploy_k8s, "get_secret_values", fake_get_secret_values)
    monkeypatch.setattr(deploy_k8s, "run", fake_run)

    assert deploy_k8s.apply_secret("test-context", path) == expected_name
    assert len(calls) == 1
    command, manifest = calls[0]
    assert command[-3:] == ["create", "-f", "-"]
    assert values["POSTGRES_PASSWORD"] not in " ".join(command)
    assert values["POSTGRES_PASSWORD"] not in manifest
    assert json.loads(manifest)["metadata"]["name"] == expected_name


def workload_and_pod(kind: str, name: str, secret_name: str):
    labels = {"app.kubernetes.io/name": name}
    pod_spec = {
        "containers": [
            {
                "name": name,
                "env": [
                    {
                        "name": "CREDENTIAL",
                        "valueFrom": {
                            "secretKeyRef": {
                                "name": secret_name,
                                "key": "credential",
                            }
                        },
                    }
                ],
            }
        ]
    }
    workload = {
        "kind": kind,
        "metadata": {"name": name},
        "spec": {
            "replicas": 1,
            "selector": {"matchLabels": labels},
            "template": {"spec": pod_spec},
        },
    }
    pod = {
        "metadata": {"name": f"{name}-pod", "labels": labels},
        "spec": pod_spec,
        "status": {"conditions": [{"type": "Ready", "status": "True"}]},
    }
    return workload, pod


def test_secret_rollout_identity_requires_ready_pods_on_expected_revision():
    expected = "fraudwatch-secrets-0123456789abcdef01234567"
    pairs = [
        workload_and_pod(kind, name, expected)
        for kind, name in SECRET_CONSUMERS
    ]
    workloads = [workload for workload, _ in pairs]
    pods = [pod for _, pod in pairs]

    assert secret_rollout_identity_errors(workloads, pods, expected) == []

    pods[0]["spec"]["containers"][0]["env"][0]["valueFrom"][
        "secretKeyRef"
    ]["name"] = "fraudwatch-secrets-stale"
    pods[1]["status"]["conditions"][0]["status"] = "False"
    errors = secret_rollout_identity_errors(workloads, pods, expected)

    assert any("references" in error for error in errors)
    assert any("not Ready" in error for error in errors)


def test_kubernetes_timeout_parser_is_explicit():
    assert timeout_seconds("90s") == 90
    assert timeout_seconds("5m") == 300
    with pytest.raises(RuntimeError, match="Unsupported"):
        timeout_seconds("1h")
