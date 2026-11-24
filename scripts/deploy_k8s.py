#!/usr/bin/env python3
"""Deploy FraudWatch to an existing Kubernetes cluster in a safe order."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
import subprocess
import sys
import time
from pathlib import Path
from urllib.parse import parse_qsl, unquote, urlsplit

try:
    from scripts.image_identity import ROOT
    from scripts.render_k8s import render
except ModuleNotFoundError:  # Direct execution puts scripts/ on sys.path.
    from image_identity import ROOT
    from render_k8s import render


NAMESPACE = "fraudwatch"
SECRET_BASENAME = "fraudwatch-secrets"
SECRET_DIGEST_LENGTH = 24
REQUIRED_SECRET_KEYS = {
    "POSTGRES_DB",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "DATABASE_URL",
    "DATABASE_URL_READ_ONLY",
    "GRAFANA_ADMIN_PASSWORD",
}
DEPLOYMENTS = ("ingestion", "scoring", "labels", "model-monitor", "monitor")
OBSERVABILITY_DEPLOYMENTS = (
    "prometheus",
    "alertmanager",
    "loki",
    "alloy",
    "grafana",
    "kube-state-metrics",
)
SECRET_CONSUMERS = (
    ("StatefulSet", "postgres"),
    ("Deployment", "ingestion"),
    ("Deployment", "scoring"),
    ("Deployment", "labels"),
    ("Deployment", "model-monitor"),
    ("Deployment", "monitor"),
    ("Deployment", "grafana"),
)
OBSERVABILITY_ROOT = ROOT / "infra/observability"
KUBERNETES_DURATION = re.compile(r"^(?P<value>[1-9][0-9]*)(?P<unit>[sm])$")
KUBERNETES_NAME = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
SECRET_PLACEHOLDER = re.compile(
    rf"(?<![-a-z0-9]){re.escape(SECRET_BASENAME)}(?![-a-z0-9])"
)


def command(*args: str) -> list[str]:
    return [str(arg) for arg in args]


def run(
    args: list[str],
    *,
    input_text: str | None = None,
    capture_output: bool = False,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        input=input_text,
        text=True,
        check=True,
        capture_output=capture_output,
    )


def assert_context(context: str, *, kubectl: str = "kubectl") -> None:
    current = run(
        command(kubectl, "config", "current-context"), capture_output=True
    ).stdout.strip()
    if current != context:
        raise RuntimeError(
            f"Refusing Kubernetes mutation: current context is {current!r}; "
            f"expected {context!r}"
        )


def load_env_keys(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for line_number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if "=" not in line:
            raise RuntimeError(f"{path}:{line_number}: expected KEY=VALUE")
        key, value = line.split("=", 1)
        key = key.strip()
        if key in values:
            raise RuntimeError(f"{path}:{line_number}: duplicate key {key}")
        values[key] = value.strip()
    missing = sorted(REQUIRED_SECRET_KEYS - values.keys())
    if missing:
        raise RuntimeError(f"{path}: missing required keys: {', '.join(missing)}")
    empty = sorted(key for key in REQUIRED_SECRET_KEYS if not values[key])
    if empty:
        raise RuntimeError(f"{path}: empty required keys: {', '.join(empty)}")
    if any("replace-with" in values[key] for key in REQUIRED_SECRET_KEYS):
        raise RuntimeError(f"{path}: replace all example placeholder values")
    validate_database_urls(path, values)
    return values


def parsed_database_url(path: Path, key: str, value: str):
    parsed = urlsplit(value)
    if parsed.scheme not in {"postgres", "postgresql"}:
        raise RuntimeError(f"{path}: {key} must use postgres:// or postgresql://")
    if parsed.fragment:
        raise RuntimeError(f"{path}: {key} must not contain a URL fragment")
    try:
        port = parsed.port
    except ValueError as error:
        raise RuntimeError(f"{path}: {key} has an invalid port") from error
    if not parsed.hostname or port is None:
        raise RuntimeError(f"{path}: {key} must include a host and port")
    return parsed


def database_url_identity(parsed) -> tuple[str, str, str, str, int, str]:
    return (
        parsed.scheme,
        unquote(parsed.username or ""),
        unquote(parsed.password or ""),
        parsed.hostname or "",
        parsed.port or 0,
        unquote(parsed.path.removeprefix("/")),
    )


def validate_database_urls(path: Path, values: dict[str, str]) -> None:
    write_url = parsed_database_url(path, "DATABASE_URL", values["DATABASE_URL"])
    read_url = parsed_database_url(
        path,
        "DATABASE_URL_READ_ONLY",
        values["DATABASE_URL_READ_ONLY"],
    )
    expected_identity = (
        write_url.scheme,
        values["POSTGRES_USER"],
        values["POSTGRES_PASSWORD"],
        "postgres",
        5432,
        values["POSTGRES_DB"],
    )
    if database_url_identity(write_url) != expected_identity:
        raise RuntimeError(
            f"{path}: DATABASE_URL must match POSTGRES_USER, POSTGRES_PASSWORD, "
            "POSTGRES_DB, and postgres:5432"
        )
    if database_url_identity(read_url) != expected_identity:
        raise RuntimeError(
            f"{path}: DATABASE_URL_READ_ONLY must use the same PostgreSQL identity"
        )

    write_query = parse_qsl(write_url.query, keep_blank_values=True)
    read_query = parse_qsl(read_url.query, keep_blank_values=True)
    read_only_options = [
        value for key, value in read_query if key == "options"
    ]
    if not any(
        "default_transaction_read_only=on" in option
        for option in read_only_options
    ):
        raise RuntimeError(
            f"{path}: DATABASE_URL_READ_ONLY must enable default_transaction_read_only"
        )
    if sorted(write_query) != sorted(
        (key, value) for key, value in read_query if key != "options"
    ):
        raise RuntimeError(
            f"{path}: DATABASE_URL_READ_ONLY must preserve DATABASE_URL query options"
        )


def secret_content_sha256(values: dict[str, str]) -> str:
    canonical = json.dumps(
        values,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def content_addressed_secret_name(values: dict[str, str]) -> str:
    digest = secret_content_sha256(values)
    return f"{SECRET_BASENAME}-{digest[:SECRET_DIGEST_LENGTH]}"


def secret_manifest(name: str, values: dict[str, str]) -> str:
    document = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": {
            "name": name,
            "namespace": NAMESPACE,
            "labels": {
                "app.kubernetes.io/name": SECRET_BASENAME,
                "app.kubernetes.io/part-of": "fraudwatch",
                "app.kubernetes.io/managed-by": "fraudwatch-deploy",
            },
        },
        "immutable": True,
        "type": "Opaque",
        "data": {
            key: base64.b64encode(value.encode("utf-8")).decode("ascii")
            for key, value in sorted(values.items())
        },
    }
    return json.dumps(document, separators=(",", ":"))


def secret_references(pod_spec: dict[str, object]) -> set[str]:
    names: set[str] = set()
    containers = list(pod_spec.get("initContainers", [])) + list(
        pod_spec.get("containers", [])
    )
    for container in containers:
        for variable in container.get("env", []):
            reference = variable.get("valueFrom", {}).get("secretKeyRef", {})
            if reference.get("name"):
                names.add(reference["name"])
        for source in container.get("envFrom", []):
            reference = source.get("secretRef", {})
            if reference.get("name"):
                names.add(reference["name"])
    for volume in pod_spec.get("volumes", []):
        reference = volume.get("secret", {})
        if reference.get("secretName"):
            names.add(reference["secretName"])
    return names


def fraudwatch_secret_references(pod_spec: dict[str, object]) -> set[str]:
    return {
        name
        for name in secret_references(pod_spec)
        if name == SECRET_BASENAME or name.startswith(f"{SECRET_BASENAME}-")
    }


def kubectl(context: str, *args: str, executable: str = "kubectl") -> list[str]:
    return command(executable, "--context", context, *args)


def apply_text(
    context: str, manifest: str, *, executable: str = "kubectl"
) -> None:
    run(kubectl(context, "apply", "-f", "-", executable=executable), input_text=manifest)


def bind_secret_name(manifest: str, name: str) -> str:
    if not KUBERNETES_NAME.fullmatch(name):
        raise RuntimeError(f"Invalid Kubernetes Secret name: {name!r}")
    return SECRET_PLACEHOLDER.sub(name, manifest)


def apply_component(
    context: str,
    overlay: Path,
    component: str,
    *,
    secret_name: str | None = None,
    executable: str = "kubectl",
) -> None:
    manifest = render(overlay / component, kubectl=executable)
    if secret_name is not None:
        manifest = bind_secret_name(manifest, secret_name)
    apply_text(
        context,
        manifest,
        executable=executable,
    )


def apply_path(
    context: str,
    path: Path,
    *,
    secret_name: str | None = None,
    executable: str = "kubectl",
) -> None:
    manifest = render(path, kubectl=executable)
    if secret_name is not None:
        manifest = bind_secret_name(manifest, secret_name)
    apply_text(context, manifest, executable=executable)


def get_json(
    context: str,
    *args: str,
    executable: str = "kubectl",
) -> dict[str, object] | None:
    output = run(
        kubectl(context, *args, "-o", "json", executable=executable),
        capture_output=True,
    ).stdout.strip()
    if not output:
        return None
    return json.loads(output)


def decode_secret_values(document: dict[str, object]) -> dict[str, str]:
    encoded_values = document.get("data", {})
    values: dict[str, str] = {}
    for key, encoded in encoded_values.items():
        try:
            values[key] = base64.b64decode(encoded, validate=True).decode("utf-8")
        except (ValueError, UnicodeDecodeError) as error:
            raise RuntimeError(
                f"Kubernetes Secret {document.get('metadata', {}).get('name', '<unknown>')} "
                f"contains invalid data for {key}"
            ) from error
    return values


def get_secret_values(
    context: str,
    name: str,
    *,
    executable: str = "kubectl",
) -> dict[str, str] | None:
    document = get_json(
        context,
        "get",
        "secret",
        name,
        "--namespace",
        NAMESPACE,
        "--ignore-not-found",
        executable=executable,
    )
    if document is None:
        return None
    return decode_secret_values(document)


def active_secret_names(
    context: str,
    *,
    executable: str = "kubectl",
) -> set[str]:
    document = get_json(
        context,
        "get",
        "deployments,statefulsets",
        "--namespace",
        NAMESPACE,
        executable=executable,
    )
    names: set[str] = set()
    if document is not None:
        for workload in document.get("items", []):
            pod_spec = workload.get("spec", {}).get("template", {}).get("spec", {})
            names.update(fraudwatch_secret_references(pod_spec))
    if not names and get_secret_values(
        context, SECRET_BASENAME, executable=executable
    ) is not None:
        names.add(SECRET_BASENAME)
    return names


def validate_secret_transition(
    active_values: list[dict[str, str]],
    desired_values: dict[str, str],
    *,
    allow_change: bool,
) -> None:
    if not active_values or all(values == desired_values for values in active_values):
        return
    if allow_change:
        return
    raise RuntimeError(
        "Refusing to change active Kubernetes Secret values. PostgreSQL and Grafana "
        "store credentials in persistent data; rotate those credentials first, then "
        "use the explicitly confirmed Secret rollout command."
    )


def apply_secret(
    context: str,
    env_file: Path,
    *,
    allow_change: bool = False,
    executable: str = "kubectl",
) -> str:
    desired_values = load_env_keys(env_file)
    name = content_addressed_secret_name(desired_values)
    existing_values = get_secret_values(context, name, executable=executable)
    if existing_values is not None and existing_values != desired_values:
        raise RuntimeError(
            f"Refusing to overwrite content-addressed Kubernetes Secret {name}: "
            "its stored content does not match its identity"
        )

    active_names = active_secret_names(context, executable=executable)
    active_values: list[dict[str, str]] = []
    for active_name in sorted(active_names):
        values = get_secret_values(context, active_name, executable=executable)
        if values is None:
            raise RuntimeError(
                f"Active Kubernetes Secret {active_name} does not exist"
            )
        active_values.append(values)
    validate_secret_transition(
        active_values,
        desired_values,
        allow_change=allow_change,
    )

    if existing_values is None:
        run(
            kubectl(context, "create", "-f", "-", executable=executable),
            input_text=secret_manifest(name, desired_values),
        )
    return name


def wait_rollout(
    context: str,
    resource: str,
    timeout: str,
    *,
    executable: str = "kubectl",
) -> None:
    run(
        kubectl(
            context,
            "rollout",
            "status",
            "--namespace",
            NAMESPACE,
            resource,
            f"--timeout={timeout}",
            executable=executable,
        )
    )


def timeout_seconds(value: str) -> int:
    match = KUBERNETES_DURATION.fullmatch(value)
    if match is None:
        raise RuntimeError(f"Unsupported Kubernetes timeout: {value!r}")
    seconds = int(match.group("value"))
    if match.group("unit") == "m":
        seconds *= 60
    return seconds


def wait_job(
    context: str,
    job: str,
    timeout: str,
    *,
    executable: str = "kubectl",
) -> None:
    deadline = time.monotonic() + timeout_seconds(timeout)
    while time.monotonic() < deadline:
        document = json.loads(
            run(
                kubectl(
                    context,
                    "get",
                    "job",
                    job,
                    "--namespace",
                    NAMESPACE,
                    "-o",
                    "json",
                    executable=executable,
                ),
                capture_output=True,
            ).stdout
        )
        status = document.get("status", {})
        if status.get("succeeded", 0) >= 1:
            return
        failed = any(
            condition.get("type") == "Failed"
            and condition.get("status") == "True"
            for condition in status.get("conditions", [])
        )
        if failed:
            reason = next(
                (
                    condition.get("reason", "unknown")
                    for condition in status.get("conditions", [])
                    if condition.get("type") == "Failed"
                    and condition.get("status") == "True"
                ),
                "unknown",
            )
            raise RuntimeError(f"Kubernetes Job {job} failed: {reason}")
        time.sleep(1)
    raise RuntimeError(f"Kubernetes Job {job} timed out after {timeout}")


def run_job(
    context: str,
    overlay: Path,
    component: str,
    job: str,
    timeout: str,
    *,
    secret_name: str | None = None,
    executable: str = "kubectl",
) -> None:
    run(
        kubectl(
            context,
            "delete",
            "job",
            job,
            "--namespace",
            NAMESPACE,
            "--ignore-not-found",
            "--wait=true",
            executable=executable,
        )
    )
    apply_component(
        context,
        overlay,
        component,
        secret_name=secret_name,
        executable=executable,
    )
    try:
        wait_job(
            context,
            job,
            timeout,
            executable=executable,
        )
    except (RuntimeError, subprocess.CalledProcessError):
        subprocess.run(
            kubectl(
                context,
                "logs",
                "--namespace",
                NAMESPACE,
                "--selector",
                f"job-name={job}",
                "--all-containers=true",
                "--prefix=true",
                executable=executable,
            ),
            text=True,
            check=False,
        )
        raise


def labels_match(labels: dict[str, str], selector: dict[str, str]) -> bool:
    return all(labels.get(key) == value for key, value in selector.items())


def pod_is_ready(pod: dict[str, object]) -> bool:
    return any(
        condition.get("type") == "Ready" and condition.get("status") == "True"
        for condition in pod.get("status", {}).get("conditions", [])
    )


def secret_rollout_identity_errors(
    workloads: list[dict[str, object]],
    pods: list[dict[str, object]],
    expected_secret_name: str,
) -> list[str]:
    errors: list[str] = []
    workload_index = {
        (workload.get("kind"), workload.get("metadata", {}).get("name")): workload
        for workload in workloads
    }
    for kind, name in SECRET_CONSUMERS:
        workload = workload_index.get((kind, name))
        identity = f"{kind.lower()}/{name}"
        if workload is None:
            errors.append(f"missing {identity}")
            continue

        template = workload.get("spec", {}).get("template", {})
        template_refs = fraudwatch_secret_references(template.get("spec", {}))
        if template_refs != {expected_secret_name}:
            errors.append(
                f"{identity} template references {sorted(template_refs)!r}"
            )

        selector = workload.get("spec", {}).get("selector", {}).get("matchLabels", {})
        desired_replicas = workload.get("spec", {}).get("replicas", 1)
        active_pods = [
            pod
            for pod in pods
            if pod.get("metadata", {}).get("deletionTimestamp") is None
            and labels_match(pod.get("metadata", {}).get("labels", {}), selector)
        ]
        if len(active_pods) != desired_replicas:
            errors.append(
                f"{identity} has {len(active_pods)} active pods; "
                f"expected {desired_replicas}"
            )
        for pod in active_pods:
            pod_name = pod.get("metadata", {}).get("name", "<unknown>")
            pod_refs = fraudwatch_secret_references(pod.get("spec", {}))
            if pod_refs != {expected_secret_name}:
                errors.append(f"pod/{pod_name} references {sorted(pod_refs)!r}")
            if not pod_is_ready(pod):
                errors.append(f"pod/{pod_name} is not Ready")
    return errors


def verify_secret_rollout(
    context: str,
    expected_secret_name: str,
    *,
    executable: str = "kubectl",
) -> None:
    workloads = get_json(
        context,
        "get",
        "deployments,statefulsets",
        "--namespace",
        NAMESPACE,
        executable=executable,
    )
    pods = get_json(
        context,
        "get",
        "pods",
        "--namespace",
        NAMESPACE,
        executable=executable,
    )
    errors = secret_rollout_identity_errors(
        [] if workloads is None else workloads.get("items", []),
        [] if pods is None else pods.get("items", []),
        expected_secret_name,
    )
    if errors:
        raise RuntimeError(
            "Kubernetes Secret rollout identity verification failed: "
            + "; ".join(errors)
        )


def deploy(
    context: str,
    overlay: Path,
    secret_env: Path,
    timeout: str,
    *,
    allow_secret_change: bool = False,
    executable: str = "kubectl",
) -> None:
    assert_context(context, kubectl=executable)
    run(
        kubectl(
            context,
            "apply",
            "-f",
            str(ROOT / "infra/k8s/base/platform/namespace.yaml"),
            executable=executable,
        )
    )
    secret_name = apply_secret(
        context,
        secret_env,
        allow_change=allow_secret_change,
        executable=executable,
    )
    apply_component(
        context,
        overlay,
        "platform",
        secret_name=secret_name,
        executable=executable,
    )
    wait_rollout(context, "statefulset/postgres", timeout, executable=executable)
    wait_rollout(context, "statefulset/kafka", timeout, executable=executable)
    run_job(
        context,
        overlay,
        "migrate",
        "fraudwatch-migrate",
        timeout,
        secret_name=secret_name,
        executable=executable,
    )
    run_job(
        context,
        overlay,
        "topics",
        "fraudwatch-topics",
        timeout,
        secret_name=secret_name,
        executable=executable,
    )
    apply_component(
        context,
        overlay,
        "apps",
        secret_name=secret_name,
        executable=executable,
    )
    for deployment in DEPLOYMENTS:
        wait_rollout(
            context, f"deployment/{deployment}", timeout, executable=executable
        )
    apply_path(
        context,
        OBSERVABILITY_ROOT,
        secret_name=secret_name,
        executable=executable,
    )
    for deployment in OBSERVABILITY_DEPLOYMENTS:
        wait_rollout(
            context, f"deployment/{deployment}", timeout, executable=executable
        )
    verify_secret_rollout(context, secret_name, executable=executable)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", required=True)
    parser.add_argument("--overlay", type=Path, required=True)
    parser.add_argument("--secret-env", type=Path, required=True)
    parser.add_argument("--timeout", default="300s")
    parser.add_argument(
        "--allow-secret-change",
        action="store_true",
        help=(
            "allow an explicitly coordinated change to active Secret values; "
            "this does not rotate credentials stored by PostgreSQL or Grafana"
        ),
    )
    args = parser.parse_args()
    try:
        deploy(
            args.context,
            args.overlay,
            args.secret_env,
            args.timeout,
            allow_secret_change=args.allow_secret_change,
        )
    except (RuntimeError, subprocess.CalledProcessError) as error:
        print(f"Kubernetes deployment failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
