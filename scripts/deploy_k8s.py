#!/usr/bin/env python3
"""Deploy FraudWatch to an existing Kubernetes cluster in a safe order."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
from pathlib import Path

try:
    from scripts.image_identity import ROOT
    from scripts.render_k8s import render
except ModuleNotFoundError:  # Direct execution puts scripts/ on sys.path.
    from image_identity import ROOT
    from render_k8s import render


NAMESPACE = "fraudwatch"
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
OBSERVABILITY_ROOT = ROOT / "infra/observability"
KUBERNETES_DURATION = re.compile(r"^(?P<value>[1-9][0-9]*)(?P<unit>[sm])$")


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
    if "default_transaction_read_only" not in values["DATABASE_URL_READ_ONLY"]:
        raise RuntimeError(
            f"{path}: DATABASE_URL_READ_ONLY must enable default_transaction_read_only"
        )
    return values


def kubectl(context: str, *args: str, executable: str = "kubectl") -> list[str]:
    return command(executable, "--context", context, *args)


def apply_text(
    context: str, manifest: str, *, executable: str = "kubectl"
) -> None:
    run(kubectl(context, "apply", "-f", "-", executable=executable), input_text=manifest)


def apply_component(
    context: str,
    overlay: Path,
    component: str,
    *,
    executable: str = "kubectl",
) -> None:
    apply_text(
        context,
        render(overlay / component, kubectl=executable),
        executable=executable,
    )


def apply_path(
    context: str,
    path: Path,
    *,
    executable: str = "kubectl",
) -> None:
    apply_text(context, render(path, kubectl=executable), executable=executable)


def apply_secret(
    context: str,
    env_file: Path,
    *,
    executable: str = "kubectl",
) -> None:
    load_env_keys(env_file)
    secret = run(
        kubectl(
            context,
            "create",
            "secret",
            "generic",
            "fraudwatch-secrets",
            "--namespace",
            NAMESPACE,
            f"--from-env-file={env_file}",
            "--dry-run=client",
            "-o",
            "yaml",
            executable=executable,
        ),
        capture_output=True,
    ).stdout
    apply_text(context, secret, executable=executable)


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
    apply_component(context, overlay, component, executable=executable)
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


def deploy(
    context: str,
    overlay: Path,
    secret_env: Path,
    timeout: str,
    *,
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
    apply_secret(context, secret_env, executable=executable)
    apply_component(context, overlay, "platform", executable=executable)
    wait_rollout(context, "statefulset/postgres", timeout, executable=executable)
    wait_rollout(context, "statefulset/kafka", timeout, executable=executable)
    run_job(
        context,
        overlay,
        "migrate",
        "fraudwatch-migrate",
        timeout,
        executable=executable,
    )
    run_job(
        context,
        overlay,
        "topics",
        "fraudwatch-topics",
        timeout,
        executable=executable,
    )
    apply_component(context, overlay, "apps", executable=executable)
    for deployment in DEPLOYMENTS:
        wait_rollout(
            context, f"deployment/{deployment}", timeout, executable=executable
        )
    apply_path(context, OBSERVABILITY_ROOT, executable=executable)
    for deployment in OBSERVABILITY_DEPLOYMENTS:
        wait_rollout(
            context, f"deployment/{deployment}", timeout, executable=executable
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", required=True)
    parser.add_argument("--overlay", type=Path, required=True)
    parser.add_argument("--secret-env", type=Path, required=True)
    parser.add_argument("--timeout", default="300s")
    args = parser.parse_args()
    try:
        deploy(
            args.context,
            args.overlay,
            args.secret_env,
            args.timeout,
        )
    except (RuntimeError, subprocess.CalledProcessError) as error:
        print(f"Kubernetes deployment failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
