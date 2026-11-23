#!/usr/bin/env python3
"""Exercise the live FraudWatch API-to-Kafka-to-scoring path in Kubernetes."""

from __future__ import annotations

import argparse
import json
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
from typing import Iterator

try:
    from scripts.deploy_k8s import NAMESPACE, assert_context
except ModuleNotFoundError:  # Direct execution puts scripts/ on sys.path.
    from deploy_k8s import NAMESPACE, assert_context


ENDPOINTS = {
    "ingestion": (18080, 8080, "/readyz"),
    "scoring": (19101, 9101, "/readyz"),
    "labels": (19102, 9102, "/readyz"),
    "monitor": (19103, 9103, "/metrics"),
    "prometheus": (19090, 9090, "/-/ready"),
    "alertmanager": (19093, 9093, "/-/ready"),
    "loki": (13100, 3100, "/ready"),
    "alloy": (12345, 12345, "/-/ready"),
    "grafana": (13000, 3000, "/api/health"),
}


def get(url: str, *, timeout: float = 3) -> str:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return response.read().decode()


def wait_http(url: str, process: subprocess.Popen[str], timeout: float) -> str:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stderr = process.stderr.read() if process.stderr else ""
            raise RuntimeError(
                f"port-forward exited with {process.returncode}: {stderr.strip()}"
            )
        try:
            return get(url)
        except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
            last_error = error
            time.sleep(0.25)
    raise RuntimeError(f"timed out waiting for {url}: {last_error}")


@contextmanager
def port_forward(
    context: str,
    service: str,
    local_port: int,
    remote_port: int,
    ready_path: str,
    timeout: float,
) -> Iterator[str]:
    process = subprocess.Popen(
        [
            "kubectl",
            "--context",
            context,
            "--namespace",
            NAMESPACE,
            "port-forward",
            "--address",
            "127.0.0.1",
            f"service/{service}",
            f"{local_port}:{remote_port}",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
    )
    base_url = f"http://127.0.0.1:{local_port}"
    try:
        wait_http(f"{base_url}{ready_path}", process, timeout)
        yield base_url
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def post_transaction(base_url: str, transaction_id: str) -> dict[str, object]:
    payload = json.dumps(
        {
            "transaction_id": transaction_id,
            "customer_id": "k8s-smoke-customer",
            "terminal_id": f"k8s-smoke-terminal-{transaction_id}",
            "amount": 19.95,
            "occurred_at": datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z"),
        }
    ).encode()
    request = urllib.request.Request(
        f"{base_url}/v1/transactions",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        return json.loads(response.read())


def query_database(
    context: str,
    sql: str,
    *,
    database_user: str,
    database_name: str,
) -> str:
    return subprocess.check_output(
        [
            "kubectl",
            "--context",
            context,
            "--namespace",
            NAMESPACE,
            "exec",
            "postgres-0",
            "--",
            "psql",
            "-U",
            database_user,
            "-d",
            database_name,
            "-Atc",
            sql,
        ],
        text=True,
        stderr=subprocess.DEVNULL,
    ).strip()


def wait_for_decision(
    context: str,
    transaction_id: str,
    timeout: float,
    *,
    database_user: str,
    database_name: str,
) -> None:
    escaped = transaction_id.replace("'", "''")
    sql = f"""
        SELECT count(*)
        FROM transactions t
        JOIN outbox_events o USING (transaction_id)
        JOIN fraud_decisions d USING (transaction_id)
        WHERE t.transaction_id = '{escaped}'
          AND o.delivered_at IS NOT NULL
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if query_database(
            context,
            sql,
            database_user=database_user,
            database_name=database_name,
        ) == "1":
            return
        time.sleep(0.5)
    raise RuntimeError(f"timed out waiting for scored transaction {transaction_id}")


def prometheus_value(base_url: str, query: str) -> float | None:
    encoded = urllib.parse.urlencode({"query": query})
    response = json.loads(get(f"{base_url}/api/v1/query?{encoded}"))
    if response.get("status") != "success":
        raise RuntimeError(f"Prometheus query failed: {response}")
    result = response["data"]["result"]
    if not result:
        return None
    return float(result[0]["value"][1])


def wait_for_observability(base_url: str, timeout: float) -> None:
    queries = {
        "healthy application targets": (
            'count(up{job=~"fraudwatch-(ingestion|scoring|labels|monitor)"} == 1)',
            4,
        ),
        "Kubernetes deployment metrics": (
            'count(kube_deployment_status_replicas_available{namespace="fraudwatch"})',
            1,
        ),
    }
    deadline = time.monotonic() + timeout
    pending = dict(queries)
    while time.monotonic() < deadline:
        for name, (query, minimum) in list(pending.items()):
            value = prometheus_value(base_url, query)
            if value is not None and value >= minimum:
                del pending[name]
        if not pending:
            return
        time.sleep(1)
    raise RuntimeError(
        "timed out waiting for Prometheus evidence: " + ", ".join(pending)
    )


def loki_has_transaction_log(base_url: str, transaction_id: str) -> bool:
    now_ns = time.time_ns()
    query = (
        '{namespace="fraudwatch", service_name="ingestion"} '
        f'|= {json.dumps(transaction_id)}'
    )
    encoded = urllib.parse.urlencode(
        {
            "query": query,
            "start": now_ns - 300_000_000_000,
            "end": now_ns,
            "limit": 20,
            "direction": "backward",
        }
    )
    response = json.loads(get(f"{base_url}/loki/api/v1/query_range?{encoded}"))
    if response.get("status") != "success":
        raise RuntimeError(f"Loki query failed: {response}")
    return any(stream.get("values") for stream in response["data"]["result"])


def wait_for_transaction_log(
    base_url: str,
    transaction_id: str,
    timeout: float,
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if loki_has_transaction_log(base_url, transaction_id):
            return
        time.sleep(1)
    raise RuntimeError(
        f"timed out waiting for Loki ingestion log for {transaction_id}"
    )


def smoke(
    context: str,
    timeout: float,
    *,
    database_user: str,
    database_name: str,
) -> str:
    assert_context(context)
    with ExitStack() as stack:
        urls = {
            service: stack.enter_context(
                port_forward(
                    context,
                    service,
                    local_port,
                    remote_port,
                    ready_path,
                    timeout,
                )
            )
            for service, (local_port, remote_port, ready_path) in ENDPOINTS.items()
        }
        get(f"{urls['ingestion']}/livez")
        for service in ("ingestion", "scoring", "labels", "monitor"):
            metrics = get(f"{urls[service]}/metrics")
            if not metrics.strip():
                raise RuntimeError(f"{service} returned empty metrics")
        grafana_health = json.loads(get(f"{urls['grafana']}/api/health"))
        if grafana_health.get("database") != "ok":
            raise RuntimeError(f"Grafana database is unhealthy: {grafana_health}")

        transaction_id = f"k8s-smoke-{uuid.uuid4().hex}"
        accepted = post_transaction(urls["ingestion"], transaction_id)
        if accepted.get("status") != "accepted" or accepted.get("duplicate") is not False:
            raise RuntimeError(f"unexpected ingestion response: {accepted}")
        wait_for_decision(
            context,
            transaction_id,
            timeout,
            database_user=database_user,
            database_name=database_name,
        )
        wait_for_observability(urls["prometheus"], timeout)
        wait_for_transaction_log(urls["loki"], transaction_id, timeout)
        return transaction_id


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", required=True)
    parser.add_argument("--timeout", type=float, default=90)
    parser.add_argument("--database-user", default="fraudwatch")
    parser.add_argument("--database-name", default="fraudwatch")
    args = parser.parse_args()
    try:
        transaction_id = smoke(
            args.context,
            args.timeout,
            database_user=args.database_user,
            database_name=args.database_name,
        )
    except (RuntimeError, subprocess.CalledProcessError, socket.timeout) as error:
        print(f"Kubernetes smoke test failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
    print(f"Kubernetes smoke test: OK ({transaction_id})")


if __name__ == "__main__":
    main()
