import json
import socket
import urllib.error
import urllib.request

import pytest
from prometheus_client import CollectorRegistry, generate_latest

from fraudwatch_ml.observability import (
    LabelMetrics,
    ScoringMetrics,
    WorkerHealth,
    start_worker_metrics,
)


class FakeTimings:
    durations_ns = {"parse": 15_000, "inference": 1_500_000}


def test_scoring_metrics_expose_counters_histogram_and_gauges():
    registry = CollectorRegistry()
    metrics = ScoringMetrics(registry=registry)
    metrics.score.observe(0.3)
    metrics.observe_result(
        new_decision=True, quarantined=False, timings=FakeTimings(),
        processing_ns=4_000_000, commit_ns=300_000,
    )
    metrics.observe_result(
        new_decision=False, quarantined=True, timings=FakeTimings(),
        processing_ns=100_000, commit_ns=20_000,
    )
    metrics.observe_result(
        new_decision=False, quarantined=False, timings=FakeTimings(),
        processing_ns=100_000, commit_ns=20_000,
    )
    metrics.commits.labels(result="success").inc()
    metrics.assignment("assigned", [object(), object()])
    metrics.polled()
    data = generate_latest(registry).decode()
    assert 'fraudwatch_scoring_events_total{outcome="scored"} 1.0' in data
    assert 'fraudwatch_scoring_events_total{outcome="quarantined"} 1.0' in data
    assert 'fraudwatch_scoring_events_total{outcome="duplicate"} 1.0' in data
    assert 'fraudwatch_scoring_stage_seconds_count{stage="inference"} 1.0' in data
    assert 'fraudwatch_scoring_stage_seconds_count{stage="total"} 3.0' in data
    assert 'fraudwatch_scoring_kafka_commits_total{result="success"} 1.0' in data
    assert 'fraudwatch_scoring_errors_total{category="transient_infrastructure"} 0.0' in data
    assert 'fraudwatch_scoring_errors_total{category="fatal_processing"} 0.0' in data
    assert "fraudwatch_scoring_kafka_assigned_partitions 2.0" in data
    assert "fraudwatch_scoring_last_poll_unixtime" in data


def test_label_metrics_only_acknowledged_events():
    registry = CollectorRegistry()
    metrics = LabelMetrics(registry=registry)
    metrics.observe_result(inserted=True, db_seconds=0.001, commit_seconds=0.0002)
    metrics.observe_result(inserted=False, db_seconds=0.001, commit_seconds=0.0002)
    metrics.observe_result(
        inserted=False, quarantined=True,
        db_seconds=0.001, commit_seconds=0.0002,
    )
    data = generate_latest(registry).decode()
    assert 'fraudwatch_labels_events_total{outcome="inserted"} 1.0' in data
    assert 'fraudwatch_labels_events_total{outcome="duplicate"} 1.0' in data
    assert 'fraudwatch_labels_events_total{outcome="quarantined"} 1.0' in data
    assert 'fraudwatch_labels_stage_seconds_count{stage="total"} 3.0' in data


def test_total_latency_covers_duplicate_and_quarantine_outcomes():
    registry = CollectorRegistry()
    metrics = ScoringMetrics(registry=registry)
    metrics.observe_result(
        new_decision=False, quarantined=False, timings=FakeTimings(),
        processing_ns=100_000, commit_ns=20_000,
    )
    metrics.observe_result(
        new_decision=False, quarantined=True, timings=FakeTimings(),
        processing_ns=100_000, commit_ns=20_000,
    )
    data = generate_latest(registry).decode()
    assert 'fraudwatch_scoring_stage_seconds_count{stage="total"} 2.0' in data


def test_worker_readiness_requires_dependencies_assignment_and_fresh_heartbeat():
    now = [100.0]
    health = WorkerHealth(
        ("model", "database", "kafka"),
        heartbeat_timeout=5,
        clock=lambda: now[0],
    )
    health.set_component("model", True)
    health.set_component("database", True)
    health.assignment("assigned", [object()])
    health.heartbeat()
    assert health.snapshot()["ready"] is True

    now[0] += 6
    assert health.snapshot()["ready"] is False
    health.heartbeat()
    health.assignment("revoked", [object()])
    assert health.snapshot()["components"]["kafka"] is False
    assert health.snapshot()["ready"] is False


def test_worker_http_separates_liveness_from_readiness():
    health = WorkerHealth(("database", "kafka"))
    registry = CollectorRegistry()
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    server, thread = start_worker_metrics(
        port, health=health, registry=registry
    )
    try:
        with urllib.request.urlopen(
            f"http://127.0.0.1:{port}/livez", timeout=2
        ) as response:
            assert response.status == 200
        with pytest.raises(urllib.error.HTTPError) as failure:
            urllib.request.urlopen(
                f"http://127.0.0.1:{port}/readyz", timeout=2
            )
        assert failure.value.code == 503
        health.set_component("database", True)
        health.assignment("assigned", [object()])
        health.heartbeat()
        with urllib.request.urlopen(
            f"http://127.0.0.1:{port}/readyz", timeout=2
        ) as response:
            assert json.load(response)["ready"] is True
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
