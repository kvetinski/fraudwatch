"""Low-cardinality Prometheus instrumentation for FraudWatch workers.

Worker metrics describe work performed by that process.  Authoritative Kafka
consumer-group lag and database backlog gauges live in the independent monitor
service so they remain fresh while a worker is stopped or rebalancing.
"""

from __future__ import annotations

import os
import time
from typing import Iterable

from prometheus_client import Counter, Gauge, Histogram, Info, REGISTRY, start_http_server

STAGE_BUCKETS = (
    0.0001, 0.00025, 0.0005, 0.001, 0.002, 0.003,
    0.005, 0.0075, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1, 5,
)
QUEUE_AGE_BUCKETS = (0.01, 0.1, 0.5, 1, 2, 5, 10, 30, 60, 300, 900, 3600, 21600, 86400)
SCORE_BUCKETS = (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.4, 0.6, 0.8, 0.95, 1.0)
BATCH_BUCKETS = (1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1000)


def start_worker_metrics(port: int, *, bind: str = "127.0.0.1"):
    """Run a /metrics HTTP endpoint on a daemon thread."""
    if not 0 < port < 65536:
        raise ValueError("Invalid Prometheus metrics port")
    return start_http_server(port, addr=bind)


class ConsumerRuntimeMetrics:
    """Metrics shared by transaction and label consumers."""

    def __init__(self, prefix: str, registry, *, topic: str, group_id: str):
        self.commits = Counter(
            f"{prefix}_kafka_commits_total",
            "Synchronous Kafka offset-commit outcomes",
            labelnames=("result",), registry=registry,
        )
        self.rebalances = Counter(
            f"{prefix}_kafka_rebalances_total",
            "Kafka consumer rebalance callbacks",
            labelnames=("event",), registry=registry,
        )
        self.assigned_partitions = Gauge(
            f"{prefix}_kafka_assigned_partitions",
            "Partitions currently assigned to this consumer process",
            registry=registry,
        )
        self.last_poll = Gauge(
            f"{prefix}_last_poll_unixtime",
            "Unix time of the most recent completed Kafka poll",
            registry=registry,
        )
        self.last_ack = Gauge(
            f"{prefix}_last_ack_unixtime",
            "Unix time of the most recent successful Kafka acknowledgment",
            registry=registry,
        )
        self.queue_age = Histogram(
            f"{prefix}_queue_age_seconds",
            "Time from Kafka record creation to successful acknowledgment",
            buckets=QUEUE_AGE_BUCKETS, registry=registry,
        )
        self.batch_size = Histogram(
            f"{prefix}_batch_messages",
            "Messages durably handled per partition commit",
            buckets=BATCH_BUCKETS, registry=registry,
        )
        self.identity = Info(
            f"{prefix}_build",
            "Build and stream identity for this worker",
            registry=registry,
        )
        self.identity.info({
            "revision": os.getenv("FRAUDWATCH_BUILD_REVISION", "development"),
            "topic": topic,
            "group": group_id,
        })
        for result in ("success", "failure"):
            self.commits.labels(result=result)
        for event in ("assigned", "revoked", "lost"):
            self.rebalances.labels(event=event)

    def polled(self) -> None:
        self.last_poll.set(time.time())

    def assignment(self, event: str, partitions: Iterable[object]) -> None:
        parts = list(partitions)
        self.rebalances.labels(event=event).inc()
        self.assigned_partitions.set(len(parts) if event == "assigned" else 0)

    def acknowledged(self, messages: Iterable[object]) -> None:
        now = time.time()
        messages = list(messages)
        self.last_ack.set(now)
        self.batch_size.observe(len(messages))
        for message in messages:
            try:
                timestamp_type, timestamp_ms = message.timestamp()
            except (AttributeError, TypeError):
                continue
            if timestamp_type and timestamp_ms is not None and timestamp_ms >= 0:
                self.queue_age.observe(max(0.0, now - timestamp_ms / 1000.0))


class ScoringMetrics(ConsumerRuntimeMetrics):
    def __init__(self, registry=None, *, topic="transactions.v1", group_id="fraudwatch-scoring-v1"):
        registry = registry if registry is not None else REGISTRY
        super().__init__("fraudwatch_scoring", registry, topic=topic, group_id=group_id)
        self.events = Counter(
            "fraudwatch_scoring_events_total",
            "Scoring records acknowledged after their PostgreSQL effect became durable",
            labelnames=("outcome",), registry=registry,
        )
        self.duration = Histogram(
            "fraudwatch_scoring_stage_seconds",
            "Scoring latency by stage; total covers every acknowledged outcome",
            labelnames=("stage",), buckets=STAGE_BUCKETS, registry=registry,
        )
        self.score = Histogram(
            "fraudwatch_scoring_fraud_score",
            "Scores for newly created durable decisions (independent of Kafka redelivery)",
            buckets=SCORE_BUCKETS, registry=registry,
        )
        self.errors = Counter(
            "fraudwatch_scoring_errors_total",
            "Scoring failures that stop processing without acknowledging Kafka",
            labelnames=("category",), registry=registry,
        )
        self.model_identity = Info(
            "fraudwatch_scoring_model",
            "Pinned model and feature schema loaded by this worker",
            registry=registry,
        )
        for outcome in ("scored", "duplicate", "quarantined"):
            self.events.labels(outcome=outcome)
        for category in (
            "poll",
            "deterministic_data",
            "transient_infrastructure",
            "fatal_processing",
        ):
            self.errors.labels(category=category)

    def set_model_identity(self, *, model_reference: str, feature_schema_version: str, model_digest: str = "unknown") -> None:
        self.model_identity.info({
            "model_reference": model_reference,
            "model_digest": model_digest,
            "feature_schema_version": feature_schema_version,
        })

    def observe_result(self, *, new_decision: bool, quarantined: bool,
                       timings, processing_ns: int, commit_ns: int) -> None:
        """Observe one record only after its Kafka offset commit succeeds."""
        if quarantined:
            outcome = "quarantined"
        elif new_decision:
            outcome = "scored"
        else:
            outcome = "duplicate"
        self.events.labels(outcome=outcome).inc()
        if new_decision:
            for stage, nanos in timings.durations_ns.items():
                self.duration.labels(stage=stage).observe(nanos / 1_000_000_000)
        self.duration.labels(stage="process_total").observe(processing_ns / 1_000_000_000)
        self.duration.labels(stage="kafka_commit").observe(commit_ns / 1_000_000_000)
        self.duration.labels(stage="total").observe((processing_ns + commit_ns) / 1_000_000_000)


class LabelMetrics(ConsumerRuntimeMetrics):
    def __init__(self, registry=None, *, topic="fraud-labels.v1", group_id="fraudwatch-labels-v1"):
        registry = registry if registry is not None else REGISTRY
        super().__init__("fraudwatch_labels", registry, topic=topic, group_id=group_id)
        self.events = Counter(
            "fraudwatch_labels_events_total",
            "Label records acknowledged after their PostgreSQL effect became durable",
            labelnames=("outcome",), registry=registry,
        )
        self.duration = Histogram(
            "fraudwatch_labels_stage_seconds",
            "Delayed-label processing latency in seconds",
            labelnames=("stage",), buckets=STAGE_BUCKETS, registry=registry,
        )
        self.errors = Counter(
            "fraudwatch_labels_errors_total",
            "Label-worker failures that stop processing without acknowledging Kafka",
            labelnames=("category",), registry=registry,
        )
        for outcome in ("inserted", "duplicate", "quarantined"):
            self.events.labels(outcome=outcome)
        for category in (
            "poll",
            "deterministic_data",
            "transient_infrastructure",
            "fatal_processing",
        ):
            self.errors.labels(category=category)

    def observe_result(self, *, inserted: bool, quarantined: bool = False,
                       db_seconds: float, commit_seconds: float) -> None:
        if quarantined:
            outcome = "quarantined"
        else:
            outcome = "inserted" if inserted else "duplicate"
        self.events.labels(outcome=outcome).inc()
        self.duration.labels(stage="database").observe(db_seconds)
        self.duration.labels(stage="kafka_commit").observe(commit_seconds)
        self.duration.labels(stage="total").observe(db_seconds + commit_seconds)
