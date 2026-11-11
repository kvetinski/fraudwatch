"""Low-cardinality Prometheus instrumentation for FraudWatch local workers.

No transaction/terminal identifiers are metric labels. All durations are seconds.
The worker is responsible for emitting a scoring observation only AFTER the
PostgreSQL transaction and Kafka commit have succeeded.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram, REGISTRY, start_http_server

STAGE_BUCKETS = (
    0.0001, 0.00025, 0.0005, 0.001, 0.002, 0.003,
    0.005, 0.0075, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1, 5,
)
SCORE_BUCKETS = (0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.4, 0.6, 0.8, 0.95, 1.0)


def start_worker_metrics(port: int, *, bind: str = "127.0.0.1"):
    """Run a /metrics HTTP endpoint on a daemon thread, for Linux host-network scrape."""
    if not 0 < port < 65536:
        raise ValueError("Invalid Prometheus metrics port")
    return start_http_server(port, addr=bind)


class ScoringMetrics:
    def __init__(self, registry=None):
        registry = registry if registry is not None else REGISTRY
        self.events = Counter(
            "fraudwatch_scoring_events_total",
            "Scoring events committed to PostgreSQL and acknowledged by Kafka",
            labelnames=("outcome",), registry=registry,
        )
        self.duration = Histogram(
            "fraudwatch_scoring_stage_seconds",
            "Latency per scoring stage in seconds, newly scored messages only",
            labelnames=("stage",), buckets=STAGE_BUCKETS, registry=registry,
        )
        self.score = Histogram(
            "fraudwatch_scoring_fraud_score",
            "Scores of decisions durably persisted in PostgreSQL",
            buckets=SCORE_BUCKETS, registry=registry,
        )
        self.kafka_commits = Counter(
            "fraudwatch_scoring_kafka_commits_total",
            "Kafka offset-commit outcomes",
            labelnames=("result",), registry=registry,
        )
        self.errors = Counter(
            "fraudwatch_scoring_errors_total",
            "Failures that escaped the scoring operation",
            labelnames=("category",), registry=registry,
        )
        self.pending_quarantine = Gauge(
            "fraudwatch_scoring_pending_quarantine",
            "Number of pending quarantined scoring events (database snapshot)",
            registry=registry,
        )
        self.blocked_terminals = Gauge(
            "fraudwatch_scoring_blocked_terminals",
            "Number of terminals blocked by event-time ordering policy",
            registry=registry,
        )
        self.kafka_lag = Gauge(
            "fraudwatch_scoring_kafka_lag",
            "Kafka log-end offset minus the committed consumer offset",
            labelnames=("partition",), registry=registry,
        )
        self.snapshot_errors = Counter(
            "fraudwatch_scoring_snapshot_errors_total",
            "Errors while polling monitoring-only PostgreSQL or Kafka gauges",
            registry=registry,
        )
        for outcome in ("scored", "duplicate", "quarantined"):
            self.events.labels(outcome=outcome)
        for result in ("success", "failure"):
            self.kafka_commits.labels(result=result)
        for category in ("poll", "process"):
            self.errors.labels(category=category)

    def observe_result(self, *, new_decision: bool, quarantined: bool,
                       timings, processing_ns: int, commit_ns: int) -> None:
        """Call ONLY once after the successful Kafka offset commit."""
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


class LabelMetrics:
    def __init__(self, registry=None):
        registry = registry if registry is not None else REGISTRY
        self.events = Counter(
            "fraudwatch_labels_events_total",
            "Delayed label events committed to PostgreSQL and acknowledged by Kafka",
            labelnames=("outcome",), registry=registry,
        )
        self.duration = Histogram(
            "fraudwatch_labels_stage_seconds",
            "Delayed label processing latency in seconds",
            labelnames=("stage",), buckets=STAGE_BUCKETS, registry=registry,
        )
        self.errors = Counter(
            "fraudwatch_labels_errors_total",
            "Label worker failures",
            labelnames=("category",), registry=registry,
        )
        for outcome in ("inserted", "duplicate"):
            self.events.labels(outcome=outcome)
        for category in ("poll", "process"):
            self.errors.labels(category=category)

    def observe_result(self, *, inserted: bool, db_seconds: float, commit_seconds: float) -> None:
        self.events.labels(outcome="inserted" if inserted else "duplicate").inc()
        self.duration.labels(stage="database").observe(db_seconds)
        self.duration.labels(stage="kafka_commit").observe(commit_seconds)
        self.duration.labels(stage="total").observe(db_seconds + commit_seconds)


def refresh_scoring_snapshot(metrics: ScoringMetrics, conn, consumer) -> None:
    """Best-effort operational gauges; errors must not affect transaction scoring.

    Call periodically (e.g. every 15 s), never once per transaction.
    Kafka lag uses *committed* consumer offsets rather than fetched positions.
    """
    pending, blocked = conn.execute(
        """
        SELECT
          (SELECT count(*) FROM scoring_quarantine WHERE status = 'pending'),
          (SELECT count(*) FROM terminal_order_blocks)
        """
    ).fetchone()
    metrics.pending_quarantine.set(pending)
    metrics.blocked_terminals.set(blocked)

    partitions = consumer.assignment()
    if not partitions:
        return
    committed = consumer.committed(partitions, timeout=3.0)
    for partition, commit in zip(partitions, committed):
        if commit.error is not None:
            raise RuntimeError(f"Kafka committed offset error: {commit.error}")
        low, high = consumer.get_watermark_offsets(partition, timeout=2.0, cached=False)
        position = commit.offset if commit.offset >= 0 else low
        metrics.kafka_lag.labels(partition=str(partition.partition)).set(
            max(0, high - position)
        )
