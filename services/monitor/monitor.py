#!/usr/bin/env python3
"""Independent read-only exporter for Kafka lag and FraudWatch durable state."""

from __future__ import annotations

import json
import logging
import os
import signal
import threading
import time

import psycopg
from confluent_kafka import Consumer, TopicPartition
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from fraudwatch_ml.features import FEATURE_NAMES
from fraudwatch_ml.json_logging import configure_json_logging

logger = logging.getLogger("fraudwatch.monitor")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgres://fraudwatch:fraudwatch_dev@127.0.0.1:5434/fraudwatch?sslmode=disable",
)
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092")
COLLECTION_INTERVAL = float(os.getenv("FRAUDWATCH_MONITOR_INTERVAL_SECONDS", "15"))
GROUPS = {
    os.getenv("KAFKA_GROUP_ID", "fraudwatch-scoring-v1"): os.getenv("KAFKA_TOPIC", "transactions.v1"),
    os.getenv("KAFKA_LABEL_GROUP_ID", "fraudwatch-labels-v1"): os.getenv("KAFKA_LABEL_TOPIC", "fraud-labels.v1"),
}

lag = Gauge("fraudwatch_kafka_consumer_group_lag", "Authoritative committed consumer-group lag", ("group", "topic", "partition"))
committed_offset = Gauge("fraudwatch_kafka_consumer_group_committed_offset", "Committed next offset", ("group", "topic", "partition"))
log_end_offset = Gauge("fraudwatch_kafka_topic_log_end_offset", "Kafka partition log-end offset", ("topic", "partition"))
outbox_pending = Gauge("fraudwatch_outbox_pending_events", "Pending transactional-outbox events")
outbox_age = Gauge("fraudwatch_outbox_oldest_pending_age_seconds", "Age of oldest pending outbox event")
quarantine_pending = Gauge("fraudwatch_quarantine_pending_events", "Pending quarantines", ("event_type",))
quarantine_age = Gauge("fraudwatch_quarantine_oldest_pending_age_seconds", "Age of oldest pending quarantine", ("event_type",))
blocked_terminals = Gauge("fraudwatch_blocked_terminals", "Terminals blocked by event-time ordering")
blocked_age = Gauge("fraudwatch_blocked_terminal_oldest_age_seconds", "Age of oldest terminal order block")
pending_labels = Gauge("fraudwatch_pending_labels", "Valid delayed labels awaiting their transaction")
pending_label_age = Gauge("fraudwatch_pending_label_oldest_age_seconds", "Age of oldest pending delayed label")
decisions = Gauge("fraudwatch_decisions", "Current projected fraud decisions")
label_coverage = Gauge("fraudwatch_label_coverage_ratio", "Fraction of decisions with delayed labels")
model_info = Gauge(
    "fraudwatch_deployed_model_info",
    "Models represented in current decisions",
    ("model_reference", "model_digest", "feature_schema_version"),
)
monitor_success = Gauge("fraudwatch_monitor_last_success_unixtime", "Unix time of last complete monitor collection")
monitor_errors = Counter("fraudwatch_monitor_collection_errors_total", "Cumulative monitor collection failures", ("source",))
monitor_duration = Histogram("fraudwatch_monitor_collection_duration_seconds", "Independent monitor collection duration", ("source",))
snapshot_age = Gauge("fraudwatch_model_monitor_snapshot_age_seconds", "Age of latest model-monitor snapshot")
snapshot_present = Gauge("fraudwatch_model_monitor_snapshot_present", "Whether a model-monitor snapshot exists")
snapshot_samples = Gauge("fraudwatch_model_monitor_sample_count", "Samples in latest monitoring window")
snapshot_labeled = Gauge("fraudwatch_model_monitor_labeled_count", "Labeled samples in latest monitoring window")
snapshot_positives = Gauge("fraudwatch_model_monitor_positive_count", "Positive labels in latest monitoring window")
snapshot_label_coverage = Gauge("fraudwatch_model_monitor_label_coverage_ratio", "Delayed-label coverage in latest monitoring window")
snapshot_eligible = Gauge("fraudwatch_model_monitor_alert_eligible", "Whether sample and positive gates are met")
score_psi = Gauge("fraudwatch_model_score_psi", "Population stability index for model scores")
feature_psi = Gauge("fraudwatch_model_feature_psi", "Population stability index by feature", ("feature",))
quality = Gauge("fraudwatch_model_quality", "Latest delayed-label quality metric", ("metric",))
quality_degradation = Gauge("fraudwatch_model_quality_degradation_ratio", "Relative degradation from validation baseline", ("metric",))


class Monitor:
    def __init__(self):
        self.stop = threading.Event()
        self.consumers = {
            group: Consumer({
                "bootstrap.servers": KAFKA_BROKERS,
                "group.id": group,
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
            })
            for group in GROUPS
        }

    def collect_kafka(self) -> None:
        lag.clear()
        committed_offset.clear()
        log_end_offset.clear()
        for group, topic in GROUPS.items():
            consumer = self.consumers[group]
            metadata = consumer.list_topics(topic=topic, timeout=5)
            topic_metadata = metadata.topics.get(topic)
            if topic_metadata is None or topic_metadata.error is not None:
                raise RuntimeError(f"Kafka metadata unavailable for {topic}")
            partitions = [TopicPartition(topic, number) for number in sorted(topic_metadata.partitions)]
            committed = consumer.committed(partitions, timeout=5)
            for partition in committed:
                if partition.error is not None:
                    raise RuntimeError(
                        f"Kafka committed-offset lookup failed for {group}: {partition.error}"
                    )
                low, high = consumer.get_watermark_offsets(partition, timeout=5, cached=False)
                offset = partition.offset if partition.offset >= 0 else low
                labels = {"group": group, "topic": topic, "partition": str(partition.partition)}
                committed_offset.labels(**labels).set(offset)
                log_end_offset.labels(topic=topic, partition=str(partition.partition)).set(high)
                lag.labels(**labels).set(max(0, high - offset))

    def collect_database(self) -> None:
        # Keep every collection in one consistent, explicitly read-only
        # PostgreSQL snapshot. This protects the database even when a caller
        # supplies credentials that would otherwise be allowed to write.
        with psycopg.connect(DATABASE_URL) as conn:
            conn.execute("SET TRANSACTION READ ONLY")
            pending, oldest = conn.execute("""
                SELECT count(*), COALESCE(EXTRACT(EPOCH FROM now() - min(created_at)), 0)
                FROM outbox_events WHERE delivered_at IS NULL
            """).fetchone()
            outbox_pending.set(pending)
            outbox_age.set(oldest)

            quarantine_pending.clear()
            quarantine_age.clear()
            rows = conn.execute("""
                SELECT event_type, count(*), COALESCE(EXTRACT(EPOCH FROM now() - min(created_at)), 0)
                FROM event_quarantine WHERE status = 'pending' GROUP BY event_type
            """).fetchall()
            values = {row[0]: (row[1], row[2]) for row in rows}
            for event_type in ("transaction", "label"):
                count, age = values.get(event_type, (0, 0))
                quarantine_pending.labels(event_type=event_type).set(count)
                quarantine_age.labels(event_type=event_type).set(age)

            count, age = conn.execute("""
                SELECT count(*), COALESCE(EXTRACT(EPOCH FROM now() - min(blocked_at)), 0)
                FROM terminal_order_blocks
            """).fetchone()
            blocked_terminals.set(count)
            blocked_age.set(age)
            count, age = conn.execute("""
                SELECT count(*), COALESCE(EXTRACT(EPOCH FROM now() - min(received_at)), 0)
                FROM pending_fraud_labels
            """).fetchone()
            pending_labels.set(count)
            pending_label_age.set(age)
            decision_count, labeled_count = conn.execute("""
                SELECT count(*), count(st.fraud_label)
                FROM fraud_decisions fd JOIN scored_transactions st USING (transaction_id)
            """).fetchone()
            decisions.set(decision_count)
            label_coverage.set(labeled_count / decision_count if decision_count else 0)
            model_info.clear()
            for reference, digest, schema in conn.execute("""
                SELECT DISTINCT model_reference, model_digest,
                                feature_schema_version
                FROM fraud_decisions
            """).fetchall():
                model_info.labels(
                    model_reference=reference,
                    model_digest=digest,
                    feature_schema_version=schema,
                ).set(1)

            row = conn.execute("""
                SELECT EXTRACT(EPOCH FROM now() - evaluated_at), sample_count,
                       labeled_count, positive_count, label_coverage,
                       alert_eligible, score_psi, feature_psi,
                       fraud_prevalence, average_precision, roc_auc,
                       precision_at_1pct, recall_at_1pct, quality_degradation
                FROM model_monitor_snapshots ORDER BY evaluated_at DESC, id DESC LIMIT 1
            """).fetchone()
            if row is not None:
                snapshot_present.set(1)
                snapshot_age.set(row[0])
                snapshot_samples.set(row[1])
                snapshot_labeled.set(row[2])
                snapshot_positives.set(row[3])
                snapshot_label_coverage.set(row[4])
                snapshot_eligible.set(int(row[5]))
                score_psi.set(row[6])
                feature_psi.clear()
                for name in FEATURE_NAMES:
                    feature_psi.labels(feature=name).set(row[7][name])
                quality_values = {
                    "fraud_prevalence": row[8], "average_precision": row[9],
                    "roc_auc": row[10], "precision_at_1pct": row[11],
                    "recall_at_1pct": row[12],
                }
                quality.clear()
                for name, value in quality_values.items():
                    if value is not None:
                        quality.labels(metric=name).set(value)
                quality_degradation.clear()
                for name, value in row[13].items():
                    if value is not None:
                        quality_degradation.labels(metric=name).set(value)
            else:
                snapshot_present.set(0)
                snapshot_age.set(0)
                snapshot_samples.set(0)
                snapshot_labeled.set(0)
                snapshot_positives.set(0)
                snapshot_label_coverage.set(0)
                snapshot_eligible.set(0)
                score_psi.set(0)
                feature_psi.clear()
                quality.clear()
                quality_degradation.clear()

    def run(self) -> None:
        while not self.stop.is_set():
            complete = True
            for source, collector in (("kafka", self.collect_kafka), ("postgres", self.collect_database)):
                started = time.monotonic()
                try:
                    collector()
                except Exception:
                    complete = False
                    monitor_errors.labels(source=source).inc()
                    logger.exception(
                        "monitor collection failed",
                        extra={
                            "source": source,
                            "outcome": "failure",
                            "duration_ms": (time.monotonic() - started) * 1000,
                        },
                    )
                finally:
                    monitor_duration.labels(source=source).observe(time.monotonic() - started)
            if complete:
                monitor_success.set(time.time())
            self.stop.wait(COLLECTION_INTERVAL)

    def close(self) -> None:
        self.stop.set()
        for consumer in self.consumers.values():
            consumer.close()


def main() -> None:
    configure_json_logging("monitor")
    if COLLECTION_INTERVAL <= 0:
        raise ValueError("FRAUDWATCH_MONITOR_INTERVAL_SECONDS must be positive")
    port = int(os.getenv("FRAUDWATCH_MONITOR_METRICS_PORT", "9103"))
    bind = os.getenv("FRAUDWATCH_METRICS_BIND", "127.0.0.1")
    start_http_server(port, addr=bind)
    monitor = Monitor()
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda *_args: monitor.stop.set())
    logger.info("monitor started", extra={"port": port, "groups": json.dumps(GROUPS, sort_keys=True)})
    try:
        monitor.run()
    finally:
        monitor.close()


if __name__ == "__main__":
    main()
