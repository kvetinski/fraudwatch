from prometheus_client import CollectorRegistry, generate_latest

from fraudwatch_ml.observability import ScoringMetrics, LabelMetrics


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
    metrics.pending_quarantine.set(2)
    metrics.blocked_terminals.set(1)
    metrics.kafka_lag.labels(partition="1").set(3)
    data = generate_latest(registry).decode()
    assert 'fraudwatch_scoring_events_total{outcome="scored"} 1.0' in data
    assert 'fraudwatch_scoring_events_total{outcome="quarantined"} 1.0' in data
    assert 'fraudwatch_scoring_events_total{outcome="duplicate"} 1.0' in data
    assert 'fraudwatch_scoring_stage_seconds_count{stage="inference"} 1.0' in data
    assert 'fraudwatch_scoring_stage_seconds_count{stage="total"} 1.0' in data
    assert 'fraudwatch_scoring_pending_quarantine 2.0' in data
    assert 'fraudwatch_scoring_blocked_terminals 1.0' in data
    assert 'fraudwatch_scoring_kafka_lag{partition="1"} 3.0' in data


def test_label_metrics_only_acknowledged_events():
    registry = CollectorRegistry()
    metrics = LabelMetrics(registry=registry)
    metrics.observe_result(inserted=True, db_seconds=0.001, commit_seconds=0.0002)
    metrics.observe_result(inserted=False, db_seconds=0.001, commit_seconds=0.0002)
    data = generate_latest(registry).decode()
    assert 'fraudwatch_labels_events_total{outcome="inserted"} 1.0' in data
    assert 'fraudwatch_labels_events_total{outcome="duplicate"} 1.0' in data
    assert 'fraudwatch_labels_stage_seconds_count{stage="total"} 2.0' in data


def test_snapshot_lag_uses_committed_offset_not_fetched_position():
    from types import SimpleNamespace
    from fraudwatch_ml.observability import refresh_scoring_snapshot

    class Connection:
        def execute(self, query):
            return self

        def fetchone(self):
            return (4, 2)

    class Consumer:
        def assignment(self):
            return [SimpleNamespace(topic="transactions.v1", partition=1)]

        def committed(self, parts, timeout):
            return [SimpleNamespace(offset=42, error=None)]

        def get_watermark_offsets(self, part, timeout, cached):
            return (0, 50)

    registry = CollectorRegistry()
    metrics = ScoringMetrics(registry=registry)
    refresh_scoring_snapshot(metrics, Connection(), Consumer())
    data = generate_latest(registry).decode()
    assert 'fraudwatch_scoring_pending_quarantine 4.0' in data
    assert 'fraudwatch_scoring_blocked_terminals 2.0' in data
    assert 'fraudwatch_scoring_kafka_lag{partition="1"} 8.0' in data
