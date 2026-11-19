import pytest

from scripts.evaluate_replay import histogram_quantile, replay_performance


def test_histogram_quantile_interpolates_cumulative_buckets():
    assert histogram_quantile(
        {0.01: 50, 0.1: 100, float("inf"): 100}, 0.95
    ) == pytest.approx(0.091)


def test_replay_performance_uses_acknowledged_path_timings():
    manifest = {
        "counts": {"transactions": 100, "labels": 100},
        "attempts": [
            {
                "started_at": "2026-01-01T00:00:00Z",
                "finished_at": "2026-01-01T00:00:10Z",
            }
        ],
        "checkpoints": [
            {
                "transactions": {"expected": 100},
                "labels": {"expected": 0},
                "timings_seconds": {
                    "transaction_submit": 1.0,
                    "transaction_wait": 3.0,
                },
            },
            {
                "transactions": {"expected": 0},
                "labels": {"expected": 100},
                "timings_seconds": {"label_publish": 1.0, "label_wait": 2.0},
            },
        ],
    }
    result = replay_performance(manifest)
    assert result["transaction_acknowledged_per_second"] == 25
    assert result["end_to_end_transactions_per_second"] == 10
    assert result["max_daily_lag_drain_seconds"] == 3
