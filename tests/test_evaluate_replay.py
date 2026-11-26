import pytest

from scripts.evaluate_replay import (
    compare_scaling_runs,
    failed_phase_gates,
    histogram_quantile,
    phase_gate_exit_code,
    replay_performance,
    validate_completed_replay,
    validate_runtime_model_evidence,
    validate_scaling_manifests,
)


MODEL_DIGEST = "sha256:" + "a" * 64


def _worker(url: str) -> dict:
    return {
        "metrics_url": url,
        "model_reference": "models:/fraudwatch-hgb/1",
        "model_digest": MODEL_DIGEST,
        "feature_schema_version": "v1",
    }


def _manifest(run_id: str, replicas: int, *, dataset_sha: str = "dataset") -> dict:
    urls = [f"http://worker-{replicas}-{index}/metrics" for index in range(replicas)]
    workers = [_worker(url) for url in urls]
    return {
        "run_id": run_id,
        "configuration": {
            "run_id": run_id,
            "mode": "kafka",
            "dataset": {
                "directory": f"/data/{run_id}",
                "start_date": "2018-04-01",
                "end_date": "2018-04-01",
                "source_days": 1,
                "per_day": 5,
                "sha256": dataset_sha,
                "files": [],
            },
            "model": {
                "reference": "models:/fraudwatch-hgb/1",
                "digest": MODEL_DIGEST,
            },
            "feature_schema_version": "v1",
            "topology": {
                "topics": {
                    "transactions.v1": {"partitions": 3},
                    "fraud-labels.v1": {"partitions": 3},
                },
                "transaction_topic": "transactions.v1",
                "label_topic": "fraud-labels.v1",
                "label_delay_days": 0,
                "http_concurrency": None,
                "scoring_metrics_urls": urls,
                "consumer_groups": {
                    "fraudwatch-scoring-v1": {"members": replicas},
                },
            },
        },
        "counts": {
            "checkpoints_complete": 1,
            "transactions": 10,
            "labels": 10,
        },
        "checkpoints": [
            {
                "timeline_date": "2018-04-01",
                "status": "complete",
                "transactions": {
                    "expected": 10,
                    "submission": {"delivered": 10},
                },
                "labels": {
                    "expected": 10,
                    "delivery": {"delivered": 10},
                },
                "scoring_workers": [dict(worker) for worker in workers],
            }
        ],
        "outcomes": {
            "decisions": 10,
            "labels": 10,
            "pending_labels": 0,
            "quarantines": 0,
            "terminal_blocks": 0,
            "model_references": ["models:/fraudwatch-hgb/1"],
            "model_digests": [MODEL_DIGEST],
            "feature_schema_versions": ["v1"],
        },
        "attempts": [
            {
                "attempt": 1,
                "started_at": "2026-01-01T00:00:00Z",
                "finished_at": "2026-01-01T00:00:10Z",
                "elapsed_seconds": 10.0,
                "status": "complete",
                "runtime": {
                    "scoring_workers": [dict(worker) for worker in workers]
                },
                "completion_runtime": {
                    "scoring_workers": [dict(worker) for worker in workers]
                },
                "work": [
                    {
                        "stage": "label_publish",
                        "timeline_date": "2018-04-01",
                        "seconds": 0.25,
                        "status": "complete",
                    },
                    {
                        "stage": "label_wait",
                        "timeline_date": "2018-04-01",
                        "seconds": 0.25,
                        "status": "complete",
                    },
                    {
                        "stage": "transaction_submit",
                        "timeline_date": "2018-04-01",
                        "seconds": 1.0,
                        "status": "complete",
                    },
                    {
                        "stage": "transaction_wait",
                        "timeline_date": "2018-04-01",
                        "seconds": 2.0,
                        "status": "complete",
                    },
                    {
                        "stage": "runtime_identity",
                        "timeline_date": "2018-04-01",
                        "seconds": 0.1,
                        "status": "complete",
                    },
                ],
            }
        ],
    }


def test_histogram_quantile_interpolates_cumulative_buckets():
    assert histogram_quantile(
        {0.01: 50, 0.1: 100, float("inf"): 100}, 0.95
    ) == pytest.approx(0.091)


def test_replay_performance_uses_acknowledged_path_timings():
    manifest = {
        "counts": {"transactions": 100, "labels": 100},
        "attempts": [
            {
                "attempt": 1,
                "started_at": "2026-01-01T00:00:00Z",
                "finished_at": "2026-01-01T00:00:04Z",
                "elapsed_seconds": 4.0,
                "status": "interrupted",
                "finish_inferred_at_resume": True,
                "work": [
                    {
                        "stage": "transaction_wait",
                        "seconds": 2.0,
                        "status": "interrupted",
                        "finish_inferred_at_attempt_close": True,
                    }
                ],
            },
            {
                "attempt": 2,
                "started_at": "2026-01-01T00:00:05Z",
                "finished_at": "2026-01-01T00:00:11Z",
                "elapsed_seconds": 6.0,
                "status": "complete",
                "work": [
                    {
                        "stage": "transaction_submit",
                        "seconds": 1.0,
                        "status": "complete",
                    },
                    {
                        "stage": "transaction_wait",
                        "seconds": 3.0,
                        "status": "complete",
                    },
                    {
                        "stage": "label_publish",
                        "seconds": 1.0,
                        "status": "complete",
                    },
                    {
                        "stage": "label_wait",
                        "seconds": 2.0,
                        "status": "complete",
                    },
                ],
            },
        ],
    }
    result = replay_performance(manifest)
    assert result["transaction_acknowledged_per_second"] == pytest.approx(100 / 6)
    assert result["end_to_end_transactions_per_second"] == 10
    assert result["max_daily_lag_drain_seconds"] == 5
    assert result["interrupted_attempts"] == 1
    assert result["inferred_finish_attempts"] == 1
    assert result["inferred_work_entries"] == 1


def test_replay_performance_rejects_an_open_attempt():
    manifest = _manifest("scale-1", 1)
    del manifest["attempts"][0]["finished_at"]
    with pytest.raises(ValueError, match="incomplete timing"):
        replay_performance(manifest)


def test_scaling_manifests_require_exact_unique_replica_set():
    primary = _manifest("full", 3)
    manifests = [
        _manifest("scale-3", 3),
        _manifest("scale-1", 1),
        _manifest("scale-2", 2),
    ]
    assert [item["run_id"] for item in validate_scaling_manifests(
        manifests, primary
    )] == ["scale-1", "scale-2", "scale-3"]

    with pytest.raises(ValueError, match="Duplicate 2-replica"):
        validate_scaling_manifests(
            [
                _manifest("scale-1", 1),
                _manifest("scale-2a", 2),
                _manifest("scale-2b", 2),
            ],
            primary,
        )


def test_scaling_manifests_reject_incompatible_workloads():
    primary = _manifest("full", 3)
    with pytest.raises(ValueError, match="incompatible workloads"):
        validate_scaling_manifests(
            [
                _manifest("scale-1", 1),
                _manifest("scale-2", 2, dataset_sha="different"),
                _manifest("scale-3", 3),
            ],
            primary,
        )


def test_runtime_model_evidence_covers_every_checkpoint():
    manifest = _manifest("full", 1)
    result = validate_runtime_model_evidence(manifest)
    assert result["checkpoints_verified"] == 1

    manifest["checkpoints"][0]["scoring_workers"][0]["model_digest"] = (
        "sha256:" + "b" * 64
    )
    with pytest.raises(ValueError, match="checkpoint.*unexpected runtime model"):
        validate_runtime_model_evidence(manifest)


def test_completed_replay_rejects_counts_that_disagree_with_checkpoints():
    manifest = _manifest("full", 1)
    assert validate_completed_replay(manifest)["checkpoints_verified"] == 1

    manifest["counts"]["transactions"] = 11
    with pytest.raises(ValueError, match="differ from checkpoints"):
        validate_completed_replay(manifest)


def test_completed_replay_rejects_a_durable_outcome_digest_mismatch():
    manifest = _manifest("full", 1)
    manifest["outcomes"]["model_digests"] = ["sha256:" + "b" * 64]

    with pytest.raises(ValueError, match="model_digests"):
        validate_completed_replay(manifest)


class _Rows:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows


class _ScalingConnection:
    def __init__(self, *, mismatched_run_id: str | None = None):
        self.mismatched_run_id = mismatched_run_id

    def execute(self, _query, parameters):
        escaped_prefix = parameters[0].removesuffix("%")
        run_id = escaped_prefix.removeprefix("replay-").removesuffix("-")
        digest = (
            "sha256:" + "b" * 64
            if run_id == self.mismatched_run_id
            else MODEL_DIGEST
        )
        rows = [
            (
                f"replay-{run_id}-{index}",
                {},
                0.5,
                "models:/fraudwatch-hgb/1",
                digest,
                "v1",
            )
            for index in range(10)
        ]
        return _Rows(rows)


def test_scaling_db_rows_must_match_the_manifest_model_digest():
    primary = _manifest("full", 3)
    manifests = [
        _manifest("scale-1", 1),
        _manifest("scale-2", 2),
        _manifest("scale-3", 3),
    ]

    with pytest.raises(AssertionError, match="scale-2.*unexpected model identity"):
        compare_scaling_runs(
            _ScalingConnection(mismatched_run_id="scale-2"),
            manifests,
            primary,
        )


def test_completed_replay_rejects_incomplete_stage_and_delivery_evidence():
    missing_stage = _manifest("full", 1)
    missing_stage["attempts"][0]["work"].pop()
    with pytest.raises(ValueError, match="incomplete stage evidence"):
        validate_completed_replay(missing_stage)

    wrong_delivery = _manifest("full", 1)
    wrong_delivery["checkpoints"][0]["labels"]["delivery"]["delivered"] = 9
    with pytest.raises(ValueError, match="delivery accounting"):
        validate_completed_replay(wrong_delivery)


def test_failed_phase_gates_are_explicit():
    gates = {
        "passing": {"passed": True},
        "failed": {"passed": False},
        "missing": {"observed": None},
    }
    assert failed_phase_gates(gates) == ["failed", "missing"]
    assert phase_gate_exit_code(gates) == 1
    assert phase_gate_exit_code({"passing": {"passed": True}}) == 0
