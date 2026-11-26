#!/usr/bin/env python3
"""Verify a completed replay in chunks and write Phase 3.12 reports."""

from __future__ import annotations

import argparse
import json
import math
import os
import platform
import shutil
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.request import urlopen

import numpy as np
import pandas as pd
from prometheus_client.parser import text_string_to_metric_families

from fraudwatch_ml.benchmark_replay import (
    MANIFEST_SCHEMA,
    atomic_write_json,
    collect_worker_model_identities,
)
from fraudwatch_ml.features import FEATURE_NAMES
from fraudwatch_ml.historical_evaluation import (
    ParityAccumulator,
    StreamingFeatureBuilder,
    holdout_metrics,
)
from fraudwatch_ml.model_integrity import sha256_tree


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATABASE_URL = (
    "postgres://fraudwatch:fraudwatch_dev@"
    "127.0.0.1:5434/fraudwatch?sslmode=disable"
)
REPLAY_WORK_STAGES = frozenset(
    {
        "label_publish",
        "label_wait",
        "transaction_submit",
        "transaction_wait",
        "runtime_identity",
    }
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument(
        "--manifest",
        type=Path,
        help="Defaults to artifacts/replays/RUN_ID/manifest.json",
    )
    parser.add_argument(
        "--model-path",
        type=Path,
        help="Portable bundle or model artifact directory to verify and load",
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL),
    )
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument(
        "--scaling-manifest",
        action="append",
        type=Path,
        default=[],
        help="Repeat once for each compatible 1-, 2-, and 3-replica run",
    )
    parser.add_argument(
        "--metrics-url",
        action="append",
        default=[],
        help="Repeat for each scoring replica, e.g. http://127.0.0.1:9201/metrics",
    )
    return parser.parse_args()


def load_manifest(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if value.get("manifest_schema") != MANIFEST_SCHEMA:
        raise ValueError(
            f"Replay manifest does not use {MANIFEST_SCHEMA}: {path}"
        )
    if value.get("status") != "complete":
        raise ValueError(f"Replay manifest is not complete: {path}")
    if value.get("run_id") != value.get("configuration", {}).get("run_id"):
        raise ValueError(f"Replay manifest has inconsistent run IDs: {path}")
    return value


def query_decisions(conn, transaction_ids: list[str]) -> dict:
    rows = conn.execute(
        """
        SELECT transaction_id, feature_vector, fraud_score,
               model_reference, model_digest, feature_schema_version
        FROM fraud_decisions
        WHERE transaction_id = ANY(%s)
        """,
        (transaction_ids,),
    ).fetchall()
    return {row[0]: row[1:] for row in rows}


def elapsed_attempt_seconds(manifest: dict) -> float:
    attempts = manifest.get("attempts")
    if not attempts:
        raise ValueError("Replay manifest contains no attempts")
    total = 0.0
    for index, attempt in enumerate(attempts, 1):
        if attempt.get("attempt") != index:
            raise ValueError("Replay attempt numbers are not contiguous")
        if "finished_at" not in attempt or "elapsed_seconds" not in attempt:
            raise ValueError(f"Replay attempt {index} has incomplete timing evidence")
        if attempt.get("status") not in {"complete", "failed", "interrupted"}:
            raise ValueError(f"Replay attempt {index} has an invalid terminal status")
        started = datetime.fromisoformat(attempt["started_at"].replace("Z", "+00:00"))
        finished = datetime.fromisoformat(attempt["finished_at"].replace("Z", "+00:00"))
        measured = (finished - started).total_seconds()
        recorded = attempt["elapsed_seconds"]
        if (
            measured < 0
            or not isinstance(recorded, (int, float))
            or not math.isfinite(recorded)
            or recorded < 0
        ):
            raise ValueError(f"Replay attempt {index} has an invalid duration")
        if abs(measured - recorded) > 0.001:
            raise ValueError(f"Replay attempt {index} duration is inconsistent")
        if not isinstance(attempt.get("work"), list):
            raise ValueError(f"Replay attempt {index} has no work ledger")
        total += recorded
    if attempts[-1]["status"] != "complete":
        raise ValueError("Completed replay does not end with a completed attempt")
    if any(attempt["status"] == "complete" for attempt in attempts[:-1]):
        raise ValueError("Replay continued after an attempt was already complete")
    return total


def _work_entries(manifest: dict) -> list[dict]:
    entries = []
    for attempt in manifest["attempts"]:
        for item in attempt["work"]:
            if (
                not isinstance(item, dict)
                or item.get("stage") not in REPLAY_WORK_STAGES
            ):
                raise ValueError("Replay work ledger has an invalid stage")
            if item.get("status") not in {"complete", "failed", "interrupted"}:
                raise ValueError("Replay work ledger has an invalid status")
            seconds = item.get("seconds")
            if (
                not isinstance(seconds, (int, float))
                or not math.isfinite(seconds)
                or seconds < 0
            ):
                raise ValueError("Replay work ledger has an invalid duration")
            entries.append(item)
    if not entries:
        raise ValueError("Replay manifest contains no measured work")
    return entries


def replay_performance(manifest: dict) -> dict:
    wall_seconds = elapsed_attempt_seconds(manifest)
    work = _work_entries(manifest)
    tx_seconds = sum(
        item["seconds"]
        for item in work
        if item["stage"] in {"transaction_submit", "transaction_wait"}
    )
    label_seconds = sum(
        item["seconds"]
        for item in work
        if item["stage"] in {"label_publish", "label_wait"}
    )
    tx_count = manifest["counts"]["transactions"]
    label_count = manifest["counts"]["labels"]
    waits_by_day: dict[str, float] = {}
    for item in work:
        if item["stage"] == "transaction_wait":
            timeline_date = item.get("timeline_date", "unknown")
            waits_by_day[timeline_date] = (
                waits_by_day.get(timeline_date, 0.0) + item["seconds"]
            )
    if tx_count <= 0 or tx_seconds <= 0 or wall_seconds <= 0:
        raise ValueError("Replay has insufficient transaction timing evidence")
    return {
        "transactions": tx_count,
        "labels": label_count,
        "transaction_path_seconds": tx_seconds,
        "label_path_seconds": label_seconds,
        "replay_attempt_seconds": wall_seconds,
        "replay_measured_work_seconds": sum(item["seconds"] for item in work),
        "attempts": len(manifest["attempts"]),
        "interrupted_attempts": sum(
            item["status"] == "interrupted" for item in manifest["attempts"]
        ),
        "inferred_finish_attempts": sum(
            bool(item.get("finish_inferred_at_resume"))
            for item in manifest["attempts"]
        ),
        "inferred_work_entries": sum(
            bool(item.get("finish_inferred_at_attempt_close")) for item in work
        ),
        "transaction_acknowledged_per_second": tx_count / tx_seconds,
        "end_to_end_transactions_per_second": tx_count / wall_seconds,
        "max_daily_lag_drain_seconds": max(waits_by_day.values(), default=0.0),
    }


def histogram_quantile(cumulative: dict[float, float], fraction: float) -> float:
    if not cumulative:
        raise ValueError("Histogram has no buckets")
    total = cumulative[max(cumulative)]
    target = total * fraction
    previous_bound = 0.0
    previous_count = 0.0
    for bound in sorted(cumulative):
        count = cumulative[bound]
        if count >= target:
            if not np.isfinite(bound) or count == previous_count:
                return previous_bound
            position = (target - previous_count) / (count - previous_count)
            return previous_bound + position * (bound - previous_bound)
        previous_bound = bound
        previous_count = count
    return previous_bound


def collect_worker_metrics(urls: list[str]) -> dict | None:
    if not urls:
        return None
    buckets: dict[str, dict[float, float]] = {}
    sums: dict[str, float] = {}
    counts: dict[str, float] = {}
    for url in urls:
        with urlopen(url, timeout=5) as response:
            text = response.read().decode("utf-8")
        for family in text_string_to_metric_families(text):
            for sample in family.samples:
                stage = sample.labels.get("stage")
                if sample.name == "fraudwatch_scoring_stage_seconds_bucket" and stage:
                    bound = float(sample.labels["le"])
                    buckets.setdefault(stage, {})[bound] = (
                        buckets.setdefault(stage, {}).get(bound, 0.0) + sample.value
                    )
                elif sample.name == "fraudwatch_scoring_stage_seconds_sum" and stage:
                    sums[stage] = sums.get(stage, 0.0) + sample.value
                elif sample.name == "fraudwatch_scoring_stage_seconds_count" and stage:
                    counts[stage] = counts.get(stage, 0.0) + sample.value
    return {
        stage: {
            "samples": int(counts.get(stage, 0)),
            "mean_ms": 1000 * sums.get(stage, 0.0) / counts[stage],
            "p95_ms_histogram": 1000 * histogram_quantile(stage_buckets, 0.95),
        }
        for stage, stage_buckets in sorted(buckets.items())
        if counts.get(stage, 0)
    }


def _escaped_prefix(run_id: str) -> str:
    value = f"replay-{run_id}-"
    return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def _worker_identity(worker: dict) -> dict[str, str]:
    return {
        key: worker.get(key, "")
        for key in (
            "model_reference",
            "model_digest",
            "feature_schema_version",
        )
    }


def validate_runtime_model_evidence(manifest: dict) -> dict:
    config = manifest["configuration"]
    group = config["topology"]["consumer_groups"]["fraudwatch-scoring-v1"]
    expected_members = group["members"]
    if not isinstance(expected_members, int) or expected_members < 1:
        raise ValueError("Replay topology has an invalid scoring-worker count")
    expected_urls = config["topology"].get("scoring_metrics_urls")
    if (
        not isinstance(expected_urls, list)
        or any(not isinstance(url, str) or not url for url in expected_urls)
        or len(expected_urls) != expected_members
        or len(set(expected_urls)) != expected_members
    ):
        raise ValueError("Replay topology has incomplete scoring metrics endpoints")
    expected_identity = {
        "model_reference": config["model"]["reference"],
        "model_digest": config["model"]["digest"],
        "feature_schema_version": config["feature_schema_version"],
    }

    def validate_workers(workers: object, context: str) -> None:
        if not isinstance(workers, list) or len(workers) != expected_members:
            raise ValueError(f"{context} has incomplete worker model evidence")
        if any(not isinstance(worker, dict) for worker in workers):
            raise ValueError(f"{context} has invalid worker evidence")
        urls = [worker.get("metrics_url") for worker in workers]
        if any(not isinstance(url, str) or not url for url in urls):
            raise ValueError(f"{context} has invalid worker endpoints")
        if sorted(urls) != sorted(expected_urls) or len(set(urls)) != len(urls):
            raise ValueError(f"{context} worker endpoints differ from its topology")
        if any(_worker_identity(worker) != expected_identity for worker in workers):
            raise ValueError(f"{context} used an unexpected runtime model")

    for index, attempt in enumerate(manifest["attempts"], 1):
        validate_workers(
            attempt.get("runtime", {}).get("scoring_workers"),
            f"Replay attempt {index}",
        )

    checkpoints = manifest.get("checkpoints")
    if not isinstance(checkpoints, list) or not checkpoints:
        raise ValueError("Replay has no checkpoint-time worker model evidence")
    for checkpoint in checkpoints:
        timeline_date = checkpoint.get("timeline_date", "unknown")
        validate_workers(
            checkpoint.get("scoring_workers"),
            f"Replay checkpoint {timeline_date}",
        )

    completion_workers = manifest["attempts"][-1].get(
        "completion_runtime", {}
    ).get("scoring_workers")
    validate_workers(completion_workers, "Replay completion")
    return {
        "expected_identity": expected_identity,
        "worker_count": expected_members,
        "attempts_verified": len(manifest["attempts"]),
        "checkpoints_verified": len(checkpoints),
        "completion_verified": True,
    }


def validate_completed_replay(manifest: dict) -> dict:
    """Require internally complete counts, outcomes, timing, and model evidence."""
    config = manifest["configuration"]
    source_days = config["dataset"].get("source_days")
    label_delay_days = config["topology"].get("label_delay_days")
    if (
        not isinstance(source_days, int)
        or source_days < 1
        or not isinstance(label_delay_days, int)
        or label_delay_days < 0
    ):
        raise ValueError("Replay configuration has an invalid timeline")

    checkpoints = manifest.get("checkpoints")
    expected_checkpoints = source_days + label_delay_days
    if not isinstance(checkpoints, list) or len(checkpoints) != expected_checkpoints:
        raise ValueError(
            f"Replay has {len(checkpoints or [])}/{expected_checkpoints} checkpoints"
        )
    dates = [checkpoint.get("timeline_date") for checkpoint in checkpoints]
    try:
        start = date.fromisoformat(config["dataset"]["start_date"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("Replay configuration has an invalid start date") from exc
    expected_dates = [
        (start + timedelta(days=offset)).isoformat()
        for offset in range(expected_checkpoints)
    ]
    if (
        any(checkpoint.get("status") != "complete" for checkpoint in checkpoints)
        or dates != expected_dates
    ):
        raise ValueError("Replay checkpoints are incomplete or out of sequence")

    expected_transactions = 0
    expected_labels = 0
    mode = config.get("mode")
    if mode not in {"kafka", "http"}:
        raise ValueError("Replay configuration has an invalid input mode")
    for checkpoint in checkpoints:
        transaction_count = checkpoint.get("transactions", {}).get("expected")
        label_count = checkpoint.get("labels", {}).get("expected")
        if (
            type(transaction_count) is not int
            or transaction_count < 0
            or type(label_count) is not int
            or label_count < 0
        ):
            raise ValueError("Replay checkpoint has invalid expected counts")
        transaction_submission = checkpoint["transactions"].get("submission", {})
        if mode == "kafka":
            submitted_transactions = transaction_submission.get("delivered")
        else:
            submitted_transactions = transaction_submission.get("submitted")
            accepted = transaction_submission.get("accepted_new")
            duplicates = transaction_submission.get("accepted_duplicate")
            if (
                type(accepted) is not int
                or type(duplicates) is not int
                or accepted < 0
                or duplicates < 0
                or accepted + duplicates != transaction_count
            ):
                raise ValueError("Replay HTTP submission accounting is incomplete")
        delivered_labels = checkpoint["labels"].get("delivery", {}).get(
            "delivered"
        )
        if (
            submitted_transactions != transaction_count
            or delivered_labels != label_count
        ):
            raise ValueError("Replay delivery accounting differs from expected counts")
        expected_transactions += transaction_count
        expected_labels += label_count
    counts = manifest.get("counts", {})
    expected_counts = {
        "checkpoints_complete": expected_checkpoints,
        "transactions": expected_transactions,
        "labels": expected_labels,
    }
    if counts != expected_counts:
        raise ValueError(
            f"Replay counts {counts!r} differ from checkpoints {expected_counts!r}"
        )
    if expected_transactions < 1 or expected_labels != expected_transactions:
        raise ValueError("Replay transaction and delayed-label counts are incomplete")

    outcomes = manifest.get("outcomes")
    if not isinstance(outcomes, dict):
        raise ValueError("Replay has no durable outcome evidence")
    expected_outcomes = {
        "decisions": expected_transactions,
        "labels": expected_labels,
        "pending_labels": 0,
        "quarantines": 0,
        "terminal_blocks": 0,
        "model_references": [config["model"]["reference"]],
        "model_digests": [config["model"]["digest"]],
        "feature_schema_versions": [config["feature_schema_version"]],
    }
    if config["mode"] == "http":
        expected_outcomes.update(
            {
                "http_transactions": expected_transactions,
                "outbox_delivered": expected_transactions,
                "outbox_pending": 0,
            }
        )
    for key, expected in expected_outcomes.items():
        if outcomes.get(key) != expected:
            raise ValueError(
                f"Replay outcome {key}={outcomes.get(key)!r}, expected {expected!r}"
            )

    work = _work_entries(manifest)
    for timeline_date in expected_dates:
        completed_stages = {
            item["stage"]
            for item in work
            if item.get("timeline_date") == timeline_date
            and item["status"] == "complete"
        }
        if completed_stages != REPLAY_WORK_STAGES:
            raise ValueError(
                f"Replay checkpoint {timeline_date} has incomplete stage evidence"
            )
    if any(item.get("timeline_date") not in set(expected_dates) for item in work):
        raise ValueError("Replay work ledger refers to an unknown timeline date")

    runtime = validate_runtime_model_evidence(manifest)
    performance = replay_performance(manifest)
    return {
        "checkpoints_verified": expected_checkpoints,
        "runtime_model_evidence": runtime,
        "performance": performance,
    }


def _scaling_workload(manifest: dict) -> dict:
    config = manifest["configuration"]
    dataset = {
        key: value
        for key, value in config["dataset"].items()
        if key != "directory"
    }
    topology = config["topology"]
    return {
        "mode": config["mode"],
        "dataset": dataset,
        "model": {
            "reference": config["model"]["reference"],
            "digest": config["model"]["digest"],
        },
        "feature_schema_version": config["feature_schema_version"],
        "stream": {
            "topics": topology["topics"],
            "transaction_topic": topology["transaction_topic"],
            "label_topic": topology["label_topic"],
            "label_delay_days": topology["label_delay_days"],
            "http_concurrency": topology.get("http_concurrency"),
        },
    }


def _replica_count(manifest: dict) -> int:
    return manifest["configuration"]["topology"]["consumer_groups"][
        "fraudwatch-scoring-v1"
    ]["members"]


def validate_scaling_manifests(
    manifests: list[dict],
    primary_manifest: dict,
) -> list[dict]:
    if len(manifests) != 3:
        raise ValueError("Exactly three scaling manifests are required")
    run_ids = [manifest["run_id"] for manifest in manifests]
    if len(set(run_ids)) != len(run_ids):
        raise ValueError("Scaling manifests must have unique run IDs")

    by_replicas: dict[int, dict] = {}
    for manifest in manifests:
        replicas = _replica_count(manifest)
        if replicas in by_replicas:
            raise ValueError(f"Duplicate {replicas}-replica scaling manifest")
        by_replicas[replicas] = manifest
    if set(by_replicas) != {1, 2, 3}:
        raise ValueError("Scaling manifests must prove exactly 1, 2, and 3 replicas")

    ordered = [by_replicas[replicas] for replicas in (1, 2, 3)]
    workload = _scaling_workload(ordered[0])
    if any(_scaling_workload(item) != workload for item in ordered[1:]):
        raise ValueError("Scaling manifests used incompatible workloads")
    expected_count = ordered[0]["counts"].get("transactions", 0)
    if expected_count <= 0 or any(
        item["counts"].get("transactions") != expected_count
        for item in ordered[1:]
    ):
        raise ValueError("Scaling manifests have incompatible transaction counts")

    primary = primary_manifest["configuration"]
    for key in ("reference", "digest"):
        if workload["model"][key] != primary["model"][key]:
            raise ValueError("Scaling and full replays used different model bytes")
    if workload["feature_schema_version"] != primary["feature_schema_version"]:
        raise ValueError("Scaling and full replays used different feature schemas")
    if workload["mode"] != primary["mode"]:
        raise ValueError("Scaling and full replays used different input modes")
    primary_topology = primary["topology"]
    if (
        workload["stream"]["transaction_topic"]
        != primary_topology["transaction_topic"]
        or workload["stream"]["topics"][
            workload["stream"]["transaction_topic"]
        ]
        != primary_topology["topics"][primary_topology["transaction_topic"]]
    ):
        raise ValueError("Scaling and full replays used different transaction topics")

    for manifest in ordered:
        validate_completed_replay(manifest)
    return ordered


def compare_scaling_runs(
    conn,
    manifests: list[dict],
    primary_manifest: dict,
) -> dict:
    manifests = validate_scaling_manifests(manifests, primary_manifest)
    run_maps = []
    summaries = []
    for manifest in manifests:
        run_id = manifest["run_id"]
        prefix = f"replay-{run_id}-"
        rows = conn.execute(
            """
            SELECT transaction_id, feature_vector, fraud_score,
                   model_reference, model_digest, feature_schema_version
            FROM fraud_decisions
            WHERE transaction_id LIKE %s ESCAPE '\\'
            """,
            (_escaped_prefix(run_id) + "%",),
        ).fetchall()
        config = manifest["configuration"]
        expected_identity = {
            (
                config["model"]["reference"],
                config["model"]["digest"],
                config["feature_schema_version"],
            )
        }
        identities = {(row[3], row[4], row[5]) for row in rows}
        if identities != expected_identity:
            raise AssertionError(
                f"Scaling run {run_id} used unexpected model identity: "
                f"{identities!r}; expected {expected_identity!r}"
            )
        run_map = {
            transaction_id.removeprefix(prefix): (features, score)
            for transaction_id, features, score, *_identity in rows
        }
        expected_count = manifest["counts"]["transactions"]
        if len(rows) != expected_count or len(run_map) != expected_count:
            raise AssertionError(
                f"Scaling run {run_id} has {len(run_map)} decisions, "
                f"expected {expected_count}"
            )
        run_maps.append(run_map)
        summaries.append(
            {
                "run_id": run_id,
                "replicas": _replica_count(manifest),
                **replay_performance(manifest),
            }
        )
    keys = set(run_maps[0])
    if any(set(run) != keys for run in run_maps[1:]):
        raise AssertionError("Scaling runs contain different source transactions")
    max_feature_error = 0.0
    max_score_error = 0.0
    for key in keys:
        base_features, base_score = run_maps[0][key]
        base_vector = np.asarray([base_features[name] for name in FEATURE_NAMES])
        if not np.all(np.isfinite(base_vector)) or not np.isfinite(base_score):
            raise AssertionError("Scaling run contains a non-finite decision")
        for run in run_maps[1:]:
            features, score = run[key]
            vector = np.asarray([features[name] for name in FEATURE_NAMES])
            if not np.all(np.isfinite(vector)) or not np.isfinite(score):
                raise AssertionError("Scaling run contains a non-finite decision")
            max_feature_error = max(
                max_feature_error, float(np.max(np.abs(vector - base_vector)))
            )
            max_score_error = max(max_score_error, abs(score - base_score))
    if max_feature_error != 0 or max_score_error != 0:
        raise AssertionError("Scaling replicas produced non-identical decisions")
    return {
        "runs": summaries,
        "compared_transactions": len(keys),
        "max_feature_absolute_error": max_feature_error,
        "max_score_absolute_error": max_score_error,
        "identical": True,
    }


def workstation_resources(conn) -> dict:
    memory_kib = {}
    for line in Path("/proc/meminfo").read_text().splitlines():
        name, value = line.split(":", 1)
        memory_kib[name] = int(value.strip().split()[0])
    disk = shutil.disk_usage(ROOT)
    database = conn.execute(
        """
        SELECT pg_database_size(current_database()),
               pg_total_relation_size('fraud_decisions'),
               pg_total_relation_size('fraud_decision_versions'),
               pg_total_relation_size('valid_event_ledger'),
               pg_total_relation_size('scored_transactions'),
               pg_total_relation_size('fraud_labels')
        """
    ).fetchone()
    return {
        "platform": platform.platform(),
        "logical_cpus": os.cpu_count(),
        "memory_total_bytes": memory_kib["MemTotal"] * 1024,
        "disk_total_bytes": disk.total,
        "disk_free_bytes_at_report": disk.free,
        "postgres_bytes": {
            "database": database[0],
            "fraud_decisions": database[1],
            "fraud_decision_versions": database[2],
            "valid_event_ledger": database[3],
            "scored_transactions": database[4],
            "fraud_labels": database[5],
        },
    }


def failed_phase_gates(gates: dict) -> list[str]:
    return [name for name, value in gates.items() if value.get("passed") is not True]


def phase_gate_exit_code(gates: dict) -> int:
    return 1 if failed_phase_gates(gates) else 0


def markdown_report(report: dict) -> str:
    perf = report["performance"]
    parity = report["correctness"]
    holdout = report["september_holdout"]
    gates = report["phase_gates"]
    outcomes = report["outcomes"]
    metrics = report.get("worker_metrics") or {}
    scaling = report.get("scaling")
    resources = report["database_and_workstation_resources"]
    lines = [
        "# FraudWatch Phase 3.12 report",
        "",
        f"Replay: `{report['run_id']}`  ",
        f"Dataset: `{report['dataset']['sha256']}`  ",
        f"Model: `{report['model']['reference']}` (`{report['model']['digest']}`)",
        "Overall gate result: "
        f"**{'PASS' if report['phase_gate_summary']['passed'] else 'FAIL'}**",
        "",
        "## Durable outcomes",
        "",
        f"- Decisions: {outcomes['decisions']:,}",
        f"- Labels: {outcomes['labels']:,}",
        f"- Pending labels: {outcomes['pending_labels']:,}",
        f"- Quarantines: {outcomes['quarantines']:,}",
        f"- Terminal blocks: {outcomes['terminal_blocks']:,}",
        "",
        "## Correctness",
        "",
        f"- Compared: {parity['records_compared']:,} decisions",
        f"- Maximum feature absolute error: {parity['max_feature_absolute_error']:.3g}",
        f"- Maximum score absolute error: {parity['max_score_absolute_error']:.3g}",
        "- Feature tolerance: atol=1e-6; score tolerance: rtol=1e-8, atol=1e-10",
        "",
        "## Performance",
        "",
        f"- Replay attempt time: {perf['replay_attempt_seconds']:.3f} s",
        f"- Attempts: {perf['attempts']} "
        f"({perf['interrupted_attempts']} interrupted; "
        f"{perf['inferred_finish_attempts']} inferred finishes)",
        f"- Inferred in-flight work entries: {perf['inferred_work_entries']}",
        f"- Transaction path: {perf['transaction_acknowledged_per_second']:.1f} tx/s",
        f"- End-to-end replay: {perf['end_to_end_transactions_per_second']:.1f} tx/s",
        f"- Maximum daily lag drain: {perf['max_daily_lag_drain_seconds']:.3f} s",
        "",
        "## Worker latency",
        "",
        "| Stage | Samples | Mean (ms) | Histogram p95 (ms) |",
        "| --- | ---: | ---: | ---: |",
    ]
    lines.extend(
        f"| {stage} | {value['samples']:,} | {value['mean_ms']:.3f} | "
        f"{value['p95_ms_histogram']:.3f} |"
        for stage, value in metrics.items()
    )
    lines.extend([
        "",
        "## Replica scaling",
        "",
    ])
    if scaling:
        lines.extend([
            "| Replicas | Run | Acknowledged tx/s | End-to-end tx/s |",
            "| ---: | --- | ---: | ---: |",
        ])
        lines.extend(
            f"| {run['replicas']} | `{run['run_id']}` | "
            f"{run['transaction_acknowledged_per_second']:.1f} | "
            f"{run['end_to_end_transactions_per_second']:.1f} |"
            for run in scaling["runs"]
        )
        lines.extend([
            "",
            f"Compared {scaling['compared_transactions']:,} source transactions; "
            f"maximum feature error {scaling['max_feature_absolute_error']:.3g}, "
            f"maximum score error {scaling['max_score_absolute_error']:.3g}.",
        ])
    else:
        lines.append("No scaling manifests were supplied.")
    lines.extend([
        "",
        "## September holdout",
        "",
        f"- Records: {holdout['records']:,}; "
        f"prevalence: {holdout['fraud_prevalence']:.4%}",
        f"- Average precision: {holdout['average_precision']:.6f}",
        f"- ROC-AUC: {holdout['roc_auc']:.6f}",
        f"- Brier score: {holdout['brier_score']:.6f}",
        f"- Precision@1%: {holdout['precision_at_1pct']:.4%}",
        f"- Recall@1%: {holdout['recall_at_1pct']:.4%}",
        "",
        "### Fraud-scenario recall",
        "",
        "| Scenario | Cases | Detected | Recall |",
        "| ---: | ---: | ---: | ---: |",
    ])
    lines.extend(
        f"| {scenario} | {value['fraud_cases']:,} | {value['detected']:,} | "
        f"{value['recall']:.4%} |"
        for scenario, value in sorted(
            holdout["scenario_recall"].items(), key=lambda item: int(item[0])
        )
    )
    lines.extend([
        "",
        "### Quantile calibration",
        "",
        "| Bin | Mean predicted | Observed fraud fraction |",
        "| ---: | ---: | ---: |",
    ])
    lines.extend(
        f"| {index} | {value['mean_predicted_probability']:.6f} | "
        f"{value['observed_fraud_fraction']:.6f} |"
        for index, value in enumerate(holdout["calibration_quantile_bins"], 1)
    )
    lines.extend([
        "",
        "## Workstation and database",
        "",
        f"- Platform: `{resources['platform']}`",
        f"- Logical CPUs: {resources['logical_cpus']}",
        f"- Memory: {resources['memory_total_bytes'] / (1024 ** 3):.2f} GiB",
        f"- Free disk at report: "
        f"{resources['disk_free_bytes_at_report'] / (1024 ** 3):.2f} GiB",
        "",
        "| PostgreSQL object | Size (MiB) |",
        "| --- | ---: |",
    ])
    lines.extend(
        f"| {name} | {size / (1024 ** 2):.1f} |"
        for name, size in resources["postgres_bytes"].items()
    )
    lines.extend([
        "",
        "## Phase gates",
        "",
    ])
    lines.extend(
        f"- {'PASS' if value['passed'] else 'FAIL'} — {name}: {value['observed']} "
        f"(target {value['target']})"
        for name, value in gates.items()
    )
    return "\n".join(lines) + "\n"


def main() -> int:
    import mlflow.sklearn
    import psycopg

    args = parse_args()
    manifest_path = args.manifest or (
        ROOT / "artifacts/replays" / args.run_id / "manifest.json"
    )
    manifest = load_manifest(manifest_path.resolve())
    if manifest["run_id"] != args.run_id:
        raise ValueError("--run-id differs from the manifest")
    config = manifest["configuration"]
    if config["dataset"]["per_day"] is not None:
        raise ValueError("Historical evaluation requires an unsampled replay")
    if config["dataset"]["source_days"] != 183:
        raise ValueError("Historical evaluation requires all 183 source days")
    primary_evidence = validate_completed_replay(manifest)
    runtime_model_evidence = primary_evidence["runtime_model_evidence"]
    performance = primary_evidence["performance"]

    configured_model_path = config["model"].get("path")
    if args.model_path is None and configured_model_path is None:
        raise ValueError("Provide --model-path to evaluate the pinned model bytes")
    model_path = (args.model_path or Path(configured_model_path)).resolve()
    nested_model_path = model_path / "model"
    model_artifact_path = (
        nested_model_path if nested_model_path.exists() else model_path
    )
    actual_digest = "sha256:" + sha256_tree(model_artifact_path)
    if actual_digest != config["model"]["digest"]:
        raise ValueError("Model artifact digest differs from replay manifest")
    model = mlflow.sklearn.load_model(str(model_artifact_path))
    scaling_manifests = [
        load_manifest(path.resolve()) for path in args.scaling_manifest
    ]
    validate_scaling_manifests(scaling_manifests, manifest)
    configured_metrics_urls = config["topology"]["scoring_metrics_urls"]
    if sorted(args.metrics_url) != sorted(configured_metrics_urls):
        raise ValueError(
            "Evaluation metrics URLs differ from the full replay topology"
        )
    evaluation_start_worker_identities = collect_worker_model_identities(
        args.metrics_url,
        expected_members=config["topology"]["consumer_groups"][
            "fraudwatch-scoring-v1"
        ]["members"],
        expected_reference=config["model"]["reference"],
        expected_digest=config["model"]["digest"],
        expected_feature_schema_version=config["feature_schema_version"],
    )
    dataset_dir = Path(config["dataset"]["directory"])
    start = date.fromisoformat(config["dataset"]["start_date"])

    builder = StreamingFeatureBuilder()
    parity = ParityAccumulator()
    holdout_labels = []
    holdout_scores = []
    holdout_ids = []
    holdout_scenarios = []
    with psycopg.connect(args.database_url, autocommit=True) as conn:
        for offset in range(config["dataset"]["source_days"]):
            day = start + timedelta(days=offset)
            frame = pd.read_pickle(dataset_dir / f"{day.isoformat()}.pkl").sort_values(
                ["TX_DATETIME", "TRANSACTION_ID"], kind="stable"
            ).reset_index(drop=True)
            vectors = []
            transaction_ids = []
            for row in frame.itertuples(index=False):
                vectors.append(
                    builder.build(
                        terminal_id=str(row.TERMINAL_ID),
                        occurred_at=row.TX_DATETIME,
                        amount=float(row.TX_AMOUNT),
                        fraud_label=int(row.TX_FRAUD),
                    )
                )
                transaction_ids.append(f"replay-{args.run_id}-{row.TRANSACTION_ID}")
            expected_features = np.vstack(vectors)
            expected_scores = model.predict_proba(
                pd.DataFrame(expected_features, columns=list(FEATURE_NAMES))
            )[:, 1]
            stored = query_decisions(conn, transaction_ids)
            if len(stored) != len(transaction_ids):
                raise AssertionError(
                    f"{day}: expected {len(transaction_ids)} decisions, "
                    f"found {len(stored)}"
                )
            actual_features = np.asarray(
                [
                    [stored[transaction_id][0][name] for name in FEATURE_NAMES]
                    for transaction_id in transaction_ids
                ],
                dtype=np.float64,
            )
            actual_scores = np.asarray(
                [stored[transaction_id][1] for transaction_id in transaction_ids]
            )
            identities = {
                (
                    stored[transaction_id][2],
                    stored[transaction_id][3],
                    stored[transaction_id][4],
                )
                for transaction_id in transaction_ids
            }
            expected_identity = {
                (
                    config["model"]["reference"],
                    config["model"]["digest"],
                    config["feature_schema_version"],
                )
            }
            if identities != expected_identity:
                raise AssertionError(
                    f"{day}: unexpected model/digest/schema identity {identities}"
                )
            parity.observe(
                expected_features,
                actual_features,
                expected_scores,
                actual_scores,
            )
            if day >= date(2018, 9, 1):
                holdout_labels.extend(frame["TX_FRAUD"].astype(int))
                holdout_scores.extend(actual_scores)
                holdout_ids.extend(frame["TRANSACTION_ID"].astype(str))
                holdout_scenarios.extend(frame["TX_FRAUD_SCENARIO"].astype(int))
            print(f"verified {day}: {len(frame)} rows", flush=True)

        scaling = compare_scaling_runs(conn, scaling_manifests, manifest)
        active_worker_identities = collect_worker_model_identities(
            args.metrics_url,
            expected_members=config["topology"]["consumer_groups"][
                "fraudwatch-scoring-v1"
            ]["members"],
            expected_reference=config["model"]["reference"],
            expected_digest=config["model"]["digest"],
            expected_feature_schema_version=config["feature_schema_version"],
        )
        worker_metrics = collect_worker_metrics(args.metrics_url)
        resources = workstation_resources(conn)

    holdout = holdout_metrics(
        np.asarray(holdout_labels, dtype=np.int8),
        np.asarray(holdout_scores, dtype=np.float64),
        np.asarray(holdout_ids),
        np.asarray(holdout_scenarios, dtype=np.int8),
    )
    three_replica_tps = None
    if scaling:
        three = [item for item in scaling["runs"] if item["replicas"] == 3]
        if three:
            three_replica_tps = three[0]["transaction_acknowledged_per_second"]
    total_p95 = None
    if worker_metrics and "total" in worker_metrics:
        total_p95 = worker_metrics["total"]["p95_ms_histogram"]
    gates = {
        "three_replica_throughput": {
            "observed": three_replica_tps,
            "target": ">= 500 acknowledged tx/s",
            "passed": three_replica_tps is not None and three_replica_tps >= 500,
        },
        "full_replay_time": {
            "observed": performance["replay_attempt_seconds"],
            "target": "< 3600 seconds",
            "passed": performance["replay_attempt_seconds"] < 3600,
        },
        "processing_p95": {
            "observed": total_p95,
            "target": "< 100 ms",
            "passed": total_p95 is not None and total_p95 < 100,
        },
        "lag_drain": {
            "observed": performance["max_daily_lag_drain_seconds"],
            "target": "< 300 seconds",
            "passed": performance["max_daily_lag_drain_seconds"] < 300,
        },
        "replica_result_identity": {
            "observed": scaling["identical"] if scaling else None,
            "target": "identical with 1/2/3 replicas",
            "passed": bool(scaling and scaling["identical"]),
        },
        "runtime_model_identity": {
            "observed": {
                "attempts_verified": runtime_model_evidence[
                    "attempts_verified"
                ],
                "checkpoints_verified": runtime_model_evidence[
                    "checkpoints_verified"
                ],
                "active_workers": len(active_worker_identities),
            },
            "target": "all replay attempts and active workers match pinned bytes",
            "passed": True,
        },
    }
    failed_gates = failed_phase_gates(gates)
    report = {
        "report_schema": "fraudwatch.phase-3.12-report.v2",
        "generated_at": datetime.now().astimezone().isoformat(),
        "run_id": args.run_id,
        "dataset": config["dataset"],
        "model": config["model"],
        "topology": config["topology"],
        "outcomes": manifest["outcomes"],
        "correctness": {
            "records_compared": parity.compared,
            "feature_atol": 1e-6,
            "score_rtol": 1e-8,
            "score_atol": 1e-10,
            "max_feature_absolute_error": parity.max_feature_absolute_error,
            "max_score_absolute_error": parity.max_score_absolute_error,
        },
        "performance": performance,
        "worker_metrics": worker_metrics,
        "runtime_model_evidence": runtime_model_evidence,
        "evaluation_start_worker_model_identities": (
            evaluation_start_worker_identities
        ),
        "active_worker_model_identities": active_worker_identities,
        "database_and_workstation_resources": resources,
        "scaling": scaling,
        "september_holdout": holdout,
        "phase_gates": gates,
        "phase_gate_summary": {
            "passed": not failed_gates,
            "failed": failed_gates,
        },
    }
    output_dir = (args.output_dir or manifest_path.parent).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "phase-3.12-report.json"
    markdown_path = output_dir / "phase-3.12-report.md"
    atomic_write_json(json_path, report)
    markdown_path.write_text(markdown_report(report), encoding="utf-8")
    print(f"JSON report: {json_path}")
    print(f"Markdown report: {markdown_path}")
    if failed_gates:
        print(
            "Phase 3.12 gates failed: " + ", ".join(failed_gates),
            file=sys.stderr,
        )
        return phase_gate_exit_code(gates)
    print("Phase 3.12 gates: PASS")
    return phase_gate_exit_code(gates)


if __name__ == "__main__":
    raise SystemExit(main())
