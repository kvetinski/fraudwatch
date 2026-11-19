#!/usr/bin/env python3
"""Verify a completed replay in chunks and write Phase 3.12 reports."""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.request import urlopen

import mlflow.sklearn
import numpy as np
import pandas as pd
import psycopg
from prometheus_client.parser import text_string_to_metric_families

from fraudwatch_ml.benchmark_replay import atomic_write_json, sha256_tree
from fraudwatch_ml.features import FEATURE_NAMES
from fraudwatch_ml.historical_evaluation import (
    ParityAccumulator,
    StreamingFeatureBuilder,
    holdout_metrics,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATABASE_URL = (
    "postgres://fraudwatch:fraudwatch_dev@"
    "127.0.0.1:5434/fraudwatch?sslmode=disable"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument(
        "--manifest",
        type=Path,
        help="Defaults to artifacts/replays/RUN_ID/manifest.json",
    )
    parser.add_argument("--model-path", type=Path)
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
        help="Repeat in 1/2/3-replica order",
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
    if value.get("status") != "complete":
        raise ValueError(f"Replay manifest is not complete: {path}")
    return value


def query_decisions(conn, transaction_ids: list[str]) -> dict:
    rows = conn.execute(
        """
        SELECT transaction_id, feature_vector, fraud_score,
               model_reference, feature_schema_version
        FROM fraud_decisions
        WHERE transaction_id = ANY(%s)
        """,
        (transaction_ids,),
    ).fetchall()
    return {row[0]: row[1:] for row in rows}


def elapsed_attempt_seconds(manifest: dict) -> float:
    total = 0.0
    for attempt in manifest["attempts"]:
        if "finished_at" not in attempt:
            continue
        started = datetime.fromisoformat(attempt["started_at"].replace("Z", "+00:00"))
        finished = datetime.fromisoformat(attempt["finished_at"].replace("Z", "+00:00"))
        total += (finished - started).total_seconds()
    return total


def replay_performance(manifest: dict) -> dict:
    tx_seconds = sum(
        checkpoint["timings_seconds"].get("transaction_submit", 0.0)
        + checkpoint["timings_seconds"].get("transaction_wait", 0.0)
        for checkpoint in manifest["checkpoints"]
        if checkpoint["transactions"]["expected"]
    )
    label_seconds = sum(
        checkpoint["timings_seconds"].get("label_publish", 0.0)
        + checkpoint["timings_seconds"].get("label_wait", 0.0)
        for checkpoint in manifest["checkpoints"]
        if checkpoint["labels"]["expected"]
    )
    tx_count = manifest["counts"]["transactions"]
    label_count = manifest["counts"]["labels"]
    wall_seconds = elapsed_attempt_seconds(manifest)
    waits = [
        checkpoint["timings_seconds"].get("transaction_wait", 0.0)
        for checkpoint in manifest["checkpoints"]
        if checkpoint["transactions"]["expected"]
    ]
    return {
        "transactions": tx_count,
        "labels": label_count,
        "transaction_path_seconds": tx_seconds,
        "label_path_seconds": label_seconds,
        "replay_attempt_seconds": wall_seconds,
        "transaction_acknowledged_per_second": tx_count / tx_seconds,
        "end_to_end_transactions_per_second": tx_count / wall_seconds,
        "max_daily_lag_drain_seconds": max(waits, default=0.0),
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


def compare_scaling_runs(conn, manifests: list[dict]) -> dict | None:
    if not manifests:
        return None
    datasets = {item["configuration"]["dataset"]["sha256"] for item in manifests}
    models = {item["configuration"]["model"]["digest"] for item in manifests}
    if len(datasets) != 1 or len(models) != 1:
        raise ValueError("Scaling runs did not use identical dataset/model bytes")
    run_maps = []
    summaries = []
    for manifest in manifests:
        run_id = manifest["run_id"]
        prefix = f"replay-{run_id}-"
        rows = conn.execute(
            """
            SELECT transaction_id, feature_vector, fraud_score
            FROM fraud_decisions
            WHERE transaction_id LIKE %s ESCAPE '\\'
            """,
            (_escaped_prefix(run_id) + "%",),
        ).fetchall()
        run_maps.append(
            {
                transaction_id.removeprefix(prefix): (features, score)
                for transaction_id, features, score in rows
            }
        )
        summaries.append(
            {
                "run_id": run_id,
                "replicas": manifest["configuration"]["topology"][
                    "consumer_groups"
                ]["fraudwatch-scoring-v1"]["members"],
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
        for run in run_maps[1:]:
            features, score = run[key]
            vector = np.asarray([features[name] for name in FEATURE_NAMES])
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
        f"- Records: {holdout['records']:,}; prevalence: {holdout['fraud_prevalence']:.4%}",
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


def main() -> None:
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

    model_path = (args.model_path or Path(config["model"]["path"])).resolve()
    actual_digest = "sha256:" + sha256_tree(model_path)
    if actual_digest != config["model"]["digest"]:
        raise ValueError("Model bundle digest differs from replay manifest")
    model = mlflow.sklearn.load_model(str(model_path / "model"))
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
                    f"{day}: expected {len(transaction_ids)} decisions, found {len(stored)}"
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
                (stored[transaction_id][2], stored[transaction_id][3])
                for transaction_id in transaction_ids
            }
            expected_identity = {
                (config["model"]["reference"], config["feature_schema_version"])
            }
            if identities != expected_identity:
                raise AssertionError(f"{day}: unexpected model/schema identity {identities}")
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

        scaling_manifests = [load_manifest(path.resolve()) for path in args.scaling_manifest]
        scaling = compare_scaling_runs(conn, scaling_manifests)
        worker_metrics = collect_worker_metrics(args.metrics_url)
        performance = replay_performance(manifest)
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
    }
    report = {
        "report_schema": "fraudwatch.phase-3.12-report.v1",
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
        "database_and_workstation_resources": resources,
        "scaling": scaling,
        "september_holdout": holdout,
        "phase_gates": gates,
    }
    output_dir = (args.output_dir or manifest_path.parent).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "phase-3.12-report.json"
    markdown_path = output_dir / "phase-3.12-report.md"
    atomic_write_json(json_path, report)
    markdown_path.write_text(markdown_report(report), encoding="utf-8")
    print(f"JSON report: {json_path}")
    print(f"Markdown report: {markdown_path}")


if __name__ == "__main__":
    main()
