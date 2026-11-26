#!/usr/bin/env python3
"""Build a model baseline and persist drift/quality monitoring windows."""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import time
import traceback

import numpy as np
import psycopg
from psycopg.types.json import Jsonb

from fraudwatch_ml.features import FEATURE_NAMES
from fraudwatch_ml.model_integrity import (
    SHA256_PATTERN,
    verify_portable_model_digest,
)
from fraudwatch_ml.model_monitoring import (
    build_monitoring_baseline,
    evaluate_monitoring_window,
)

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BUNDLE = ROOT / "artifacts/candidate-model"
DEFAULT_DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgres://fraudwatch:fraudwatch_dev@127.0.0.1:5434/fraudwatch?sslmode=disable",
)


def utc_datetime(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise argparse.ArgumentTypeError("timestamp must include a timezone")
    return parsed.astimezone(timezone.utc)


def read_bundle(bundle: Path) -> tuple[dict, Path]:
    manifest_path = bundle / "manifest.json"
    if not manifest_path.is_file():
        raise SystemExit(f"Missing model manifest: {manifest_path}")
    return json.loads(manifest_path.read_text(encoding="utf-8")), manifest_path


def verified_baseline_identity(baseline: dict) -> dict:
    """Require a deployable byte identity before querying or persisting data."""
    identity = baseline.get("identity")
    if not isinstance(identity, dict):
        raise SystemExit("Monitoring baseline has no identity object")
    for field in (
        "model_reference",
        "feature_schema_version",
        "dataset_fingerprint",
    ):
        if not isinstance(identity.get(field), str) or not identity[field]:
            raise SystemExit(f"Monitoring baseline has an invalid {field}")
    digest = identity.get("model_digest")
    if not isinstance(digest, str) or SHA256_PATTERN.fullmatch(digest) is None:
        raise SystemExit("Monitoring baseline has an invalid model_digest")
    return identity


def latest_replay_report(model_reference: str, model_digest: str) -> dict | None:
    reports = sorted(
        (ROOT / "artifacts/replays").glob("*/phase-3.12-report.json"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    for path in reports:
        report = json.loads(path.read_text(encoding="utf-8"))
        model = report.get("model", {})
        if (
            model.get("reference") == model_reference
            and model.get("digest") == model_digest
        ):
            return report
    return None


def load_rows(
    conn,
    *,
    model_reference: str,
    model_digest: str,
    feature_schema_version: str,
    start: datetime | None,
    end: datetime | None,
    limit: int | None,
) -> tuple[np.ndarray, np.ndarray, list[int | None]]:
    clauses = [
        "fd.model_reference = %s",
        "fd.model_digest = %s",
        "fd.feature_schema_version = %s",
    ]
    parameters: list[object] = [
        model_reference,
        model_digest,
        feature_schema_version,
    ]
    if start is not None:
        clauses.append("fd.scored_at >= %s")
        parameters.append(start)
    if end is not None:
        clauses.append("fd.scored_at < %s")
        parameters.append(end)
    limit_sql = ""
    if limit is not None:
        limit_sql = "LIMIT %s"
        parameters.append(limit)
    rows = conn.execute(
        f"""
        SELECT fd.fraud_score, fd.feature_vector, st.fraud_label
        FROM fraud_decisions fd
        JOIN scored_transactions st USING (transaction_id)
        WHERE {' AND '.join(clauses)}
        ORDER BY fd.transaction_id
        {limit_sql}
        """,
        parameters,
    ).fetchall()
    if not rows:
        raise SystemExit("No decisions matched the model and time window")
    scores = np.asarray([row[0] for row in rows], dtype=np.float64)
    features = np.asarray(
        [[row[1][name] for name in FEATURE_NAMES] for row in rows],
        dtype=np.float64,
    )
    labels = [row[2] for row in rows]
    return features, scores, labels


def build_baseline(args) -> None:
    bundle = args.bundle.resolve()
    manifest, manifest_path = read_bundle(bundle)
    reference = manifest["model_reference"]
    model_digest = verify_portable_model_digest(bundle, manifest)
    feature_schema_version = manifest.get("feature_schema_version")
    if not isinstance(feature_schema_version, str) or not feature_schema_version:
        raise SystemExit("Model manifest has no feature schema version")
    with psycopg.connect(args.database_url) as conn:
        features, scores, labels = load_rows(
            conn,
            model_reference=reference,
            model_digest=model_digest,
            feature_schema_version=feature_schema_version,
            start=args.start,
            end=args.end,
            limit=args.max_samples,
        )
    if any(label is None for label in labels):
        raise SystemExit("Baseline rows must all have delayed labels")
    report = latest_replay_report(reference, model_digest)
    dataset_fingerprint = args.dataset_fingerprint
    if dataset_fingerprint is None and report:
        dataset_fingerprint = f"sha256:{report['dataset']['sha256']}"
    if not dataset_fingerprint:
        raise SystemExit("Provide --dataset-fingerprint or retain a matching Phase 3.12 report")
    baseline = build_monitoring_baseline(
        features,
        scores,
        labels,
        model_reference=reference,
        model_digest=model_digest,
        dataset_fingerprint=dataset_fingerprint,
        feature_schema_version=feature_schema_version,
    )
    if report and report.get("september_holdout"):
        baseline["validation_metrics"] = {
            name: report["september_holdout"].get(name)
            for name in (
                "fraud_prevalence", "average_precision", "roc_auc",
                "precision_at_1pct", "recall_at_1pct",
            )
        }
    output = args.output or bundle / "monitoring-baseline.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(baseline, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if output.parent == bundle:
        manifest.update({
            "model_digest": model_digest,
            "feature_schema_version": baseline["identity"]["feature_schema_version"],
            "dataset_fingerprint": dataset_fingerprint,
            "validation_metrics": baseline["validation_metrics"],
            "monitoring_baseline_available": True,
        })
        manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({
        "baseline": str(output),
        "model_reference": reference,
        "sample_count": baseline["sample_count"],
        "positive_count": baseline["positive_count"],
        "dataset_fingerprint": dataset_fingerprint,
    }, indent=2))


def evaluate(args, *, emit: bool = True) -> dict:
    baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
    identity = verified_baseline_identity(baseline)
    with psycopg.connect(args.database_url) as conn:
        features, scores, labels = load_rows(
            conn,
            model_reference=identity["model_reference"],
            model_digest=identity["model_digest"],
            feature_schema_version=identity["feature_schema_version"],
            start=args.start,
            end=args.end,
            limit=None,
        )
        result = evaluate_monitoring_window(features, scores, labels, baseline)
        conn.execute(
            """
            INSERT INTO model_monitor_snapshots (
                window_start, window_end, model_reference, model_digest,
                feature_schema_version, dataset_fingerprint, sample_count,
                labeled_count, positive_count, label_coverage, score_psi,
                feature_psi, fraud_prevalence, average_precision, roc_auc,
                precision_at_1pct, recall_at_1pct, quality_degradation,
                alert_eligible
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (
                model_reference, model_digest, feature_schema_version,
                window_start, window_end
            ) DO UPDATE SET
                dataset_fingerprint = EXCLUDED.dataset_fingerprint,
                sample_count = EXCLUDED.sample_count,
                labeled_count = EXCLUDED.labeled_count,
                positive_count = EXCLUDED.positive_count,
                label_coverage = EXCLUDED.label_coverage,
                score_psi = EXCLUDED.score_psi,
                feature_psi = EXCLUDED.feature_psi,
                fraud_prevalence = EXCLUDED.fraud_prevalence,
                average_precision = EXCLUDED.average_precision,
                roc_auc = EXCLUDED.roc_auc,
                precision_at_1pct = EXCLUDED.precision_at_1pct,
                recall_at_1pct = EXCLUDED.recall_at_1pct,
                quality_degradation = EXCLUDED.quality_degradation,
                alert_eligible = EXCLUDED.alert_eligible,
                evaluated_at = now()
            """,
            (
                args.start, args.end,
                identity["model_reference"], identity["model_digest"],
                identity["feature_schema_version"], identity["dataset_fingerprint"],
                result["sample_count"], result["labeled_count"], result["positive_count"],
                result["label_coverage"], result["score_psi"], Jsonb(result["feature_psi"]),
                result["fraud_prevalence"], result["average_precision"], result["roc_auc"],
                result["precision_at_1pct"], result["recall_at_1pct"],
                Jsonb(result["quality_degradation"]), result["alert_eligible"],
            ),
        )
        conn.commit()
    if emit:
        print(json.dumps(result, indent=2, sort_keys=True))
    return result


def schedule(args) -> None:
    """Evaluate one fixed delayed-label day repeatedly and idempotently."""
    if args.interval_seconds <= 0 or args.label_lag_days < 0:
        raise SystemExit("schedule interval must be positive and label lag nonnegative")
    while True:
        now = datetime.now(timezone.utc)
        day = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
        end = day - timedelta(days=args.label_lag_days)
        scheduled = argparse.Namespace(
            database_url=args.database_url,
            baseline=args.baseline,
            start=end - timedelta(days=1),
            end=end,
        )
        try:
            result = evaluate(scheduled, emit=False)
            print(json.dumps({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "severity": "INFO",
                "service": "model-monitor-evaluator",
                "build_revision": os.getenv("FRAUDWATCH_BUILD_REVISION", "development"),
                "message": "model monitoring window evaluated",
                "window_start": scheduled.start.isoformat(),
                "window_end": scheduled.end.isoformat(),
                "sample_count": result["sample_count"],
                "labeled_count": result["labeled_count"],
                "positive_count": result["positive_count"],
                "alert_eligible": result["alert_eligible"],
            }, separators=(",", ":")), flush=True)
        except (Exception, SystemExit) as exc:
            print(json.dumps({
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "service": "model-monitor-evaluator",
                "severity": "ERROR",
                "build_revision": os.getenv("FRAUDWATCH_BUILD_REVISION", "development"),
                "message": str(exc),
                "exception": traceback.format_exc(),
                "window_start": scheduled.start.isoformat(),
                "window_end": scheduled.end.isoformat(),
            }, separators=(",", ":")), flush=True)
        time.sleep(args.interval_seconds)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=DEFAULT_DATABASE_URL)
    subparsers = parser.add_subparsers(dest="command", required=True)

    baseline_parser = subparsers.add_parser("baseline", help="Build reference bins from labeled decisions")
    baseline_parser.add_argument("--bundle", type=Path, default=DEFAULT_BUNDLE)
    baseline_parser.add_argument("--output", type=Path)
    baseline_parser.add_argument("--start", type=utc_datetime)
    baseline_parser.add_argument("--end", type=utc_datetime)
    baseline_parser.add_argument("--max-samples", type=int, default=250_000)
    baseline_parser.add_argument("--dataset-fingerprint")
    baseline_parser.set_defaults(handler=build_baseline)

    now = datetime.now(timezone.utc)
    evaluate_parser = subparsers.add_parser("evaluate", help="Persist one monitoring window")
    evaluate_parser.add_argument("--baseline", type=Path, default=DEFAULT_BUNDLE / "monitoring-baseline.json")
    evaluate_parser.add_argument("--start", type=utc_datetime, default=now - timedelta(days=1))
    evaluate_parser.add_argument("--end", type=utc_datetime, default=now)
    evaluate_parser.set_defaults(handler=evaluate)

    schedule_parser = subparsers.add_parser("schedule", help="Continuously evaluate complete delayed-label days")
    schedule_parser.add_argument("--baseline", type=Path, default=DEFAULT_BUNDLE / "monitoring-baseline.json")
    schedule_parser.add_argument("--interval-seconds", type=float, default=3600)
    schedule_parser.add_argument("--label-lag-days", type=int, default=7)
    schedule_parser.set_defaults(handler=schedule)
    args = parser.parse_args()
    if hasattr(args, "max_samples") and args.max_samples < 1000:
        parser.error("--max-samples must be at least 1000")
    if getattr(args, "start", None) and getattr(args, "end", None) and args.end <= args.start:
        parser.error("--end must be after --start")
    args.handler(args)


if __name__ == "__main__":
    main()
