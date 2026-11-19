#!/usr/bin/env python3
"""Run a resumable, manifest-backed historical replay through FraudWatch."""

from __future__ import annotations

import argparse
import json
import os
import re
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import psycopg

from fraudwatch_ml.benchmark_replay import (
    KafkaPublisher,
    ManifestStore,
    assert_complete_outcomes,
    assert_daily_model_identity,
    collect_run_outcomes,
    dataset_identity,
    format_utc,
    iter_day_records,
    replay_timeline,
    sha256_tree,
    source_day_for_labels,
    source_day_for_transactions,
    submit_http_records,
    transaction_ids,
    validate_run_id,
    wait_for_rows,
)
from fraudwatch_ml.features import LABEL_DELAY_DAYS
from fraudwatch_ml.scoring import FEATURE_SCHEMA_VERSION


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA_DIR = ROOT / "data/handbook-raw/data"
DEFAULT_OUTPUT_DIR = ROOT / "artifacts/replays"
TRANSACTION_TOPIC = "transactions.v1"
LABEL_TOPIC = "fraud-labels.v1"


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected YYYY-MM-DD") from exc


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--run-id",
        required=True,
        help="Stable caller-supplied ID; rerun the same command to resume",
    )
    parser.add_argument(
        "--mode",
        choices=("kafka", "http"),
        default="kafka",
        help="Direct scoring capacity path or HTTP/outbox end-to-end path",
    )
    parser.add_argument("--start-date", type=parse_date, default=date(2018, 4, 1))
    parser.add_argument(
        "--days",
        type=int,
        default=183,
        help="Number of source days (seven trailing label days are automatic)",
    )
    parser.add_argument(
        "--per-day",
        type=int,
        default=0,
        help="Deterministic daily sample size; 0 streams every record",
    )
    parser.add_argument("--timeout", type=float, default=600.0)
    parser.add_argument("--dataset-dir", type=Path, default=DEFAULT_DATA_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--database-url",
        default=os.getenv(
            "DATABASE_URL",
            "postgres://fraudwatch:fraudwatch_dev@"
            "127.0.0.1:5434/fraudwatch?sslmode=disable",
        ),
    )
    parser.add_argument(
        "--kafka-brokers",
        default=os.getenv("KAFKA_BROKERS", "localhost:9092"),
    )
    parser.add_argument(
        "--api-url",
        default=os.getenv(
            "FRAUDWATCH_INGEST_URL",
            "http://127.0.0.1:8080/v1/transactions",
        ),
    )
    parser.add_argument("--http-concurrency", type=int, default=16)
    parser.add_argument("--http-timeout", type=float, default=15.0)
    parser.add_argument(
        "--model-reference",
        help="Concrete worker model, for example models:/fraudwatch-hgb/1",
    )
    parser.add_argument(
        "--model-digest",
        help="SHA-256 of the exact model bundle when --model-path is unavailable",
    )
    parser.add_argument(
        "--model-path",
        type=Path,
        help="File or directory whose content digest pins the replay model",
    )
    args = parser.parse_args()

    try:
        validate_run_id(args.run_id)
    except ValueError as exc:
        parser.error(str(exc))
    if args.days < 1:
        parser.error("--days must be positive")
    if args.per_day < 0:
        parser.error("--per-day must be zero or positive")
    if args.timeout <= 0 or args.http_timeout <= 0:
        parser.error("timeouts must be positive")
    if args.http_concurrency < 1:
        parser.error("--http-concurrency must be positive")
    return args


def resolve_model_identity(args: argparse.Namespace) -> dict:
    default_bundle = ROOT / "artifacts/candidate-model"
    model_path = args.model_path
    if model_path is None and default_bundle.is_dir():
        model_path = default_bundle

    bundle_manifest = None
    if model_path is not None:
        possible_manifest = model_path / "manifest.json" if model_path.is_dir() else None
        if possible_manifest is not None and possible_manifest.is_file():
            bundle_manifest = json.loads(possible_manifest.read_text(encoding="utf-8"))

    reference = args.model_reference
    if reference is None and bundle_manifest is not None:
        reference = bundle_manifest.get("model_reference")
    if not reference or not re.fullmatch(r"models:/[^/]+/[0-9]+", reference):
        raise ValueError(
            "A concrete --model-reference (models:/NAME/VERSION) is required; "
            "it may also come from artifacts/candidate-model/manifest.json"
        )

    supplied_digest = args.model_digest
    if supplied_digest is not None:
        supplied_digest = supplied_digest.removeprefix("sha256:").lower()
        if re.fullmatch(r"[0-9a-f]{64}", supplied_digest) is None:
            raise ValueError("--model-digest must be a 64-character SHA-256 hex value")

    calculated_digest = sha256_tree(model_path) if model_path is not None else None
    if supplied_digest is not None and calculated_digest is not None:
        if supplied_digest != calculated_digest:
            raise ValueError("--model-digest differs from --model-path contents")
    digest = calculated_digest or supplied_digest
    if digest is None:
        raise ValueError(
            "Provide --model-path or --model-digest so the manifest pins model bytes"
        )
    return {
        "reference": reference,
        "digest": f"sha256:{digest}",
        "path": str(model_path.resolve()) if model_path is not None else None,
    }


def build_source_paths(dataset_dir: Path, start: date, days: int):
    return [
        (
            start + timedelta(days=offset),
            dataset_dir / f"{(start + timedelta(days=offset)).isoformat()}.pkl",
        )
        for offset in range(days)
    ]


def elapsed(started: float) -> float:
    return round(time.perf_counter() - started, 6)


def main() -> None:
    args = parse_args()
    dataset_dir = args.dataset_dir.resolve()
    source_paths = build_source_paths(dataset_dir, args.start_date, args.days)
    source_by_day = dict(source_paths)
    dataset = dataset_identity(source_paths)
    dataset_hash_by_day = {
        date.fromisoformat(item["date"]): item["sha256"]
        for item in dataset["files"]
    }
    model = resolve_model_identity(args)
    publisher = KafkaPublisher(args.kafka_brokers)
    topology = publisher.topology((TRANSACTION_TOPIC, LABEL_TOPIC))
    consumer_groups = publisher.consumer_groups(
        {
            "fraudwatch-scoring-v1": TRANSACTION_TOPIC,
            "fraudwatch-labels-v1": LABEL_TOPIC,
        }
    )
    for group_id, topic in (
        ("fraudwatch-scoring-v1", TRANSACTION_TOPIC),
        ("fraudwatch-labels-v1", LABEL_TOPIC),
    ):
        assigned = len(consumer_groups[group_id]["assignments"])
        partitions = topology["topics"][topic]["partitions"]
        if assigned != partitions:
            raise RuntimeError(
                f"Consumer group {group_id} has {assigned}/{partitions} "
                "partitions assigned"
            )
    daily_limit = args.per_day or None

    configuration = {
        "run_id": args.run_id,
        "mode": args.mode,
        "dataset": {
            "directory": str(dataset_dir),
            "start_date": args.start_date.isoformat(),
            "end_date": source_paths[-1][0].isoformat(),
            "source_days": args.days,
            "per_day": daily_limit,
            **dataset,
        },
        "model": model,
        "feature_schema_version": FEATURE_SCHEMA_VERSION,
        "topology": {
            **topology,
            "transaction_topic": TRANSACTION_TOPIC,
            "label_topic": LABEL_TOPIC,
            "consumer_groups": consumer_groups,
            "label_delay_days": LABEL_DELAY_DAYS,
            "http_concurrency": args.http_concurrency if args.mode == "http" else None,
            "api_url": args.api_url if args.mode == "http" else None,
        },
    }
    manifest_path = args.output_dir.resolve() / args.run_id / "manifest.json"
    manifest = ManifestStore(manifest_path, configuration)
    if manifest.value["status"] == "complete":
        print(f"Replay already complete: {manifest_path}")
        print(json.dumps(manifest.value["outcomes"], indent=2, sort_keys=True))
        return

    runtime = {
        "started_at": format_utc(datetime.now(timezone.utc)),
        "processing_timeout_seconds": args.timeout,
        "http_timeout_seconds": args.http_timeout if args.mode == "http" else None,
    }
    manifest.begin_attempt(runtime)
    print(f"Replay manifest: {manifest_path}", flush=True)
    print(f"Replay ID: {args.run_id}", flush=True)
    print(f"Mode: {args.mode}", flush=True)
    print(f"Dataset: sha256:{dataset['sha256']}", flush=True)
    print(f"Model: {model['reference']} ({model['digest']})", flush=True)

    try:
        with psycopg.connect(args.database_url, autocommit=True) as conn:
            for timeline_day in replay_timeline(args.start_date, args.days):
                if timeline_day.isoformat() in manifest.completed_dates:
                    print(f"{timeline_day}: checkpoint already complete", flush=True)
                    continue

                checkpoint_started = time.perf_counter()
                timings = {}
                label_source_day = source_day_for_labels(
                    timeline_day,
                    start=args.start_date,
                    source_days=args.days,
                )
                labels = []
                if label_source_day is not None:
                    labels = [
                        record.label_event()
                        for record in iter_day_records(
                            source_by_day[label_source_day],
                            run_id=args.run_id,
                            limit=daily_limit,
                        )
                    ]

                started = time.perf_counter()
                label_delivery = publisher.publish(
                    LABEL_TOPIC,
                    [(event["transaction_id"], event) for event in labels],
                )
                timings["label_publish"] = elapsed(started)

                started = time.perf_counter()
                wait_for_rows(
                    conn,
                    "fraud_labels",
                    [event["transaction_id"] for event in labels],
                    timeout=args.timeout,
                )
                timings["label_wait"] = elapsed(started)

                transaction_source_day = source_day_for_transactions(
                    timeline_day,
                    start=args.start_date,
                    source_days=args.days,
                )
                records = []
                if transaction_source_day is not None:
                    records = list(
                        iter_day_records(
                            source_by_day[transaction_source_day],
                            run_id=args.run_id,
                            limit=daily_limit,
                        )
                    )

                started = time.perf_counter()
                if args.mode == "kafka":
                    transaction_submission = publisher.publish(
                        TRANSACTION_TOPIC,
                        [
                            (record.terminal_id, record.transaction)
                            for record in records
                        ],
                    )
                else:
                    transaction_submission = submit_http_records(
                        records,
                        api_url=args.api_url,
                        concurrency=args.http_concurrency,
                        timeout=args.http_timeout,
                    )
                timings["transaction_submit"] = elapsed(started)

                ids = transaction_ids(records)
                started = time.perf_counter()
                wait_for_rows(
                    conn,
                    "fraud_decisions",
                    ids,
                    timeout=args.timeout,
                )
                assert_daily_model_identity(
                    conn,
                    ids,
                    model_reference=model["reference"],
                    feature_schema_version=FEATURE_SCHEMA_VERSION,
                )
                timings["transaction_wait"] = elapsed(started)
                timings["checkpoint_total"] = elapsed(checkpoint_started)

                completed_at = format_utc(datetime.now(timezone.utc))
                checkpoint = {
                    "timeline_date": timeline_day.isoformat(),
                    "source_date": (
                        transaction_source_day.isoformat()
                        if transaction_source_day is not None else None
                    ),
                    "label_source_date": (
                        label_source_day.isoformat()
                        if label_source_day is not None else None
                    ),
                    "source_sha256": (
                        dataset_hash_by_day[transaction_source_day]
                        if transaction_source_day is not None else None
                    ),
                    "transactions": {
                        "expected": len(records),
                        "submission": transaction_submission,
                    },
                    "labels": {
                        "expected": len(labels),
                        "delivery": label_delivery,
                    },
                    "timings_seconds": timings,
                    "completed_at": completed_at,
                }
                manifest.checkpoint(checkpoint)
                print(
                    f"{timeline_day}: decisions={len(records)} "
                    f"labels={len(labels)} seconds={timings['checkpoint_total']:.3f}",
                    flush=True,
                )

            expected_transactions = manifest.value["counts"]["transactions"]
            expected_labels = manifest.value["counts"]["labels"]
            outcomes = collect_run_outcomes(
                conn,
                run_id=args.run_id,
                mode=args.mode,
            )
            assert_complete_outcomes(
                outcomes,
                expected_transactions=expected_transactions,
                expected_labels=expected_labels,
                model_reference=model["reference"],
                feature_schema_version=FEATURE_SCHEMA_VERSION,
                mode=args.mode,
            )
            manifest.complete(outcomes)
    except BaseException as exc:
        manifest.fail(exc)
        raise

    print("REPLAY COMPLETE", flush=True)
    print(json.dumps(manifest.value["outcomes"], indent=2, sort_keys=True))
    print(f"Manifest: {manifest_path}")


if __name__ == "__main__":
    main()
