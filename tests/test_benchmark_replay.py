from __future__ import annotations

import json
import threading
from collections import defaultdict
from datetime import date, datetime, timezone

import pandas as pd
import pytest

from fraudwatch_ml.benchmark_replay import (
    MANIFEST_SCHEMA,
    ManifestStore,
    ReplayRecord,
    assert_complete_outcomes,
    dataset_identity,
    iter_day_records,
    replay_timeline,
    source_day_for_labels,
    source_day_for_transactions,
    submit_http_records,
    validate_run_id,
)


def _record(transaction_id: str, terminal_id: str) -> ReplayRecord:
    occurred_at = datetime(2018, 4, 1, tzinfo=timezone.utc)
    return ReplayRecord(
        transaction={
            "transaction_id": transaction_id,
            "customer_id": "customer",
            "terminal_id": terminal_id,
            "amount": 1.0,
            "occurred_at": "2018-04-01T00:00:00Z",
        },
        fraud_label=0,
        occurred_at=occurred_at,
    )


def test_run_id_is_safe_for_event_ids_and_manifest_paths():
    assert validate_run_id("capacity-01_test.v1") == "capacity-01_test.v1"
    for invalid in ("", "../escape", "contains space", "x" * 65):
        with pytest.raises(ValueError):
            validate_run_id(invalid)


def test_day_reader_orders_records_and_keeps_only_one_day_in_memory(tmp_path):
    path = tmp_path / "2018-04-01.pkl"
    pd.DataFrame(
        {
            "TRANSACTION_ID": [30, 10, 20],
            "TX_DATETIME": [
                "2018-04-01T03:00:00",
                "2018-04-01T01:00:00",
                "2018-04-01T02:00:00",
            ],
            "CUSTOMER_ID": [3, 1, 2],
            "TERMINAL_ID": [8, 8, 9],
            "TX_AMOUNT": [3.0, 1.0, 2.0],
            "TX_FRAUD": [0, 1, 0],
        }
    ).to_pickle(path)

    records = list(iter_day_records(path, run_id="sample-1", limit=None))
    assert [item.transaction_id for item in records] == [
        "replay-sample-1-10",
        "replay-sample-1-20",
        "replay-sample-1-30",
    ]
    assert records[0].transaction["occurred_at"] == "2018-04-01T01:00:00Z"
    assert records[0].label_event()["observed_at"] == "2018-04-08T01:00:00Z"

    sampled = list(iter_day_records(path, run_id="sample-1", limit=2))
    assert [item.transaction_id for item in sampled] == [
        "replay-sample-1-10",
        "replay-sample-1-30",
    ]


def test_timeline_flushes_the_final_seven_days_of_labels():
    start = date(2018, 4, 1)
    timeline = list(replay_timeline(start, source_days=2))
    assert timeline == [date(2018, 4, day) for day in range(1, 10)]

    transaction_days = [
        source_day_for_transactions(day, start=start, source_days=2)
        for day in timeline
    ]
    label_days = [
        source_day_for_labels(day, start=start, source_days=2)
        for day in timeline
    ]
    assert [day for day in transaction_days if day is not None] == [
        date(2018, 4, 1),
        date(2018, 4, 2),
    ]
    assert [day for day in label_days if day is not None] == [
        date(2018, 4, 1),
        date(2018, 4, 2),
    ]
    assert label_days[-1] == date(2018, 4, 2)


def test_dataset_fingerprint_changes_with_source_bytes(tmp_path):
    first = tmp_path / "2018-04-01.pkl"
    second = tmp_path / "2018-04-02.pkl"
    first.write_bytes(b"day-one")
    second.write_bytes(b"day-two")
    paths = [(date(2018, 4, 1), first), (date(2018, 4, 2), second)]

    original = dataset_identity(paths)
    second.write_bytes(b"changed")
    changed = dataset_identity(paths)

    assert original["sha256"] != changed["sha256"]
    assert original["files"][0]["name"] == "2018-04-01.pkl"


def test_manifest_checkpoints_atomically_and_rejects_changed_resume(tmp_path):
    path = tmp_path / "run" / "manifest.json"
    configuration = {"run_id": "resume-1", "mode": "kafka"}
    store = ManifestStore(path, configuration)
    store.begin_attempt({"processing_timeout_seconds": 10})
    store.checkpoint(
        {
            "timeline_date": "2018-04-01",
            "transactions": {"expected": 3},
            "labels": {"expected": 1},
            "timings_seconds": {"checkpoint_total": 0.5},
            "completed_at": "2018-04-01T00:00:01Z",
        }
    )

    persisted = json.loads(path.read_text())
    assert persisted["manifest_schema"] == MANIFEST_SCHEMA
    assert persisted["counts"] == {
        "checkpoints_complete": 1,
        "transactions": 3,
        "labels": 1,
    }
    assert ManifestStore(path, configuration).completed_dates == {"2018-04-01"}
    with pytest.raises(ValueError, match="configuration differs"):
        ManifestStore(path, {"run_id": "resume-1", "mode": "http"})


def test_http_submission_is_concurrent_across_lanes_but_ordered_per_terminal():
    records = [
        _record("a-1", "terminal-a"),
        _record("b-1", "terminal-b"),
        _record("a-2", "terminal-a"),
        _record("b-2", "terminal-b"),
    ]
    observed = defaultdict(list)
    lock = threading.Lock()

    def sender(_url, event, _timeout):
        with lock:
            observed[event["terminal_id"]].append(event["transaction_id"])
        return event["transaction_id"] == "b-1"

    outcome = submit_http_records(
        records,
        api_url="http://example.test/v1/transactions",
        concurrency=8,
        timeout=1,
        sender=sender,
    )

    assert observed["terminal-a"] == ["a-1", "a-2"]
    assert observed["terminal-b"] == ["b-1", "b-2"]
    assert outcome == {
        "submitted": 4,
        "accepted_new": 3,
        "accepted_duplicate": 1,
        "concurrency": 8,
    }


def test_completion_gate_rejects_any_unexpected_quarantine():
    outcomes = {
        "decisions": 2,
        "labels": 2,
        "pending_labels": 0,
        "quarantines": 1,
        "terminal_blocks": 0,
        "model_references": ["models:/fraudwatch-hgb/1"],
        "feature_schema_versions": ["v1"],
    }
    with pytest.raises(RuntimeError, match="quarantines=1"):
        assert_complete_outcomes(
            outcomes,
            expected_transactions=2,
            expected_labels=2,
            model_reference="models:/fraudwatch-hgb/1",
            feature_schema_version="v1",
            mode="kafka",
        )
