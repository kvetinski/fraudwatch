"""Reusable primitives for reproducible, resumable historical replays."""

from __future__ import annotations

import hashlib
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Iterable, Iterator, Sequence
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pandas as pd

from fraudwatch_ml.features import LABEL_DELAY_DAYS
from fraudwatch_ml.replay_data import select_day


MANIFEST_SCHEMA = "fraudwatch.replay-manifest.v1"
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
REQUIRED_COLUMNS = frozenset(
    {
        "TRANSACTION_ID",
        "TX_DATETIME",
        "CUSTOMER_ID",
        "TERMINAL_ID",
        "TX_AMOUNT",
        "TX_FRAUD",
    }
)


@dataclass(frozen=True)
class ReplayRecord:
    transaction: dict
    fraud_label: int
    occurred_at: datetime

    @property
    def transaction_id(self) -> str:
        return self.transaction["transaction_id"]

    @property
    def terminal_id(self) -> str:
        return self.transaction["terminal_id"]

    def label_event(self) -> dict:
        return {
            "transaction_id": self.transaction_id,
            "fraud_label": self.fraud_label,
            "observed_at": format_utc(
                self.occurred_at + timedelta(days=LABEL_DELAY_DAYS)
            ),
        }


def validate_run_id(run_id: str) -> str:
    if RUN_ID_PATTERN.fullmatch(run_id) is None:
        raise ValueError(
            "run ID must be 1-64 characters containing only letters, "
            "digits, dot, underscore, or hyphen"
        )
    return run_id


def format_utc(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def canonical_payload(event: dict) -> bytes:
    return json.dumps(
        event,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")


def iter_day_records(
    path: Path,
    *,
    run_id: str,
    limit: int | None,
) -> Iterator[ReplayRecord]:
    """Load one bounded source day and yield it in stable event-time order."""
    validate_run_id(run_id)
    if limit is not None and limit < 1:
        raise ValueError("daily limit must be positive or omitted")
    if not path.is_file():
        raise FileNotFoundError(path)

    frame = pd.read_pickle(path)
    missing = REQUIRED_COLUMNS.difference(frame.columns)
    if missing:
        raise ValueError(f"Dataset day is missing columns: {sorted(missing)}")

    if limit is None:
        selected = frame.sort_values(
            ["TX_DATETIME", "TRANSACTION_ID"], kind="stable"
        ).reset_index(drop=True)
    else:
        selected = select_day(frame, limit)

    for row in selected.itertuples(index=False):
        occurred = pd.Timestamp(row.TX_DATETIME)
        if occurred.tzinfo is None:
            occurred = occurred.tz_localize("UTC")
        else:
            occurred = occurred.tz_convert("UTC")
        occurred_at = occurred.to_pydatetime()
        transaction_id = f"replay-{run_id}-{row.TRANSACTION_ID}"
        yield ReplayRecord(
            transaction={
                "transaction_id": transaction_id,
                "customer_id": str(row.CUSTOMER_ID),
                "terminal_id": f"replay-{run_id}-{row.TERMINAL_ID}",
                "amount": float(row.TX_AMOUNT),
                "occurred_at": format_utc(occurred_at),
            },
            fraud_label=int(row.TX_FRAUD),
            occurred_at=occurred_at,
        )


def replay_timeline(start: date, source_days: int) -> Iterator[date]:
    if source_days < 1:
        raise ValueError("source_days must be positive")
    total_days = source_days + LABEL_DELAY_DAYS
    for offset in range(total_days):
        yield start + timedelta(days=offset)


def source_day_for_labels(
    timeline_day: date,
    *,
    start: date,
    source_days: int,
) -> date | None:
    candidate = timeline_day - timedelta(days=LABEL_DELAY_DAYS)
    end = start + timedelta(days=source_days - 1)
    return candidate if start <= candidate <= end else None


def source_day_for_transactions(
    timeline_day: date,
    *,
    start: date,
    source_days: int,
) -> date | None:
    end = start + timedelta(days=source_days - 1)
    return timeline_day if start <= timeline_day <= end else None


def sha256_file(path: Path, *, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(chunk_size), b""):
            digest.update(chunk)
    return digest.hexdigest()


def dataset_identity(paths: Sequence[tuple[date, Path]]) -> dict:
    """Fingerprint exact daily source bytes without loading the whole dataset."""
    aggregate = hashlib.sha256()
    files = []
    for day, path in paths:
        if not path.is_file():
            raise FileNotFoundError(path)
        file_digest = sha256_file(path)
        size = path.stat().st_size
        entry = {
            "date": day.isoformat(),
            "name": path.name,
            "bytes": size,
            "sha256": file_digest,
        }
        files.append(entry)
        aggregate.update(
            f"{entry['date']}\0{entry['name']}\0{size}\0{file_digest}\n".encode()
        )
    return {"sha256": aggregate.hexdigest(), "files": files}


def sha256_tree(path: Path) -> str:
    """Hash file contents and relative names for one immutable model bundle."""
    if not path.exists():
        raise FileNotFoundError(path)
    if path.is_file():
        return sha256_file(path)

    aggregate = hashlib.sha256()
    files = sorted(item for item in path.rglob("*") if item.is_file())
    if not files:
        raise ValueError(f"Model path contains no files: {path}")
    for item in files:
        relative = item.relative_to(path).as_posix()
        digest = sha256_file(item)
        size = item.stat().st_size
        aggregate.update(f"{relative}\0{size}\0{digest}\n".encode())
    return aggregate.hexdigest()


def atomic_write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


class ManifestStore:
    """Fail-closed manifest persistence with immutable resume configuration."""

    def __init__(self, path: Path, configuration: dict):
        self.path = path
        if path.exists():
            self.value = json.loads(path.read_text(encoding="utf-8"))
            if self.value.get("manifest_schema") != MANIFEST_SCHEMA:
                raise ValueError("Unsupported replay manifest schema")
            if self.value.get("configuration") != configuration:
                raise ValueError(
                    "Replay configuration differs from the existing manifest; "
                    "use a new run ID"
                )
        else:
            now = format_utc(datetime.now(timezone.utc))
            self.value = {
                "manifest_schema": MANIFEST_SCHEMA,
                "run_id": configuration["run_id"],
                "status": "created",
                "created_at": now,
                "updated_at": now,
                "attempts": [],
                "configuration": configuration,
                "checkpoints": [],
                "counts": {},
                "timings_seconds": {},
                "outcomes": None,
                "failure": None,
            }
            self._save()

    @property
    def completed_dates(self) -> set[str]:
        return {
            checkpoint["timeline_date"]
            for checkpoint in self.value["checkpoints"]
            if checkpoint.get("status") == "complete"
        }

    def begin_attempt(self, runtime: dict | None = None) -> None:
        now = format_utc(datetime.now(timezone.utc))
        self.value["status"] = "running"
        self.value["updated_at"] = now
        self.value["failure"] = None
        attempt = {"started_at": now}
        if runtime is not None:
            attempt["runtime"] = runtime
        self.value["attempts"].append(attempt)
        self._save()

    def checkpoint(self, checkpoint: dict) -> None:
        day = checkpoint["timeline_date"]
        if day in self.completed_dates:
            raise ValueError(f"Checkpoint is already complete: {day}")
        checkpoint = {**checkpoint, "status": "complete"}
        self.value["checkpoints"].append(checkpoint)
        self.value["updated_at"] = checkpoint["completed_at"]
        self._recalculate()
        self._save()

    def complete(self, outcomes: dict) -> None:
        now = format_utc(datetime.now(timezone.utc))
        self.value["status"] = "complete"
        self.value["updated_at"] = now
        self.value["outcomes"] = outcomes
        self.value["failure"] = None
        self._finish_attempt(now, "complete")
        self._recalculate()
        self._save()

    def fail(self, error: BaseException) -> None:
        now = format_utc(datetime.now(timezone.utc))
        self.value["status"] = "failed"
        self.value["updated_at"] = now
        self.value["failure"] = {
            "type": type(error).__name__,
            "message": str(error)[:2000],
            "at": now,
        }
        self._finish_attempt(now, "failed")
        self._recalculate()
        self._save()

    def _finish_attempt(self, now: str, status: str) -> None:
        if not self.value["attempts"]:
            return
        current = self.value["attempts"][-1]
        if "finished_at" not in current:
            current.update({"finished_at": now, "status": status})

    def _recalculate(self) -> None:
        checkpoints = [
            item for item in self.value["checkpoints"]
            if item.get("status") == "complete"
        ]
        self.value["counts"] = {
            "checkpoints_complete": len(checkpoints),
            "transactions": sum(
                item["transactions"]["expected"] for item in checkpoints
            ),
            "labels": sum(item["labels"]["expected"] for item in checkpoints),
        }
        timing_keys = {
            key
            for item in checkpoints
            for key in item.get("timings_seconds", {})
        }
        self.value["timings_seconds"] = {
            key: round(
                sum(item.get("timings_seconds", {}).get(key, 0.0) for item in checkpoints),
                6,
            )
            for key in sorted(timing_keys)
        }

    def _save(self) -> None:
        atomic_write_json(self.path, self.value)


class KafkaPublisher:
    """Synchronous all-ack publisher with delivery accounting."""

    def __init__(self, brokers: str):
        from confluent_kafka import Producer

        self._brokers = brokers
        self._producer = Producer(
            {
                "bootstrap.servers": brokers,
                "acks": "all",
                "enable.idempotence": True,
            }
        )

    def topology(self, topics: Sequence[str], *, timeout: float = 10.0) -> dict:
        metadata = self._producer.list_topics(timeout=timeout)
        result = {"brokers": len(metadata.brokers), "topics": {}}
        for topic in topics:
            topic_metadata = metadata.topics.get(topic)
            if topic_metadata is None or topic_metadata.error is not None:
                error = None if topic_metadata is None else topic_metadata.error
                raise RuntimeError(f"Kafka topic unavailable: {topic}: {error}")
            result["topics"][topic] = {
                "partitions": len(topic_metadata.partitions),
            }
        return result

    def consumer_groups(
        self,
        groups: dict[str, str],
        *,
        timeout: float = 30.0,
    ) -> dict:
        """Describe stable group membership without persisting ephemeral IDs."""
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": self._brokers})
        deadline = time.monotonic() + timeout
        last_error: BaseException | None = None
        while time.monotonic() < deadline:
            try:
                request_timeout = max(1.0, min(5.0, deadline - time.monotonic()))
                futures = admin.describe_consumer_groups(
                    list(groups), request_timeout=request_timeout
                )
                result = {}
                for group_id, expected_topic in groups.items():
                    description = futures[group_id].result(request_timeout + 1)
                    state = getattr(
                        description.state, "name", str(description.state)
                    )
                    assignments = sorted(
                        {
                            (partition.topic, partition.partition)
                            for member in description.members
                            for partition in member.assignment.topic_partitions
                        }
                    )
                    if state != "STABLE" or not description.members:
                        raise RuntimeError(
                            f"Consumer group {group_id} is not active and stable: "
                            f"state={state} members={len(description.members)}"
                        )
                    unexpected = [
                        [topic, partition]
                        for topic, partition in assignments
                        if topic != expected_topic
                    ]
                    if unexpected:
                        raise RuntimeError(
                            f"Consumer group {group_id} has unexpected "
                            f"assignments: {unexpected}"
                        )
                    result[group_id] = {
                        "state": state,
                        "members": len(description.members),
                        "partition_assignor": description.partition_assignor,
                        "assignments": [
                            {"topic": topic, "partition": partition}
                            for topic, partition in assignments
                        ],
                    }
                return result
            except Exception as exc:
                last_error = exc
                time.sleep(0.5)
        raise RuntimeError(
            f"Consumer groups did not become stable within {timeout:g}s: "
            f"{last_error}"
        ) from last_error

    def publish(self, topic: str, events: Sequence[tuple[str, dict]]) -> dict:
        if not events:
            return {"delivered": 0, "partitions": [], "offset_ranges": {}}

        deliveries = []

        def delivered(error, message) -> None:
            deliveries.append((error, message))

        for key, event in events:
            while True:
                try:
                    self._producer.produce(
                        topic,
                        key=key.encode("utf-8"),
                        value=canonical_payload(event),
                        on_delivery=delivered,
                    )
                    break
                except BufferError:
                    self._producer.poll(0.1)

        remaining = self._producer.flush(60.0)
        if remaining or len(deliveries) != len(events):
            raise RuntimeError(
                f"Kafka delivery incomplete for {topic}: "
                f"delivered={len(deliveries)} expected={len(events)} remaining={remaining}"
            )
        failures = [error for error, _ in deliveries if error is not None]
        if failures:
            raise RuntimeError(f"Kafka delivery failed for {topic}: {failures[0]}")

        offsets: dict[int, list[int]] = {}
        for _, message in deliveries:
            offsets.setdefault(message.partition(), []).append(message.offset())
        return {
            "delivered": len(deliveries),
            "partitions": sorted(offsets),
            "offset_ranges": {
                str(partition): {"first": min(values), "last": max(values)}
                for partition, values in sorted(offsets.items())
            },
        }


def _stable_lane(terminal_id: str, concurrency: int) -> int:
    digest = hashlib.sha256(terminal_id.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big") % concurrency


def ordered_http_lanes(
    records: Sequence[ReplayRecord], concurrency: int
) -> list[list[ReplayRecord]]:
    """Shard terminals while preserving event order within every terminal."""
    if concurrency < 1:
        raise ValueError("HTTP concurrency must be positive")
    lanes: list[list[ReplayRecord]] = [[] for _ in range(concurrency)]
    for record in records:
        lanes[_stable_lane(record.terminal_id, concurrency)].append(record)
    return lanes


def post_transaction(api_url: str, event: dict, timeout: float) -> bool:
    request = Request(
        api_url,
        data=canonical_payload(event),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            body = response.read()
            status = response.status
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")[:1000]
        raise RuntimeError(f"HTTP ingestion failed: {exc.code}: {detail}") from exc
    if status != 202:
        raise RuntimeError(f"HTTP ingestion returned {status}, expected 202")
    result = json.loads(body)
    if (
        result.get("transaction_id") != event["transaction_id"]
        or result.get("status") != "accepted"
        or type(result.get("duplicate")) is not bool
    ):
        raise RuntimeError("HTTP ingestion returned an invalid acceptance response")
    return result["duplicate"]


def submit_http_records(
    records: Sequence[ReplayRecord],
    *,
    api_url: str,
    concurrency: int,
    timeout: float,
    sender: Callable[[str, dict, float], bool] = post_transaction,
) -> dict:
    lanes = [lane for lane in ordered_http_lanes(records, concurrency) if lane]

    def submit_lane(lane: Sequence[ReplayRecord]) -> tuple[int, int]:
        accepted = 0
        duplicates = 0
        for record in lane:
            duplicate = sender(api_url, record.transaction, timeout)
            accepted += int(not duplicate)
            duplicates += int(duplicate)
        return accepted, duplicates

    accepted = 0
    duplicates = 0
    with ThreadPoolExecutor(max_workers=max(1, len(lanes))) as executor:
        for lane_accepted, lane_duplicates in executor.map(submit_lane, lanes):
            accepted += lane_accepted
            duplicates += lane_duplicates
    return {
        "submitted": len(records),
        "accepted_new": accepted,
        "accepted_duplicate": duplicates,
        "concurrency": concurrency,
    }


def wait_for_rows(
    conn,
    table: str,
    transaction_ids: Sequence[str],
    *,
    timeout: float,
    poll_interval: float = 0.25,
) -> int:
    if table not in {"fraud_decisions", "fraud_labels"}:
        raise ValueError("Unsupported replay result table")
    if not transaction_ids:
        return 0

    deadline = time.monotonic() + timeout
    count = 0
    while time.monotonic() < deadline:
        count = conn.execute(
            f"SELECT count(*) FROM {table} WHERE transaction_id = ANY(%s)",
            (list(transaction_ids),),
        ).fetchone()[0]
        if count == len(transaction_ids):
            return count
        time.sleep(poll_interval)
    raise TimeoutError(
        f"{table}: expected {len(transaction_ids)} rows, found {count} "
        f"after {timeout:g}s"
    )


def assert_daily_model_identity(
    conn,
    transaction_ids: Sequence[str],
    *,
    model_reference: str,
    feature_schema_version: str,
) -> None:
    if not transaction_ids:
        return
    rows = conn.execute(
        """
        SELECT DISTINCT model_reference, feature_schema_version
        FROM fraud_decisions
        WHERE transaction_id = ANY(%s)
        """,
        (list(transaction_ids),),
    ).fetchall()
    identities = {(row[0], row[1]) for row in rows}
    expected = {(model_reference, feature_schema_version)}
    if identities != expected:
        raise RuntimeError(
            f"Replay decisions used unexpected model/schema identities: {identities}"
        )


def _like_prefix(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return escaped + "%"


def collect_run_outcomes(conn, *, run_id: str, mode: str) -> dict:
    transaction_prefix = _like_prefix(f"replay-{run_id}-")
    terminal_prefix = _like_prefix(f"replay-{run_id}-")

    decisions = conn.execute(
        """
        SELECT count(*), array_agg(DISTINCT model_reference),
               array_agg(DISTINCT feature_schema_version)
        FROM fraud_decisions
        WHERE transaction_id LIKE %s ESCAPE '\\'
        """,
        (transaction_prefix,),
    ).fetchone()
    labels = conn.execute(
        "SELECT count(*) FROM fraud_labels WHERE transaction_id LIKE %s ESCAPE '\\'",
        (transaction_prefix,),
    ).fetchone()[0]
    pending_labels = conn.execute(
        """
        SELECT count(*) FROM pending_fraud_labels
        WHERE transaction_id LIKE %s ESCAPE '\\'
        """,
        (transaction_prefix,),
    ).fetchone()[0]
    quarantines = conn.execute(
        """
        SELECT count(*)
        FROM event_quarantine AS q
        JOIN valid_event_quarantine_links AS ql ON ql.quarantine_id = q.id
        JOIN valid_event_ledger AS l ON l.id = ql.event_ledger_id
        WHERE l.transaction_id LIKE %s ESCAPE '\\'
        """,
        (transaction_prefix,),
    ).fetchone()[0]
    blocks = conn.execute(
        """
        SELECT count(*) FROM terminal_order_blocks
        WHERE terminal_id LIKE %s ESCAPE '\\'
        """,
        (terminal_prefix,),
    ).fetchone()[0]

    result = {
        "decisions": decisions[0],
        "labels": labels,
        "pending_labels": pending_labels,
        "quarantines": quarantines,
        "terminal_blocks": blocks,
        "model_references": sorted(decisions[1] or []),
        "feature_schema_versions": sorted(decisions[2] or []),
    }

    if mode == "http":
        ingress = conn.execute(
            """
            SELECT count(*),
                   count(*) FILTER (WHERE o.delivered_at IS NOT NULL),
                   count(*) FILTER (WHERE o.delivered_at IS NULL)
            FROM transactions AS t
            JOIN outbox_events AS o USING (transaction_id)
            WHERE t.transaction_id LIKE %s ESCAPE '\\'
            """,
            (transaction_prefix,),
        ).fetchone()
        result["http_transactions"] = ingress[0]
        result["outbox_delivered"] = ingress[1]
        result["outbox_pending"] = ingress[2]
    return result


def assert_complete_outcomes(
    outcomes: dict,
    *,
    expected_transactions: int,
    expected_labels: int,
    model_reference: str,
    feature_schema_version: str,
    mode: str,
) -> None:
    expected = {
        "decisions": expected_transactions,
        "labels": expected_labels,
        "pending_labels": 0,
        "quarantines": 0,
        "terminal_blocks": 0,
        "model_references": [model_reference],
        "feature_schema_versions": [feature_schema_version],
    }
    for key, value in expected.items():
        if outcomes.get(key) != value:
            raise RuntimeError(
                f"Replay outcome {key}={outcomes.get(key)!r}, expected {value!r}"
            )
    if mode == "http":
        ingress_expected = {
            "http_transactions": expected_transactions,
            "outbox_delivered": expected_transactions,
            "outbox_pending": 0,
        }
        for key, value in ingress_expected.items():
            if outcomes.get(key) != value:
                raise RuntimeError(
                    f"Replay outcome {key}={outcomes.get(key)!r}, expected {value!r}"
                )


def transaction_ids(records: Iterable[ReplayRecord]) -> list[str]:
    return [record.transaction_id for record in records]
