#!/usr/bin/env python3
"""FraudWatch event-quarantine operations.

Corrected transactions always pass through the Go ingestion API and its
transactional outbox. Recovery attempts use leases: an interrupted or ambiguous
submission must be reconciled against PostgreSQL before the same correction can
be retried. Raw payload retention applies only to resolved or ignored records.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import sys
import uuid
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import psycopg
from psycopg.rows import dict_row


DEFAULT_DSN = (
    "postgres://fraudwatch:fraudwatch_dev@"
    "127.0.0.1:5434/fraudwatch?sslmode=disable"
)
DEFAULT_API = "http://127.0.0.1:8080/v1/transactions"
ACTIVE_RECOVERY_STATES = ("sending", "accepted", "uncertain")


class OperationError(Exception):
    """Operator-visible error; does not imply a database rollback failure."""


def _db(dsn: str):
    return psycopg.connect(dsn, autocommit=True, row_factory=dict_row)


def _row(conn, qid: int, *, lock: bool = False):
    query = """
        SELECT id, event_type, source_topic, source_partition, source_offset,
               kafka_key, kafka_payload, source_payload_sha256,
               source_payload_was_null,
               reason_code, error_type, error_message,
               status, created_at, resolved_at, payload_purged_at
        FROM event_quarantine WHERE id = %s
    """ + (" FOR UPDATE" if lock else "")
    row = conn.execute(query, (qid,)).fetchone()
    if row is None:
        raise OperationError(f"Quarantine record {qid} not found")
    return row


def list_records(conn, status: str = "pending", limit: int = 30):
    if status not in ("pending", "resolved", "ignored", "all"):
        raise OperationError("Unsupported status")
    if not 1 <= limit <= 500:
        raise OperationError("Limit must be between 1 and 500")
    where = "" if status == "all" else "WHERE status = %s"
    params = (limit,) if status == "all" else (status, limit)
    return conn.execute(
        f"""
        SELECT id, event_type, source_topic, source_partition, source_offset,
               reason_code, left(error_message, 100) AS error_message,
               status, created_at, resolved_at, payload_purged_at
        FROM event_quarantine
        {where}
        ORDER BY created_at DESC, id DESC LIMIT %s
        """,
        params,
    ).fetchall()


def details(conn, qid: int):
    row = _row(conn, qid)
    attempts = conn.execute(
        """
        SELECT id, transaction_id, state, http_status, error_message,
               lease_token, lease_expires_at, created_at, updated_at
        FROM event_quarantine_recovery_attempts
        WHERE quarantine_id = %s ORDER BY id
        """,
        (qid,),
    ).fetchall()
    audit = conn.execute(
        """
        SELECT action, note, replacement_transaction_id, created_at
        FROM event_quarantine_audit
        WHERE quarantine_id = %s ORDER BY id
        """,
        (qid,),
    ).fetchall()
    return row, attempts, audit


def ignore_record(conn, qid: int, reason: str):
    reason = reason.strip()
    if not reason:
        raise OperationError("An explicit reason is required")
    with conn.transaction():
        row = _row(conn, qid, lock=True)
        if row["status"] != "pending":
            raise OperationError(f"Cannot ignore record in state {row['status']}")
        active = conn.execute(
            """
            SELECT 1 FROM event_quarantine_recovery_attempts
            WHERE quarantine_id = %s
              AND state IN ('sending', 'accepted', 'uncertain') LIMIT 1
            """,
            (qid,),
        ).fetchone()
        if active:
            raise OperationError(
                "A replacement may still be in flight; reconcile it first"
            )
        conn.execute(
            """
            UPDATE event_quarantine
            SET status = 'ignored', resolved_at = now()
            WHERE id = %s
            """,
            (qid,),
        )
        conn.execute(
            """
            INSERT INTO event_quarantine_audit(quarantine_id, action, note)
            VALUES (%s, 'ignored', %s)
            """,
            (qid, reason),
        )


def _reject_constant(name: str):
    raise OperationError(f"Invalid JSON numeric constant {name}")


def parse_correction(path: str) -> tuple[bytes, str]:
    payload = Path(path).read_bytes()
    try:
        data = json.loads(payload, parse_constant=_reject_constant)
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise OperationError(f"Invalid replacement JSON: {exc}") from exc
    if not isinstance(data, dict):
        raise OperationError("Corrected transaction must be a JSON object")
    txid = data.get("transaction_id")
    if not isinstance(txid, str) or not txid.strip():
        raise OperationError("Corrected transaction needs a nonempty transaction_id")
    return payload, txid


def _set_attempt(
    conn,
    attempt_id: int,
    lease_token: str,
    state: str,
    status=None,
    error=None,
):
    with conn.transaction():
        updated = conn.execute(
            """
            UPDATE event_quarantine_recovery_attempts
            SET state = %s, http_status = %s, error_message = %s,
                updated_at = now()
            WHERE id = %s AND lease_token = %s AND state = 'sending'
            RETURNING quarantine_id, transaction_id
            """,
            (
                state,
                status,
                str(error)[:1000] if error else None,
                attempt_id,
                lease_token,
            ),
        ).fetchone()
        if updated is None:
            raise OperationError(
                "Recovery lease is no longer active; run reconcile before acting"
            )
        note = json.dumps(
            {
                "attempt_id": attempt_id,
                "state": state,
                "http_status": status,
                "error": str(error)[:1000] if error else None,
            },
            sort_keys=True,
        )
        conn.execute(
            """
            INSERT INTO event_quarantine_audit (
                quarantine_id, action, note, replacement_transaction_id
            ) VALUES (%s, 'recovery_result', %s, %s)
            """,
            (updated["quarantine_id"], note, updated["transaction_id"]),
        )


def _durable_recovery_snapshot(conn, transaction_id: str):
    transaction = conn.execute(
        """
        SELECT payload_hash, payload
        FROM transactions WHERE transaction_id = %s
        """,
        (transaction_id,),
    ).fetchone()
    outbox = conn.execute(
        """
        SELECT payload, delivered_at
        FROM outbox_events WHERE transaction_id = %s
        """,
        (transaction_id,),
    ).fetchone()
    scored = conn.execute(
        "SELECT 1 FROM scored_transactions WHERE transaction_id = %s",
        (transaction_id,),
    ).fetchone()
    decision = conn.execute(
        """
        SELECT source_payload_hash
        FROM fraud_decisions WHERE transaction_id = %s
        """,
        (transaction_id,),
    ).fetchone()
    return transaction, outbox, scored, decision


def _validate_recovery_snapshot(snapshot):
    transaction, outbox, scored, decision = snapshot
    accepted = transaction is not None or outbox is not None
    scored_any = scored is not None or decision is not None

    if (transaction is None) != (outbox is None):
        raise OperationError("Go transaction and outbox rows are inconsistent")
    if (scored is None) != (decision is None):
        raise OperationError("Scored transaction and decision rows are inconsistent")
    if scored_any and not accepted:
        raise OperationError("Scoring rows exist without Go transaction/outbox rows")

    delivered = False
    if accepted:
        stored_payload = bytes(transaction["payload"])
        stored_hash = bytes(transaction["payload_hash"])
        if hashlib.sha256(stored_payload).digest() != stored_hash:
            raise OperationError("Go transaction payload hash is inconsistent")
        if bytes(outbox["payload"]) != stored_payload:
            raise OperationError("Go transaction and outbox payloads differ")
        delivered = outbox["delivered_at"] is not None
        if decision is not None:
            if bytes(decision["source_payload_hash"]) != stored_hash:
                raise OperationError("Scored payload hash differs from Go outbox payload")

    return {
        "accepted": accepted,
        "outbox_delivered": delivered,
        "scored": scored is not None and decision is not None,
    }


def reconcile_record(conn, qid: int):
    """Reconcile the active lease before retrying or resolving a correction."""
    problem = None
    report = None
    with conn.transaction():
        row = _row(conn, qid, lock=True)
        if row["status"] != "pending":
            raise OperationError(f"Cannot reconcile record in state {row['status']}")
        attempts = conn.execute(
            """
            SELECT id, transaction_id, state, lease_expires_at
            FROM event_quarantine_recovery_attempts
            WHERE quarantine_id = %s
              AND state IN ('sending', 'accepted', 'uncertain')
            ORDER BY id DESC
            FOR UPDATE
            """,
            (qid,),
        ).fetchall()
        if len(attempts) != 1:
            raise OperationError("Expected exactly one active recovery attempt")

        attempt = attempts[0]
        try:
            durable = _validate_recovery_snapshot(
                _durable_recovery_snapshot(conn, attempt["transaction_id"])
            )
        except OperationError as exc:
            durable = {
                "accepted": False,
                "outbox_delivered": False,
                "scored": False,
            }
            new_state = "uncertain"
            problem = str(exc)
        else:
            now = conn.execute(
                "SELECT now() AS current_time"
            ).fetchone()["current_time"]
            if durable["accepted"]:
                new_state = "accepted"
            elif attempt["state"] == "accepted":
                new_state = "uncertain"
                problem = "HTTP acceptance has no matching Go transaction/outbox rows"
            elif now < attempt["lease_expires_at"]:
                raise OperationError(
                    f"Recovery lease remains active until {attempt['lease_expires_at']}"
                )
            else:
                new_state = "retryable"

        conn.execute(
            """
            UPDATE event_quarantine_recovery_attempts
            SET state = %s, error_message = %s, updated_at = now()
            WHERE id = %s
            """,
            (new_state, problem, attempt["id"]),
        )
        note = json.dumps(
            {"attempt_id": attempt["id"], "state": new_state, **durable},
            sort_keys=True,
        )
        conn.execute(
            """
            INSERT INTO event_quarantine_audit (
                quarantine_id, action, note, replacement_transaction_id
            ) VALUES (%s, 'recovery_reconciled', %s, %s)
            """,
            (qid, note, attempt["transaction_id"]),
        )
        report = {
            "quarantine_id": qid,
            "attempt_id": attempt["id"],
            "transaction_id": attempt["transaction_id"],
            "state": new_state,
            **durable,
        }

    if problem is not None:
        raise OperationError(problem)
    return report


def resubmit_record(
    conn,
    qid: int,
    filename: str,
    api_url: str,
    timeout: float = 10.0,
    lease_seconds: int = 60,
):
    """Lease and submit a corrected transaction without resolving the source."""
    if timeout <= 0:
        raise OperationError("HTTP timeout must be positive")
    if lease_seconds < max(30, int(timeout) + 10):
        raise OperationError("Recovery lease must safely exceed the HTTP timeout")

    payload, txid = parse_correction(filename)
    digest = hashlib.sha256(payload).digest()
    lease_token = uuid.uuid4().hex
    with conn.transaction():
        row = _row(conn, qid, lock=True)
        if row["status"] != "pending":
            raise OperationError(f"Cannot resubmit record in state {row['status']}")
        if row["event_type"] != "transaction":
            raise OperationError("Only transaction quarantines support HTTP correction")
        if conn.execute(
            """
            SELECT 1 FROM event_quarantine_recovery_attempts
            WHERE quarantine_id = %s
              AND state IN ('sending', 'accepted', 'uncertain')
            """,
            (qid,),
        ).fetchone():
            raise OperationError(
                "A replacement is active or ambiguous; run reconcile before retrying"
            )
        attempt_id = conn.execute(
            """
            INSERT INTO event_quarantine_recovery_attempts (
                quarantine_id, transaction_id, submitted_payload_sha256,
                lease_token, lease_expires_at
            ) VALUES (%s, %s, %s, %s,
                      now() + (%s * interval '1 second'))
            RETURNING id
            """,
            (qid, txid, digest, lease_token, lease_seconds),
        ).fetchone()["id"]
        conn.execute(
            """
            INSERT INTO event_quarantine_audit (
                quarantine_id, action, note, replacement_transaction_id
            ) VALUES (%s, 'recovery_started', %s, %s)
            """,
            (qid, f"attempt={attempt_id} lease={lease_token}", txid),
        )

    request = Request(
        api_url,
        data=payload,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            http_status = response.status
            if http_status != 202:
                _set_attempt(
                    conn,
                    attempt_id,
                    lease_token,
                    "uncertain",
                    http_status,
                    "Unexpected successful status, expected 202",
                )
                raise OperationError(
                    f"Unexpected API status {http_status}; run reconcile"
                )
    except HTTPError as exc:
        state = "rejected" if 400 <= exc.code < 500 else "uncertain"
        _set_attempt(conn, attempt_id, lease_token, state, exc.code, exc.reason)
        raise OperationError(f"Go ingestion returned HTTP {exc.code} ({state})") from exc
    except (URLError, TimeoutError, OSError) as exc:
        _set_attempt(conn, attempt_id, lease_token, "uncertain", error=exc)
        raise OperationError(
            "Submission outcome is uncertain. Wait for the lease, then run reconcile; "
            "do not blindly retry"
        ) from exc

    _set_attempt(conn, attempt_id, lease_token, "accepted", 202)
    return txid, attempt_id


def resolve_record(conn, qid: int, note: str = "Replacement scored"):
    note = note.strip()
    if not note:
        raise OperationError("Resolution note cannot be empty")
    with conn.transaction():
        row = _row(conn, qid, lock=True)
        if row["status"] != "pending":
            raise OperationError(f"Cannot resolve record in state {row['status']}")
        attempts = conn.execute(
            """
            SELECT id, transaction_id
            FROM event_quarantine_recovery_attempts
            WHERE quarantine_id = %s AND state = 'accepted'
            ORDER BY id DESC
            """,
            (qid,),
        ).fetchall()
        if len(attempts) != 1:
            raise OperationError(
                "Expected exactly one accepted recovery; run reconcile first"
            )
        attempt = attempts[0]
        durable = _validate_recovery_snapshot(
            _durable_recovery_snapshot(conn, attempt["transaction_id"])
        )
        if not durable["accepted"] or not durable["outbox_delivered"]:
            raise OperationError("Replacement is not durably delivered through the Go outbox")
        if not durable["scored"]:
            raise OperationError("Replacement is not durably scored yet")

        conn.execute(
            """
            UPDATE event_quarantine SET status = 'resolved', resolved_at = now()
            WHERE id = %s
            """,
            (qid,),
        )
        conn.execute(
            """
            INSERT INTO event_quarantine_audit (
                quarantine_id, action, note, replacement_transaction_id
            ) VALUES (%s, 'resolved', %s, %s)
            """,
            (qid, note, attempt["transaction_id"]),
        )
    return attempt["transaction_id"]


def purge_payloads(conn, *, older_than_days: int = 30, limit: int = 1000):
    """Purge raw resolved/ignored payloads while retaining hashes and audit."""
    if older_than_days < 30:
        raise OperationError("Payload retention must be at least 30 days")
    if not 1 <= limit <= 10_000:
        raise OperationError("Purge limit must be between 1 and 10000")

    with conn.transaction():
        rows = conn.execute(
            """
            SELECT id
            FROM event_quarantine
            WHERE status IN ('resolved', 'ignored')
              AND resolved_at <= now() - (%s * interval '1 day')
              AND kafka_payload IS NOT NULL
            ORDER BY resolved_at, id
            LIMIT %s
            FOR UPDATE SKIP LOCKED
            """,
            (older_than_days, limit),
        ).fetchall()
        ids = [row["id"] for row in rows]
        if not ids:
            return []
        conn.execute(
            """
            UPDATE event_quarantine
            SET kafka_payload = NULL, payload_purged_at = now()
            WHERE id = ANY(%s)
            """,
            (ids,),
        )
        conn.execute(
            """
            INSERT INTO event_quarantine_audit (
                quarantine_id, action, note
            )
            SELECT purged.quarantine_id, 'payload_purged', %s
            FROM unnest(%s::bigint[]) AS purged(quarantine_id)
            """,
            (f"Raw payload purged after {older_than_days} days", ids),
        )
        return ids


def _json_display(obj):
    return json.dumps(obj, indent=2, default=str, ensure_ascii=False)


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL", DEFAULT_DSN),
        help="PostgreSQL DSN (default: DATABASE_URL or local fraudwatch)",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p_list = sub.add_parser("list", help="List quarantined Kafka records")
    p_list.add_argument(
        "--status",
        default="pending",
        choices=["pending", "resolved", "ignored", "all"],
    )
    p_list.add_argument("--limit", type=int, default=30)

    p_show = sub.add_parser("show", help="Inspect one quarantined record")
    p_show.add_argument("id", type=int)
    p_show.add_argument(
        "--payload",
        action="store_true",
        help="Print raw payload as UTF-8 or base64; may be sensitive",
    )

    p_ignore = sub.add_parser("ignore", help="Explicitly discard with audit reason")
    p_ignore.add_argument("id", type=int)
    p_ignore.add_argument("--reason", required=True)

    p_submit = sub.add_parser("resubmit", help="Lease and send corrected transaction")
    p_submit.add_argument("id", type=int)
    p_submit.add_argument("--file", required=True)
    p_submit.add_argument(
        "--url", default=os.getenv("FRAUDWATCH_INGEST_URL", DEFAULT_API)
    )
    p_submit.add_argument("--timeout", type=float, default=10.0)
    p_submit.add_argument("--lease-seconds", type=int, default=60)

    p_reconcile = sub.add_parser(
        "reconcile", help="Check durable Go/outbox/scoring state for one recovery"
    )
    p_reconcile.add_argument("id", type=int)

    p_resolve = sub.add_parser("resolve", help="Resolve after verified scoring")
    p_resolve.add_argument("id", type=int)
    p_resolve.add_argument("--note", default="Replacement scored")

    p_purge = sub.add_parser(
        "purge", help="Purge old resolved/ignored raw payloads"
    )
    p_purge.add_argument("--older-than-days", type=int, default=30)
    p_purge.add_argument("--limit", type=int, default=1000)
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        with _db(args.database_url) as conn:
            if args.command == "list":
                print(_json_display(list_records(conn, args.status, args.limit)))
            elif args.command == "show":
                row, attempts, audit = details(conn, args.id)
                payload = row.pop("kafka_payload")
                key = row.pop("kafka_key")
                digest = bytes(row.pop("source_payload_sha256")).hex()
                result = {
                    "record": row,
                    "kafka_key_base64": (
                        base64.b64encode(key).decode() if key is not None else None
                    ),
                    "payload_available": payload is not None,
                    "payload_size": len(payload) if payload is not None else None,
                    "payload_sha256": digest,
                    "recovery_attempts": attempts,
                    "audit": audit,
                }
                if args.payload:
                    if payload is None:
                        result["payload"] = None
                    else:
                        try:
                            result["payload_utf8"] = payload.decode("utf-8")
                        except UnicodeDecodeError:
                            result["payload_base64"] = base64.b64encode(payload).decode()
                print(_json_display(result))
            elif args.command == "ignore":
                ignore_record(conn, args.id, args.reason)
                print(f"Ignored quarantine {args.id} (reason audited)")
            elif args.command == "resubmit":
                txid, attempt_id = resubmit_record(
                    conn,
                    args.id,
                    args.file,
                    args.url,
                    args.timeout,
                    args.lease_seconds,
                )
                print(f"Go API accepted replacement {txid} (attempt={attempt_id}).")
                print("Quarantine remains pending until reconcile/resolve verifies it.")
            elif args.command == "reconcile":
                print(_json_display(reconcile_record(conn, args.id)))
            elif args.command == "resolve":
                txid = resolve_record(conn, args.id, args.note)
                print(f"Resolved quarantine {args.id} against scored replacement {txid}.")
            elif args.command == "purge":
                ids = purge_payloads(
                    conn,
                    older_than_days=args.older_than_days,
                    limit=args.limit,
                )
                print(_json_display({"purged": len(ids), "quarantine_ids": ids}))
    except (OperationError, psycopg.Error, OSError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
