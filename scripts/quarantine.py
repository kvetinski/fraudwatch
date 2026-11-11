#!/usr/bin/env python3
"""FraudWatch quarantine operations.

Usage:
  python scripts/quarantine.py list [--status pending|resolved|ignored|all]
  python scripts/quarantine.py show ID [--payload]
  python scripts/quarantine.py ignore ID --reason '...'
  python scripts/quarantine.py resubmit ID --file corrected.json
  python scripts/quarantine.py resolve ID [--note '...']

Only operate against trusted development PostgreSQL and ingestion endpoints.
Corrected events go through the Go HTTP API / transactional outbox, never
straight to Kafka. A resubmission keeps the original quarantine pending until
the replacement is durably scored and explicitly resolved.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import sys
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import psycopg
from psycopg.rows import dict_row

DEFAULT_DSN = (
    "postgres://fraudwatch:fraudwatch_dev@"
    "127.0.0.1:5434/fraudwatch?sslmode=disable"
)
DEFAULT_API = "http://127.0.0.1:8080/transactions"


class OperationError(Exception):
    """Operator-visible error; does not imply a database rollback failure."""


def _db(dsn: str):
    return psycopg.connect(dsn, autocommit=True, row_factory=dict_row)


def _row(conn, qid: int, *, lock: bool = False):
    query = """
        SELECT id, source_topic, source_partition, source_offset,
               kafka_key, kafka_payload, error_type, error_message,
               status, created_at, resolved_at
        FROM scoring_quarantine WHERE id = %s
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
        SELECT id, source_topic, source_partition, source_offset,
               error_type, left(error_message, 100) AS error_message,
               status, created_at
        FROM scoring_quarantine
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
               created_at, updated_at
        FROM scoring_quarantine_resubmissions
        WHERE quarantine_id = %s ORDER BY id
        """, (qid,),
    ).fetchall()
    audit = conn.execute(
        """
        SELECT action, note, replacement_transaction_id, created_at
        FROM scoring_quarantine_audit
        WHERE quarantine_id = %s ORDER BY id
        """, (qid,),
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
            SELECT 1 FROM scoring_quarantine_resubmissions
            WHERE quarantine_id = %s
              AND state IN ('sending', 'accepted', 'uncertain') LIMIT 1
            """, (qid,),
        ).fetchone()
        if active:
            raise OperationError(
                "A replacement may still be in flight; resolve or investigate first"
            )
        conn.execute(
            """
            UPDATE scoring_quarantine
            SET status = 'ignored', resolved_at = now()
            WHERE id = %s
            """, (qid,),
        )
        conn.execute(
            """
            INSERT INTO scoring_quarantine_audit(quarantine_id, action, note)
            VALUES (%s, 'ignored', %s)
            """, (qid, reason),
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
    # Go ingestion owns the authoritative schema validation.
    return payload, txid


def _set_attempt(conn, attempt_id: int, state: str, status=None, error=None):
    with conn.transaction():
        conn.execute(
            """
            UPDATE scoring_quarantine_resubmissions
            SET state = %s, http_status = %s, error_message = %s,
                updated_at = now()
            WHERE id = %s
            """, (state, status, str(error)[:1000] if error else None, attempt_id),
        )


def resubmit_record(conn, qid: int, filename: str, api_url: str, timeout: float = 10.0):
    """Return submission status; never mark original quarantine resolved here.

    Log intent in PostgreSQL before making the HTTP request. Any ambiguous
    network/5xx outcome is left as 'uncertain': do not auto-resend it. Reconcile
    by checking the named decision, which is safe if the API actually accepted.
    """
    payload, txid = parse_correction(filename)
    digest = hashlib.sha256(payload).digest()
    with conn.transaction():
        row = _row(conn, qid, lock=True)
        if row["status"] != "pending":
            raise OperationError(f"Cannot resubmit record in state {row['status']}")
        if conn.execute(
            "SELECT 1 FROM fraud_decisions WHERE transaction_id = %s", (txid,),
        ).fetchone():
            raise OperationError(
                "This transaction ID was already scored. Use a fresh ID or "
                "investigate the conflicting payload; never overwrite a decision"
            )
        if conn.execute(
            """
            SELECT 1 FROM scoring_quarantine_resubmissions
            WHERE quarantine_id = %s AND state IN ('sending','accepted','uncertain')
            """, (qid,),
        ).fetchone():
            raise OperationError(
                "A replacement is already accepted or may be in flight. "
                "Use show/resolve rather than submitting another one"
            )
        try:
            attempt_id = conn.execute(
                """
                INSERT INTO scoring_quarantine_resubmissions(
                    quarantine_id, transaction_id, submitted_payload_sha256
                ) VALUES (%s,%s,%s) RETURNING id
                """, (qid, txid, digest),
            ).fetchone()["id"]
        except psycopg.errors.UniqueViolation as exc:
            raise OperationError("This exact replacement was already attempted") from exc

    request = Request(
        api_url, data=payload, method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            http_status = response.status
            if http_status != 202:
                _set_attempt(conn, attempt_id, "uncertain", http_status,
                             "Unexpected successful status, expected 202")
                raise OperationError(f"Unexpected API status {http_status}; investigate")
    except HTTPError as exc:
        state = "rejected" if 400 <= exc.code < 500 else "uncertain"
        _set_attempt(conn, attempt_id, state, exc.code, exc.reason)
        raise OperationError(f"Go ingestion returned HTTP {exc.code} ({state})") from exc
    except (URLError, TimeoutError, OSError) as exc:
        _set_attempt(conn, attempt_id, "uncertain", error=exc)
        raise OperationError(
            "Submission outcome is uncertain. Do not blindly retry; "
            f"check transaction_id={txid} in the Go outbox and fraud_decisions"
        ) from exc

    _set_attempt(conn, attempt_id, "accepted", 202)
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
            SELECT id, transaction_id, state
            FROM scoring_quarantine_resubmissions
            WHERE quarantine_id = %s AND state IN ('accepted','uncertain','sending')
            ORDER BY id DESC
            """, (qid,),
        ).fetchall()
        if len(attempts) != 1:
            raise OperationError(
                "Expected exactly one accepted/possibly-in-flight replacement attempt"
            )
        attempt = attempts[0]
        txid = attempt["transaction_id"]
        proof = conn.execute(
            """
            SELECT d.source_payload_hash, o.payload
            FROM fraud_decisions d
            JOIN scored_transactions s USING (transaction_id)
            JOIN outbox_events o USING (transaction_id)
            WHERE d.transaction_id = %s
            """, (txid,),
        ).fetchone()
        if proof is None:
            raise OperationError(
                f"Replacement {txid} is not durably scored through the Go outbox yet"
            )
        if bytes(proof["source_payload_hash"]) != hashlib.sha256(
            bytes(proof["payload"])
        ).digest():
            raise OperationError(
                "Scored Kafka payload hash does not match durable outbox payload"
            )
        conn.execute(
            """
            UPDATE scoring_quarantine SET status='resolved', resolved_at=now()
            WHERE id=%s
            """, (qid,),
        )
        conn.execute(
            """
            INSERT INTO scoring_quarantine_audit(
                quarantine_id, action, note, replacement_transaction_id
            ) VALUES (%s, 'resolved', %s, %s)
            """, (qid, note, txid),
        )
        if attempt["state"] != "accepted":
            conn.execute(
                """
                UPDATE scoring_quarantine_resubmissions
                SET state='accepted', updated_at=now()
                WHERE id=%s
                """, (attempt["id"],),
            )
    return txid


def _json_display(obj):
    return json.dumps(obj, indent=2, default=str, ensure_ascii=False)


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database-url", default=os.getenv("DATABASE_URL", DEFAULT_DSN),
        help="PostgreSQL DSN (default: DATABASE_URL or local fraudwatch)",
    )
    sub = parser.add_subparsers(dest="command", required=True)
    p_list = sub.add_parser("list", help="List quarantined Kafka records")
    p_list.add_argument("--status", default="pending",
                        choices=["pending", "resolved", "ignored", "all"])
    p_list.add_argument("--limit", type=int, default=30)
    p_show = sub.add_parser("show", help="Inspect one quarantined record")
    p_show.add_argument("id", type=int)
    p_show.add_argument("--payload", action="store_true",
                        help="Print raw payload as UTF-8 or base64; may be sensitive")
    p_ignore = sub.add_parser("ignore", help="Explicitly discard with audit reason")
    p_ignore.add_argument("id", type=int)
    p_ignore.add_argument("--reason", required=True)
    p_submit = sub.add_parser("resubmit", help="Send corrected JSON through Go ingestion")
    p_submit.add_argument("id", type=int)
    p_submit.add_argument("--file", required=True)
    p_submit.add_argument("--url", default=os.getenv("FRAUDWATCH_INGEST_URL", DEFAULT_API))
    p_submit.add_argument("--timeout", type=float, default=10.0)
    p_resolve = sub.add_parser("resolve", help="Resolve only after scored replacement exists")
    p_resolve.add_argument("id", type=int)
    p_resolve.add_argument("--note", default="Replacement scored")
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
                result = {
                    "record": row,
                    "kafka_key_base64": base64.b64encode(key).decode() if key is not None else None,
                    "payload_size": len(payload) if payload is not None else None,
                    "payload_sha256": hashlib.sha256(payload).hexdigest() if payload is not None else None,
                    "resubmissions": attempts,
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
                txid, aid = resubmit_record(conn, args.id, args.file, args.url, args.timeout)
                print(f"Go API accepted replacement {txid} (attempt={aid}).")
                print("Quarantine remains pending until the worker scores it; then run resolve.")
            elif args.command == "resolve":
                txid = resolve_record(conn, args.id, args.note)
                print(f"Resolved quarantine {args.id} against scored replacement {txid}.")
    except (OperationError, psycopg.Error, OSError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
