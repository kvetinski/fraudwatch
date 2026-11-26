#!/usr/bin/env python3
"""Operator CLI for fail-closed terminal history reconciliation."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import psycopg
from psycopg.rows import dict_row

from fraudwatch_ml.reconciliation import (
    abandon_run,
    get_run,
    preview_terminal,
    promote_run,
    replay_run,
)


DEFAULT_DSN = (
    "postgres://fraudwatch:fraudwatch_dev@"
    "127.0.0.1:5434/fraudwatch?sslmode=disable"
)


def _load_scorer():
    from services.scoring.worker import load_scorer

    return load_scorer()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Preview, replay, verify, and promote one blocked terminal",
    )
    parser.add_argument(
        "--database-url",
        default=os.getenv("DATABASE_URL", DEFAULT_DSN),
        help="PostgreSQL DSN (default: DATABASE_URL)",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    preview = commands.add_parser("preview", help="Freeze the replay source")
    preview.add_argument("terminal_id")
    preview.add_argument("--requested-by", required=True)
    preview.add_argument("--note", required=True)

    replay = commands.add_parser("replay", help="Build and verify candidates")
    replay.add_argument("run_id", type=int)

    promote = commands.add_parser(
        "promote", help="Promote verified candidates and unblock atomically"
    )
    promote.add_argument("run_id", type=int)
    promote.add_argument(
        "--expected-source-sha256",
        required=True,
        help="Exact source hash printed by preview",
    )

    show = commands.add_parser("show", help="Display run and audit history")
    show.add_argument("run_id", type=int)

    abandon = commands.add_parser(
        "abandon", help="Close an unpromoted run after source or policy changes"
    )
    abandon.add_argument("run_id", type=int)
    abandon.add_argument("--requested-by", required=True)
    abandon.add_argument("--note", required=True)
    return parser


def _run_dict(run) -> dict:
    return {
        "run_id": run.run_id,
        "terminal_id": run.terminal_id,
        "status": run.status,
        "model_reference": run.model_reference,
        "model_digest": run.model_digest,
        "feature_schema_version": run.feature_schema_version,
        "source_event_count": run.source_event_count,
        "source_event_sha256": run.source_event_sha256.hex(),
        "candidate_count": run.candidate_count,
        "candidate_sha256": (
            None if run.candidate_sha256 is None else run.candidate_sha256.hex()
        ),
        "parity_verified": run.parity_verified,
    }


def main() -> None:
    args = _parser().parse_args()
    with psycopg.connect(
        args.database_url,
        autocommit=True,
        row_factory=dict_row,
    ) as conn:
        if args.command == "preview":
            scorer = _load_scorer()
            run = preview_terminal(
                conn,
                terminal_id=args.terminal_id,
                model_reference=scorer.model_reference,
                model_digest=scorer.model_digest,
                requested_by=args.requested_by,
                note=args.note,
            )
            output = _run_dict(run)
        elif args.command == "replay":
            run = replay_run(conn, _load_scorer(), run_id=args.run_id)
            output = _run_dict(run)
        elif args.command == "promote":
            run = promote_run(
                conn,
                run_id=args.run_id,
                expected_source_sha256=args.expected_source_sha256,
            )
            output = _run_dict(run)
        elif args.command == "show":
            run = get_run(conn, args.run_id)
            audit = conn.execute(
                """
                SELECT action, note, created_at
                FROM terminal_reconciliation_audit
                WHERE reconciliation_run_id = %s
                ORDER BY id
                """,
                (args.run_id,),
            ).fetchall()
            output = _run_dict(run)
            output["audit"] = [
                {
                    "action": row["action"],
                    "note": row["note"],
                    "created_at": row["created_at"].isoformat(),
                }
                for row in audit
            ]
        else:
            run = abandon_run(
                conn,
                run_id=args.run_id,
                requested_by=args.requested_by,
                note=args.note,
            )
            output = _run_dict(run)
    print(json.dumps(output, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
