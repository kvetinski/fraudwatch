#!/usr/bin/env python3
"""Apply ordered FraudWatch SQL migrations with checksums and mutual exclusion."""

from __future__ import annotations

import argparse
import hashlib
import os
import re
from pathlib import Path

import psycopg


ROOT = Path(__file__).resolve().parents[1]
MIGRATIONS = ROOT / "infra" / "migrations"
MIGRATION_NAME = re.compile(r"^[0-9]{3}_[a-z0-9_]+\.sql$")
LOCK_NAME = "fraudwatch:schema-migrations"


def migration_files(directory: Path = MIGRATIONS) -> list[Path]:
    files = sorted(path for path in directory.glob("*.sql") if path.is_file())
    invalid = [path.name for path in files if not MIGRATION_NAME.fullmatch(path.name)]
    if invalid:
        raise RuntimeError(f"Invalid migration filenames: {', '.join(invalid)}")
    if not files:
        raise RuntimeError(f"No migrations found under {directory}")
    return files


def apply_migrations(database_url: str, directory: Path = MIGRATIONS) -> int:
    files = migration_files(directory)
    applied_count = 0
    with psycopg.connect(database_url, autocommit=True) as conn:
        conn.execute(
            "SELECT pg_advisory_lock(hashtextextended(%s, 0))",
            (LOCK_NAME,),
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version text PRIMARY KEY,
                sha256 text NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
                applied_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        applied = dict(
            conn.execute(
                "SELECT version, sha256 FROM schema_migrations"
            ).fetchall()
        )
        for path in files:
            sql = path.read_text(encoding="utf-8")
            digest = hashlib.sha256(sql.encode()).hexdigest()
            existing = applied.get(path.name)
            if existing is not None:
                if existing != digest:
                    raise RuntimeError(
                        f"Migration checksum changed after application: {path.name}"
                    )
                print(f"Migration {path.name}: already applied")
                continue
            print(f"Migration {path.name}: applying")
            with conn.transaction():
                conn.execute(sql, prepare=False)
                conn.execute(
                    "INSERT INTO schema_migrations (version, sha256) VALUES (%s, %s)",
                    (path.name, digest),
                )
            applied_count += 1
        print(
            f"Migration ledger: {len(files)} verified, {applied_count} newly applied"
        )
    return applied_count


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database-url",
        action="append",
        default=[],
        help="PostgreSQL URL; repeat to migrate multiple databases",
    )
    parser.add_argument("--directory", type=Path, default=MIGRATIONS)
    args = parser.parse_args()
    urls = args.database_url or [os.getenv("DATABASE_URL", "")]
    if not all(urls):
        parser.error("set DATABASE_URL or pass --database-url")
    for url in urls:
        apply_migrations(url, args.directory)


if __name__ == "__main__":
    main()
