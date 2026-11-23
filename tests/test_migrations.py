import hashlib
import os
from pathlib import Path

import psycopg
import pytest

from scripts.migrate_database import migration_files


ROOT = Path(__file__).resolve().parents[1]


def test_migration_ledger_matches_repository():
    test_database_url = os.getenv("TEST_DATABASE_URL")
    if not test_database_url:
        pytest.skip("TEST_DATABASE_URL not configured")
    files = migration_files()
    expected = {
        path.name: hashlib.sha256(path.read_bytes()).hexdigest()
        for path in files
    }
    with psycopg.connect(test_database_url) as conn:
        actual = dict(
            conn.execute(
                "SELECT version, sha256 FROM schema_migrations ORDER BY version"
            ).fetchall()
        )
    assert actual == expected
