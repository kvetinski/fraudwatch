import hashlib
import os
from pathlib import Path
import uuid

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


def test_model_identity_migration_enforces_verified_new_rows():
    test_database_url = os.getenv("TEST_DATABASE_URL")
    if not test_database_url:
        pytest.skip("TEST_DATABASE_URL not configured")
    transaction_id = f"model-identity-guard-{uuid.uuid4().hex}"
    with psycopg.connect(test_database_url, autocommit=True) as conn:
        trigger_names = {
            row[0]
            for row in conn.execute(
                """
                SELECT tgname
                FROM pg_trigger
                WHERE NOT tgisinternal
                  AND tgrelid IN (
                      'fraud_decisions'::regclass,
                      'fraud_decision_versions'::regclass,
                      'terminal_reconciliation_runs'::regclass,
                      'model_monitor_snapshots'::regclass
                  )
                """
            ).fetchall()
        }
        assert {
            "fraud_decisions_verified_model_digest",
            "fraud_decision_versions_verified_model_digest",
            "terminal_reconciliation_runs_verified_model_digest",
            "model_monitor_snapshots_verified_model_digest",
        } <= trigger_names

        try:
            with pytest.raises(
                psycopg.errors.CheckViolation,
                match="reserved for rows predating migration 013",
            ):
                conn.execute(
                    """
                    INSERT INTO fraud_decisions (
                        transaction_id, fraud_score, model_reference,
                        model_digest, feature_schema_version, feature_vector,
                        source_payload_hash, kafka_partition, kafka_offset
                    ) VALUES (
                        %s, 0.1, 'models:/fraudwatch-hgb/legacy-test',
                        'legacy:unverified', 'v1', '{}'::jsonb, %s, 0, 0
                    )
                    """,
                    (transaction_id, b"x" * 32),
                )
        finally:
            conn.execute(
                "DELETE FROM fraud_decisions WHERE transaction_id = %s",
                (transaction_id,),
            )

        monitor_constraint = conn.execute(
            """
            SELECT pg_get_constraintdef(oid)
            FROM pg_constraint
            WHERE conname = 'model_monitor_snapshots_model_digest_format'
            """
        ).fetchone()
        assert monitor_constraint is not None
        assert "sha256:" in monitor_constraint[0]
