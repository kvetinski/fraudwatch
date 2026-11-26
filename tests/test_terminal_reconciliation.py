"""Integration coverage for audited terminal replay and atomic promotion."""

import json
import os
import uuid

import numpy as np
import psycopg
import pytest
from psycopg.rows import dict_row

from fraudwatch_ml.labels import store_label
from fraudwatch_ml.reconciliation import (
    ReconciliationError,
    abandon_run,
    preview_terminal,
    promote_run,
    replay_run,
)
from fraudwatch_ml.terminal_order import TerminalQuarantined
from services.scoring.worker import LoadedScorer, process_event


TEST_MODEL_DIGEST = "sha256:" + "a" * 64
OTHER_TEST_MODEL_DIGEST = "sha256:" + "b" * 64


class HistorySensitiveModel:
    def predict_proba(self, features):
        count = float(features.iloc[0]["terminal_7d_count"])
        fraud_probability = min(0.99, 0.1 + count * 0.1)
        return np.array(
            [[1.0 - fraud_probability, fraud_probability]],
            dtype=np.float64,
        )


def _payload(transaction_id: str, terminal_id: str, occurred_at: str) -> bytes:
    return json.dumps(
        {
            "transaction_id": transaction_id,
            "customer_id": f"customer-{transaction_id}",
            "terminal_id": terminal_id,
            "amount": 25.0,
            "occurred_at": occurred_at,
        },
        separators=(",", ":"),
    ).encode()


def _label_payload(transaction_id: str, observed_at: str) -> bytes:
    return json.dumps(
        {
            "transaction_id": transaction_id,
            "fraud_label": 1,
            "observed_at": observed_at,
        },
        separators=(",", ":"),
    ).encode()


def _process(conn, scorer, payload: bytes, terminal_id: str, offset: int):
    return process_event(
        conn,
        scorer,
        payload,
        partition=2,
        offset=offset,
        source_topic="transactions.v1",
        kafka_key=terminal_id.encode(),
    )


def _cleanup(conn, terminal_id: str, transaction_ids: list[str]) -> None:
    conn.execute(
        "DELETE FROM terminal_order_blocks WHERE terminal_id = %s",
        (terminal_id,),
    )
    run_ids = [
        row[0]
        for row in conn.execute(
            "SELECT id FROM terminal_reconciliation_runs WHERE terminal_id = %s",
            (terminal_id,),
        ).fetchall()
    ]
    if run_ids:
        conn.execute(
            "DELETE FROM terminal_reconciliation_audit "
            "WHERE reconciliation_run_id = ANY(%s)",
            (run_ids,),
        )
    conn.execute(
        "DELETE FROM fraud_decisions WHERE transaction_id = ANY(%s)",
        (transaction_ids,),
    )
    conn.execute(
        "DELETE FROM fraud_decision_versions WHERE transaction_id = ANY(%s)",
        (transaction_ids,),
    )
    if run_ids:
        conn.execute(
            "DELETE FROM terminal_reconciliation_runs WHERE id = ANY(%s)",
            (run_ids,),
        )
    quarantine_ids = [
        row[0]
        for row in conn.execute(
            """
            SELECT links.quarantine_id
            FROM valid_event_quarantine_links AS links
            JOIN valid_event_ledger AS ledger ON ledger.id = links.event_ledger_id
            WHERE ledger.transaction_id = ANY(%s)
            """,
            (transaction_ids,),
        ).fetchall()
    ]
    if quarantine_ids:
        conn.execute(
            "DELETE FROM event_quarantine_audit WHERE quarantine_id = ANY(%s)",
            (quarantine_ids,),
        )
        conn.execute(
            "DELETE FROM event_quarantine WHERE id = ANY(%s)",
            (quarantine_ids,),
        )
    conn.execute(
        "DELETE FROM fraud_labels WHERE transaction_id = ANY(%s)",
        (transaction_ids,),
    )
    conn.execute(
        "DELETE FROM pending_fraud_labels WHERE transaction_id = ANY(%s)",
        (transaction_ids,),
    )
    conn.execute(
        "DELETE FROM scored_transactions WHERE transaction_id = ANY(%s)",
        (transaction_ids,),
    )
    conn.execute(
        "DELETE FROM valid_event_ledger WHERE transaction_id = ANY(%s)",
        (transaction_ids,),
    )


def test_replay_preserves_original_and_promotes_atomically():
    dsn = os.getenv("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TEST_DATABASE_URL not configured")

    salt = uuid.uuid4().hex
    terminal_id = f"reconcile-{salt}"
    transaction_ids = [f"reconcile-{position}-{salt}" for position in range(4)]
    late_id, original_id, later_id, future_id = transaction_ids
    payloads = {
        original_id: _payload(original_id, terminal_id, "2018-04-10T00:00:00Z"),
        late_id: _payload(late_id, terminal_id, "2018-04-01T00:00:00Z"),
        later_id: _payload(later_id, terminal_id, "2018-04-18T00:00:00Z"),
    }
    offsets = [uuid.uuid4().int % (2**61) for _ in range(4)]
    scorer = LoadedScorer(
        HistorySensitiveModel(),
        "models:/fraudwatch-hgb/reconciliation-test",
        model_digest=TEST_MODEL_DIGEST,
    )

    with psycopg.connect(dsn, autocommit=True) as conn:
        try:
            assert _process(
                conn, scorer, payloads[original_id], terminal_id, offsets[0]
            ) is True
            assert store_label(
                conn,
                _label_payload(original_id, "2018-04-17T00:00:00Z"),
                kafka_key=original_id.encode(),
                source_topic="fraud-labels.v1",
                source_partition=1,
                source_offset=offsets[0],
            ) is True
            assert isinstance(
                _process(conn, scorer, payloads[late_id], terminal_id, offsets[1]),
                TerminalQuarantined,
            )
            assert store_label(
                conn,
                _label_payload(late_id, "2018-04-09T00:00:00Z"),
                kafka_key=late_id.encode(),
                source_topic="fraud-labels.v1",
                source_partition=1,
                source_offset=offsets[1],
            ) is True
            assert isinstance(
                _process(conn, scorer, payloads[later_id], terminal_id, offsets[2]),
                TerminalQuarantined,
            )

            preview = preview_terminal(
                conn,
                terminal_id=terminal_id,
                model_reference=scorer.model_reference,
                model_digest=scorer.model_digest,
                requested_by="pytest",
                note="verify complete late-event recovery",
            )
            assert preview.status == "previewed"
            assert preview.source_event_count == 5
            assert conn.execute(
                """
                SELECT model_digest FROM terminal_reconciliation_runs
                WHERE id = %s
                """,
                (preview.run_id,),
            ).fetchone()[0] == TEST_MODEL_DIGEST

            verified = replay_run(conn, scorer, run_id=preview.run_id)
            assert verified.status == "verified"
            assert verified.parity_verified is True
            assert verified.candidate_count == 3

            current = conn.execute(
                """
                SELECT decision_version, fraud_score, model_digest
                FROM fraud_decisions
                WHERE transaction_id = %s
                """,
                (original_id,),
            ).fetchone()
            assert current == (1, 0.1, TEST_MODEL_DIGEST)
            assert conn.execute(
                """
                SELECT count(*) FROM fraud_decisions
                WHERE transaction_id = ANY(%s)
                """,
                (transaction_ids,),
            ).fetchone()[0] == 1

            promoted = promote_run(
                conn,
                run_id=preview.run_id,
                expected_source_sha256=preview.source_event_sha256.hex(),
            )
            assert promoted.status == "promoted"
            assert conn.execute(
                "SELECT count(*) FROM terminal_order_blocks WHERE terminal_id = %s",
                (terminal_id,),
            ).fetchone()[0] == 0
            decisions = conn.execute(
                """
                SELECT transaction_id, decision_version, fraud_score,
                       model_digest
                FROM fraud_decisions
                WHERE transaction_id = ANY(%s)
                ORDER BY transaction_id
                """,
                (transaction_ids,),
            ).fetchall()
            assert len(decisions) == 3
            original = next(
                row for row in decisions if row[0] == original_id
            )
            assert original[1] == 2
            assert original[2] == pytest.approx(0.2)
            assert {row[3] for row in decisions} == {TEST_MODEL_DIGEST}
            assert conn.execute(
                """
                SELECT feature_vector ->> 'terminal_7d_fraud_count'
                FROM fraud_decisions WHERE transaction_id = %s
                """,
                (original_id,),
            ).fetchone()[0] == "1.0"
            assert conn.execute(
                """
                SELECT count(*) FROM fraud_labels
                WHERE transaction_id = ANY(%s)
                """,
                ([late_id, original_id],),
            ).fetchone()[0] == 2
            assert conn.execute(
                """
                SELECT count(*) FROM pending_fraud_labels
                WHERE transaction_id = %s
                """,
                (late_id,),
            ).fetchone()[0] == 0
            label_projection = conn.execute(
                """
                SELECT fraud_label, label_observed_at
                FROM scored_transactions
                WHERE transaction_id = %s
                """,
                (late_id,),
            ).fetchone()
            assert label_projection[0] == 1
            assert (
                label_projection[1].isoformat()
                == "2018-04-09T00:00:00+00:00"
            )

            assert _process(
                conn,
                scorer,
                _payload(future_id, terminal_id, "2018-04-20T00:00:00Z"),
                terminal_id,
                offsets[3],
            ) is True
            future_features = conn.execute(
                """
                SELECT feature_vector FROM fraud_decisions
                WHERE transaction_id = %s
                """,
                (future_id,),
            ).fetchone()[0]
            assert future_features["terminal_30d_count"] == 2.0
            assert future_features["terminal_30d_fraud_count"] == 2.0
            assert future_features["terminal_30d_fraud_rate"] == 1.0

            versions = conn.execute(
                """
                SELECT decision_version, origin,
                       promoted_at IS NOT NULL AS promoted, model_digest
                FROM fraud_decision_versions
                WHERE transaction_id = %s
                ORDER BY decision_version
                """,
                (original_id,),
            ).fetchall()
            assert versions == [
                (1, "stream", True, TEST_MODEL_DIGEST),
                (2, "reconciliation", True, TEST_MODEL_DIGEST),
            ]
            assert {
                row[0]
                for row in conn.execute(
                    """
                    SELECT action FROM terminal_reconciliation_audit
                    WHERE reconciliation_run_id = %s
                    """,
                    (preview.run_id,),
                ).fetchall()
            } == {"previewed", "replayed", "verified", "promoted", "unblocked"}
            assert {
                row[0]
                for row in conn.execute(
                    """
                    SELECT q.status
                    FROM event_quarantine AS q
                    JOIN valid_event_quarantine_links AS links
                      ON links.quarantine_id = q.id
                    JOIN valid_event_ledger AS ledger
                      ON ledger.id = links.event_ledger_id
                    WHERE ledger.terminal_id = %s
                    """,
                    (terminal_id,),
                ).fetchall()
            } == {"resolved"}
        finally:
            _cleanup(conn, terminal_id, transaction_ids)


def test_promotion_rejects_source_changes_after_verification():
    dsn = os.getenv("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TEST_DATABASE_URL not configured")

    salt = uuid.uuid4().hex
    terminal_id = f"reconcile-change-{salt}"
    ids = [f"change-{position}-{salt}" for position in range(3)]
    original_id, late_id, changed_id = ids
    scorer = LoadedScorer(
        HistorySensitiveModel(),
        "models:/fraudwatch-hgb/reconciliation-test",
        model_digest=TEST_MODEL_DIGEST,
    )
    offsets = [uuid.uuid4().int % (2**61) for _ in range(3)]

    with psycopg.connect(dsn, autocommit=True) as conn:
        try:
            assert _process(
                conn,
                scorer,
                _payload(original_id, terminal_id, "2018-04-10T00:00:00Z"),
                terminal_id,
                offsets[0],
            ) is True
            assert isinstance(
                _process(
                    conn,
                    scorer,
                    _payload(late_id, terminal_id, "2018-04-01T00:00:00Z"),
                    terminal_id,
                    offsets[1],
                ),
                TerminalQuarantined,
            )
            preview = preview_terminal(
                conn,
                terminal_id=terminal_id,
                model_reference=scorer.model_reference,
                model_digest=scorer.model_digest,
                requested_by="pytest",
                note="source mutation guard",
            )
            different_digest_scorer = LoadedScorer(
                HistorySensitiveModel(),
                scorer.model_reference,
                model_digest=OTHER_TEST_MODEL_DIGEST,
            )
            with pytest.raises(ReconciliationError, match="digest"):
                replay_run(
                    conn,
                    different_digest_scorer,
                    run_id=preview.run_id,
                )
            assert conn.execute(
                """
                SELECT status FROM terminal_reconciliation_runs WHERE id = %s
                """,
                (preview.run_id,),
            ).fetchone()[0] == "previewed"
            assert conn.execute(
                """
                SELECT count(*) FROM fraud_decision_versions
                WHERE reconciliation_run_id = %s
                """,
                (preview.run_id,),
            ).fetchone()[0] == 0
            replay_run(conn, scorer, run_id=preview.run_id)

            assert isinstance(
                _process(
                    conn,
                    scorer,
                    _payload(changed_id, terminal_id, "2018-04-18T00:00:00Z"),
                    terminal_id,
                    offsets[2],
                ),
                TerminalQuarantined,
            )
            with pytest.raises(ReconciliationError, match="source changed"):
                promote_run(
                    conn,
                    run_id=preview.run_id,
                    expected_source_sha256=preview.source_event_sha256.hex(),
                )
            assert conn.execute(
                "SELECT status FROM terminal_reconciliation_runs WHERE id = %s",
                (preview.run_id,),
            ).fetchone()[0] == "verified"
            assert conn.execute(
                "SELECT count(*) FROM terminal_order_blocks WHERE terminal_id = %s",
                (terminal_id,),
            ).fetchone()[0] == 1
            abandoned = abandon_run(
                conn,
                run_id=preview.run_id,
                requested_by="pytest",
                note="source changed after verification",
            )
            assert abandoned.status == "abandoned"
            replacement = preview_terminal(
                conn,
                terminal_id=terminal_id,
                model_reference=scorer.model_reference,
                model_digest=scorer.model_digest,
                requested_by="pytest",
                note="replacement source snapshot",
            )
            assert replacement.status == "previewed"
            assert replacement.source_event_count == 3
        finally:
            _cleanup(conn, terminal_id, ids)


def test_operator_workflow_supports_dictionary_rows():
    dsn = os.getenv("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TEST_DATABASE_URL not configured")

    salt = uuid.uuid4().hex
    terminal_id = f"reconcile-dict-{salt}"
    ids = [f"dict-{position}-{salt}" for position in range(2)]
    original_id, late_id = ids
    scorer = LoadedScorer(
        HistorySensitiveModel(),
        "models:/fraudwatch-hgb/reconciliation-test",
        model_digest=TEST_MODEL_DIGEST,
    )
    offsets = [uuid.uuid4().int % (2**61) for _ in range(2)]

    with psycopg.connect(dsn, autocommit=True) as worker_conn:
        try:
            assert _process(
                worker_conn,
                scorer,
                _payload(original_id, terminal_id, "2018-04-10T00:00:00Z"),
                terminal_id,
                offsets[0],
            ) is True
            assert isinstance(
                _process(
                    worker_conn,
                    scorer,
                    _payload(late_id, terminal_id, "2018-04-01T00:00:00Z"),
                    terminal_id,
                    offsets[1],
                ),
                TerminalQuarantined,
            )
            with psycopg.connect(
                dsn,
                autocommit=True,
                row_factory=dict_row,
            ) as operator_conn:
                preview = preview_terminal(
                    operator_conn,
                    terminal_id=terminal_id,
                    model_reference=scorer.model_reference,
                    model_digest=scorer.model_digest,
                    requested_by="pytest",
                    note="exercise CLI database row shape",
                )
                verified = replay_run(
                    operator_conn,
                    scorer,
                    run_id=preview.run_id,
                )
                assert verified.status == "verified"
                promoted = promote_run(
                    operator_conn,
                    run_id=preview.run_id,
                    expected_source_sha256=preview.source_event_sha256.hex(),
                )
                assert promoted.status == "promoted"
        finally:
            _cleanup(worker_conn, terminal_id, ids)
