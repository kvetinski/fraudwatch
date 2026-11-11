"""PostgreSQL integration tests for quarantine operator controls.

Requires migration 005 and TEST_DATABASE_URL pointing at fraudwatch_test.
These tests mock the Go HTTP response; they do not claim real Kafka delivery.
"""

import hashlib
import json
import os
import uuid
from urllib.error import HTTPError, URLError

import pytest

psycopg = pytest.importorskip("psycopg")
from psycopg.rows import dict_row

from scripts import quarantine as qm


@pytest.fixture
def record():
    dsn = os.getenv("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TEST_DATABASE_URL is not configured")
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        txid = f"quarantine-operator-test-{uuid.uuid4().hex}"
        source_topic = f"quarantine-test-{uuid.uuid4().hex}"
        qid = conn.execute(
            """
            INSERT INTO scoring_quarantine (
                source_topic, source_partition, source_offset,
                kafka_payload, error_type, error_message
            ) VALUES (%s, 0, 0, %s, 'InvalidTransaction', 'Bad JSON')
            RETURNING id
            """,
            (source_topic, b"{broken"),
        ).fetchone()["id"]
        try:
            yield conn, qid, txid
        finally:
            # Reverse FK order; cleanup is intentional even after an assertion.
            conn.execute("DELETE FROM fraud_decisions WHERE transaction_id = %s", (txid,))
            conn.execute("DELETE FROM scored_transactions WHERE transaction_id = %s", (txid,))
            conn.execute("DELETE FROM outbox_events WHERE transaction_id = %s", (txid,))
            conn.execute("DELETE FROM transactions WHERE transaction_id = %s", (txid,))
            conn.execute("DELETE FROM scoring_quarantine_audit WHERE quarantine_id = %s", (qid,))
            conn.execute("DELETE FROM scoring_quarantine_resubmissions WHERE quarantine_id = %s", (qid,))
            conn.execute("DELETE FROM scoring_quarantine WHERE id = %s", (qid,))


def _file(tmp_path, txid):
    payload = json.dumps({
        "transaction_id": txid,
        "customer_id": "1",
        "terminal_id": txid,
        "occurred_at": "2018-04-01T00:00:00Z",
        "amount": 0.0,
    }).encode()
    filename = tmp_path / "corrected.json"
    filename.write_bytes(payload)
    return filename, payload


def test_parse_rejects_invalid_correction(tmp_path):
    path = tmp_path / "wrong.json"
    path.write_text('{"transaction_id": "x", "amount": NaN}')
    with pytest.raises(qm.OperationError, match="numeric constant"):
        qm.parse_correction(str(path))
    path.write_text('{"amount": 1}')
    with pytest.raises(qm.OperationError, match="transaction_id"):
        qm.parse_correction(str(path))


def test_ignore_requires_reason_and_persists_audit(record):
    conn, qid, _ = record
    with pytest.raises(qm.OperationError):
        qm.ignore_record(conn, qid, "  ")
    qm.ignore_record(conn, qid, "irrecoverably malformed source event")
    state = conn.execute(
        "SELECT status, resolved_at FROM scoring_quarantine WHERE id=%s", (qid,),
    ).fetchone()
    assert state["status"] == "ignored"
    assert state["resolved_at"] is not None
    audit = conn.execute(
        "SELECT action, note FROM scoring_quarantine_audit WHERE quarantine_id=%s", (qid,),
    ).fetchone()
    assert audit["action"] == "ignored"
    assert audit["note"] == "irrecoverably malformed source event"
    with pytest.raises(qm.OperationError):
        qm.ignore_record(conn, qid, "again")


class FakeAccepted:
    status = 202

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_resubmit_stays_pending_until_scored_and_resolve_checks_hash(record, tmp_path, monkeypatch):
    conn, qid, txid = record
    filename, payload = _file(tmp_path, txid)
    monkeypatch.setattr(qm, "urlopen", lambda req, timeout: FakeAccepted())

    submitted, aid = qm.resubmit_record(
        conn, qid, str(filename), "http://localhost:8080/transactions"
    )
    assert submitted == txid
    state = conn.execute("SELECT status FROM scoring_quarantine WHERE id=%s", (qid,)).fetchone()
    assert state["status"] == "pending"
    attempt = conn.execute(
        "SELECT state, http_status FROM scoring_quarantine_resubmissions WHERE id=%s",
        (aid,),
    ).fetchone()
    assert attempt == {"state": "accepted", "http_status": 202}
    with pytest.raises(qm.OperationError, match="not durably scored"):
        qm.resolve_record(conn, qid)

    payload_hash = hashlib.sha256(payload).digest()
    conn.execute(
        "INSERT INTO transactions(transaction_id, payload_hash, payload) VALUES (%s,%s,%s)",
        (txid, payload_hash, payload),
    )
    conn.execute(
        """
        INSERT INTO outbox_events(transaction_id, topic, kafka_key, payload)
        VALUES(%s, 'transactions.v1', %s, %s)
        """, (txid, txid, payload),
    )
    conn.execute(
        """
        INSERT INTO scored_transactions(transaction_id, terminal_id, occurred_at)
        VALUES (%s,%s,%s)
        """, (txid, txid, "2018-04-01T00:00:00+00:00"),
    )
    conn.execute(
        """
        INSERT INTO fraud_decisions(
            transaction_id, fraud_score, model_reference,
            feature_schema_version, feature_vector,
            source_payload_hash, kafka_partition, kafka_offset
        ) VALUES (%s, 0.10, 'models:/fraudwatch-hgb/1', 'v1', '{}'::jsonb,
                  %s, 0, 987654321)
        """, (txid, b"x" * 32),
    )
    with pytest.raises(qm.OperationError, match="hash"):
        qm.resolve_record(conn, qid)
    conn.execute(
        "UPDATE fraud_decisions SET source_payload_hash=%s WHERE transaction_id=%s",
        (payload_hash, txid),
    )
    assert qm.resolve_record(conn, qid, "reviewed replacement") == txid
    final = conn.execute("SELECT status, resolved_at FROM scoring_quarantine WHERE id=%s", (qid,)).fetchone()
    assert final["status"] == "resolved"
    assert final["resolved_at"] is not None
    audit = conn.execute("SELECT action, replacement_transaction_id FROM scoring_quarantine_audit WHERE quarantine_id=%s", (qid,)).fetchone()
    assert audit["action"] == "resolved"
    assert audit["replacement_transaction_id"] == txid


def test_uncertain_network_outcome_prevents_blind_retry(record, tmp_path, monkeypatch):
    conn, qid, txid = record
    filename, _ = _file(tmp_path, txid)

    def unavailable(req, timeout):
        raise URLError("connection reset")

    monkeypatch.setattr(qm, "urlopen", unavailable)
    with pytest.raises(qm.OperationError, match="uncertain"):
        qm.resubmit_record(conn, qid, str(filename), "http://localhost:8080/transactions")
    state = conn.execute(
        "SELECT state FROM scoring_quarantine_resubmissions WHERE quarantine_id=%s", (qid,),
    ).fetchone()["state"]
    assert state == "uncertain"
    with pytest.raises(qm.OperationError, match="already accepted or may be in flight"):
        qm.resubmit_record(conn, qid, str(filename), "http://localhost:8080/transactions")
    with pytest.raises(qm.OperationError, match="may still be in flight"):
        qm.ignore_record(conn, qid, "not safe")


def test_http_400_rejection_can_be_ignored(record, tmp_path, monkeypatch):
    conn, qid, txid = record
    filename, _ = _file(tmp_path, txid)

    def rejected(req, timeout):
        raise HTTPError(req.full_url, 400, "bad request", {}, None)

    monkeypatch.setattr(qm, "urlopen", rejected)
    with pytest.raises(qm.OperationError, match="HTTP 400"):
        qm.resubmit_record(conn, qid, str(filename), "http://localhost:8080/transactions")
    state = conn.execute(
        "SELECT state FROM scoring_quarantine_resubmissions WHERE quarantine_id=%s", (qid,),
    ).fetchone()["state"]
    assert state == "rejected"
    qm.ignore_record(conn, qid, "source cannot be reconstructed")
    assert conn.execute(
        "SELECT status FROM scoring_quarantine WHERE id=%s", (qid,),
    ).fetchone()["status"] == "ignored"
