"""PostgreSQL integration tests for leased event-quarantine recovery."""

import hashlib
import json
import os
import uuid
from urllib.error import HTTPError, URLError

import pytest

psycopg = pytest.importorskip("psycopg")
from psycopg.rows import dict_row

from fraudwatch_ml.quarantine import InvalidTransaction, store_quarantined
from scripts import quarantine as qm


@pytest.fixture
def record():
    dsn = os.getenv("TEST_DATABASE_URL")
    if not dsn:
        pytest.skip("TEST_DATABASE_URL is not configured")
    with psycopg.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        txid = f"quarantine-operator-test-{uuid.uuid4().hex}"
        source_topic = f"quarantine-test-{uuid.uuid4().hex}"
        payload = b"{broken"
        qid = conn.execute(
            """
            INSERT INTO event_quarantine (
                event_type, source_topic, source_partition, source_offset,
                kafka_payload, source_payload_sha256, source_payload_was_null,
                reason_code,
                error_type, error_message
            ) VALUES (
                'transaction', %s, 0, 0, %s, %s, false,
                'transaction.contract_violation',
                'InvalidTransaction', 'Bad JSON'
            ) RETURNING id
            """,
            (source_topic, payload, hashlib.sha256(payload).digest()),
        ).fetchone()["id"]
        conn.execute(
            """
            INSERT INTO event_quarantine_audit(quarantine_id, action, note)
            VALUES (%s, 'quarantined', 'test fixture')
            """,
            (qid,),
        )
        try:
            yield conn, qid, txid
        finally:
            conn.execute("DELETE FROM fraud_decisions WHERE transaction_id = %s", (txid,))
            conn.execute("DELETE FROM scored_transactions WHERE transaction_id = %s", (txid,))
            conn.execute("DELETE FROM outbox_events WHERE transaction_id = %s", (txid,))
            conn.execute("DELETE FROM transactions WHERE transaction_id = %s", (txid,))
            conn.execute(
                "DELETE FROM event_quarantine_recovery_attempts WHERE quarantine_id = %s",
                (qid,),
            )
            conn.execute(
                "DELETE FROM event_quarantine_audit WHERE quarantine_id = %s", (qid,)
            )
            conn.execute("DELETE FROM event_quarantine WHERE id = %s", (qid,))


def _file(tmp_path, txid):
    payload = json.dumps({
        "transaction_id": txid,
        "customer_id": "1",
        "terminal_id": txid,
        "amount": 0.0,
        "occurred_at": "2018-04-01T00:00:00Z",
    }).encode()
    filename = tmp_path / "corrected.json"
    filename.write_bytes(payload)
    return filename, payload


def _insert_durable_replacement(conn, txid, payload, *, correct_hash=True):
    payload_hash = hashlib.sha256(payload).digest()
    conn.execute(
        "INSERT INTO transactions(transaction_id, payload_hash, payload) VALUES (%s,%s,%s)",
        (txid, payload_hash, payload),
    )
    conn.execute(
        """
        INSERT INTO outbox_events(
            transaction_id, topic, kafka_key, payload, delivered_at,
            kafka_partition, kafka_offset
        ) VALUES(%s, 'transactions.v1', %s, %s, now(), 0, 123)
        """,
        (txid, txid, payload),
    )
    conn.execute(
        """
        INSERT INTO scored_transactions(transaction_id, terminal_id, occurred_at)
        VALUES (%s,%s,%s)
        """,
        (txid, txid, "2018-04-01T00:00:00+00:00"),
    )
    decision_hash = payload_hash if correct_hash else b"x" * 32
    conn.execute(
        """
        INSERT INTO fraud_decisions(
            transaction_id, fraud_score, model_reference, model_digest,
            feature_schema_version, feature_vector,
            source_payload_hash, kafka_partition, kafka_offset
        ) VALUES (
            %s, 0.10, 'models:/fraudwatch-hgb/1',
            'sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa',
            'v1', '{}'::jsonb, %s, 0, 123
        )
        """,
        (txid, decision_hash),
    )


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
        "SELECT status, resolved_at FROM event_quarantine WHERE id=%s", (qid,)
    ).fetchone()
    assert state["status"] == "ignored"
    assert state["resolved_at"] is not None
    audit = conn.execute(
        """
        SELECT action, note FROM event_quarantine_audit
        WHERE quarantine_id=%s ORDER BY id DESC LIMIT 1
        """,
        (qid,),
    ).fetchone()
    assert audit == {
        "action": "ignored",
        "note": "irrecoverably malformed source event",
    }


class FakeAccepted:
    status = 202

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_resolve_requires_consistent_go_outbox_and_scoring_state(
    record, tmp_path, monkeypatch
):
    conn, qid, txid = record
    filename, payload = _file(tmp_path, txid)
    monkeypatch.setattr(qm, "urlopen", lambda req, timeout: FakeAccepted())

    submitted, attempt_id = qm.resubmit_record(
        conn, qid, str(filename), "http://localhost:8080/v1/transactions"
    )
    assert submitted == txid
    attempt = conn.execute(
        """
        SELECT state, http_status, lease_token, lease_expires_at
        FROM event_quarantine_recovery_attempts WHERE id=%s
        """,
        (attempt_id,),
    ).fetchone()
    assert attempt["state"] == "accepted"
    assert attempt["http_status"] == 202
    assert attempt["lease_token"]
    assert attempt["lease_expires_at"] is not None

    with pytest.raises(qm.OperationError, match="not durably delivered"):
        qm.resolve_record(conn, qid)

    _insert_durable_replacement(conn, txid, payload, correct_hash=False)
    with pytest.raises(qm.OperationError, match="hash"):
        qm.resolve_record(conn, qid)
    conn.execute(
        """
        UPDATE fraud_decisions SET source_payload_hash=%s
        WHERE transaction_id=%s
        """,
        (hashlib.sha256(payload).digest(), txid),
    )

    report = qm.reconcile_record(conn, qid)
    assert report["state"] == "accepted"
    assert report["accepted"] is True
    assert report["outbox_delivered"] is True
    assert report["scored"] is True
    assert qm.resolve_record(conn, qid, "reviewed replacement") == txid

    final = conn.execute(
        "SELECT status, resolved_at FROM event_quarantine WHERE id=%s", (qid,)
    ).fetchone()
    assert final["status"] == "resolved"
    assert final["resolved_at"] is not None


def test_uncertain_attempt_requires_expired_lease_and_reconcile_before_retry(
    record, tmp_path, monkeypatch
):
    conn, qid, txid = record
    filename, _ = _file(tmp_path, txid)

    def unavailable(req, timeout):
        raise URLError("connection reset")

    monkeypatch.setattr(qm, "urlopen", unavailable)
    with pytest.raises(qm.OperationError, match="uncertain"):
        qm.resubmit_record(
            conn, qid, str(filename), "http://localhost:8080/v1/transactions"
        )
    with pytest.raises(qm.OperationError, match="lease remains active"):
        qm.reconcile_record(conn, qid)
    with pytest.raises(qm.OperationError, match="run reconcile"):
        qm.resubmit_record(
            conn, qid, str(filename), "http://localhost:8080/v1/transactions"
        )

    conn.execute(
        """
        UPDATE event_quarantine_recovery_attempts
        SET lease_expires_at = now() - interval '1 second'
        WHERE quarantine_id = %s
        """,
        (qid,),
    )
    report = qm.reconcile_record(conn, qid)
    assert report["state"] == "retryable"
    assert report["accepted"] is False

    monkeypatch.setattr(qm, "urlopen", lambda req, timeout: FakeAccepted())
    _, retry_id = qm.resubmit_record(
        conn, qid, str(filename), "http://localhost:8080/v1/transactions"
    )
    assert retry_id != report["attempt_id"]


def test_http_400_rejection_can_be_ignored(record, tmp_path, monkeypatch):
    conn, qid, txid = record
    filename, _ = _file(tmp_path, txid)

    def rejected(req, timeout):
        raise HTTPError(req.full_url, 400, "bad request", {}, None)

    monkeypatch.setattr(qm, "urlopen", rejected)
    with pytest.raises(qm.OperationError, match="HTTP 400"):
        qm.resubmit_record(
            conn, qid, str(filename), "http://localhost:8080/v1/transactions"
        )
    state = conn.execute(
        """
        SELECT state FROM event_quarantine_recovery_attempts
        WHERE quarantine_id=%s
        """,
        (qid,),
    ).fetchone()["state"]
    assert state == "rejected"
    qm.ignore_record(conn, qid, "source cannot be reconstructed")


def test_payload_retention_keeps_hash_and_audit(record):
    conn, qid, _ = record
    original_hash = conn.execute(
        "SELECT source_payload_sha256 FROM event_quarantine WHERE id=%s", (qid,)
    ).fetchone()["source_payload_sha256"]
    conn.execute(
        """
        UPDATE event_quarantine
        SET status='ignored', resolved_at=now() - interval '31 days'
        WHERE id=%s
        """,
        (qid,),
    )

    assert qm.purge_payloads(conn, older_than_days=30) == [qid]
    row = conn.execute(
        """
        SELECT kafka_payload, source_payload_sha256, payload_purged_at
        FROM event_quarantine WHERE id=%s
        """,
        (qid,),
    ).fetchone()
    assert row["kafka_payload"] is None
    assert bytes(row["source_payload_sha256"]) == bytes(original_hash)
    assert row["payload_purged_at"] is not None
    assert conn.execute(
        """
        SELECT count(*) FROM event_quarantine_audit
        WHERE quarantine_id=%s AND action='payload_purged'
        """,
        (qid,),
    ).fetchone()["count"] == 1

    # A later replay can still be identified by coordinates + retained digest
    # even though the resolved record's raw payload has been purged.
    source = conn.execute(
        """
        SELECT source_topic, source_partition, source_offset
        FROM event_quarantine WHERE id=%s
        """,
        (qid,),
    ).fetchone()
    assert store_quarantined(
        conn,
        source_topic=source["source_topic"],
        source_partition=source["source_partition"],
        source_offset=source["source_offset"],
        kafka_key=None,
        kafka_payload=b"{broken",
        error=InvalidTransaction(
            "Bad JSON", reason_code="transaction.contract_violation"
        ),
    ) is False
