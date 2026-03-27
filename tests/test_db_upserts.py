"""
Tests for db.py — upsert idempotency, sync_state, and hash-based skipping.

These tests verify the SQL logic by mocking the psycopg2 connection layer.
They confirm that:
  - Repeated upsert_ticket calls produce the correct SQL (INSERT ... ON CONFLICT)
  - first_ingested_at is only set on INSERT (not on UPDATE)
  - last_ingested_at and last_seen_at always update to 'now'
  - upsert_action has the same idempotent behaviour
  - upsert_sync_state only advances last_successful_sync_at on success
"""

import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call

import pytest


# ── Helpers to mock the DB connection ────────────────────────────────

@pytest.fixture(autouse=True)
def _mock_db_enabled(monkeypatch):
    """Ensure db module thinks a DATABASE_URL is configured."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://test:test@localhost/test")
    # Reimport config to pick up the env var
    import config
    monkeypatch.setattr(config, "DATABASE_URL", "postgresql://test:test@localhost/test")


@pytest.fixture
def mock_conn():
    """Return a mock connection + cursor that db.get_conn() will return."""
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn, cur


@pytest.fixture
def patch_pool(mock_conn):
    """Patch get_conn/put_conn so no real Postgres is needed."""
    conn, cur = mock_conn
    with patch("db.get_conn", return_value=conn), \
         patch("db.put_conn"):
        yield conn, cur


# ── upsert_ticket ────────────────────────────────────────────────────

def test_upsert_ticket_sql_uses_on_conflict(patch_pool):
    """Verify the upsert SQL contains ON CONFLICT ... DO UPDATE."""
    conn, cur = patch_pool
    import db

    now = datetime(2026, 3, 13, 12, 0, 0, tzinfo=timezone.utc)
    ticket = {
        "ticket_id": 12345,
        "ticket_number": "29696",
        "ticket_name": "Test Ticket",
        "status": "Open",
        "severity": "High",
        "product_name": "Widget",
        "assignee": "Agent",
        "customer": "Acme",
        "date_created": now,
        "date_modified": now,
        "closed_at": None,
        "days_opened": 10,
        "days_since_modified": 0,
        "source_updated_at": now,
        "source_payload": {"raw": "data"},
    }

    db.upsert_ticket(ticket, now=now)

    # The cursor should have been called with SQL containing ON CONFLICT
    sql_executed = cur.execute.call_args[0][0]
    assert "ON CONFLICT" in sql_executed
    assert "DO UPDATE" in sql_executed
    # first_ingested_at should NOT appear in the UPDATE clause
    # It should only be in the INSERT VALUES
    update_part = sql_executed.split("DO UPDATE SET")[1]
    assert "first_ingested_at" not in update_part
    # last_ingested_at and last_seen_at should be in the UPDATE clause
    assert "last_ingested_at" in update_part
    assert "last_seen_at" in update_part

    conn.commit.assert_called_once()


def test_upsert_ticket_repeated_call_does_not_change_sql(patch_pool):
    """Calling upsert_ticket twice with the same ticket_id uses the same SQL."""
    conn, cur = patch_pool
    import db

    now = datetime(2026, 3, 13, 12, 0, 0, tzinfo=timezone.utc)
    ticket = {"ticket_id": 12345, "ticket_number": "29696"}

    db.upsert_ticket(ticket, now=now)
    first_sql = cur.execute.call_args_list[0][0][0]

    db.upsert_ticket(ticket, now=now)
    second_sql = cur.execute.call_args_list[1][0][0]

    assert first_sql == second_sql


# ── upsert_action ────────────────────────────────────────────────────

def test_upsert_action_sql_uses_on_conflict(patch_pool):
    """Verify the action upsert SQL contains ON CONFLICT ... DO UPDATE."""
    conn, cur = patch_pool
    import db

    now = datetime(2026, 3, 13, 12, 0, 0, tzinfo=timezone.utc)
    action = {
        "action_id": 99999,
        "ticket_id": 12345,
        "ticket_number": "29696",
        "created_at": now,
        "action_type": "Comment",
        "creator_id": "1",
        "creator_name": "Test",
        "party": "inh",
        "is_visible": True,
        "description": "Test action",
        "cleaned_description": "Test action",
        "action_class": "technical_work",
        "is_empty": False,
        "is_customer_visible": True,
        "source_payload": {"raw": "data"},
    }

    db.upsert_action(action, now=now)

    sql_executed = cur.execute.call_args[0][0]
    assert "ON CONFLICT" in sql_executed
    assert "DO UPDATE" in sql_executed
    update_part = sql_executed.split("DO UPDATE SET")[1]
    assert "first_ingested_at" not in update_part
    assert "last_ingested_at" in update_part
    assert "last_seen_at" in update_part

    conn.commit.assert_called_once()


# ── upsert_sync_state ───────────────────────────────────────────────

def test_upsert_sync_state_success_updates_successful_sync_at(patch_pool):
    """When is_success=True, last_successful_sync_at should be set."""
    conn, cur = patch_pool
    import db

    db.upsert_sync_state("teamsupport", status="completed", is_success=True)

    sql_executed = cur.execute.call_args[0][0]
    params = cur.execute.call_args[0][1]
    assert "ON CONFLICT" in sql_executed
    assert params["is_success"] is True
    conn.commit.assert_called_once()


def test_upsert_sync_state_failure_preserves_successful_sync_at(patch_pool):
    """When is_success=False, last_successful_sync_at should NOT advance."""
    conn, cur = patch_pool
    import db

    db.upsert_sync_state("teamsupport", status="failed", error="boom", is_success=False)

    params = cur.execute.call_args[0][1]
    assert params["is_success"] is False
    assert params["error"] == "boom"
    conn.commit.assert_called_once()


# ── create_ingest_run / complete_ingest_run ──────────────────────────

def test_create_ingest_run_returns_uuid(patch_pool):
    conn, cur = patch_pool
    import db

    run_id = db.create_ingest_run("teamsupport", config_snapshot={"test": True})
    assert isinstance(run_id, uuid.UUID)
    conn.commit.assert_called_once()


def test_complete_ingest_run_updates_status(patch_pool):
    conn, cur = patch_pool
    import db

    run_id = uuid.uuid4()
    db.complete_ingest_run(
        run_id,
        status="completed",
        tickets_seen=10,
        tickets_upserted=10,
        actions_seen=50,
        actions_upserted=50,
    )

    sql_executed = cur.execute.call_args[0][0]
    assert "UPDATE ingest_runs" in sql_executed
    conn.commit.assert_called_once()


# ── connection pool helpers ─────────────────────────────────────────

def test_get_conn_uses_unkeyed_pool_connection(monkeypatch):
    import db

    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    pool = MagicMock()
    pool.getconn.return_value = conn

    monkeypatch.setattr(db, "get_pool", lambda: pool)
    monkeypatch.setattr(db, "DATABASE_SCHEMA", "tickets_ai")

    result = db.get_conn()

    assert result is conn
    pool.getconn.assert_called_once_with()
    cur.execute.assert_called_once_with("SET search_path TO %s;", ("tickets_ai",))


def test_put_conn_returns_connection_without_manual_key(monkeypatch):
    import db

    conn = MagicMock()
    pool = MagicMock()

    monkeypatch.setattr(db, "get_pool", lambda: pool)

    db.put_conn(conn)

    pool.putconn.assert_called_once_with(conn)


# ── get_latest_enrichment_hash ───────────────────────────────────────

def test_get_latest_enrichment_hash_returns_hash():
    """When a prior enrichment row exists, return its hash."""
    import db
    with patch("db.fetch_one", return_value=("abc123hash",)):
        result = db.get_latest_enrichment_hash(12345, "priority")
    assert result == "abc123hash"


def test_get_latest_enrichment_hash_returns_none_when_empty():
    """When no prior enrichment exists, return None."""
    import db
    with patch("db.fetch_one", return_value=None):
        result = db.get_latest_enrichment_hash(12345, "priority")
    assert result is None


# ── get_current_hashes ───────────────────────────────────────────────

def test_get_current_hashes_returns_both():
    import db
    with patch("db.fetch_one", return_value=("thread_abc", "tech_def")):
        result = db.get_current_hashes(12345)
    assert result == {"thread_hash": "thread_abc", "technical_core_hash": "tech_def"}


def test_get_current_hashes_returns_none_when_no_rollup():
    import db
    with patch("db.fetch_one", return_value=None):
        result = db.get_current_hashes(12345)
    assert result == {"thread_hash": None, "technical_core_hash": None}
