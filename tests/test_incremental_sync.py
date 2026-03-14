"""
Tests for the incremental sync pipeline (run_ingest.py).

These tests mock the DB and API layers to verify:
  - Watermark is read from sync_state and applied with safety buffer
  - Single-ticket sync does NOT advance the global watermark
  - Replay (--since / --days) does NOT advance the global watermark
  - Normal incremental sync DOES advance the watermark on success
  - Failed runs do NOT advance the watermark
  - get_sync_state returns correct structure
  - Empty result sets still produce a successful run
"""

import uuid
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock, patch, call

import pytest


# ── Helpers to mock the DB connection ────────────────────────────────

@pytest.fixture(autouse=True)
def _mock_db_enabled(monkeypatch):
    """Ensure db module thinks a DATABASE_URL is configured."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://test:test@localhost/test")
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


# ── get_sync_state ───────────────────────────────────────────────────

def test_get_sync_state_returns_dict_when_row_exists():
    """get_sync_state should return a dict with watermark when row exists."""
    import db
    now = datetime(2026, 3, 13, 12, 0, 0, tzinfo=timezone.utc)
    row = ("teamsupport", now, now, "completed", None, None, now)
    with patch("db.fetch_one", return_value=row):
        result = db.get_sync_state("teamsupport")
    assert result is not None
    assert result["source_name"] == "teamsupport"
    assert result["last_successful_sync_at"] == now
    assert result["last_status"] == "completed"


def test_get_sync_state_returns_none_when_no_row():
    """get_sync_state should return None for first-ever sync."""
    import db
    with patch("db.fetch_one", return_value=None):
        result = db.get_sync_state("teamsupport")
    assert result is None


# ── Watermark-based sync behaviour ──────────────────────────────────

def _make_ticket(tid, tnum, date_modified):
    """Helper to build a minimal raw TS ticket dict."""
    return {
        "ID": str(tid),
        "TicketNumber": str(tnum),
        "Name": f"Test Ticket {tnum}",
        "DateCreated": "2026-01-01T00:00:00Z",
        "DateModified": date_modified,
        "Status": "Open",
    }


@pytest.fixture
def _mock_sync_deps(monkeypatch):
    """Patch all external dependencies used by run_ingest._sync."""
    import config
    monkeypatch.setattr(config, "MAX_TICKETS", 0)  # unlimited
    monkeypatch.setattr(config, "SAFETY_BUFFER_MINUTES", 10)
    monkeypatch.setattr(config, "INITIAL_BACKFILL_DAYS", 0)


def test_incremental_sync_reads_watermark_and_applies_buffer(_mock_sync_deps, monkeypatch):
    """Normal sync should read the watermark, subtract the safety buffer,
    and use that as the effective from_ts for filtering tickets."""
    import run_ingest

    watermark = datetime(2026, 3, 13, 10, 0, 0, tzinfo=timezone.utc)
    state = {
        "source_name": "teamsupport",
        "last_successful_sync_at": watermark,
        "last_attempted_sync_at": watermark,
        "last_status": "completed",
        "last_error": None,
        "last_cursor": None,
        "updated_at": watermark,
    }

    # Ticket modified at 09:55 — inside the 10-min buffer overlap
    ticket_in_buffer = _make_ticket(1, "100", "2026-03-13T09:55:00Z")
    # Ticket modified at 09:40 — outside the buffer
    ticket_old = _make_ticket(2, "200", "2026-03-13T09:40:00Z")
    # Ticket modified at 10:30 — clearly after the watermark
    ticket_new = _make_ticket(3, "300", "2026-03-13T10:30:00Z")

    run_id = uuid.uuid4()
    with patch("db.get_sync_state", return_value=state), \
         patch("db.create_ingest_run", return_value=run_id), \
         patch("db.upsert_sync_state"), \
         patch("db.complete_ingest_run"), \
         patch("db.upsert_ticket_with_actions"), \
         patch("run_ingest.fetch_inhance_user_ids"), \
         patch("run_ingest.fetch_open_tickets", return_value=[ticket_in_buffer, ticket_old, ticket_new]), \
         patch("run_ingest.fetch_all_activities", return_value=[]), \
         patch("db.upsert_sync_state") as mock_sync_state:

        result = run_ingest._sync(dry_run=False, verbose=False)

    # ticket_old (09:40) should be filtered out.  ticket_in_buffer (09:55) and
    # ticket_new (10:30) should pass the filter (>= 09:50 = watermark - 10min).
    assert result["status"] == "completed"
    assert result["tickets_seen"] == 2
    assert result["tickets_upserted"] == 2


def test_incremental_sync_advances_watermark_on_success(_mock_sync_deps, monkeypatch):
    """A successful normal incremental sync should advance the watermark."""
    import run_ingest

    watermark = datetime(2026, 3, 13, 10, 0, 0, tzinfo=timezone.utc)
    state = {
        "source_name": "teamsupport",
        "last_successful_sync_at": watermark,
        "last_attempted_sync_at": watermark,
        "last_status": "completed",
        "last_error": None,
        "last_cursor": None,
        "updated_at": watermark,
    }
    run_id = uuid.uuid4()

    sync_state_calls = []

    def capture_sync_state(*args, **kwargs):
        sync_state_calls.append(kwargs)

    with patch("db.get_sync_state", return_value=state), \
         patch("db.create_ingest_run", return_value=run_id), \
         patch("db.upsert_sync_state", side_effect=capture_sync_state), \
         patch("db.complete_ingest_run"), \
         patch("db.upsert_ticket_with_actions"), \
         patch("run_ingest.fetch_inhance_user_ids"), \
         patch("run_ingest.fetch_open_tickets", return_value=[]), \
         patch("run_ingest.fetch_all_activities", return_value=[]):

        result = run_ingest._sync(dry_run=False)

    assert result["status"] == "completed"
    # The final upsert_sync_state call should have is_success=True
    final_call = sync_state_calls[-1]
    assert final_call["is_success"] is True


def test_targeted_sync_does_not_advance_watermark(_mock_sync_deps, monkeypatch):
    """Syncing specific tickets should NOT advance last_successful_sync_at."""
    import run_ingest

    ticket = _make_ticket(1, "100", "2026-03-13T10:00:00Z")
    run_id = uuid.uuid4()

    sync_state_calls = []

    def capture_sync_state(*args, **kwargs):
        sync_state_calls.append(kwargs)

    with patch("db.create_ingest_run", return_value=run_id), \
         patch("db.upsert_sync_state", side_effect=capture_sync_state), \
         patch("db.complete_ingest_run"), \
         patch("db.upsert_ticket_with_actions"), \
         patch("run_ingest.fetch_inhance_user_ids"), \
         patch("run_ingest.fetch_open_tickets", return_value=[ticket]), \
         patch("run_ingest.fetch_all_activities", return_value=[]):

        result = run_ingest._sync(ticket_numbers=["100"], dry_run=False)

    assert result["status"] == "completed"
    # The final upsert_sync_state should have is_success=False
    # (targeted sync must NOT advance watermark)
    final_call = sync_state_calls[-1]
    assert final_call["is_success"] is False


def test_replay_since_does_not_advance_watermark(_mock_sync_deps, monkeypatch):
    """Replay mode (--since) should NOT advance last_successful_sync_at."""
    import run_ingest

    ticket = _make_ticket(1, "100", "2026-03-13T10:00:00Z")
    run_id = uuid.uuid4()

    sync_state_calls = []

    def capture_sync_state(*args, **kwargs):
        sync_state_calls.append(kwargs)

    since = datetime(2026, 3, 10, 0, 0, 0, tzinfo=timezone.utc)
    with patch("db.create_ingest_run", return_value=run_id), \
         patch("db.upsert_sync_state", side_effect=capture_sync_state), \
         patch("db.complete_ingest_run"), \
         patch("db.upsert_ticket_with_actions"), \
         patch("run_ingest.fetch_inhance_user_ids"), \
         patch("run_ingest.fetch_open_tickets", return_value=[ticket]), \
         patch("run_ingest.fetch_all_activities", return_value=[]):

        result = run_ingest._sync(since=since, dry_run=False)

    assert result["status"] == "completed"
    final_call = sync_state_calls[-1]
    assert final_call["is_success"] is False


def test_failed_run_does_not_advance_watermark(_mock_sync_deps, monkeypatch):
    """A failed run must never advance the watermark."""
    import run_ingest

    run_id = uuid.uuid4()

    sync_state_calls = []

    def capture_sync_state(*args, **kwargs):
        sync_state_calls.append(kwargs)

    with patch("db.get_sync_state", return_value=None), \
         patch("db.create_ingest_run", return_value=run_id), \
         patch("db.upsert_sync_state", side_effect=capture_sync_state), \
         patch("db.complete_ingest_run"), \
         patch("run_ingest.fetch_inhance_user_ids", side_effect=Exception("API down")):

        result = run_ingest._sync(dry_run=False)

    assert result["status"] == "failed"
    final_call = sync_state_calls[-1]
    assert final_call["is_success"] is False
    assert final_call["status"] == "failed"


def test_empty_result_set_still_marks_success(_mock_sync_deps, monkeypatch):
    """An empty ticket result should still be treated as a successful run."""
    import run_ingest

    run_id = uuid.uuid4()

    with patch("db.get_sync_state", return_value=None), \
         patch("db.create_ingest_run", return_value=run_id), \
         patch("db.upsert_sync_state"), \
         patch("db.complete_ingest_run"), \
         patch("run_ingest.fetch_inhance_user_ids"), \
         patch("run_ingest.fetch_open_tickets", return_value=[]):

        result = run_ingest._sync(dry_run=False)

    assert result["status"] == "completed"
    assert result["tickets_seen"] == 0
    assert result["tickets_upserted"] == 0


def test_ticket_id_sync_does_not_advance_watermark(_mock_sync_deps, monkeypatch):
    """Syncing by ticket_id should NOT advance the watermark."""
    import run_ingest

    ticket = _make_ticket(12345, "100", "2026-03-13T10:00:00Z")
    run_id = uuid.uuid4()

    sync_state_calls = []

    def capture_sync_state(*args, **kwargs):
        sync_state_calls.append(kwargs)

    with patch("db.create_ingest_run", return_value=run_id), \
         patch("db.upsert_sync_state", side_effect=capture_sync_state), \
         patch("db.complete_ingest_run"), \
         patch("db.upsert_ticket_with_actions"), \
         patch("run_ingest.fetch_inhance_user_ids"), \
         patch("run_ingest.fetch_ticket_by_id", return_value=[ticket]), \
         patch("run_ingest.fetch_all_activities", return_value=[]):

        result = run_ingest._sync(ticket_ids=["12345"], dry_run=False)

    assert result["status"] == "completed"
    final_call = sync_state_calls[-1]
    assert final_call["is_success"] is False
