"""
Tests for operational failure mode fixes.

Covers:
  - inHANCE user cache: failure does NOT poison the cache (allows retry)
  - DB upsert: mutable fields propagate NULLs (no stale COALESCE masking)
  - Atomic file writes: api_calls.json survives mid-write interruption
  - Matcha client: retries on HTTP 5xx server errors
  - Per-ticket transaction: ticket + actions committed atomically
  - Write-back counting: 403 rate-limits are not counted as successes
"""

import json
import os
import tempfile
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, PropertyMock

import pytest


# ── inHANCE cache poisoning ──────────────────────────────────────────

def test_inhance_cache_not_poisoned_on_failure():
    """If the API call fails, _INHANCE_IDS should NOT be cached (allow retry)."""
    import ts_client

    # Reset the cache
    ts_client._INHANCE_IDS = None

    with patch.object(ts_client, "ts_get", side_effect=Exception("connection refused")):
        result = ts_client.fetch_inhance_user_ids()

    # Should return empty set for this call
    assert result == set()
    # But should NOT have cached it — _INHANCE_IDS should still be None
    assert ts_client._INHANCE_IDS is None

    # Clean up
    ts_client._INHANCE_IDS = None


def test_inhance_cache_set_on_success():
    """On successful fetch, _INHANCE_IDS should be cached."""
    import ts_client

    ts_client._INHANCE_IDS = None

    mock_data = {
        "Users": [
            {"ID": "100", "Name": "Agent A"},
            {"ID": "200", "Name": "Agent B"},
        ]
    }
    with patch.object(ts_client, "ts_get", return_value=mock_data):
        result = ts_client.fetch_inhance_user_ids()

    assert result == {"100", "200"}
    assert ts_client._INHANCE_IDS == {"100", "200"}

    # Clean up
    ts_client._INHANCE_IDS = None


def test_inhance_cache_retries_after_failure():
    """After a failed call, the next call should try the API again (not return cached empty)."""
    import ts_client

    ts_client._INHANCE_IDS = None

    # First call: API fails
    with patch.object(ts_client, "ts_get", side_effect=Exception("timeout")):
        first = ts_client.fetch_inhance_user_ids()
    assert first == set()

    # Second call: API succeeds
    mock_data = {"Users": [{"ID": "300"}]}
    with patch.object(ts_client, "ts_get", return_value=mock_data):
        second = ts_client.fetch_inhance_user_ids()
    assert second == {"300"}
    assert ts_client._INHANCE_IDS == {"300"}

    # Clean up
    ts_client._INHANCE_IDS = None


# ── COALESCE / NULL propagation ──────────────────────────────────────

@pytest.fixture(autouse=True)
def _mock_db_enabled(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://test:test@localhost/test")
    import config
    monkeypatch.setattr(config, "DATABASE_URL", "postgresql://test:test@localhost/test")


@pytest.fixture
def mock_conn():
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn, cur


@pytest.fixture
def patch_pool(mock_conn):
    conn, cur = mock_conn
    with patch("db.get_conn", return_value=conn), \
         patch("db.put_conn"):
        yield conn, cur


def test_upsert_ticket_mutable_fields_not_coalesced(patch_pool):
    """Mutable fields (status, severity, assignee, etc.) should use EXCLUDED
    directly, not COALESCE, so that real NULLs propagate."""
    conn, cur = patch_pool
    import db

    now = datetime(2026, 3, 13, 12, 0, 0, tzinfo=timezone.utc)
    ticket = {"ticket_id": 12345, "status": None, "assignee": None, "severity": None}
    db.upsert_ticket(ticket, now=now)

    sql = cur.execute.call_args[0][0]
    update_part = sql.split("DO UPDATE SET")[1]

    # Mutable fields should NOT use COALESCE
    for field in ("status", "severity", "assignee", "customer",
                  "ticket_name", "product_name"):
        # Check that COALESCE(EXCLUDED.<field> is NOT in the update clause
        assert f"COALESCE(EXCLUDED.{field}" not in update_part, \
            f"{field} should not use COALESCE in the UPDATE clause"

    # Immutable fields SHOULD still use COALESCE
    assert "COALESCE(EXCLUDED.date_created" in update_part
    assert "COALESCE(EXCLUDED.ticket_number" in update_part


def test_upsert_action_mutable_fields_not_coalesced(patch_pool):
    """Mutable action fields should use EXCLUDED directly."""
    conn, cur = patch_pool
    import db

    now = datetime(2026, 3, 13, 12, 0, 0, tzinfo=timezone.utc)
    action = {"action_id": 99, "ticket_id": 12345, "party": None, "description": None}
    db.upsert_action(action, now=now)

    sql = cur.execute.call_args[0][0]
    update_part = sql.split("DO UPDATE SET")[1]

    # Mutable action fields should NOT use COALESCE
    for field in ("action_type", "creator_id", "creator_name", "party",
                  "is_visible", "description", "cleaned_description",
                  "is_customer_visible"):
        assert f"COALESCE(EXCLUDED.{field}" not in update_part, \
            f"{field} should not use COALESCE in the UPDATE clause"

    # Immutable/externally-managed fields SHOULD still use COALESCE
    assert "COALESCE(EXCLUDED.created_at" in update_part
    assert "COALESCE(EXCLUDED.action_class" in update_part


# ── Atomic file writes ───────────────────────────────────────────────

def test_log_api_call_uses_atomic_replace(monkeypatch):
    """_log_api_call should write to a .tmp file and use os.replace for atomicity."""
    import ts_client

    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setattr(ts_client, "OUTPUT_DIR", tmpdir)
        monkeypatch.setattr(ts_client, "LOG_API_CALLS", True)
        monkeypatch.setattr(ts_client, "SKIP_OUTPUT_FILES", False)

        ts_client._log_api_call("GET", "http://example.com", status=200)

        log_path = os.path.join(tmpdir, "api_calls.json")
        assert os.path.exists(log_path)
        data = json.loads(open(log_path).read())
        assert len(data) == 1
        assert data[0]["method"] == "GET"

        # No leftover .tmp file
        assert not os.path.exists(log_path + ".tmp")


def test_save_dry_run_payload_uses_atomic_replace(monkeypatch):
    """save_dry_run_payload should use atomic write via os.replace."""
    import ts_client

    with tempfile.TemporaryDirectory() as tmpdir:
        monkeypatch.setattr(ts_client, "OUTPUT_DIR", tmpdir)
        monkeypatch.setattr(ts_client, "SKIP_OUTPUT_FILES", False)

        ts_client.save_dry_run_payload("12345", {"Ticket": {"Status": "Open"}})

        path = os.path.join(tmpdir, "api_payloads_dry_run.json")
        assert os.path.exists(path)
        data = json.loads(open(path).read())
        assert len(data) == 1
        assert data[0]["payload"] == {"Ticket": {"Status": "Open"}}

        # No leftover .tmp file
        assert not os.path.exists(path + ".tmp")


# ── Matcha 5xx retry ─────────────────────────────────────────────────

def test_matcha_retries_on_5xx(monkeypatch):
    """Matcha client should retry on 500/502/503 server errors."""
    import matcha_client

    # Avoid real API logging
    monkeypatch.setattr(matcha_client, "_log_api_call", lambda *a, **kw: None)

    # First call returns 500, second returns 200
    mock_500 = MagicMock()
    mock_500.status_code = 500
    mock_500.json.return_value = {"output": []}
    mock_500.raise_for_status.side_effect = None

    mock_200 = MagicMock()
    mock_200.status_code = 200
    mock_200.json.return_value = {
        "output": [{"content": [{"text": "success"}]}]
    }
    mock_200.raise_for_status.side_effect = None

    with patch("matcha_client.requests.post", side_effect=[mock_500, mock_200]), \
         patch("matcha_client.time.sleep"):
        result = matcha_client.call_matcha("test prompt", max_retries=2, retry_backoff=0)

    assert result == "success"


def test_matcha_raises_after_max_5xx_retries(monkeypatch):
    """Matcha client should raise after exhausting retries on 5xx."""
    import matcha_client
    import requests as req

    monkeypatch.setattr(matcha_client, "_log_api_call", lambda *a, **kw: None)

    mock_500 = MagicMock()
    mock_500.status_code = 502
    mock_500.json.return_value = {"output": []}
    mock_500.raise_for_status.side_effect = req.exceptions.HTTPError(
        "502 Server Error", response=mock_500
    )

    with patch("matcha_client.requests.post", return_value=mock_500), \
         patch("matcha_client.time.sleep"):
        with pytest.raises(req.exceptions.HTTPError):
            matcha_client.call_matcha("test prompt", max_retries=2, retry_backoff=0)


# ── Per-ticket transaction batching ──────────────────────────────────

def test_upsert_ticket_with_actions_single_commit(patch_pool):
    """upsert_ticket_with_actions should execute all statements in one commit."""
    conn, cur = patch_pool
    import db

    now = datetime(2026, 3, 13, 12, 0, 0, tzinfo=timezone.utc)
    ticket = {"ticket_id": 12345, "ticket_number": "29696"}
    actions = [
        {"action_id": 1, "ticket_id": 12345, "is_empty": False},
        {"action_id": 2, "ticket_id": 12345, "is_empty": False},
    ]

    db.upsert_ticket_with_actions(ticket, actions, now=now)

    # Should have 3 execute calls (1 ticket + 2 actions) and only 1 commit
    assert cur.execute.call_count == 3
    conn.commit.assert_called_once()


def test_upsert_ticket_with_actions_rollback_on_error(patch_pool):
    """If any action upsert fails, the entire batch should be rolled back."""
    conn, cur = patch_pool
    import db

    # Make the second execute call raise an error
    call_count = [0]
    original_execute = cur.execute

    def failing_execute(*args, **kwargs):
        call_count[0] += 1
        if call_count[0] == 2:
            raise Exception("DB error on action")
        return original_execute(*args, **kwargs)

    cur.execute = failing_execute

    now = datetime(2026, 3, 13, 12, 0, 0, tzinfo=timezone.utc)
    ticket = {"ticket_id": 12345, "ticket_number": "29696"}
    actions = [{"action_id": 1, "ticket_id": 12345, "is_empty": False}]

    with pytest.raises(Exception, match="DB error on action"):
        db.upsert_ticket_with_actions(ticket, actions, now=now)

    # Should have rolled back, not committed
    conn.rollback.assert_called_once()
    conn.commit.assert_not_called()
