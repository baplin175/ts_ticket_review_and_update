"""
Tests for run_analytics.py — participants, handoffs, wait-states, snapshots, health.

These tests verify the analytics rebuild logic by mocking the psycopg2
connection layer, following the same patterns as test_db_upserts.py.
"""

import json
from datetime import date, datetime, timezone
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


# ── Wait-state inference ─────────────────────────────────────────────

def test_infer_state_cust_action_returns_waiting_on_support():
    """A customer action should result in 'waiting_on_support'."""
    from run_analytics import _infer_state
    assert _infer_state("cust", "customer_problem_statement", "I have a problem") == "waiting_on_support"


def test_infer_state_inh_waiting_on_customer():
    """An inh action classified as waiting_on_customer."""
    from run_analytics import _infer_state
    assert _infer_state("inh", "waiting_on_customer", "waiting for files") == "waiting_on_customer"


def test_infer_state_inh_scheduling():
    """An inh scheduling action should return 'scheduled'."""
    from run_analytics import _infer_state
    assert _infer_state("inh", "scheduling", "meeting tomorrow") == "scheduled"


def test_infer_state_inh_delivery_confirmation():
    """An inh delivery_confirmation action should return 'resolved'."""
    from run_analytics import _infer_state
    assert _infer_state("inh", "delivery_confirmation", "deployed") == "resolved"


def test_infer_state_inh_dev_keyword():
    """An inh action mentioning dev team should return 'waiting_on_dev'."""
    from run_analytics import _infer_state
    assert _infer_state("inh", "status_update", "Escalated to dev team for review") == "waiting_on_dev"


def test_infer_state_inh_ps_keyword():
    """An inh action mentioning professional services returns 'waiting_on_ps'."""
    from run_analytics import _infer_state
    assert _infer_state("inh", "status_update", "Handing off to professional services") == "waiting_on_ps"


def test_infer_state_inh_general_returns_active_work():
    """A general inh action should return 'active_work'."""
    from run_analytics import _infer_state
    assert _infer_state("inh", "technical_work", "Fixed the bug in the SQL query") == "active_work"


# ── rebuild_ticket_participants ──────────────────────────────────────

def test_rebuild_participants_builds_from_actions(patch_pool):
    """Verify participants are aggregated from actions correctly."""
    conn, cur = patch_pool
    import db
    from run_analytics import rebuild_ticket_participants

    t1 = datetime(2026, 3, 1, 10, 0, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 3, 1, 11, 0, 0, tzinfo=timezone.utc)
    t3 = datetime(2026, 3, 1, 12, 0, 0, tzinfo=timezone.utc)

    # Return actions for ticket 100
    actions = [
        (1, "user1", "Customer A", "cust", t1, False),
        (2, "user2", "Agent B", "inh", t2, False),
        (3, "user1", "Customer A", "cust", t3, False),
    ]

    call_count = [0]
    def mock_fetch_all(sql, params=()):
        call_count[0] += 1
        if "ticket_actions" in sql:
            return actions
        return []

    with patch("db.fetch_all", side_effect=mock_fetch_all), \
         patch("db.execute") as mock_exec:
        result = rebuild_ticket_participants([100])

    assert result == 1  # one ticket processed

    # Should have DELETE + 2 INSERTs (2 participants)
    exec_calls = mock_exec.call_args_list
    delete_calls = [c for c in exec_calls if "DELETE" in str(c)]
    insert_calls = [c for c in exec_calls if "INSERT" in str(c)]
    assert len(delete_calls) == 1
    assert len(insert_calls) == 2


def test_rebuild_participants_marks_first_response(patch_pool):
    """The first inh participant after first cust action gets first_response_flag."""
    conn, cur = patch_pool
    from run_analytics import rebuild_ticket_participants

    t1 = datetime(2026, 3, 1, 10, 0, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 3, 1, 11, 0, 0, tzinfo=timezone.utc)

    actions = [
        (1, "cust1", "Cust", "cust", t1, False),
        (2, "agent1", "Agent", "inh", t2, False),
    ]

    inserts = []

    def mock_fetch_all(sql, params=()):
        if "ticket_actions" in sql:
            return actions
        return []

    def mock_execute(sql, params=()):
        if "INSERT INTO ticket_participants" in sql:
            inserts.append(params)

    with patch("db.fetch_all", side_effect=mock_fetch_all), \
         patch("db.execute", side_effect=mock_execute):
        rebuild_ticket_participants([100])

    # Agent should have first_response_flag=True
    agent_insert = [p for p in inserts if p[1] == "agent1"]
    assert len(agent_insert) == 1
    assert agent_insert[0][7] is True  # first_response_flag

    # Customer should not
    cust_insert = [p for p in inserts if p[1] == "cust1"]
    assert len(cust_insert) == 1
    assert cust_insert[0][7] is False


# ── rebuild_ticket_handoffs ──────────────────────────────────────────

def test_rebuild_handoffs_detects_party_change(patch_pool):
    """A party change between consecutive actions should create a handoff."""
    conn, cur = patch_pool
    from run_analytics import rebuild_ticket_handoffs

    t1 = datetime(2026, 3, 1, 10, 0, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 3, 1, 11, 0, 0, tzinfo=timezone.utc)

    actions = [
        (1, "cust1", "Cust", "cust", t1, False, "customer_problem_statement"),
        (2, "agent1", "Agent", "inh", t2, False, "technical_work"),
    ]

    handoffs = []

    def mock_fetch_all(sql, params=()):
        if "ticket_actions" in sql:
            return actions
        return []

    def mock_execute(sql, params=()):
        if "INSERT INTO ticket_handoffs" in sql:
            handoffs.append(params)

    with patch("db.fetch_all", side_effect=mock_fetch_all), \
         patch("db.execute", side_effect=mock_execute):
        result = rebuild_ticket_handoffs([100])

    assert result == 1
    assert len(handoffs) == 1
    # Handoff from cust to inh
    assert handoffs[0][1] == "cust"  # from_party
    assert handoffs[0][2] == "inh"   # to_party


def test_rebuild_handoffs_no_change_no_handoff(patch_pool):
    """No handoff when consecutive actions are from the same party."""
    conn, cur = patch_pool
    from run_analytics import rebuild_ticket_handoffs

    t1 = datetime(2026, 3, 1, 10, 0, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 3, 1, 11, 0, 0, tzinfo=timezone.utc)

    actions = [
        (1, "agent1", "Agent A", "inh", t1, False, "technical_work"),
        (2, "agent2", "Agent B", "inh", t2, False, "status_update"),
    ]

    handoffs = []

    def mock_fetch_all(sql, params=()):
        if "ticket_actions" in sql:
            return actions
        return []

    def mock_execute(sql, params=()):
        if "INSERT INTO ticket_handoffs" in sql:
            handoffs.append(params)

    with patch("db.fetch_all", side_effect=mock_fetch_all), \
         patch("db.execute", side_effect=mock_execute):
        rebuild_ticket_handoffs([100])

    assert len(handoffs) == 0


# ── rebuild_ticket_wait_states ───────────────────────────────────────

def test_rebuild_wait_states_creates_segments(patch_pool):
    """Wait states should create segments based on action stream."""
    conn, cur = patch_pool
    from run_analytics import rebuild_ticket_wait_states

    t1 = datetime(2026, 3, 1, 10, 0, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 3, 1, 11, 0, 0, tzinfo=timezone.utc)
    t3 = datetime(2026, 3, 1, 12, 0, 0, tzinfo=timezone.utc)

    actions = [
        (1, "cust", t1, False, "customer_problem_statement", "I have a problem"),
        (2, "inh", t2, False, "technical_work", "Working on fix"),
        (3, "cust", t3, False, "customer_problem_statement", "Still broken"),
    ]

    segments = []

    def mock_fetch_all(sql, params=()):
        if "ticket_actions" in sql:
            return actions
        return []

    def mock_execute(sql, params=()):
        if "INSERT INTO ticket_wait_states" in sql:
            segments.append(params)

    with patch("db.fetch_all", side_effect=mock_fetch_all), \
         patch("db.execute", side_effect=mock_execute):
        result = rebuild_ticket_wait_states([100])

    assert result == 1
    # 3 actions with 3 state changes: waiting_on_support -> active_work -> waiting_on_support
    assert len(segments) == 3
    assert segments[0][1] == "waiting_on_support"  # state_name
    assert segments[1][1] == "active_work"
    assert segments[2][1] == "waiting_on_support"

    # First two segments should have duration, last one should be open-ended
    assert segments[0][4] is not None  # duration_minutes
    assert segments[1][4] is not None
    assert segments[2][3] is None  # end_at (open-ended)
    assert segments[2][4] is None  # duration_minutes (open-ended)


def test_rebuild_wait_states_empty_actions(patch_pool):
    """No segments for a ticket with no meaningful actions."""
    conn, cur = patch_pool
    from run_analytics import rebuild_ticket_wait_states

    def mock_fetch_all(sql, params=()):
        if "ticket_actions" in sql:
            return []
        return []

    def mock_execute(sql, params=()):
        pass

    with patch("db.fetch_all", side_effect=mock_fetch_all), \
         patch("db.execute", side_effect=mock_execute):
        result = rebuild_ticket_wait_states([100])

    assert result == 1  # Still processes the ticket


# ── snapshot_tickets_daily ───────────────────────────────────────────

def test_snapshot_daily_creates_rows(patch_pool):
    """Snapshot should create rows for each ticket."""
    conn, cur = patch_pool
    from run_analytics import snapshot_tickets_daily

    now = datetime(2026, 3, 14, 12, 0, 0, tzinfo=timezone.utc)
    created = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    ticket_rows = [
        (100, "T100", "Test Ticket", "Open", "Agent", "Widget", "Acme",
         created, now, None, 73, 0, now),
    ]

    snapshots = []

    def mock_fetch_all(sql, params=()):
        if "FROM tickets t" in sql:
            return ticket_rows
        if "ticket_priority_scores" in sql:
            return [(100, 5)]
        if "ticket_complexity_scores" in sql:
            return [(100, 3)]
        if "ticket_wait_states" in sql:
            return [(100, "active_work")]
        return []

    def mock_execute(sql, params=()):
        if "INSERT INTO ticket_snapshots_daily" in sql:
            snapshots.append(params)

    with patch("db.fetch_all", side_effect=mock_fetch_all), \
         patch("db.execute", side_effect=mock_execute):
        result = snapshot_tickets_daily(snapshot_date=date(2026, 3, 14))

    assert result == 1
    assert len(snapshots) == 1
    # Verify key fields
    s = snapshots[0]
    assert s[0] == date(2026, 3, 14)  # snapshot_date
    assert s[1] == 100  # ticket_id
    assert s[8] is True  # open_flag (closed_at is None, status is Open)
    assert s[11] == 5  # priority
    assert s[12] == 3  # overall_complexity


def test_snapshot_daily_open_flag_closed_ticket(patch_pool):
    """A closed ticket should have open_flag=False."""
    conn, cur = patch_pool
    from run_analytics import snapshot_tickets_daily

    closed_at = datetime(2026, 3, 10, 12, 0, 0, tzinfo=timezone.utc)
    created = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

    ticket_rows = [
        (100, "T100", "Closed Ticket", "Closed", "Agent", "Widget", "Acme",
         created, closed_at, closed_at, 69, 4, closed_at),
    ]

    snapshots = []

    def mock_fetch_all(sql, params=()):
        if "FROM tickets t" in sql:
            return ticket_rows
        return []

    def mock_execute(sql, params=()):
        if "INSERT INTO ticket_snapshots_daily" in sql:
            snapshots.append(params)

    with patch("db.fetch_all", side_effect=mock_fetch_all), \
         patch("db.execute", side_effect=mock_execute):
        snapshot_tickets_daily(snapshot_date=date(2026, 3, 14))

    assert snapshots[0][8] is False  # open_flag


def test_snapshot_daily_high_priority_flag(patch_pool):
    """Priority <= 3 should set high_priority_flag=True."""
    conn, cur = patch_pool
    from run_analytics import snapshot_tickets_daily

    created = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    now = datetime(2026, 3, 14, 12, 0, 0, tzinfo=timezone.utc)

    ticket_rows = [
        (100, "T100", "Urgent", "Open", "Agent", "Widget", "Acme",
         created, now, None, 73, 0, now),
    ]

    snapshots = []

    def mock_fetch_all(sql, params=()):
        if "FROM tickets t" in sql:
            return ticket_rows
        if "ticket_priority_scores" in sql:
            return [(100, 2)]  # priority 2 → high
        return []

    def mock_execute(sql, params=()):
        if "INSERT INTO ticket_snapshots_daily" in sql:
            snapshots.append(params)

    with patch("db.fetch_all", side_effect=mock_fetch_all), \
         patch("db.execute", side_effect=mock_execute):
        snapshot_tickets_daily(snapshot_date=date(2026, 3, 14))

    assert snapshots[0][14] is True  # high_priority_flag


def test_snapshot_daily_high_complexity_flag(patch_pool):
    """overall_complexity >= 4 should set high_complexity_flag=True."""
    conn, cur = patch_pool
    from run_analytics import snapshot_tickets_daily

    created = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    now = datetime(2026, 3, 14, 12, 0, 0, tzinfo=timezone.utc)

    ticket_rows = [
        (100, "T100", "Complex", "Open", "Agent", "Widget", "Acme",
         created, now, None, 73, 0, now),
    ]

    snapshots = []

    def mock_fetch_all(sql, params=()):
        if "FROM tickets t" in sql:
            return ticket_rows
        if "ticket_complexity_scores" in sql:
            return [(100, 4)]  # complexity 4 → high
        return []

    def mock_execute(sql, params=()):
        if "INSERT INTO ticket_snapshots_daily" in sql:
            snapshots.append(params)

    with patch("db.fetch_all", side_effect=mock_fetch_all), \
         patch("db.execute", side_effect=mock_execute):
        snapshot_tickets_daily(snapshot_date=date(2026, 3, 14))

    assert snapshots[0][15] is True  # high_complexity_flag


# ── rebuild_customer_ticket_health ───────────────────────────────────

def test_rebuild_customer_health_computes_pressure_score(patch_pool):
    """Verify pressure score formula: open + 2*hp + 1.5*hc + 3*frust."""
    conn, cur = patch_pool
    from run_analytics import rebuild_customer_ticket_health

    customer_rows = [
        ("Acme", 10, 3, 2, 3.5, [100, 101, 102]),
    ]

    health_rows = []

    def mock_fetch_all(sql, params=()):
        if "FROM ticket_snapshots_daily" in sql or "latest" in sql.lower():
            return customer_rows
        if "ticket_complexity_scores" in sql:
            return []
        if "ticket_sentiment" in sql:
            return []
        if "ticket_clusters" in sql:
            return []
        if "product_name" in sql:
            return []
        return []

    def mock_execute(sql, params=()):
        if "INSERT INTO customer_ticket_health" in sql:
            health_rows.append(params)

    with patch("db.fetch_all", side_effect=mock_fetch_all), \
         patch("db.execute", side_effect=mock_execute):
        result = rebuild_customer_ticket_health(as_of_date=date(2026, 3, 14))

    assert result == 1
    assert len(health_rows) == 1
    # Pressure: 10 + 2*3 + 1.5*2 + 3*0 = 10 + 6 + 3 + 0 = 19
    pressure = health_rows[0][11]  # ticket_load_pressure_score
    assert pressure == 19.0


# ── rebuild_product_ticket_health ────────────────────────────────────

def test_rebuild_product_health_computes_rates(patch_pool):
    """Verify dev_touched_rate and customer_wait_rate derivation."""
    conn, cur = patch_pool
    from run_analytics import rebuild_product_ticket_health

    product_rows = [
        ("Widget", 4, [100, 101, 102, 103]),
    ]

    health_rows = []

    def mock_fetch_all(sql, params=()):
        if "FROM ticket_snapshots_daily" in sql or "latest" in sql.lower():
            return product_rows
        if "ticket_complexity_scores" in sql:
            return [(100, 3, 2, 1), (101, 4, 3, 2)]
        if "ticket_wait_states" in sql:
            return [(100, "waiting_on_customer"), (101, "active_work")]
        if "ticket_clusters" in sql:
            return []
        if "ticket_issue_summaries" in sql:
            return []
        if "action_class" in sql:
            return [(100,), (102,)]  # 2 of 4 tickets have dev involvement
        return []

    def mock_execute(sql, params=()):
        if "INSERT INTO product_ticket_health" in sql:
            health_rows.append(params)

    with patch("db.fetch_all", side_effect=mock_fetch_all), \
         patch("db.execute", side_effect=mock_execute):
        result = rebuild_product_ticket_health(as_of_date=date(2026, 3, 14))

    assert result == 1
    assert len(health_rows) == 1
    # dev_touched_rate: 2/4 = 0.5
    assert health_rows[0][8] == 0.5  # dev_touched_rate
    # customer_wait_rate: 1/4 = 0.25
    assert health_rows[0][9] == 0.25  # customer_wait_rate


# ── Migration file existence ─────────────────────────────────────────

def test_migration_003_exists():
    """Migration 003_analytics_extension.sql should exist."""
    import os
    path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "migrations", "003_analytics_extension.sql"
    )
    assert os.path.isfile(path)
    content = open(path).read()
    assert "ticket_wait_states" in content
    assert "ticket_participants" in content
    assert "ticket_handoffs" in content
    assert "ticket_snapshots_daily" in content
    assert "ticket_issue_summaries" in content
    assert "ticket_embeddings" in content
    assert "cluster_runs" in content
    assert "ticket_clusters" in content
    assert "cluster_catalog" in content
    assert "ticket_interventions" in content
    assert "customer_ticket_health" in content
    assert "product_ticket_health" in content
    assert "enrichment_runs" in content


def test_migration_004_exists():
    """Migration 004_analytics_views.sql should exist."""
    import os
    path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "migrations", "004_analytics_views.sql"
    )
    assert os.path.isfile(path)
    content = open(path).read()
    assert "vw_latest_ticket_sentiment" in content
    assert "vw_latest_ticket_priority" in content
    assert "vw_latest_ticket_complexity" in content
    assert "vw_latest_ticket_issue_summary" in content
    assert "vw_ticket_analytics_core" in content
    assert "vw_ticket_complexity_breakdown" in content
    assert "vw_ticket_wait_profile" in content
    assert "vw_customer_support_risk" in content
    assert "vw_product_pain_patterns" in content
    assert "vw_intervention_opportunities" in content
    assert "vw_backlog_daily" in content
    assert "vw_backlog_weekly" in content
    assert "vw_backlog_weekly_eow" in content
    assert "vw_backlog_aging_current" in content
    assert "vw_backlog_weekly_from_dates" in content
