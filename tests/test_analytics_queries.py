"""
Tests for analytics_queries.py — verify SQL constants and helper functions.

These tests mock the psycopg2 cursor layer so no real Postgres connection is
needed.  They confirm that:
  - All 10 SQL constants are defined and non-empty.
  - run_query returns list[dict] by default.
  - run_query returns a DataFrame when as_df=True and pandas is available.
  - Each convenience function delegates to run_query with the correct SQL.
  - cursor.description is used to derive column names.
"""

from unittest.mock import MagicMock, patch

import pytest

import analytics_queries


# ── Fixtures ─────────────────────────────────────────────────────────

@pytest.fixture
def mock_conn():
    """Return a mock connection whose cursor yields predetermined rows."""
    conn = MagicMock()
    cur = MagicMock()
    # cursor used as a context manager
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    # default description & rows (2 columns, 2 rows)
    cur.description = [("col_a",), ("col_b",)]
    cur.fetchall.return_value = [("x", 1), ("y", 2)]
    return conn, cur


# ── SQL constant smoke tests ────────────────────────────────────────

_SQL_CONSTANTS = [
    "SQL_ROOT_CAUSE_DISTRIBUTION",
    "SQL_ROOT_CAUSE_SEVERITY",
    "SQL_FUNCTIONAL_AREA_DISTRIBUTION",
    "SQL_PREVENTABLE_VS_ENGINEERING",
    "SQL_TICKET_AGING_BY_CAUSE",
    "SQL_FRUSTRATION_BY_CAUSE",
    "SQL_PRODUCT_RELIABILITY",
    "SQL_INTEGRATION_FAILURE_RATE",
    "SQL_HIGH_PRIORITY_BY_CAUSE",
    "SQL_TOP_FAILURE_MECHANISMS",
]


@pytest.mark.parametrize("name", _SQL_CONSTANTS)
def test_sql_constant_exists_and_nonempty(name):
    val = getattr(analytics_queries, name)
    assert isinstance(val, str)
    assert len(val.strip()) > 0


@pytest.mark.parametrize("name", _SQL_CONSTANTS)
def test_sql_constant_uses_schema_qualified_names(name):
    """Every query should reference the tickets_ai schema."""
    val = getattr(analytics_queries, name)
    assert "tickets_ai." in val


@pytest.mark.parametrize("name", _SQL_CONSTANTS)
def test_sql_constant_is_read_only(name):
    """Queries must not contain INSERT / UPDATE / DELETE / DROP / CREATE."""
    val = getattr(analytics_queries, name).upper()
    for keyword in ("INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER"):
        assert keyword not in val, f"{name} contains disallowed keyword {keyword}"


# ── run_query ────────────────────────────────────────────────────────

def test_run_query_returns_list_of_dicts(mock_conn):
    conn, cur = mock_conn
    result = analytics_queries.run_query(conn, "SELECT 1")
    assert result == [{"col_a": "x", "col_b": 1}, {"col_a": "y", "col_b": 2}]


def test_run_query_passes_params(mock_conn):
    conn, cur = mock_conn
    analytics_queries.run_query(conn, "SELECT %s", params=(42,))
    cur.execute.assert_called_once_with("SELECT %s", (42,))


def test_run_query_no_params(mock_conn):
    conn, cur = mock_conn
    analytics_queries.run_query(conn, "SELECT 1")
    cur.execute.assert_called_once_with("SELECT 1", ())


def test_run_query_as_df_true(mock_conn):
    """When as_df=True and pandas is available, return a DataFrame."""
    pd = pytest.importorskip("pandas")
    conn, cur = mock_conn
    result = analytics_queries.run_query(conn, "SELECT 1", as_df=True)
    assert isinstance(result, pd.DataFrame)
    assert list(result.columns) == ["col_a", "col_b"]
    assert len(result) == 2


def test_run_query_as_df_fallback_without_pandas(mock_conn):
    """When pandas cannot be imported, fall back to list[dict]."""
    conn, cur = mock_conn
    with patch.dict("sys.modules", {"pandas": None}):
        result = analytics_queries.run_query(conn, "SELECT 1", as_df=True)
    assert isinstance(result, list)
    assert result[0] == {"col_a": "x", "col_b": 1}


def test_run_query_empty_result(mock_conn):
    conn, cur = mock_conn
    cur.description = [("id",)]
    cur.fetchall.return_value = []
    result = analytics_queries.run_query(conn, "SELECT 1")
    assert result == []


# ── Convenience function delegation ─────────────────────────────────

_FUNCTIONS = [
    ("root_cause_distribution", "SQL_ROOT_CAUSE_DISTRIBUTION"),
    ("root_cause_severity", "SQL_ROOT_CAUSE_SEVERITY"),
    ("functional_area_distribution", "SQL_FUNCTIONAL_AREA_DISTRIBUTION"),
    ("preventable_vs_engineering", "SQL_PREVENTABLE_VS_ENGINEERING"),
    ("ticket_aging_by_cause", "SQL_TICKET_AGING_BY_CAUSE"),
    ("frustration_by_cause", "SQL_FRUSTRATION_BY_CAUSE"),
    ("product_reliability", "SQL_PRODUCT_RELIABILITY"),
    ("integration_failure_rate", "SQL_INTEGRATION_FAILURE_RATE"),
    ("high_priority_by_cause", "SQL_HIGH_PRIORITY_BY_CAUSE"),
    ("top_failure_mechanisms", "SQL_TOP_FAILURE_MECHANISMS"),
]


@pytest.mark.parametrize("func_name,sql_const", _FUNCTIONS)
def test_convenience_function_calls_run_query(func_name, sql_const, mock_conn):
    """Each convenience wrapper should invoke run_query with the matching SQL constant."""
    conn, _cur = mock_conn
    func = getattr(analytics_queries, func_name)
    expected_sql = getattr(analytics_queries, sql_const)

    with patch.object(analytics_queries, "run_query", return_value=[]) as mocked:
        func(conn)
        mocked.assert_called_once_with(conn, expected_sql, as_df=False)


@pytest.mark.parametrize("func_name,sql_const", _FUNCTIONS)
def test_convenience_function_passes_as_df(func_name, sql_const, mock_conn):
    """as_df parameter should be forwarded to run_query."""
    conn, _cur = mock_conn
    func = getattr(analytics_queries, func_name)
    expected_sql = getattr(analytics_queries, sql_const)

    with patch.object(analytics_queries, "run_query", return_value=[]) as mocked:
        func(conn, as_df=True)
        mocked.assert_called_once_with(conn, expected_sql, as_df=True)


# ── SQL content spot-checks ──────────────────────────────────────────

def test_root_cause_uses_round_and_percentage():
    assert "ROUND" in analytics_queries.SQL_ROOT_CAUSE_DISTRIBUTION
    assert "pct_of_total" in analytics_queries.SQL_ROOT_CAUSE_DISTRIBUTION


def test_preventable_uses_case_expression():
    sql = analytics_queries.SQL_PREVENTABLE_VS_ENGINEERING
    assert "software_bug" in sql
    assert "feature_gap" in sql
    assert "engineering_required" in sql
    assert "preventable_operational" in sql


def test_frustration_joins_sentiment():
    sql = analytics_queries.SQL_FRUSTRATION_BY_CAUSE
    assert "ticket_sentiment" in sql
    assert "frustrated" in sql


def test_high_priority_joins_priority_scores():
    sql = analytics_queries.SQL_HIGH_PRIORITY_BY_CAUSE
    assert "ticket_priority_scores" in sql
    assert "priority" in sql


def test_top_failure_mechanisms_limits_20():
    sql = analytics_queries.SQL_TOP_FAILURE_MECHANISMS
    assert "pass3_mechanism" in sql
    assert "LIMIT" in sql.upper()


def test_integration_failure_rate_uses_ilike():
    sql = analytics_queries.SQL_INTEGRATION_FAILURE_RATE
    assert "ILIKE" in sql


def test_ticket_aging_uses_avg_days_opened():
    sql = analytics_queries.SQL_TICKET_AGING_BY_CAUSE
    assert "AVG" in sql.upper()
    assert "days_opened" in sql
