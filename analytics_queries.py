"""
analytics_queries — Operational analytics SQL queries for the support-analytics pipeline.

This module provides 10 read-only analytics queries against the ``tickets_ai``
schema plus a generic ``run_query`` helper and one convenience function per
query.

Usage:
    import analytics_queries
    rows = analytics_queries.root_cause_distribution(conn)
    df   = analytics_queries.top_failure_mechanisms(conn, as_df=True)

All queries are PostgreSQL-compatible, use schema-qualified table names, and
return either ``list[dict]`` (default) or a ``pandas.DataFrame`` when
*as_df=True*.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

# ---------------------------------------------------------------------------
# 1. Root Cause Distribution
# ---------------------------------------------------------------------------

SQL_ROOT_CAUSE_DISTRIBUTION = """\
SELECT
    root_cause_class,
    COUNT(*)                                         AS ticket_count,
    ROUND(100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0), 1)
                                                     AS pct_of_total
FROM   tickets_ai.ticket_issue_summaries
GROUP  BY root_cause_class
ORDER  BY ticket_count DESC;
"""

# ---------------------------------------------------------------------------
# 2. Root Cause × Severity
# ---------------------------------------------------------------------------

SQL_ROOT_CAUSE_SEVERITY = """\
SELECT
    root_cause_class,
    severity,
    COUNT(*) AS ticket_count
FROM   tickets_ai.ticket_issue_summaries
GROUP  BY root_cause_class, severity
ORDER  BY root_cause_class, severity;
"""

# ---------------------------------------------------------------------------
# 3. Tickets by Functional Area
# ---------------------------------------------------------------------------

SQL_FUNCTIONAL_AREA_DISTRIBUTION = """\
SELECT
    functional_area,
    COUNT(*) AS ticket_count
FROM   tickets_ai.ticket_issue_summaries
GROUP  BY functional_area
ORDER  BY ticket_count DESC;
"""

# ---------------------------------------------------------------------------
# 4. Preventable vs Engineering Tickets
# ---------------------------------------------------------------------------

SQL_PREVENTABLE_VS_ENGINEERING = """\
SELECT
    CASE
        WHEN root_cause_class IN ('software_bug', 'feature_gap')
            THEN 'engineering_required'
        ELSE 'preventable_operational'
    END                       AS category,
    COUNT(*)                  AS ticket_count,
    ROUND(100.0 * COUNT(*) / NULLIF(SUM(COUNT(*)) OVER (), 0), 1)
                              AS pct_of_total
FROM   tickets_ai.ticket_issue_summaries
GROUP  BY category
ORDER  BY ticket_count DESC;
"""

# ---------------------------------------------------------------------------
# 5. Ticket Aging by Root Cause
# ---------------------------------------------------------------------------

SQL_TICKET_AGING_BY_CAUSE = """\
SELECT
    s.root_cause_class,
    COUNT(*)                                 AS ticket_count,
    ROUND(AVG(t.days_opened)::numeric, 1)   AS avg_days_opened
FROM   tickets_ai.ticket_issue_summaries s
JOIN   tickets_ai.tickets t
       ON t.ticket_number = s.ticket_number
GROUP  BY s.root_cause_class
ORDER  BY avg_days_opened DESC;
"""

# ---------------------------------------------------------------------------
# 6. Customer Frustration by Cause
# ---------------------------------------------------------------------------

SQL_FRUSTRATION_BY_CAUSE = """\
WITH latest_sentiment AS (
    SELECT DISTINCT ON (ticket_id)
           ticket_id,
           frustrated
    FROM   tickets_ai.ticket_sentiment
    ORDER  BY ticket_id, scored_at DESC
)
SELECT
    s.root_cause_class,
    COUNT(*) FILTER (WHERE ls.frustrated = 'Yes')  AS frustrated_count,
    COUNT(*)                                        AS total_count,
    ROUND(
        100.0
        * COUNT(*) FILTER (WHERE ls.frustrated = 'Yes')
        / NULLIF(COUNT(*), 0),
        1
    )                                               AS frustration_rate_pct
FROM   tickets_ai.ticket_issue_summaries s
JOIN   tickets_ai.tickets t
       ON t.ticket_number = s.ticket_number
LEFT JOIN latest_sentiment ls
       ON ls.ticket_id = t.ticket_id
GROUP  BY s.root_cause_class
ORDER  BY frustration_rate_pct DESC NULLS LAST;
"""

# ---------------------------------------------------------------------------
# 7. Product Reliability
# ---------------------------------------------------------------------------

SQL_PRODUCT_RELIABILITY = """\
SELECT
    t.product_name,
    COUNT(*) AS ticket_count
FROM   tickets_ai.ticket_issue_summaries s
JOIN   tickets_ai.tickets t
       ON t.ticket_number = s.ticket_number
GROUP  BY t.product_name
ORDER  BY ticket_count DESC;
"""

# ---------------------------------------------------------------------------
# 8. Integration Failure Rate
# ---------------------------------------------------------------------------

SQL_INTEGRATION_FAILURE_RATE = """\
SELECT
    COUNT(*) FILTER (WHERE s.functional_area ILIKE '%%integration%%')
                                                     AS integration_tickets,
    COUNT(*)                                         AS total_analyzed,
    ROUND(
        100.0
        * COUNT(*) FILTER (WHERE s.functional_area ILIKE '%%integration%%')
        / NULLIF(COUNT(*), 0),
        1
    )                                                AS integration_pct
FROM   tickets_ai.ticket_issue_summaries s;
"""

# ---------------------------------------------------------------------------
# 9. High Priority Backlog by Root Cause
# ---------------------------------------------------------------------------

SQL_HIGH_PRIORITY_BY_CAUSE = """\
WITH latest_priority AS (
    SELECT DISTINCT ON (ticket_id)
           ticket_id,
           priority
    FROM   tickets_ai.ticket_priority_scores
    ORDER  BY ticket_id, scored_at DESC
)
SELECT
    s.root_cause_class,
    COUNT(*) AS high_priority_count
FROM   tickets_ai.ticket_issue_summaries s
JOIN   tickets_ai.tickets t
       ON t.ticket_number = s.ticket_number
JOIN   latest_priority lp
       ON lp.ticket_id = t.ticket_id
WHERE  lp.priority <= 3
GROUP  BY s.root_cause_class
ORDER  BY high_priority_count DESC;
"""

# ---------------------------------------------------------------------------
# 10. Top Failure Mechanisms
# ---------------------------------------------------------------------------

SQL_TOP_FAILURE_MECHANISMS = """\
SELECT
    mechanism,
    COUNT(*) AS occurrence_count
FROM   tickets_ai.ticket_llm_pass_results
WHERE  pass_name = 'pass2_mechanism'
  AND  mechanism IS NOT NULL
GROUP  BY mechanism
ORDER  BY occurrence_count DESC
LIMIT  20;
"""


# ═══════════════════════════════════════════════════════════════════════
# Generic query runner
# ═══════════════════════════════════════════════════════════════════════

def run_query(
    conn,
    sql: str,
    params: Optional[Sequence] = None,
    as_df: bool = False,
) -> Any:
    """Execute a read-only SQL query and return the results.

    Parameters
    ----------
    conn : psycopg2 connection
        An open database connection (caller is responsible for lifecycle).
    sql : str
        The SQL query to execute.
    params : sequence, optional
        Bind parameters for the query (psycopg2 ``%s`` style).
    as_df : bool, default False
        If *True* and pandas is available, return a ``pandas.DataFrame``.
        Otherwise return ``list[dict]``.

    Returns
    -------
    list[dict] | pandas.DataFrame
        Query results.
    """
    with conn.cursor() as cur:
        cur.execute(sql, params or ())
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()

    if as_df:
        try:
            import pandas as pd  # noqa: F811
            return pd.DataFrame(rows, columns=columns)
        except ImportError:
            pass  # fall through to list[dict]

    return [dict(zip(columns, row)) for row in rows]


# ═══════════════════════════════════════════════════════════════════════
# Per-query convenience functions
# ═══════════════════════════════════════════════════════════════════════

def root_cause_distribution(conn, *, as_df: bool = False):
    """Count tickets by *root_cause_class* with percentage of total."""
    return run_query(conn, SQL_ROOT_CAUSE_DISTRIBUTION, as_df=as_df)


def root_cause_severity(conn, *, as_df: bool = False):
    """Count tickets grouped by *root_cause_class* and *severity*."""
    return run_query(conn, SQL_ROOT_CAUSE_SEVERITY, as_df=as_df)


def functional_area_distribution(conn, *, as_df: bool = False):
    """Count tickets grouped by *functional_area*."""
    return run_query(conn, SQL_FUNCTIONAL_AREA_DISTRIBUTION, as_df=as_df)


def preventable_vs_engineering(conn, *, as_df: bool = False):
    """Split tickets into *engineering_required* vs *preventable_operational*."""
    return run_query(conn, SQL_PREVENTABLE_VS_ENGINEERING, as_df=as_df)


def ticket_aging_by_cause(conn, *, as_df: bool = False):
    """Average *days_opened* by *root_cause_class*."""
    return run_query(conn, SQL_TICKET_AGING_BY_CAUSE, as_df=as_df)


def frustration_by_cause(conn, *, as_df: bool = False):
    """Frustrated-ticket count and frustration rate by *root_cause_class*."""
    return run_query(conn, SQL_FRUSTRATION_BY_CAUSE, as_df=as_df)


def product_reliability(conn, *, as_df: bool = False):
    """Count analyzed tickets by *product_name*."""
    return run_query(conn, SQL_PRODUCT_RELIABILITY, as_df=as_df)


def integration_failure_rate(conn, *, as_df: bool = False):
    """Count integration-related tickets and their share of all analyzed tickets."""
    return run_query(conn, SQL_INTEGRATION_FAILURE_RATE, as_df=as_df)


def high_priority_by_cause(conn, *, as_df: bool = False):
    """Count high-priority (priority ≤ 3) tickets by *root_cause_class*."""
    return run_query(conn, SQL_HIGH_PRIORITY_BY_CAUSE, as_df=as_df)


def top_failure_mechanisms(conn, *, as_df: bool = False):
    """Top 20 failure mechanisms from Pass 2 results."""
    return run_query(conn, SQL_TOP_FAILURE_MECHANISMS, as_df=as_df)


# ═══════════════════════════════════════════════════════════════════════
# Example usage (for reference)
# ═══════════════════════════════════════════════════════════════════════
#
#   import psycopg2
#   import analytics_queries
#
#   conn = psycopg2.connect("postgresql://user:pass@localhost:5432/Work")
#   try:
#       # List of dicts
#       rows = analytics_queries.root_cause_distribution(conn)
#       for r in rows:
#           print(r)
#
#       # Pandas DataFrame
#       df = analytics_queries.top_failure_mechanisms(conn, as_df=True)
#       print(df)
#   finally:
#       conn.close()
