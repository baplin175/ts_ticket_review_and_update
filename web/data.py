"""
Read-only data access layer for the web dashboard.

Imports db.py and config.py from the parent project without modifying them.
All functions are SELECT-only — no writes to Postgres or TeamSupport.
The one exception is saved_reports (dashboard-local CRUD, never touches TS).
"""

import json
import sys
import os
import uuid as _uuid
from datetime import datetime, date
from decimal import Decimal

# Allow imports of db / config from the project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import psycopg2.extras  # noqa: E402
import db               # noqa: E402


# ── Query helpers ────────────────────────────────────────────────────

def _serialize_value(v):
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, _uuid.UUID):
        return str(v)
    return v


def _serialize_rows(rows):
    return [{k: _serialize_value(v) for k, v in dict(r).items()} for r in rows]


def query(sql, params=()):
    """Run a SELECT and return a list of dicts."""
    conn = db.get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            return _serialize_rows(cur.fetchall())
    finally:
        db.put_conn(conn)


def query_one(sql, params=()):
    """Run a SELECT and return one dict (or None)."""
    conn = db.get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            return {k: _serialize_value(v) for k, v in dict(row).items()} if row else None
    finally:
        db.put_conn(conn)


# ── Overview ─────────────────────────────────────────────────────────

def get_open_ticket_stats():
    return query_one("""
        SELECT
            COUNT(*) AS total_open,
            COUNT(*) FILTER (WHERE priority IS NOT NULL AND priority <= 3) AS high_priority,
            COUNT(*) FILTER (WHERE overall_complexity >= 4) AS high_complexity,
            COUNT(*) FILTER (WHERE frustrated = 'Yes') AS frustrated
        FROM vw_ticket_analytics_core
        WHERE closed_at IS NULL
    """) or {"total_open": 0, "high_priority": 0, "high_complexity": 0, "frustrated": 0}


def get_backlog_daily():
    return query(
        "SELECT * FROM vw_backlog_daily WHERE snapshot_date >= '2024-07-01' ORDER BY snapshot_date"
    )


def get_backlog_daily_by_severity():
    return query(
        "SELECT * FROM vw_backlog_daily_by_severity WHERE snapshot_date >= '2024-07-01' ORDER BY snapshot_date, severity_tier"
    )


def get_backlog_aging():
    return query("SELECT * FROM vw_backlog_aging_current")


def get_aging_by_product(min_open=50):
    """Aging breakdown per product (PowerMan-consolidated), only products with >= min_open tickets."""
    return query("""
        WITH product_aging AS (
            SELECT
                CASE
                    WHEN LOWER(product_name) LIKE 'pm%%'
                      OR LOWER(product_name) LIKE '%%power%%'
                    THEN 'PowerMan'
                    ELSE COALESCE(NULLIF(product_name, ''), 'Unknown')
                END AS product_name,
                CASE
                    WHEN age_days <  7  THEN '0-6'
                    WHEN age_days < 14  THEN '7-13'
                    WHEN age_days < 30  THEN '14-29'
                    WHEN age_days < 60  THEN '30-59'
                    WHEN age_days < 90  THEN '60-89'
                    ELSE '90+'
                END AS age_bucket,
                age_days
            FROM ticket_snapshots_daily
            WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM ticket_snapshots_daily)
              AND open_flag
        ),
        product_totals AS (
            SELECT product_name, COUNT(*) AS total
            FROM product_aging
            GROUP BY product_name
            HAVING COUNT(*) >= %s
        )
        SELECT pa.product_name, pa.age_bucket, COUNT(*) AS ticket_count,
               pt.total AS product_total
        FROM product_aging pa
        JOIN product_totals pt ON pt.product_name = pa.product_name
        GROUP BY pa.product_name, pa.age_bucket, pt.total
        ORDER BY pt.total DESC, pa.product_name, MIN(pa.age_days)
    """, (min_open,))


def get_open_by_product():
    return query("""
        SELECT product_name, severity_tier, ticket_count
        FROM vw_backlog_product_severity_powman
        ORDER BY product_name, severity_tier
    """)


def get_open_by_status():
    return query("""
        SELECT COALESCE(status, 'Unknown') AS status, COUNT(*) AS count
        FROM tickets
        WHERE closed_at IS NULL
        GROUP BY status
        ORDER BY count DESC
    """)


# ── Ticket list ──────────────────────────────────────────────────────

def get_ticket_list():
    return query("""
        SELECT ticket_id, ticket_number, ticket_name, status, severity,
               product_name, assignee, customer,
               date_created, date_modified, days_opened, days_since_modified,
               action_count, customer_message_count, inhance_message_count,
               priority, priority_explanation,
               overall_complexity, frustrated
        FROM vw_ticket_analytics_core
        ORDER BY date_modified DESC NULLS LAST
    """)


# ── Ticket detail ───────────────────────────────────────────────────

def get_ticket_detail(ticket_id):
    return query_one("""
        SELECT * FROM vw_ticket_analytics_core WHERE ticket_id = %s
    """, (ticket_id,))


def get_ticket_actions(ticket_id):
    return query("""
        SELECT action_id, created_at, action_type, creator_name, party,
               is_visible, cleaned_description, description,
               action_class, is_empty
        FROM ticket_actions
        WHERE ticket_id = %s
        ORDER BY created_at DESC
    """, (ticket_id,))


def get_ticket_wait_profile(ticket_id):
    return query_one("""
        SELECT * FROM vw_ticket_wait_profile WHERE ticket_id = %s
    """, (ticket_id,))


# ── Health ───────────────────────────────────────────────────────────

def get_customer_health():
    return query("""
        SELECT * FROM customer_ticket_health
        WHERE as_of_date = (SELECT MAX(as_of_date) FROM customer_ticket_health)
        ORDER BY ticket_load_pressure_score DESC NULLS LAST
    """)


def get_product_health():
    return query("""
        SELECT * FROM product_ticket_health
        WHERE as_of_date = (SELECT MAX(as_of_date) FROM product_ticket_health)
        ORDER BY ticket_volume DESC NULLS LAST
    """)


# ── Drill-down ───────────────────────────────────────────────────────

AGE_BUCKET_RANGES = {
    "0-6": (0, 7), "7-13": (7, 14), "14-29": (14, 30),
    "30-59": (30, 60), "60-89": (60, 90), "90+": (90, None),
}

SEVERITY_TIER_SQL = {
    "High":   "(t.severity LIKE '1%%' OR LOWER(t.severity) LIKE '%%high%%')",
    "Low":    "(t.severity LIKE '3%%' OR LOWER(t.severity) LIKE '%%low%%')",
    "Medium": "NOT (t.severity LIKE '1%%' OR LOWER(t.severity) LIKE '%%high%%') "
              "AND NOT (t.severity LIKE '3%%' OR LOWER(t.severity) LIKE '%%low%%')",
}


KPI_FILTERS = {
    "total_open": [],  # base open-ticket conditions are enough
    "high_priority": ["v.priority IS NOT NULL AND v.priority <= 3"],
    "high_complexity": ["v.overall_complexity >= 4"],
    "frustrated": ["v.frustrated = 'Yes'"],
}


def get_drilldown_tickets(product=None, severity_tier=None, age_bucket=None,
                          kpi_filter=None):
    """Return open tickets matching chart drill-down filters."""
    conditions = [
        "t.closed_at IS NULL",
        "COALESCE(t.status, '') NOT IN ('Closed', 'Resolved')",
    ]
    params = []

    if kpi_filter and kpi_filter in KPI_FILTERS:
        conditions.extend(KPI_FILTERS[kpi_filter])

    if product:
        if product == "PowerMan":
            conditions.append(
                "(LOWER(t.product_name) LIKE 'pm%%' OR LOWER(t.product_name) LIKE '%%power%%')"
            )
        else:
            conditions.append("t.product_name = %s")
            params.append(product)

    if severity_tier and severity_tier in SEVERITY_TIER_SQL:
        conditions.append(SEVERITY_TIER_SQL[severity_tier])

    if age_bucket and age_bucket in AGE_BUCKET_RANGES:
        lo, hi = AGE_BUCKET_RANGES[age_bucket]
        conditions.append("EXTRACT(DAY FROM now() - t.date_created)::int >= %s")
        params.append(lo)
        if hi is not None:
            conditions.append("EXTRACT(DAY FROM now() - t.date_created)::int < %s")
            params.append(hi)

    where = " AND ".join(conditions)
    return query(f"""
        SELECT v.ticket_id, v.ticket_number, v.ticket_name, v.status, v.severity,
               v.product_name, v.assignee, v.customer, v.days_opened, v.priority,
               v.overall_complexity, v.frustrated, v.date_modified
        FROM tickets t
        JOIN vw_ticket_analytics_core v ON v.ticket_id = t.ticket_id
        WHERE {where}
        ORDER BY v.date_modified DESC NULLS LAST
    """, tuple(params))


# ── Sync info ────────────────────────────────────────────────────────

def get_sync_status():
    return query("""
        SELECT source_name, last_successful_sync_at, last_status, last_error
        FROM sync_state ORDER BY source_name
    """)


def get_recent_ingest_runs(limit=10):
    return query("""
        SELECT ingest_run_id, source_name, started_at, completed_at,
               status, tickets_seen, tickets_upserted,
               actions_seen, actions_upserted, error_text
        FROM ingest_runs
        ORDER BY started_at DESC
        LIMIT %s
    """, (limit,))


# ── Root Cause (LLM pass results) ───────────────────────────────────

def get_root_cause_tickets():
    """Return tickets that have at least one pass1, pass3, or pass4 result."""
    return query("""
        SELECT
            t.ticket_id,
            t.ticket_number,
            t.ticket_name,
            t.product_name,
            t.customer,
            t.status,
            p1.phenomenon,
            p1.component,
            p1.operation,
            p1.canonical_failure,
            p1.confidence,
            p1.status        AS pass1_status,
            p1.completed_at  AS pass1_completed_at,
            p3.mechanism,
            p3.evidence,
            p3.status        AS pass3_status,
            p3.completed_at  AS pass3_completed_at,
            p4.mechanism_class,
            p4.intervention_type,
            p4.intervention_action,
            p4.status        AS pass4_status,
            p4.completed_at  AS pass4_completed_at
        FROM tickets t
        LEFT JOIN LATERAL (
            SELECT phenomenon, component, operation, canonical_failure,
                   (parsed_json->>'confidence') AS confidence,
                   status, completed_at
            FROM ticket_llm_pass_results lp
            WHERE lp.ticket_id = t.ticket_id
              AND lp.pass_name = 'pass1_phenomenon'
            ORDER BY CASE WHEN lp.status = 'success' THEN 0 ELSE 1 END,
                     lp.updated_at DESC
            LIMIT 1
        ) p1 ON TRUE
        LEFT JOIN LATERAL (
            SELECT mechanism,
                   (parsed_json->>'evidence') AS evidence,
                   status, completed_at
            FROM ticket_llm_pass_results lp
            WHERE lp.ticket_id = t.ticket_id
              AND lp.pass_name = 'pass3_mechanism'
            ORDER BY CASE WHEN lp.status = 'success' THEN 0 ELSE 1 END,
                     lp.updated_at DESC
            LIMIT 1
        ) p3 ON TRUE
        LEFT JOIN LATERAL (
            SELECT mechanism_class, intervention_type, intervention_action,
                   status, completed_at
            FROM ticket_llm_pass_results lp
            WHERE lp.ticket_id = t.ticket_id
              AND lp.pass_name = 'pass4_intervention'
            ORDER BY CASE WHEN lp.status = 'success' THEN 0 ELSE 1 END,
                     lp.updated_at DESC
            LIMIT 1
        ) p4 ON TRUE
        WHERE p1.status = 'success'
        ORDER BY COALESCE(p4.completed_at, p3.completed_at, p1.completed_at) DESC NULLS LAST
    """)


def get_root_cause_detail(ticket_id):
    """Return full pass results + cleaned thread for a single ticket."""
    passes = query("""
        SELECT id, pass_name, status, phenomenon, component, operation,
               unexpected_state, canonical_failure, mechanism,
               mechanism_class, intervention_type, intervention_action,
               parsed_json,
               raw_response_text, error_message, prompt_version,
               model_name, started_at, completed_at
        FROM ticket_llm_pass_results
        WHERE ticket_id = %s
        ORDER BY pass_name, completed_at DESC NULLS LAST
    """, (ticket_id,))

    thread = query_one("""
        SELECT full_thread_text, technical_core_text
        FROM ticket_thread_rollups
        WHERE ticket_id = %s
    """, (ticket_id,))

    return {"passes": passes, "thread": thread}


# ── Root Cause analytics (dashboard visualizations) ─────────────────

def get_root_cause_stats():
    """KPI counts for the root cause dashboard."""
    return query_one("""
        SELECT
            COUNT(DISTINCT CASE WHEN p1.status = 'success' THEN p1.ticket_id END) AS pass1_success,
            COUNT(DISTINCT CASE WHEN p3.status = 'success' THEN p3.ticket_id END) AS pass3_success,
            COUNT(DISTINCT CASE WHEN p4.status = 'success' THEN p4.ticket_id END) AS pass4_success,
            COUNT(DISTINCT p4.mechanism_class)
                FILTER (WHERE p4.status = 'success' AND p4.mechanism_class IS NOT NULL
                        AND p4.mechanism_class != 'other')                        AS distinct_mechanism_classes,
            COUNT(DISTINCT p1.component)
                FILTER (WHERE p1.status = 'success' AND p1.component IS NOT NULL) AS distinct_components
        FROM tickets t
        LEFT JOIN LATERAL (
            SELECT ticket_id, status, component
            FROM ticket_llm_pass_results lp
            WHERE lp.ticket_id = t.ticket_id AND lp.pass_name = 'pass1_phenomenon'
            ORDER BY CASE WHEN lp.status='success' THEN 0 ELSE 1 END, lp.updated_at DESC
            LIMIT 1
        ) p1 ON TRUE
        LEFT JOIN LATERAL (
            SELECT ticket_id, status
            FROM ticket_llm_pass_results lp
            WHERE lp.ticket_id = t.ticket_id AND lp.pass_name = 'pass3_mechanism'
            ORDER BY CASE WHEN lp.status='success' THEN 0 ELSE 1 END, lp.updated_at DESC
            LIMIT 1
        ) p3 ON TRUE
        LEFT JOIN LATERAL (
            SELECT ticket_id, status, mechanism_class
            FROM ticket_llm_pass_results lp
            WHERE lp.ticket_id = t.ticket_id AND lp.pass_name = 'pass4_intervention'
            ORDER BY CASE WHEN lp.status='success' THEN 0 ELSE 1 END, lp.updated_at DESC
            LIMIT 1
        ) p4 ON TRUE
        WHERE p1.status IS NOT NULL OR p3.status IS NOT NULL OR p4.status IS NOT NULL
    """) or {
        "pass1_success": 0, "pass3_success": 0, "pass4_success": 0,
        "distinct_mechanism_classes": 0, "distinct_components": 0,
    }


def get_mechanism_class_distribution():
    """Mechanism class counts from successful Pass 4 results."""
    return query("""
        SELECT mechanism_class, ticket_count
        FROM vw_intervention_roi
        ORDER BY ticket_count DESC
    """)


def get_intervention_type_distribution():
    """Intervention type counts from successful Pass 4 results."""
    return query("""
        SELECT intervention_type, SUM(ticket_count)::int AS ticket_count
        FROM vw_intervention_roi
        GROUP BY intervention_type
        ORDER BY ticket_count DESC
    """)


def get_component_distribution(limit=20):
    """Top components by ticket count from successful Pass 1 results."""
    return query("""
        SELECT component, COUNT(*) AS ticket_count
        FROM ticket_llm_pass_results
        WHERE pass_name = 'pass1_phenomenon'
          AND status = 'success'
          AND component IS NOT NULL
          AND component != ''
        GROUP BY component
        ORDER BY ticket_count DESC
        LIMIT %s
    """, (limit,))


def get_operation_distribution():
    """Operation verb counts from successful Pass 1 results."""
    return query("""
        SELECT operation, COUNT(*) AS ticket_count
        FROM ticket_llm_pass_results
        WHERE pass_name = 'pass1_phenomenon'
          AND status = 'success'
          AND operation IS NOT NULL
          AND operation != ''
        GROUP BY operation
        ORDER BY ticket_count DESC
    """)


def get_top_engineering_fixes(limit=25):
    """Top engineering fixes ranked by ticket count (from vw_intervention_roi)."""
    return query("""
        SELECT mechanism_class, intervention_type,
               ticket_count, representative_action
        FROM vw_intervention_roi
        ORDER BY ticket_count DESC
        LIMIT %s
    """, (limit,))


def get_root_cause_by_product(limit=10):
    """Mechanism class counts broken down by product (top products only)."""
    return query("""
        WITH ranked_products AS (
            SELECT t.product_name, COUNT(*) AS total
            FROM ticket_llm_pass_results lp
            JOIN tickets t ON t.ticket_id = lp.ticket_id
            WHERE lp.pass_name = 'pass4_intervention'
              AND lp.status = 'success'
              AND lp.mechanism_class IS NOT NULL
            GROUP BY t.product_name
            ORDER BY total DESC
            LIMIT %s
        )
        SELECT
            COALESCE(NULLIF(t.product_name, ''), 'Unknown') AS product_name,
            lp.mechanism_class,
            COUNT(*) AS ticket_count
        FROM ticket_llm_pass_results lp
        JOIN tickets t ON t.ticket_id = lp.ticket_id
        JOIN ranked_products rp ON rp.product_name = t.product_name
        WHERE lp.pass_name = 'pass4_intervention'
          AND lp.status = 'success'
          AND lp.mechanism_class IS NOT NULL
        GROUP BY t.product_name, lp.mechanism_class
        ORDER BY t.product_name, ticket_count DESC
    """, (limit,))


def get_root_cause_sankey(component_limit=15):
    """Flow data for Sankey: component → mechanism_class → intervention_type."""
    return query("""
        WITH top_components AS (
            SELECT component
            FROM ticket_llm_pass_results
            WHERE pass_name = 'pass1_phenomenon'
              AND status = 'success'
              AND component IS NOT NULL AND component != ''
            GROUP BY component
            ORDER BY COUNT(*) DESC
            LIMIT %s
        )
        SELECT
            COALESCE(NULLIF(p1.component, ''), 'Unknown') AS component,
            COALESCE(p4.mechanism_class, 'unclassified')  AS mechanism_class,
            COALESCE(p4.intervention_type, 'unmapped')    AS intervention_type,
            COUNT(*) AS ticket_count
        FROM ticket_llm_pass_results p1
        LEFT JOIN LATERAL (
            SELECT mechanism_class, intervention_type
            FROM ticket_llm_pass_results lp
            WHERE lp.ticket_id = p1.ticket_id
              AND lp.pass_name = 'pass4_intervention'
              AND lp.status = 'success'
            ORDER BY lp.updated_at DESC
            LIMIT 1
        ) p4 ON TRUE
        WHERE p1.pass_name = 'pass1_phenomenon'
          AND p1.status = 'success'
          AND p1.component IS NOT NULL AND p1.component != ''
          AND p1.component IN (SELECT component FROM top_components)
        GROUP BY p1.component, p4.mechanism_class, p4.intervention_type
        HAVING COUNT(*) >= 1
        ORDER BY ticket_count DESC
    """, (component_limit,))


def get_pipeline_completion_funnel():
    """Count tickets at each pipeline pass stage for a funnel chart."""
    return query_one("""
        SELECT
            (SELECT COUNT(DISTINCT ticket_id) FROM ticket_llm_pass_results
             WHERE pass_name = 'pass1_phenomenon' AND status = 'success') AS pass1,
            (SELECT COUNT(DISTINCT ticket_id) FROM ticket_llm_pass_results
             WHERE pass_name = 'pass3_mechanism' AND status = 'success')  AS pass3,
            (SELECT COUNT(DISTINCT ticket_id) FROM ticket_llm_pass_results
             WHERE pass_name = 'pass4_intervention' AND status = 'success') AS pass4
    """) or {"pass1": 0, "pass3": 0, "pass4": 0}


# ── Saved reports (dashboard-local CRUD — never touches TeamSupport) ─

def _execute(sql, params=()):
    """Run an INSERT/UPDATE/DELETE and commit."""
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        db.put_conn(conn)


def get_saved_reports():
    return query("""
        SELECT id, name, filter_model, created_at
        FROM saved_reports
        ORDER BY name
    """)


def save_report(name, filter_model):
    """Upsert a saved report by name."""
    _execute("""
        INSERT INTO saved_reports (name, filter_model)
        VALUES (%s, %s)
        ON CONFLICT (name) DO UPDATE SET filter_model = EXCLUDED.filter_model,
                                         created_at = now()
    """, (name, json.dumps(filter_model)))


def delete_report(report_id):
    _execute("DELETE FROM saved_reports WHERE id = %s", (report_id,))
