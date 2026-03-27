"""
Read-only data access layer for the web dashboard.

Imports db.py and config.py from the parent project without modifying them.
All functions are SELECT-only — no writes to Postgres or TeamSupport.
The one exception is saved_reports (dashboard-local CRUD, never touches TS).
"""

import json
import uuid as _uuid
from datetime import datetime, date
from decimal import Decimal

import psycopg2.extras  # noqa: E402
from psycopg2 import errors as psycopg2_errors  # noqa: E402
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


# ── Azure DevOps work items ─────────────────────────────────────────

def get_open_work_items():
    """Return all non-closed, non-PBI work items across all projects."""
    return query("""
        SELECT work_item_id, project, work_item_type, title, state, reason,
               assigned_to, area_path, iteration_path, priority,
               board_column, tags, billable, work_type, comment_count,
               created_date, changed_date, state_change_date,
               completed_work, remaining_work, original_estimate
        FROM work_items
        WHERE state NOT IN ('Closed', 'Removed')
          AND work_item_type <> 'Product Backlog Item'
        ORDER BY changed_date DESC
    """)


def get_work_item_kpis():
    """Return summary KPI stats for open non-PBI work items."""
    return query_one("""
        SELECT COUNT(*) AS total_open,
               COUNT(*) FILTER (WHERE work_item_type = 'Bug') AS bugs,
               COUNT(*) FILTER (WHERE work_item_type = 'Feature') AS features,
               COUNT(*) FILTER (WHERE work_item_type = 'Task') AS tasks,
               COUNT(*) FILTER (WHERE work_item_type = 'Epic') AS epics,
               COUNT(DISTINCT project) AS projects,
               COUNT(DISTINCT assigned_to) AS assignees
        FROM work_items
        WHERE state NOT IN ('Closed', 'Removed')
          AND work_item_type <> 'Product Backlog Item'
    """)


def get_do_comments(work_item_id):
    """Fetch comments for a DO work item from the Azure DevOps API (live call)."""
    try:
        from azdevops_client import get_comments
        raw = get_comments(int(work_item_id), top=50)
        if not isinstance(raw, list):
            raw = raw.get("value", []) if isinstance(raw, dict) else []
        return raw
    except Exception:
        return []


def get_work_item_detail(work_item_id):
    """Return a single work item row from the DB."""
    return query_one("""
        SELECT work_item_id, project, work_item_type, title, state, reason,
               assigned_to, area_path, iteration_path, priority,
               board_column, tags, description,
               created_date, changed_date, state_change_date,
               completed_work, remaining_work, original_estimate
        FROM work_items
        WHERE work_item_id = %s
    """, (int(work_item_id),))


EXCLUDED_HEALTH_GROUPS = ("Marketing", "Sales (S)")
EXCLUDED_CUSTOMERS = ("InHance Internal",)


def _customer_exclusion_clause(*, column="customer"):
    placeholders = ",".join(["%s"] * len(EXCLUDED_CUSTOMERS))
    return f"COALESCE({column}, '') NOT IN ({placeholders})", list(EXCLUDED_CUSTOMERS)


def _group_filter_clause(group_names, *, column="group_name"):
    groups = [str(g) for g in (group_names or []) if g is not None]
    if not groups:
        return "", []
    placeholders = ",".join(["%s"] * len(groups))
    return f" AND COALESCE({column}, '') IN ({placeholders})", groups


def _default_group_exclusion_clause(*, column="group_name"):
    placeholders = ",".join(["%s"] * len(EXCLUDED_HEALTH_GROUPS))
    return f"COALESCE({column}, '') NOT IN ({placeholders})", list(EXCLUDED_HEALTH_GROUPS)


# ── Overview ─────────────────────────────────────────────────────────

def get_open_ticket_stats():
    cust_sql, cust_params = _customer_exclusion_clause(column="customer")
    return query_one(f"""
        SELECT
            COUNT(*) AS total_open,
            COUNT(*) FILTER (WHERE priority IS NOT NULL AND priority <= 3) AS high_priority,
            COUNT(*) FILTER (WHERE overall_complexity >= 4) AS high_complexity,
            COUNT(*) FILTER (WHERE frustrated = 'Yes') AS frustrated
        FROM vw_operational_open_tickets
        WHERE {cust_sql}
    """, tuple(cust_params)) or {"total_open": 0, "high_priority": 0, "high_complexity": 0, "frustrated": 0}


def get_backlog_daily():
    return query(
        "SELECT * FROM vw_backlog_daily WHERE snapshot_date >= '2024-07-01' ORDER BY snapshot_date"
    )


def get_backlog_daily_by_severity():
    return query(
        "SELECT * FROM vw_backlog_daily_by_severity WHERE snapshot_date >= '2024-07-01' ORDER BY snapshot_date, severity_tier"
    )


def get_filtered_backlog_daily(filters):
    """Reconstruct daily backlog from tickets table with optional filters.

    filters: dict of field -> list-of-values, e.g. {"assignee": ["Ben Aplin"]}.
    Supported fields: status, severity, product_name, assignee, customer.
    """
    extra_conditions = []
    params = []
    _FIELD_MAP = {
        "status": "t.status",
        "severity": "t.severity",
        "product_name": "t.product_name",
        "assignee": "t.assignee",
        "customer": "t.customer",
        "group_name": "t.group_name",
    }
    for field, values in (filters or {}).items():
        col = _FIELD_MAP.get(field)
        if col and values:
            placeholders = ",".join(["%s"] * len(values))
            extra_conditions.append(f"{col} IN ({placeholders})")
            params.extend(values)

    extra_where = (" AND " + " AND ".join(extra_conditions)) if extra_conditions else ""

    # Total open per day
    daily_rows = query(f"""
        SELECT d.snapshot_date, COUNT(*) AS open_backlog
        FROM (SELECT DISTINCT snapshot_date FROM daily_open_counts) d
        JOIN tickets t
          ON t.date_created IS NOT NULL
         AND t.date_created::date <= d.snapshot_date
         AND COALESCE(t.status, '') != 'Open'
         AND COALESCE(t.assignee, '') != 'Marketing'
         AND COALESCE(t.group_name, '') != 'Marketing'
         AND COALESCE(t.customer, '') NOT IN ({','.join(['%s'] * len(EXCLUDED_CUSTOMERS))})
         AND (
             (t.closed_at IS NOT NULL AND t.closed_at::date > d.snapshot_date)
             OR
             (t.closed_at IS NULL AND COALESCE(t.status, '') NOT IN ('Closed', 'Resolved'))
         ){extra_where}
        WHERE d.snapshot_date >= '2024-07-01'
        GROUP BY d.snapshot_date
        ORDER BY d.snapshot_date
    """, tuple(list(EXCLUDED_CUSTOMERS) + params))

    # Severity breakdown per day (same filter)
    severity_rows = query(f"""
        SELECT d.snapshot_date,
            CASE
                WHEN t.severity LIKE '1%%' OR LOWER(t.severity) LIKE '%%high%%'
                THEN 'High'
                WHEN t.severity LIKE '3%%' OR LOWER(t.severity) LIKE '%%low%%'
                THEN 'Low'
                ELSE 'Medium'
            END AS severity_tier,
            COUNT(*) AS ticket_count
        FROM (SELECT DISTINCT snapshot_date FROM daily_open_counts) d
        JOIN tickets t
          ON t.date_created IS NOT NULL
         AND t.date_created::date <= d.snapshot_date
         AND COALESCE(t.status, '') != 'Open'
         AND COALESCE(t.assignee, '') != 'Marketing'
         AND COALESCE(t.group_name, '') != 'Marketing'
         AND COALESCE(t.customer, '') NOT IN ({','.join(['%s'] * len(EXCLUDED_CUSTOMERS))})
         AND (
             (t.closed_at IS NOT NULL AND t.closed_at::date > d.snapshot_date)
             OR
             (t.closed_at IS NULL AND COALESCE(t.status, '') NOT IN ('Closed', 'Resolved'))
         ){extra_where}
        WHERE d.snapshot_date >= '2024-07-01'
        GROUP BY d.snapshot_date, 2
        ORDER BY d.snapshot_date, severity_tier
    """, tuple(list(EXCLUDED_CUSTOMERS) + params))

    return daily_rows, severity_rows


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
    cust_sql, cust_params = _customer_exclusion_clause(column="customer")
    return query(f"""
        SELECT COALESCE(status, 'Unknown') AS status, COUNT(*) AS count
        FROM vw_operational_open_tickets
        WHERE {cust_sql}
        GROUP BY status
        ORDER BY count DESC
    """, tuple(cust_params))


# ── Ticket list ──────────────────────────────────────────────────────

def get_ticket_list():
    cust_sql, cust_params = _customer_exclusion_clause(column="customer")
    return query(f"""
        SELECT ticket_id, ticket_number, ticket_name, status, severity,
               product_name, assignee, customer, group_name,
               date_created, date_modified, days_opened, days_since_modified,
               action_count, customer_message_count, inhance_message_count,
               priority, priority_explanation,
               overall_complexity, frustrated,
               do_number, do_status
        FROM vw_ticket_analytics_core
        WHERE {cust_sql}
        ORDER BY date_modified DESC NULLS LAST
    """, tuple(cust_params))


# ── Ticket detail ───────────────────────────────────────────────────

def get_ticket_detail(ticket_id):
    return query_one("""
        SELECT * FROM vw_ticket_analytics_core WHERE ticket_id = %s
    """, (ticket_id,))


def get_ticket_complexity_detail(ticket_id):
    """Return the full complexity analysis for a ticket (summary, evidence, noise, duration note)."""
    return query_one("""
        SELECT complexity_summary, evidence, noise_factors,
               duration_vs_complexity_note, primary_complexity_drivers
        FROM vw_latest_ticket_complexity
        WHERE ticket_id = %s
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


# ── Ticket events ───────────────────────────────────────────────────

def insert_ticket_event(ticket_id, event_type, detail=None, created_by=None):
    """Record a user-initiated event against a ticket."""
    import json
    _execute("""
        INSERT INTO ticket_events (ticket_id, event_type, detail, created_by)
        VALUES (%s, %s, %s, %s)
    """, (ticket_id, event_type, json.dumps(detail or {}), created_by))


def get_ticket_events(ticket_id):
    """Return all events for a ticket, newest first."""
    return query("""
        SELECT id, ticket_id, event_type, detail, created_by, created_at
        FROM ticket_events
        WHERE ticket_id = %s
        ORDER BY created_at DESC
    """, (ticket_id,))


# ── Ticket exclusions ────────────────────────────────────────────────

def get_ticket_exclusions(ticket_id):
    """Return the exclusion flags for a ticket, or None if no row exists."""
    return query_one("""
        SELECT exclude_priority, exclude_sentiment, exclude_complexity, reason
        FROM ticket_exclusions
        WHERE ticket_id = %s
    """, (ticket_id,))


def get_ticket_number(ticket_id):
    """Return the ticket_number string for a given ticket_id, or None."""
    row = query_one("SELECT ticket_number FROM tickets WHERE ticket_id = %s", (ticket_id,))
    return row["ticket_number"] if row else None


def upsert_ticket_exclusions(ticket_id, *, exclude_priority: bool,
                              exclude_sentiment: bool, exclude_complexity: bool,
                              reason: str | None = None):
    """Insert or update the exclusion flags for a ticket."""
    _execute("""
        INSERT INTO ticket_exclusions
            (ticket_id, exclude_priority, exclude_sentiment, exclude_complexity, reason)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (ticket_id) DO UPDATE SET
            exclude_priority   = EXCLUDED.exclude_priority,
            exclude_sentiment  = EXCLUDED.exclude_sentiment,
            exclude_complexity = EXCLUDED.exclude_complexity,
            reason             = EXCLUDED.reason
    """, (ticket_id, exclude_priority, exclude_sentiment, exclude_complexity, reason or None))


# ── Health ───────────────────────────────────────────────────────────

def get_group_names():
    try:
        return [
            row["group_name"]
            for row in query("""
                SELECT DISTINCT COALESCE(group_name, '') AS group_name
                FROM tickets
                WHERE COALESCE(group_name, '') <> ''
                  AND COALESCE(group_name, '') NOT IN %s
                ORDER BY group_name
            """, (EXCLUDED_HEALTH_GROUPS,))
            if row.get("group_name")
        ]
    except (psycopg2_errors.UndefinedColumn, psycopg2_errors.UndefinedTable, psycopg2.OperationalError):
        return []


def get_customer_health():
    try:
        exclusion_sql, exclusion_params = _default_group_exclusion_clause(column="group_name")
        return query(f"""
            WITH latest_date AS (
                SELECT MAX(as_of_date) AS as_of_date
                FROM customer_health_ticket_contributors
                WHERE score_formula_version = %s
            ),
            filtered AS (
                SELECT c.*
                FROM customer_health_ticket_contributors c
                JOIN latest_date d ON d.as_of_date = c.as_of_date
                WHERE c.score_formula_version = %s
                  AND {exclusion_sql}
            ),
            customer_metadata AS (
                SELECT
                    customer_name AS customer,
                    BOOL_OR(COALESCE(is_active, FALSE)) AS is_active,
                    BOOL_OR(COALESCE(key_acct, FALSE)) AS is_key_account
                FROM customer_attributes
                GROUP BY customer_name
            ),
            display_customers AS (
                SELECT customer, is_key_account
                FROM customer_metadata
                WHERE is_active IS TRUE
                  AND customer NOT IN (SELECT unnest(%s::text[]))

                UNION

                SELECT DISTINCT filtered.customer, FALSE AS is_key_account
                FROM filtered
                LEFT JOIN customer_metadata
                  ON customer_metadata.customer = filtered.customer
                WHERE customer_metadata.customer IS NULL
                  AND filtered.customer NOT IN (SELECT unnest(%s::text[]))
            )
            SELECT
                latest_date.as_of_date AS as_of_date,
                display_customers.customer,
                CASE WHEN COALESCE(display_customers.is_key_account, FALSE) THEN 'Yes' ELSE '' END AS key_account,
                COUNT(*) FILTER (WHERE pressure_contribution >= 1.0) AS open_ticket_count,
                COUNT(*) FILTER (
                    WHERE priority IS NOT NULL
                      AND priority <= 3
                      AND LOWER(COALESCE(status, '')) NOT IN ('closed', 'resolved', 'open')
                ) AS high_priority_count,
                COUNT(*) FILTER (
                    WHERE overall_complexity IS NOT NULL
                      AND overall_complexity >= 4
                      AND LOWER(COALESCE(status, '')) NOT IN ('closed', 'resolved', 'open')
                ) AS high_complexity_count,
                COALESCE(ROUND(AVG(overall_complexity)::numeric, 2), 0) AS avg_complexity,
                NULL::numeric AS avg_elapsed_drag,
                0 AS reopen_count_90d,
                COUNT(*) FILTER (WHERE frustrated = 'Yes') AS frustration_count_90d,
                jsonb_agg(DISTINCT cluster_id) FILTER (WHERE cluster_id IS NOT NULL) AS top_cluster_ids,
                jsonb_agg(DISTINCT product_name) FILTER (WHERE product_name IS NOT NULL) AS top_products,
                COALESCE(ROUND(SUM(pressure_contribution), 2), 0) AS ticket_load_pressure_score,
                COALESCE(ROUND(SUM(total_contribution), 2), 0) AS customer_health_score,
                CASE
                    WHEN COALESCE(ROUND(SUM(total_contribution), 2), 0) < 15 THEN 'healthy'
                    WHEN COALESCE(ROUND(SUM(total_contribution), 2), 0) < 30 THEN 'watch'
                    WHEN COALESCE(ROUND(SUM(total_contribution), 2), 0) < 50 THEN 'at_risk'
                    ELSE 'critical'
                END AS customer_health_band,
                COALESCE(ROUND(SUM(pressure_contribution), 2), 0) AS pressure_score,
                COALESCE(ROUND(SUM(aging_contribution), 2), 0) AS aging_score,
                COALESCE(ROUND(SUM(friction_contribution), 2), 0) AS friction_score,
                COALESCE(ROUND(SUM(concentration_contribution), 2), 0) AS concentration_score,
                COALESCE(ROUND(SUM(breadth_contribution), 2), 0) AS breadth_score,
                NULL::jsonb AS factor_summary_json,
                %s AS score_formula_version
            FROM latest_date
            JOIN display_customers ON TRUE
            LEFT JOIN filtered
              ON filtered.customer = display_customers.customer
            GROUP BY latest_date.as_of_date, display_customers.customer, display_customers.is_key_account
            ORDER BY customer_health_score DESC NULLS LAST, ticket_load_pressure_score DESC NULLS LAST
        """, ("v1", "v1", *exclusion_params, list(EXCLUDED_CUSTOMERS), list(EXCLUDED_CUSTOMERS), "v1"))
    except (psycopg2_errors.UndefinedColumn, psycopg2_errors.UndefinedTable):
        return query("""
            SELECT *,
                   ''::text AS key_account
            FROM customer_ticket_health
            WHERE as_of_date = (SELECT MAX(as_of_date) FROM customer_ticket_health)
            ORDER BY ticket_load_pressure_score DESC NULLS LAST
        """)
    except psycopg2.OperationalError:
        return []


def get_product_health():
    exclusion_sql, exclusion_params = _default_group_exclusion_clause(column="group_name")
    return query(f"""
        SELECT
            MAX(as_of_date) AS as_of_date,
            product_name,
            SUM(ticket_volume) AS ticket_volume,
            ROUND(
                SUM(COALESCE(avg_complexity, 0) * ticket_volume)
                / NULLIF(SUM(CASE WHEN avg_complexity IS NOT NULL THEN ticket_volume ELSE 0 END), 0),
                2
            ) AS avg_complexity,
            ROUND(
                SUM(COALESCE(avg_coordination_load, 0) * ticket_volume)
                / NULLIF(SUM(CASE WHEN avg_coordination_load IS NOT NULL THEN ticket_volume ELSE 0 END), 0),
                2
            ) AS avg_coordination_load,
            ROUND(
                SUM(COALESCE(avg_elapsed_drag, 0) * ticket_volume)
                / NULLIF(SUM(CASE WHEN avg_elapsed_drag IS NOT NULL THEN ticket_volume ELSE 0 END), 0),
                2
            ) AS avg_elapsed_drag,
            NULL::jsonb AS top_clusters,
            NULL::jsonb AS top_mechanisms,
            ROUND(
                SUM(COALESCE(dev_touched_rate, 0) * ticket_volume)
                / NULLIF(SUM(CASE WHEN dev_touched_rate IS NOT NULL THEN ticket_volume ELSE 0 END), 0),
                4
            ) AS dev_touched_rate,
            ROUND(
                SUM(COALESCE(customer_wait_rate, 0) * ticket_volume)
                / NULLIF(SUM(CASE WHEN customer_wait_rate IS NOT NULL THEN ticket_volume ELSE 0 END), 0),
                4
            ) AS customer_wait_rate
        FROM product_ticket_health
        WHERE as_of_date = (SELECT MAX(as_of_date) FROM product_ticket_health)
          AND {exclusion_sql}
        GROUP BY product_name
        ORDER BY ticket_volume DESC NULLS LAST
    """, tuple(exclusion_params))


def get_tickets_by_customers(customer_names, group_names=None):
    """Return open tickets for one or more customers, optionally scoped to selected groups."""
    if not customer_names:
        return []
    placeholders = ",".join(["%s"] * len(customer_names))
    params = list(customer_names)
    group_extra = ""
    if group_names is None:
        exclusion_sql, exclusion_params = _default_group_exclusion_clause(column="v.group_name")
        group_extra = f" AND {exclusion_sql}"
        params.extend(exclusion_params)
    else:
        group_extra, group_params = _group_filter_clause(group_names, column="v.group_name")
        params.extend(group_params)
    return query(f"""
        SELECT v.ticket_id, v.ticket_number, v.ticket_name, v.status, v.severity,
               v.group_name, v.product_name, v.assignee, v.customer, v.days_opened, v.priority,
               v.overall_complexity, v.frustrated, v.date_modified,
               v.do_number, v.do_status
        FROM vw_operational_open_tickets v
        WHERE v.customer IN ({placeholders})
          {group_extra}
        ORDER BY v.date_modified DESC NULLS LAST
    """, tuple(params))


def get_customer_groups(customer):
    try:
        exclusion_sql, exclusion_params = _default_group_exclusion_clause(column="group_name")
        return [
            row["group_name"]
            for row in query(f"""
                SELECT DISTINCT COALESCE(group_name, '') AS group_name
                FROM customer_health_ticket_contributors
                WHERE customer = %s
                  AND score_formula_version = %s
                  AND {exclusion_sql}
                ORDER BY group_name
            """, (customer, "v1", *exclusion_params))
            if row.get("group_name")
        ]
    except (psycopg2_errors.UndefinedColumn, psycopg2_errors.UndefinedTable, psycopg2.OperationalError):
        return []


def get_customer_health_history(customer, group_names=None, days=None):
    """Return daily health snapshots for one customer aggregated across selected groups."""
    try:
        params = [customer, "v1"]
        extra = ""
        if days is not None:
            extra = "AND as_of_date >= CURRENT_DATE - (%s::int - 1)"
            params.append(days)
        if group_names is None:
            group_extra_sql, group_extra_params = _default_group_exclusion_clause(column="group_name")
        else:
            group_extra_sql, group_extra_params = _group_filter_clause(group_names, column="group_name")
            if not group_extra_sql:
                return []
            group_extra_sql = group_extra_sql.lstrip(" AND")
        params.extend(group_extra_params)
        return query(f"""
            WITH filtered AS (
                SELECT *
                FROM customer_health_ticket_contributors
                WHERE customer = %s
                  AND score_formula_version = %s
                  AND {group_extra_sql}
                  {extra}
            )
            SELECT
                as_of_date,
                customer,
                ROUND(SUM(total_contribution), 2) AS customer_health_score,
                CASE
                    WHEN ROUND(SUM(total_contribution), 2) < 15 THEN 'healthy'
                    WHEN ROUND(SUM(total_contribution), 2) < 30 THEN 'watch'
                    WHEN ROUND(SUM(total_contribution), 2) < 50 THEN 'at_risk'
                    ELSE 'critical'
                END AS customer_health_band,
                ROUND(SUM(pressure_contribution), 2) AS pressure_score,
                ROUND(SUM(aging_contribution), 2) AS aging_score,
                ROUND(SUM(friction_contribution), 2) AS friction_score,
                ROUND(SUM(concentration_contribution), 2) AS concentration_score,
                ROUND(SUM(breadth_contribution), 2) AS breadth_score,
                ROUND(SUM(pressure_contribution), 2) AS ticket_load_pressure_score,
                COUNT(*) FILTER (WHERE pressure_contribution >= 1.0) AS open_ticket_count,
                COUNT(*) FILTER (
                    WHERE priority IS NOT NULL
                      AND priority <= 3
                      AND LOWER(COALESCE(status, '')) NOT IN ('closed', 'resolved', 'open')
                ) AS high_priority_count,
                COUNT(*) FILTER (
                    WHERE overall_complexity IS NOT NULL
                      AND overall_complexity >= 4
                      AND LOWER(COALESCE(status, '')) NOT IN ('closed', 'resolved', 'open')
                ) AS high_complexity_count,
                COUNT(*) FILTER (WHERE frustrated = 'Yes') AS frustration_count_90d,
                jsonb_agg(DISTINCT cluster_id) FILTER (WHERE cluster_id IS NOT NULL) AS top_cluster_ids,
                jsonb_agg(DISTINCT product_name) FILTER (WHERE product_name IS NOT NULL) AS top_products,
                NULL::jsonb AS factor_summary_json
            FROM filtered
            GROUP BY as_of_date, customer
            ORDER BY as_of_date
        """, tuple(params))
    except (psycopg2_errors.UndefinedColumn, psycopg2_errors.UndefinedTable, psycopg2.OperationalError):
        return []


def get_customer_health_contributors(customer, as_of_date, group_names=None):
    """Return ticket-level drivers for one customer/day ordered by contribution."""
    try:
        params = [customer, as_of_date, "v1"]
        if group_names is None:
            group_extra_sql, group_extra_params = _default_group_exclusion_clause(column="group_name")
        else:
            group_extra_sql, group_extra_params = _group_filter_clause(group_names, column="group_name")
            if not group_extra_sql:
                return []
            group_extra_sql = group_extra_sql.lstrip(" AND")
        params.extend(group_extra_params)
        return query(f"""
            SELECT
                as_of_date,
                customer,
                group_name,
                ticket_id,
                ticket_number,
                ticket_name,
                product_name,
                status,
                severity,
                assignee,
                days_opened,
                date_modified,
                priority,
                overall_complexity,
                frustrated,
                cluster_id,
                mechanism_class,
                intervention_type,
                pressure_contribution,
                aging_contribution,
                friction_contribution,
                concentration_contribution,
                breadth_contribution,
                total_contribution,
                score_formula_version
            FROM customer_health_ticket_contributors
            WHERE customer = %s
              AND as_of_date = %s
              AND score_formula_version = %s
              AND {group_extra_sql}
            ORDER BY total_contribution DESC, days_opened DESC NULLS LAST, date_modified DESC NULLS LAST, ticket_id
        """, tuple(params))
    except (psycopg2_errors.UndefinedColumn, psycopg2_errors.UndefinedTable, psycopg2.OperationalError):
        return []


def get_customer_health_explanations(customer):
    try:
        return query("""
            SELECT id, customer, as_of_date, group_filter_json, group_filter_label,
                   model_name, prompt_version, explanation_text, created_at
            FROM customer_health_explanations
            WHERE customer = %s
            ORDER BY created_at DESC, id DESC
        """, (customer,))
    except (psycopg2_errors.UndefinedColumn, psycopg2_errors.UndefinedTable, psycopg2.OperationalError):
        return []


def save_customer_health_explanation(
    *,
    customer,
    as_of_date,
    group_filter_json,
    group_filter_label,
    model_name,
    prompt_version,
    explanation_text,
    raw_context_json,
    raw_response_text,
):
    return _execute_returning("""
        INSERT INTO customer_health_explanations (
            customer, as_of_date, group_filter_json, group_filter_label,
            model_name, prompt_version, explanation_text, raw_context_json,
            raw_response_text, created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
        RETURNING id, customer, as_of_date, group_filter_json, group_filter_label,
                  model_name, prompt_version, explanation_text, created_at
    """, (
        customer,
        as_of_date,
        json.dumps(group_filter_json),
        group_filter_label,
        model_name,
        prompt_version,
        explanation_text,
        json.dumps(raw_context_json),
        raw_response_text,
    ))


def get_all_health_plans():
    try:
        return query("""
            SELECT id, customer, as_of_date, group_filter_label,
                   target_band, projected_score, projected_band,
                   jsonb_array_length(COALESCE(tickets_to_resolve, '[]'::jsonb))
                       AS tickets_to_resolve_count,
                   tickets_to_resolve,
                   plan_text, created_at,
                   (raw_context_json -> 'simulation' ->> 'current_score')::numeric AS current_score
            FROM customer_health_improvement_plans
            ORDER BY created_at DESC, id DESC
        """)
    except (psycopg2_errors.UndefinedColumn, psycopg2_errors.UndefinedTable,
            psycopg2.OperationalError):
        return []


def get_customer_health_plans(customer):
    try:
        return query("""
            SELECT id, customer, as_of_date, group_filter_json, group_filter_label,
                   target_band, projected_score, projected_band, tickets_to_resolve,
                   model_name, prompt_version, plan_text, created_at
            FROM customer_health_improvement_plans
            WHERE customer = %s
            ORDER BY created_at DESC, id DESC
        """, (customer,))
    except (psycopg2_errors.UndefinedColumn, psycopg2_errors.UndefinedTable,
            psycopg2.OperationalError):
        return []


def save_customer_health_plan(
    *,
    customer,
    as_of_date,
    group_filter_json,
    group_filter_label,
    target_band,
    projected_score,
    projected_band,
    tickets_to_resolve,
    model_name,
    prompt_version,
    plan_text,
    raw_context_json,
    raw_response_text,
):
    return _execute_returning("""
        INSERT INTO customer_health_improvement_plans (
            customer, as_of_date, group_filter_json, group_filter_label,
            target_band, projected_score, projected_band, tickets_to_resolve,
            model_name, prompt_version, plan_text, raw_context_json,
            raw_response_text, created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
        RETURNING id, customer, as_of_date, group_filter_json, group_filter_label,
                  target_band, projected_score, projected_band, tickets_to_resolve,
                  model_name, prompt_version, plan_text, created_at
    """, (
        customer,
        as_of_date,
        json.dumps(group_filter_json),
        group_filter_label,
        target_band,
        projected_score,
        projected_band,
        json.dumps(tickets_to_resolve, default=str),
        model_name,
        prompt_version,
        plan_text,
        json.dumps(raw_context_json, default=str),
        raw_response_text,
    ))


# ── Operations / Analyst behaviour ──────────────────────────────────

_ANALYST_EXCLUSIONS = ("Customer Support (CS)", "Cogsdale Support",
                       "Development (D)", "Professional Services (PS)",
                       "Cogsdale GP Support (GP)")

_SUPPORT_ANALYSTS_CACHE = None


def _get_support_analysts():
    """Return tuple of analyst names whose TeamSupport title contains 'Support' (cached)."""
    global _SUPPORT_ANALYSTS_CACHE
    if _SUPPORT_ANALYSTS_CACHE is not None:
        return _SUPPORT_ANALYSTS_CACHE
    try:
        from ts_client import ts_get, TS_BASE
        data = ts_get(f"{TS_BASE}/Users", params={"Organization": "inHANCE"})
        users = data.get("Users") or data.get("User") or []
        if isinstance(users, dict):
            users = [users]
        names = []
        for u in users:
            title = (u.get("Title") or "").strip()
            if "support" not in title.lower():
                continue
            fn = (u.get("FirstName") or "").strip()
            ln = (u.get("LastName") or "").strip()
            name = f"{fn} {ln}".strip()
            if name:
                names.append(name)
        _SUPPORT_ANALYSTS_CACHE = tuple(names) if names else ("__NO_MATCH__",)
    except Exception as e:
        print(f"[data] Failed to fetch support analysts: {e}", flush=True)
        _SUPPORT_ANALYSTS_CACHE = ("__NO_MATCH__",)
    return _SUPPORT_ANALYSTS_CACHE


def get_analyst_scorecard(months=6):
    """Per-analyst summary: closures, complexity, own-work ratio, cherry-pick signals."""
    cust_sql, cust_params = _customer_exclusion_clause(column="v.customer")
    support_analysts = _get_support_analysts()
    support_ph = ",".join(["%s"] * len(support_analysts))
    return query(f"""
        WITH closed AS (
            SELECT v.ticket_id, v.assignee, v.overall_complexity,
                   v.intrinsic_complexity, v.days_opened, v.frustrated,
                   v.priority, v.severity, v.closed_at
            FROM vw_ticket_analytics_core v
            WHERE v.closed_at IS NOT NULL
              AND v.closed_at >= CURRENT_DATE - (%s || ' months')::interval
              AND v.assignee IS NOT NULL
              AND {cust_sql}
              AND v.assignee IN ({support_ph})
        ),
        inh_actions AS (
            SELECT ta.ticket_id, ta.creator_name,
                   COUNT(*) FILTER (WHERE NOT COALESCE(ta.is_empty, FALSE)) AS nonempty
            FROM ticket_actions ta
            JOIN closed c ON c.ticket_id = ta.ticket_id
            WHERE ta.party = 'inh'
            GROUP BY ta.ticket_id, ta.creator_name
        ),
        ticket_work AS (
            SELECT c.ticket_id, c.assignee,
                   COALESCE(SUM(ia.nonempty), 0) AS total_inh,
                   COALESCE(SUM(CASE WHEN ia.creator_name = c.assignee THEN ia.nonempty ELSE 0 END), 0) AS own,
                   COUNT(DISTINCT ia.creator_name) FILTER (WHERE ia.creator_name != c.assignee) AS others
            FROM closed c
            LEFT JOIN inh_actions ia ON ia.ticket_id = c.ticket_id
            GROUP BY c.ticket_id, c.assignee
        )
        SELECT
            c.assignee,
            COUNT(*)::int AS tickets_closed,
            ROUND(AVG(c.overall_complexity), 2) AS avg_complexity,
            ROUND(AVG(c.days_opened), 1) AS avg_days_open,
            COUNT(*) FILTER (WHERE c.overall_complexity >= 4)::int AS high_complexity_count,
            ROUND(100.0 * COUNT(*) FILTER (WHERE c.overall_complexity >= 4) / NULLIF(COUNT(*), 0), 1) AS pct_high_complexity,
            COUNT(*) FILTER (WHERE c.priority IS NOT NULL AND c.priority <= 3)::int AS high_priority_count,
            COUNT(*) FILTER (WHERE c.frustrated = 'Yes')::int AS frustrated_count,
            ROUND(AVG(CASE WHEN tw.total_inh > 0 THEN tw.own::numeric / tw.total_inh ELSE 1 END), 3) AS avg_own_work_ratio,
            ROUND(AVG(tw.others), 2) AS avg_other_contributors,
            COUNT(*) FILTER (WHERE tw.total_inh > 0 AND tw.own = 0)::int AS zero_contribution_closes,
            ROUND(100.0 * COUNT(*) FILTER (
                WHERE tw.total_inh > 0 AND tw.own::numeric / tw.total_inh < 0.25
            ) / NULLIF(COUNT(*), 0), 1) AS pct_low_contribution
        FROM closed c
        JOIN ticket_work tw ON tw.ticket_id = c.ticket_id AND tw.assignee = c.assignee
        GROUP BY c.assignee
        HAVING COUNT(*) >= 10
        ORDER BY tickets_closed DESC
    """, (str(months), *cust_params, *support_analysts))


def get_analyst_complexity_distribution(months=6):
    """Complexity score distribution per analyst for closed tickets."""
    cust_sql, cust_params = _customer_exclusion_clause(column="v.customer")
    support_analysts = _get_support_analysts()
    support_ph = ",".join(["%s"] * len(support_analysts))
    return query(f"""
        SELECT v.assignee, v.overall_complexity, COUNT(*)::int AS cnt
        FROM vw_ticket_analytics_core v
        WHERE v.closed_at IS NOT NULL
          AND v.closed_at >= CURRENT_DATE - (%s || ' months')::interval
          AND v.assignee IS NOT NULL
          AND v.overall_complexity IS NOT NULL
          AND {cust_sql}
          AND v.assignee IN ({support_ph})
        GROUP BY v.assignee, v.overall_complexity
        ORDER BY v.assignee, v.overall_complexity
    """, (str(months), *cust_params, *support_analysts))


def get_analyst_monthly_closures(months=12, date_from=None, date_to=None,
                                  severity_tier=None):
    """Monthly closure counts per analyst.
    If date_from/date_to are provided (YYYY-MM-DD strings), use those instead of months.
    If severity_tier is 'split', returns rows with an additional severity_tier column.
    If severity_tier is 'High'/'Medium'/'Low', filters to that tier only."""
    cust_sql, cust_params = _customer_exclusion_clause(column="v.customer")
    support_analysts = _get_support_analysts()
    support_ph = ",".join(["%s"] * len(support_analysts))

    sev_case = """CASE
        WHEN v.severity LIKE '1%%' OR LOWER(v.severity) LIKE '%%high%%' THEN 'High'
        WHEN v.severity LIKE '3%%' OR LOWER(v.severity) LIKE '%%low%%' THEN 'Low'
        ELSE 'Medium'
    END"""

    # Build severity column / filter / grouping pieces
    if severity_tier == "split":
        sev_select = f", {sev_case} AS severity_tier"
        sev_filter = ""
        sev_group = ", 3"
        sev_order = "ORDER BY 1, 4 DESC"
    elif severity_tier in ("High", "Medium", "Low"):
        sev_select = ""
        sev_filter = f" AND {sev_case} = %s"
        sev_group = ""
        sev_order = "ORDER BY 1, 3 DESC"
    else:
        sev_select = ""
        sev_filter = ""
        sev_group = ""
        sev_order = "ORDER BY 1, 3 DESC"

    if date_from and date_to:
        params = [date_from, date_to, *cust_params]
        if severity_tier in ("High", "Medium", "Low"):
            params.append(severity_tier)
        params.extend(support_analysts)
        return query(f"""
            SELECT TO_CHAR(v.closed_at, 'YYYY-MM') AS month,
                   v.assignee{sev_select},
                   COUNT(*)::int AS closed_count
            FROM vw_ticket_analytics_core v
            WHERE v.closed_at IS NOT NULL
              AND v.closed_at >= %s::date
              AND v.closed_at < %s::date + INTERVAL '1 day'
              AND v.assignee IS NOT NULL
              AND {cust_sql}{sev_filter}
              AND v.assignee IN ({support_ph})
            GROUP BY 1, 2{sev_group}
            {sev_order}
        """, tuple(params))

    params = [str(months), *cust_params]
    if severity_tier in ("High", "Medium", "Low"):
        params.append(severity_tier)
    params.extend(support_analysts)
    return query(f"""
        SELECT TO_CHAR(v.closed_at, 'YYYY-MM') AS month,
               v.assignee{sev_select},
               COUNT(*)::int AS closed_count
        FROM vw_ticket_analytics_core v
        WHERE v.closed_at IS NOT NULL
          AND v.closed_at >= CURRENT_DATE - (%s || ' months')::interval
          AND v.assignee IS NOT NULL
          AND {cust_sql}{sev_filter}
          AND v.assignee IN ({support_ph})
        GROUP BY 1, 2{sev_group}
        {sev_order}
    """, tuple(params))


def get_monthly_tickets_created(date_from=None, date_to=None, months=12):
    """Monthly ticket creation counts for CS-group customers."""
    cust_sql, cust_params = _customer_exclusion_clause(column="v.customer")
    support_analysts = _get_support_analysts()
    support_ph = ",".join(["%s"] * len(support_analysts))
    if date_from and date_to:
        return query(f"""
            SELECT TO_CHAR(v.date_created, 'YYYY-MM') AS month,
                   COUNT(*)::int AS created_count
            FROM vw_ticket_analytics_core v
            WHERE v.date_created IS NOT NULL
              AND v.date_created >= %s::date
              AND v.date_created < %s::date + INTERVAL '1 day'
              AND {cust_sql}
              AND v.assignee IN ({support_ph})
            GROUP BY 1
            ORDER BY 1
        """, (date_from, date_to, *cust_params, *support_analysts))
    return query(f"""
        SELECT TO_CHAR(v.date_created, 'YYYY-MM') AS month,
               COUNT(*)::int AS created_count
        FROM vw_ticket_analytics_core v
        WHERE v.date_created IS NOT NULL
          AND v.date_created >= CURRENT_DATE - (%s || ' months')::interval
          AND {cust_sql}
          AND v.assignee IN ({support_ph})
        GROUP BY 1
        ORDER BY 1
    """, (str(months), *cust_params, *support_analysts))


def get_analyst_swooper_tickets(assignee, months=6):
    """Tickets where <assignee> closed but did < 25%% of InHance actions."""
    cust_sql, cust_params = _customer_exclusion_clause(column="v.customer")
    return query(f"""
        WITH inh_actions AS (
            SELECT ta.ticket_id, ta.creator_name,
                   COUNT(*) FILTER (WHERE NOT COALESCE(ta.is_empty, FALSE)) AS nonempty
            FROM ticket_actions ta
            WHERE ta.party = 'inh'
            GROUP BY ta.ticket_id, ta.creator_name
        ),
        ticket_work AS (
            SELECT v.ticket_id,
                   COALESCE(SUM(ia.nonempty), 0) AS total_inh,
                   COALESCE(SUM(CASE WHEN ia.creator_name = %s THEN ia.nonempty ELSE 0 END), 0) AS own
            FROM vw_ticket_analytics_core v
            LEFT JOIN inh_actions ia ON ia.ticket_id = v.ticket_id
            WHERE v.closed_at IS NOT NULL
              AND v.closed_at >= CURRENT_DATE - (%s || ' months')::interval
              AND v.assignee = %s
              AND {cust_sql}
            GROUP BY v.ticket_id
        )
        SELECT v.ticket_id, v.ticket_number, v.ticket_name, v.status, v.severity,
               v.product_name, v.assignee, v.customer, v.days_opened, v.priority,
               v.overall_complexity, v.frustrated, v.date_modified, v.closed_at,
               v.do_number, v.do_status,
               tw.total_inh, tw.own,
               CASE WHEN tw.total_inh > 0 THEN ROUND(tw.own::numeric / tw.total_inh, 3) ELSE 1 END AS own_ratio
        FROM ticket_work tw
        JOIN vw_ticket_analytics_core v ON v.ticket_id = tw.ticket_id
        WHERE tw.total_inh > 0 AND tw.own::numeric / tw.total_inh < 0.25
        ORDER BY v.closed_at DESC NULLS LAST
    """, (assignee, str(months), assignee, *cust_params))


def get_analyst_action_profile(months=6):
    """Per analyst: % of own actions that are technical_work vs scheduling."""
    cust_sql, cust_params = _customer_exclusion_clause(column="v.customer")
    support_analysts = _get_support_analysts()
    support_ph = ",".join(["%s"] * len(support_analysts))
    return query(f"""
        WITH closed AS (
            SELECT v.ticket_id, v.assignee
            FROM vw_ticket_analytics_core v
            WHERE v.closed_at IS NOT NULL
              AND v.closed_at >= CURRENT_DATE - (%s || ' months')::interval
              AND v.assignee IS NOT NULL
              AND {cust_sql}
              AND v.assignee IN ({support_ph})
        ),
        own_actions AS (
            SELECT c.assignee,
                   ta.action_class,
                   COUNT(*) AS cnt
            FROM closed c
            JOIN ticket_actions ta ON ta.ticket_id = c.ticket_id
                                   AND ta.creator_name = c.assignee
                                   AND ta.party = 'inh'
                                   AND NOT COALESCE(ta.is_empty, FALSE)
            GROUP BY c.assignee, ta.action_class
        ),
        totals AS (
            SELECT assignee, SUM(cnt) AS total FROM own_actions GROUP BY assignee
        )
        SELECT t.assignee,
               ROUND(100.0 * COALESCE(SUM(oa.cnt) FILTER (WHERE oa.action_class = 'technical_work'), 0) / NULLIF(t.total, 0), 1) AS pct_technical,
               ROUND(100.0 * COALESCE(SUM(oa.cnt) FILTER (WHERE oa.action_class = 'scheduling'), 0) / NULLIF(t.total, 0), 1) AS pct_scheduling
        FROM totals t
        LEFT JOIN own_actions oa ON oa.assignee = t.assignee
        WHERE t.total >= 20
        GROUP BY t.assignee, t.total
        ORDER BY pct_technical
    """, (str(months), *cust_params, *support_analysts))


def get_analyst_severity_profile(months=6):
    """Per analyst: % of closures that were high severity (Sev 1)."""
    cust_sql, cust_params = _customer_exclusion_clause(column="v.customer")
    support_analysts = _get_support_analysts()
    support_ph = ",".join(["%s"] * len(support_analysts))
    return query(f"""
        SELECT v.assignee,
               COUNT(*)::int AS total_closed,
               ROUND(100.0 * COUNT(*) FILTER (
                   WHERE v.severity LIKE '1%%' OR LOWER(v.severity) LIKE '%%high%%'
               ) / NULLIF(COUNT(*), 0), 1) AS pct_high_severity,
               ROUND(100.0 * COUNT(*) FILTER (
                   WHERE v.severity LIKE '3%%' OR LOWER(v.severity) LIKE '%%low%%'
               ) / NULLIF(COUNT(*), 0), 1) AS pct_low_severity
        FROM vw_ticket_analytics_core v
        WHERE v.closed_at IS NOT NULL
          AND v.closed_at >= CURRENT_DATE - (%s || ' months')::interval
          AND v.assignee IS NOT NULL
          AND {cust_sql}
          AND v.assignee IN ({support_ph})
        GROUP BY v.assignee
        HAVING COUNT(*) >= 10
        ORDER BY pct_high_severity
    """, (str(months), *cust_params, *support_analysts))


def get_analyst_reassignment_profile(months=6):
    """Avg within-InHance handoffs per closed high-severity ticket, by analyst and severity."""
    cust_sql, cust_params = _customer_exclusion_clause(column="v.customer")
    support_analysts = _get_support_analysts()
    support_ph = ",".join(["%s"] * len(support_analysts))
    return query(f"""
        WITH closed AS (
            SELECT v.ticket_id, v.assignee, v.severity
            FROM vw_ticket_analytics_core v
            WHERE v.closed_at IS NOT NULL
              AND v.closed_at >= CURRENT_DATE - (%s || ' months')::interval
              AND v.assignee IS NOT NULL
              AND (v.severity LIKE '0%%' OR v.severity LIKE '1%%'
                   OR LOWER(v.severity) LIKE '%%system down%%'
                   OR LOWER(v.severity) LIKE '%%high%%')
              AND {cust_sql}
              AND v.assignee IN ({support_ph})
        ),
        handoff_counts AS (
            SELECT c.ticket_id, c.assignee, c.severity,
                   COUNT(h.handoff_id)::int AS handoffs
            FROM closed c
            LEFT JOIN ticket_handoffs h
                   ON h.ticket_id = c.ticket_id
                  AND h.handoff_reason = 'participant_switch_within_inh'
            GROUP BY c.ticket_id, c.assignee, c.severity
        )
        SELECT assignee, severity,
               COUNT(*)::int AS tickets,
               ROUND(AVG(handoffs), 2) AS avg_handoffs
        FROM handoff_counts
        GROUP BY assignee, severity
        ORDER BY assignee, severity
    """, (str(months), *cust_params, *support_analysts))


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
                          kpi_filter=None, group_name=None):
    """Return open tickets matching chart drill-down filters."""
    conditions = ["TRUE"]
    params = []

    if kpi_filter and kpi_filter in KPI_FILTERS:
        conditions.extend(KPI_FILTERS[kpi_filter])

    if group_name:
        if group_name == "Unassigned":
            conditions.append("(v.group_name IS NULL OR v.group_name = '')")
        else:
            conditions.append("v.group_name = %s")
            params.append(group_name)

    if product:
        if product == "PowerMan":
            conditions.append(
                "(LOWER(v.product_name) LIKE 'pm%%' OR LOWER(v.product_name) LIKE '%%power%%')"
            )
        else:
            conditions.append("v.product_name = %s")
            params.append(product)

    if severity_tier and severity_tier in SEVERITY_TIER_SQL:
        conditions.append(SEVERITY_TIER_SQL[severity_tier])

    if age_bucket and age_bucket in AGE_BUCKET_RANGES:
        lo, hi = AGE_BUCKET_RANGES[age_bucket]
        conditions.append("EXTRACT(DAY FROM now() - v.date_created)::int >= %s")
        params.append(lo)
        if hi is not None:
            conditions.append("EXTRACT(DAY FROM now() - v.date_created)::int < %s")
            params.append(hi)

    cust_sql, cust_params = _customer_exclusion_clause(column="v.customer")
    conditions.append(cust_sql)
    params.extend(cust_params)

    where = " AND ".join(conditions)
    return query(f"""
        SELECT v.ticket_id, v.ticket_number, v.ticket_name, v.status, v.severity,
               v.product_name, v.assignee, v.customer, v.days_opened, v.priority,
               v.overall_complexity, v.frustrated, v.date_modified
        FROM vw_operational_open_tickets v
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
    """Return tickets in the latest deterministic mechanism cluster run."""
    return query("""
        WITH latest_p1 AS (
            SELECT DISTINCT ON (ticket_id)
                ticket_id,
                phenomenon,
                component,
                operation,
                canonical_failure,
                (parsed_json->>'confidence') AS confidence,
                status,
                completed_at
            FROM ticket_llm_pass_results
            WHERE pass_name = 'pass1_phenomenon'
            ORDER BY ticket_id,
                     CASE WHEN status = 'success' THEN 0 ELSE 1 END,
                     updated_at DESC
        ),
        latest_p3 AS (
            SELECT DISTINCT ON (ticket_id)
                ticket_id,
                mechanism,
                (parsed_json->>'evidence') AS evidence,
                status,
                completed_at
            FROM ticket_llm_pass_results
            WHERE pass_name = 'pass3_mechanism'
            ORDER BY ticket_id,
                     CASE WHEN status = 'success' THEN 0 ELSE 1 END,
                     updated_at DESC
        ),
        latest_p4 AS (
            SELECT DISTINCT ON (ticket_id)
                ticket_id,
                mechanism_class,
                intervention_type,
                intervention_action,
                status,
                completed_at
            FROM ticket_llm_pass_results
            WHERE pass_name = 'pass4_intervention'
            ORDER BY ticket_id,
                     CASE WHEN status = 'success' THEN 0 ELSE 1 END,
                     updated_at DESC
        )
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
            tc.cluster_id     AS mechanism_class,
            p4.intervention_type,
            p4.intervention_action,
            p4.status        AS pass4_status,
            p4.completed_at  AS pass4_completed_at
        FROM vw_latest_mechanism_ticket_clusters tc
        JOIN tickets t ON t.ticket_id = tc.ticket_id
        LEFT JOIN latest_p1 p1 ON p1.ticket_id = t.ticket_id
        LEFT JOIN latest_p3 p3 ON p3.ticket_id = t.ticket_id
        LEFT JOIN latest_p4 p4 ON p4.ticket_id = t.ticket_id
        WHERE tc.cluster_id IS NOT NULL
          AND COALESCE(t.status, '') != 'Open'
          AND COALESCE(t.assignee, '') != 'Marketing'
          AND COALESCE(t.customer, '') NOT IN %s
        ORDER BY COALESCE(p4.completed_at, p3.completed_at, p1.completed_at) DESC NULLS LAST
    """, (EXCLUDED_CUSTOMERS,))


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
        WITH latest_p1 AS (
            SELECT DISTINCT ON (ticket_id)
                ticket_id,
                status,
                component
            FROM ticket_llm_pass_results
            WHERE pass_name = 'pass1_phenomenon'
            ORDER BY ticket_id,
                     CASE WHEN status = 'success' THEN 0 ELSE 1 END,
                     updated_at DESC
        ),
        latest_p3 AS (
            SELECT DISTINCT ON (ticket_id)
                ticket_id,
                status
            FROM ticket_llm_pass_results
            WHERE pass_name = 'pass3_mechanism'
            ORDER BY ticket_id,
                     CASE WHEN status = 'success' THEN 0 ELSE 1 END,
                     updated_at DESC
        ),
        scoped_tickets AS (
            SELECT ticket_id
            FROM tickets
            WHERE COALESCE(status, '') != 'Open'
              AND COALESCE(assignee, '') != 'Marketing'
              AND COALESCE(customer, '') NOT IN %s
        )
        SELECT
            COUNT(DISTINCT st.ticket_id)
                FILTER (WHERE p1.status = 'success') AS pass1_success,
            COUNT(DISTINCT st.ticket_id)
                FILTER (WHERE p3.status = 'success') AS pass3_success,
            COUNT(DISTINCT tc.ticket_id)             AS pass4_success,
            COUNT(DISTINCT cc.cluster_id)
                FILTER (WHERE cc.cluster_id IS NOT NULL AND cc.cluster_id != 'other')
                                                  AS distinct_mechanism_classes,
            COUNT(DISTINCT p1.component)
                FILTER (WHERE p1.status = 'success' AND p1.component IS NOT NULL)
                                                  AS distinct_components
        FROM scoped_tickets st
        LEFT JOIN latest_p1 p1 ON p1.ticket_id = st.ticket_id
        LEFT JOIN latest_p3 p3 ON p3.ticket_id = st.ticket_id
        LEFT JOIN vw_latest_mechanism_ticket_clusters tc ON tc.ticket_id = st.ticket_id
        LEFT JOIN vw_latest_mechanism_cluster_catalog cc ON cc.cluster_id = tc.cluster_id
    """, (EXCLUDED_CUSTOMERS,)) or {
        "pass1_success": 0, "pass3_success": 0, "pass4_success": 0,
        "distinct_mechanism_classes": 0, "distinct_components": 0,
    }


def get_mechanism_class_distribution():
    """Mechanism class counts from the latest deterministic cluster catalog."""
    return query("""
        SELECT cluster_id AS mechanism_class, ticket_count
        FROM vw_latest_mechanism_cluster_catalog
        ORDER BY ticket_count DESC
    """)


def get_root_cause_cluster_catalog():
    """Persisted deterministic cluster catalog for the root-cause dashboard."""
    return query("""
        SELECT
            cluster_id,
            cluster_label,
            ticket_count,
            percent_of_total,
            customer_count,
            product_count,
            dominant_component,
            dominant_operation,
            dominant_intervention_type,
            example_ticket_ids,
            example_mechanisms,
            subclusters
        FROM vw_latest_mechanism_cluster_catalog
        ORDER BY ticket_count DESC, cluster_id
    """)


def get_intervention_type_distribution():
    """Intervention type counts from tickets in the latest cluster run."""
    return query("""
        WITH latest_p4 AS (
            SELECT DISTINCT ON (ticket_id)
                ticket_id,
                intervention_type
            FROM ticket_llm_pass_results
            WHERE pass_name = 'pass4_intervention'
              AND status = 'success'
            ORDER BY ticket_id, updated_at DESC
        )
        SELECT p4.intervention_type, COUNT(*)::int AS ticket_count
        FROM vw_latest_mechanism_ticket_clusters tc
        JOIN latest_p4 p4 ON p4.ticket_id = tc.ticket_id
        GROUP BY intervention_type
        ORDER BY ticket_count DESC
    """)


def get_component_distribution(limit=20):
    """Top components by ticket count within the latest cluster run."""
    return query("""
        WITH latest_p1 AS (
            SELECT DISTINCT ON (ticket_id)
                ticket_id,
                component
            FROM ticket_llm_pass_results
            WHERE pass_name = 'pass1_phenomenon'
              AND status = 'success'
            ORDER BY ticket_id, updated_at DESC
        )
        SELECT p1.component, COUNT(*) AS ticket_count
        FROM vw_latest_mechanism_ticket_clusters tc
        JOIN latest_p1 p1 ON p1.ticket_id = tc.ticket_id
        WHERE p1.component IS NOT NULL
          AND p1.component != ''
        GROUP BY component
        ORDER BY ticket_count DESC
        LIMIT %s
    """, (limit,))


def get_operation_distribution():
    """Operation verb counts within the latest cluster run."""
    return query("""
        WITH latest_p1 AS (
            SELECT DISTINCT ON (ticket_id)
                ticket_id,
                operation
            FROM ticket_llm_pass_results
            WHERE pass_name = 'pass1_phenomenon'
              AND status = 'success'
            ORDER BY ticket_id, updated_at DESC
        )
        SELECT p1.operation, COUNT(*) AS ticket_count
        FROM vw_latest_mechanism_ticket_clusters tc
        JOIN latest_p1 p1 ON p1.ticket_id = tc.ticket_id
        WHERE p1.operation IS NOT NULL
          AND p1.operation != ''
        GROUP BY operation
        ORDER BY ticket_count DESC
    """)


def get_top_engineering_fixes(limit=25):
    """Top engineering fixes ranked by ticket count in the latest cluster run."""
    return query("""
        WITH latest_p4 AS (
            SELECT DISTINCT ON (ticket_id)
                ticket_id,
                intervention_type,
                intervention_action
            FROM ticket_llm_pass_results
            WHERE pass_name = 'pass4_intervention'
              AND status = 'success'
            ORDER BY ticket_id, updated_at DESC
        )
        SELECT
            tc.cluster_id AS mechanism_class,
            p4.intervention_type,
            COUNT(*)::int AS ticket_count,
            MODE() WITHIN GROUP (ORDER BY p4.intervention_action) AS representative_action
        FROM vw_latest_mechanism_ticket_clusters tc
        JOIN latest_p4 p4 ON p4.ticket_id = tc.ticket_id
        GROUP BY tc.cluster_id, p4.intervention_type
        ORDER BY ticket_count DESC, mechanism_class, intervention_type
        LIMIT %s
    """, (limit,))


def get_tickets_by_fixes(fix_keys):
    """Return tickets matching a list of (mechanism_class, intervention_type) pairs."""
    if not fix_keys:
        return []
    # Build OR conditions for each pair
    conditions = []
    params = []
    for mc, it in fix_keys:
        conditions.append("(tc.cluster_id = %s AND p4.intervention_type = %s)")
        params.extend([mc, it])
    where = " OR ".join(conditions)
    return query(f"""
        WITH latest_p4 AS (
            SELECT DISTINCT ON (ticket_id)
                ticket_id,
                intervention_type,
                intervention_action
            FROM ticket_llm_pass_results
            WHERE pass_name = 'pass4_intervention'
              AND status = 'success'
            ORDER BY ticket_id, updated_at DESC
        )
        SELECT DISTINCT v.ticket_id, v.ticket_number, v.ticket_name, v.status, v.severity,
               v.product_name, v.assignee, v.customer, v.days_opened, v.priority,
               v.overall_complexity, v.frustrated, v.date_modified,
               v.do_number, v.do_status,
               tc.cluster_id AS mechanism_class, p4.intervention_type, p4.intervention_action
        FROM vw_latest_mechanism_ticket_clusters tc
        JOIN latest_p4 p4 ON p4.ticket_id = tc.ticket_id
        JOIN vw_ticket_analytics_core v ON v.ticket_id = tc.ticket_id
        WHERE tc.cluster_id IS NOT NULL
          AND ({where})
        ORDER BY v.date_modified DESC NULLS LAST
    """, tuple(params))


def get_root_cause_by_product(limit=10):
    """Mechanism class counts broken down by product for the latest cluster run."""
    return query("""
        WITH ranked_products AS (
            SELECT t.product_name, COUNT(*) AS total
            FROM vw_latest_mechanism_ticket_clusters tc
            JOIN tickets t ON t.ticket_id = tc.ticket_id
            WHERE tc.cluster_id IS NOT NULL
            GROUP BY t.product_name
            ORDER BY total DESC
            LIMIT %s
        )
        SELECT
            COALESCE(NULLIF(t.product_name, ''), 'Unknown') AS product_name,
            tc.cluster_id AS mechanism_class,
            COUNT(*) AS ticket_count
        FROM vw_latest_mechanism_ticket_clusters tc
        JOIN tickets t ON t.ticket_id = tc.ticket_id
        JOIN ranked_products rp ON rp.product_name = t.product_name
        WHERE tc.cluster_id IS NOT NULL
        GROUP BY t.product_name, tc.cluster_id
        ORDER BY t.product_name, ticket_count DESC
    """, (limit,))


def get_root_cause_sankey(component_limit=15):
    """Flow data for Sankey: component → mechanism_class → intervention_type."""
    return query("""
        WITH top_components AS (
            WITH latest_p1 AS (
                SELECT DISTINCT ON (ticket_id)
                    ticket_id,
                    component
                FROM ticket_llm_pass_results
                WHERE pass_name = 'pass1_phenomenon'
                  AND status = 'success'
                ORDER BY ticket_id, updated_at DESC
            )
            SELECT component
            FROM vw_latest_mechanism_ticket_clusters tc
            JOIN latest_p1 p1 ON p1.ticket_id = tc.ticket_id
            WHERE p1.component IS NOT NULL AND p1.component != ''
            GROUP BY component
            ORDER BY COUNT(*) DESC
            LIMIT %s
        ),
        latest_p1 AS (
            SELECT DISTINCT ON (ticket_id)
                ticket_id,
                component
            FROM ticket_llm_pass_results
            WHERE pass_name = 'pass1_phenomenon'
              AND status = 'success'
            ORDER BY ticket_id, updated_at DESC
        ),
        latest_p4 AS (
            SELECT DISTINCT ON (ticket_id)
                ticket_id,
                intervention_type
            FROM ticket_llm_pass_results
            WHERE pass_name = 'pass4_intervention'
              AND status = 'success'
            ORDER BY ticket_id, updated_at DESC
        )
        SELECT
            COALESCE(NULLIF(p1.component, ''), 'Unknown') AS component,
            COALESCE(tc.cluster_id, 'unclassified')       AS mechanism_class,
            COALESCE(p4.intervention_type, 'unmapped')    AS intervention_type,
            COUNT(*) AS ticket_count
        FROM vw_latest_mechanism_ticket_clusters tc
        JOIN latest_p1 p1 ON p1.ticket_id = tc.ticket_id
        LEFT JOIN latest_p4 p4 ON p4.ticket_id = tc.ticket_id
        WHERE p1.component IS NOT NULL AND p1.component != ''
          AND p1.component IN (SELECT component FROM top_components)
        GROUP BY p1.component, tc.cluster_id, p4.intervention_type
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


def get_saved_reports(page='tickets'):
    return query("""
        SELECT id, name, filter_model, created_at, sort_order
        FROM saved_reports
        WHERE page = %s
        ORDER BY sort_order, name
    """, (page,))


def save_report(name, filter_model, page='tickets'):
    """Upsert a saved report by name+page and return the saved row."""
    # Assign next sort_order within this page
    max_row = query_one("SELECT COALESCE(MAX(sort_order), 0) + 1 AS next_order FROM saved_reports WHERE page = %s", (page,))
    next_order = max_row["next_order"] if max_row else 1
    return _execute_returning("""
        INSERT INTO saved_reports (name, filter_model, sort_order, page)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (name, page) DO UPDATE SET filter_model = EXCLUDED.filter_model,
                                               created_at = now()
        RETURNING id, name, filter_model, created_at, sort_order
    """, (name, json.dumps(filter_model), next_order, page))


def delete_report(report_id):
    _execute("DELETE FROM saved_reports WHERE id = %s", (report_id,))


def reorder_report(report_id, direction):
    """Move a saved report tab left (-1) or right (+1), within the same page.

    Swaps sort_order with the adjacent report in the given direction.
    """
    current = query_one(
        "SELECT id, sort_order, page FROM saved_reports WHERE id = %s", (report_id,)
    )
    if not current:
        return
    cur_order = current["sort_order"]
    cur_page = current.get("page") or "tickets"
    if direction in (-1, "left"):  # move left
        neighbor = query_one(
            "SELECT id, sort_order FROM saved_reports WHERE page = %s AND sort_order < %s ORDER BY sort_order DESC LIMIT 1",
            (cur_page, cur_order)
        )
    else:  # move right
        neighbor = query_one(
            "SELECT id, sort_order FROM saved_reports WHERE page = %s AND sort_order > %s ORDER BY sort_order ASC LIMIT 1",
            (cur_page, cur_order)
        )
    if not neighbor:
        return
    # Swap
    _execute(
        "UPDATE saved_reports SET sort_order = %s WHERE id = %s",
        (neighbor["sort_order"], current["id"]),
    )
    _execute(
        "UPDATE saved_reports SET sort_order = %s WHERE id = %s",
        (cur_order, neighbor["id"]),
    )


# ── Runtime dashboards (shared in v1, ownership-ready for future) ───

def _execute_returning(sql, params=()):
    """Run an INSERT/UPDATE/DELETE ... RETURNING and commit."""
    conn = db.get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
        conn.commit()
        return {k: _serialize_value(v) for k, v in dict(row).items()} if row else None
    except Exception:
        conn.rollback()
        raise
    finally:
        db.put_conn(conn)


def _is_dashboard_read_fallback_error(exc):
    cause = getattr(exc, "__cause__", None)
    fallback_types = (
        psycopg2_errors.UndefinedTable,
        psycopg2.OperationalError,
    )
    return isinstance(exc, fallback_types) or isinstance(cause, fallback_types)


def list_dashboards(owner_type="global", owner_id=None, include_inactive=False):
    conditions = ["owner_type = %s"]
    params = [owner_type]

    if owner_id is None:
        conditions.append("owner_id IS NULL")
    else:
        conditions.append("owner_id = %s")
        params.append(owner_id)

    if not include_inactive:
        conditions.append("is_active = TRUE")

    where = " AND ".join(conditions)
    try:
        return query(f"""
            SELECT id, name, slug, description, icon, sort_order, is_default,
                   is_active, owner_type, owner_id, created_at, updated_at
            FROM dashboards
            WHERE {where}
            ORDER BY sort_order, name
        """, tuple(params))
    except Exception as exc:
        if _is_dashboard_read_fallback_error(exc):
            return []
        raise


def get_dashboard_by_slug(slug, owner_type="global", owner_id=None, include_inactive=False):
    conditions = ["slug = %s", "owner_type = %s"]
    params = [slug, owner_type]

    if owner_id is None:
        conditions.append("owner_id IS NULL")
    else:
        conditions.append("owner_id = %s")
        params.append(owner_id)

    if not include_inactive:
        conditions.append("is_active = TRUE")

    where = " AND ".join(conditions)
    try:
        return query_one(f"""
            SELECT id, name, slug, description, icon, sort_order, is_default,
                   is_active, owner_type, owner_id, created_at, updated_at
            FROM dashboards
            WHERE {where}
        """, tuple(params))
    except Exception as exc:
        if _is_dashboard_read_fallback_error(exc):
            return None
        raise


def get_dashboard_tree(dashboard_id):
    try:
        dashboard = query_one("""
            SELECT id, name, slug, description, icon, sort_order, is_default,
                   is_active, owner_type, owner_id, created_at, updated_at
            FROM dashboards
            WHERE id = %s
        """, (dashboard_id,))
    except Exception as exc:
        if _is_dashboard_read_fallback_error(exc):
            return None
        raise
    if not dashboard:
        return None

    try:
        sections = query("""
            SELECT id, dashboard_id, title, description, layout_columns,
                   sort_order, is_active, created_at, updated_at
            FROM dashboard_sections
            WHERE dashboard_id = %s
              AND is_active = TRUE
            ORDER BY sort_order, id
        """, (dashboard_id,))

        widgets = query("""
            SELECT id, section_id, widget_type, title, query_key, query_params_json,
                   display_config_json, sort_order, is_active, created_at, updated_at
            FROM dashboard_widgets
            WHERE section_id IN (
                SELECT id FROM dashboard_sections
                WHERE dashboard_id = %s
                  AND is_active = TRUE
            )
              AND is_active = TRUE
            ORDER BY sort_order, id
        """, (dashboard_id,))
    except Exception as exc:
        if _is_dashboard_read_fallback_error(exc):
            return None
        raise

    widgets_by_section = {}
    for widget in widgets:
        widgets_by_section.setdefault(widget["section_id"], []).append(widget)

    dashboard["sections"] = []
    for section in sections:
        section["widgets"] = widgets_by_section.get(section["id"], [])
        dashboard["sections"].append(section)
    return dashboard


def create_dashboard(name, slug, description=None, icon=None, sort_order=0,
                     is_default=False, is_active=True, owner_type="global", owner_id=None):
    return _execute_returning("""
        INSERT INTO dashboards (
            name, slug, description, icon, sort_order, is_default,
            is_active, owner_type, owner_id, created_at, updated_at
        )
        VALUES (
            %s, %s, %s, %s, %s, %s,
            %s, %s, %s, now(), now()
        )
        RETURNING id, name, slug, description, icon, sort_order, is_default,
                  is_active, owner_type, owner_id, created_at, updated_at
    """, (
        name, slug, description, icon, sort_order, is_default,
        is_active, owner_type, owner_id,
    ))


def update_dashboard(dashboard_id, name, slug, description=None, icon=None,
                     sort_order=0, is_default=False, is_active=True):
    return _execute_returning("""
        UPDATE dashboards
        SET name = %s,
            slug = %s,
            description = %s,
            icon = %s,
            sort_order = %s,
            is_default = %s,
            is_active = %s,
            updated_at = now()
        WHERE id = %s
        RETURNING id, name, slug, description, icon, sort_order, is_default,
                  is_active, owner_type, owner_id, created_at, updated_at
    """, (
        name, slug, description, icon, sort_order, is_default, is_active, dashboard_id,
    ))


def delete_dashboard(dashboard_id):
    _execute("DELETE FROM dashboards WHERE id = %s", (dashboard_id,))


def create_dashboard_section(dashboard_id, title=None, description=None,
                             layout_columns=1, sort_order=0, is_active=True):
    return _execute_returning("""
        INSERT INTO dashboard_sections (
            dashboard_id, title, description, layout_columns, sort_order,
            is_active, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, now(), now())
        RETURNING id, dashboard_id, title, description, layout_columns,
                  sort_order, is_active, created_at, updated_at
    """, (dashboard_id, title, description, layout_columns, sort_order, is_active))


def update_dashboard_section(section_id, title=None, description=None,
                             layout_columns=1, sort_order=0, is_active=True):
    return _execute_returning("""
        UPDATE dashboard_sections
        SET title = %s,
            description = %s,
            layout_columns = %s,
            sort_order = %s,
            is_active = %s,
            updated_at = now()
        WHERE id = %s
        RETURNING id, dashboard_id, title, description, layout_columns,
                  sort_order, is_active, created_at, updated_at
    """, (title, description, layout_columns, sort_order, is_active, section_id))


def delete_dashboard_section(section_id):
    _execute("DELETE FROM dashboard_sections WHERE id = %s", (section_id,))


def create_dashboard_widget(section_id, widget_type, title=None, query_key=None,
                            query_params=None, display_config=None, sort_order=0,
                            is_active=True):
    return _execute_returning("""
        INSERT INTO dashboard_widgets (
            section_id, widget_type, title, query_key, query_params_json,
            display_config_json, sort_order, is_active, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now(), now())
        RETURNING id, section_id, widget_type, title, query_key, query_params_json,
                  display_config_json, sort_order, is_active, created_at, updated_at
    """, (
        section_id,
        widget_type,
        title,
        query_key,
        json.dumps(query_params or {}),
        json.dumps(display_config or {}),
        sort_order,
        is_active,
    ))


def update_dashboard_widget(widget_id, widget_type, title=None, query_key=None,
                            query_params=None, display_config=None, sort_order=0,
                            is_active=True):
    return _execute_returning("""
        UPDATE dashboard_widgets
        SET widget_type = %s,
            title = %s,
            query_key = %s,
            query_params_json = %s,
            display_config_json = %s,
            sort_order = %s,
            is_active = %s,
            updated_at = now()
        WHERE id = %s
        RETURNING id, section_id, widget_type, title, query_key, query_params_json,
                  display_config_json, sort_order, is_active, created_at, updated_at
    """, (
        widget_type,
        title,
        query_key,
        json.dumps(query_params or {}),
        json.dumps(display_config or {}),
        sort_order,
        is_active,
        widget_id,
    ))


def delete_dashboard_widget(widget_id):
    _execute("DELETE FROM dashboard_widgets WHERE id = %s", (widget_id,))


# ── Cluster Rollup Analysis ─────────────────────────────────────────

def get_top_clusters(product_names=None, top_n=5):
    """Return top N L1 clusters per product using a window function.

    Parameters
    ----------
    product_names : list[str] | None
        Optional list of product names to filter on. None = all products.
    top_n : int
        Number of top clusters to return per product (default 5).
    """
    base = """
        SELECT *
        FROM (
            SELECT
                product_name,
                mechanism_class,
                cluster_key_l1,
                ticket_count,
                ROW_NUMBER() OVER (
                    PARTITION BY product_name
                    ORDER BY ticket_count DESC
                ) AS rn
            FROM v_cluster_summary_l1
        ) t
        WHERE rn <= %s
    """
    params = [top_n]
    if product_names:
        placeholders = ",".join(["%s"] * len(product_names))
        base += f" AND product_name IN ({placeholders})"
        params.extend(product_names)
    base += " ORDER BY product_name, rn"
    return query(base, tuple(params))


def get_top_clusters_for_customer(customer_names, top_n=3, open_only=True):
    """Return top N issue clusters for the given customer(s).

    Joins tickets → pass results → rollup map to aggregate cluster counts
    filtered by customer name. Returns both L1 cluster key and mechanism class.
    """
    if not customer_names:
        return []
    placeholders = ",".join(["%s"] * len(customer_names))
    sql = f"""
        SELECT
            r4.mechanism_class,
            COALESCE(m.cluster_key_l1, r5.cluster_key) AS cluster_key_l1,
            COUNT(*) AS ticket_count
        FROM tickets t
        JOIN ticket_llm_pass_results r4
            ON r4.ticket_id = t.ticket_id
           AND r4.pass_name = 'pass4_intervention'
           AND r4.status = 'success'
           AND r4.mechanism_class IS NOT NULL
        JOIN ticket_llm_pass_results r5
            ON r5.ticket_id = t.ticket_id
           AND r5.pass_name = 'pass5_cluster_key'
           AND r5.status = 'success'
           AND r5.cluster_key IS NOT NULL
        LEFT JOIN cluster_key_rollup_map m
            ON m.cluster_key = r5.cluster_key
           AND m.is_active = TRUE
        WHERE t.customer IN ({placeholders})
    """
    params = list(customer_names)
    if open_only:
        sql += " AND t.status NOT ILIKE %s"
        params.append("%Closed%")
    sql += """
        GROUP BY r4.mechanism_class, COALESCE(m.cluster_key_l1, r5.cluster_key)
        ORDER BY COUNT(*) DESC
        LIMIT %s
    """
    params.append(top_n)
    return query(sql, tuple(params))


def get_cluster_examples(product_name, mechanism_class, cluster_key_l1):
    """Return example tickets for a specific L1 cluster."""
    return query("""
        SELECT
            ticket_id,
            product_name,
            mechanism_class,
            cluster_key_l1,
            cluster_key_l2,
            mechanism,
            intervention_action
        FROM v_cluster_examples
        WHERE product_name = %s
          AND mechanism_class = %s
          AND cluster_key_l1 = %s
    """, (product_name, mechanism_class, cluster_key_l1))


def save_cluster_recommendation(product_name, mechanism_class, cluster_key_l1,
                                ticket_count, recommended_change,
                                where_to_implement, why_it_prevents_recurrence,
                                confidence=None, source_model=None):
    """Persist a cluster-level recommendation."""
    return _execute_returning("""
        INSERT INTO cluster_recommendations (
            product_name, mechanism_class, cluster_key_l1, ticket_count,
            recommended_change, where_to_implement, why_it_prevents_recurrence,
            confidence, source_model
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id, product_name, mechanism_class, cluster_key_l1,
                  ticket_count, recommended_change, where_to_implement,
                  why_it_prevents_recurrence, confidence, source_model, created_at
    """, (
        product_name, mechanism_class, cluster_key_l1, ticket_count,
        recommended_change, where_to_implement, why_it_prevents_recurrence,
        confidence, source_model,
    ))


def get_cluster_recommendations(product_name=None, cluster_key_l1=None):
    """Fetch saved cluster recommendations, optionally filtered."""
    sql = "SELECT * FROM cluster_recommendations WHERE 1=1"
    params = []
    if product_name:
        sql += " AND product_name = %s"
        params.append(product_name)
    if cluster_key_l1:
        sql += " AND cluster_key_l1 = %s"
        params.append(cluster_key_l1)
    sql += " ORDER BY created_at DESC"
    return query(sql, tuple(params))


def get_cluster_summary_l2():
    """Return L2-level cluster summary (current cluster_key granularity)."""
    return query("""
        SELECT product_name, mechanism_class, cluster_key, ticket_count
        FROM v_cluster_summary_l2
        ORDER BY ticket_count DESC
    """)


def get_cluster_summary_l1():
    """Return L1-level cluster summary (rolled-up broader buckets)."""
    return query("""
        SELECT product_name, mechanism_class, cluster_key_l1, ticket_count
        FROM v_cluster_summary_l1
        ORDER BY ticket_count DESC
    """)


# ── Deep Dive (per-analyst / per-product) ────────────────────────────

def _deep_dive_base_conditions(assignees=None, products=None, months=12):
    """Build shared WHERE conditions + params for deep-dive queries.

    Always restricts to support analysts (title contains 'Support').
    """
    conditions = ["v.closed_at >= CURRENT_DATE - (%s || ' months')::interval"]
    params = [months]
    cust_sql, cust_params = _customer_exclusion_clause(column="v.customer")
    conditions.append(cust_sql)
    params.extend(cust_params)
    # Restrict to support analysts
    support_analysts = _get_support_analysts()
    support_ph = ",".join(["%s"] * len(support_analysts))
    conditions.append(f"v.assignee IN ({support_ph})")
    params.extend(support_analysts)
    if assignees:
        ph = ",".join(["%s"] * len(assignees))
        conditions.append(f"v.assignee IN ({ph})")
        params.extend(assignees)
    if products:
        ph = ",".join(["%s"] * len(products))
        conditions.append(f"v.product_name IN ({ph})")
        params.extend(products)
    return " AND ".join(conditions), params


def get_deep_dive_filter_options():
    """Return distinct assignee and product_name lists for filter dropdowns (support analysts only)."""
    cust_sql, cust_params = _customer_exclusion_clause(column="customer")
    support_analysts = _get_support_analysts()
    support_ph = ",".join(["%s"] * len(support_analysts))
    assignees = query(f"""
        SELECT DISTINCT assignee FROM vw_ticket_analytics_core
        WHERE assignee IS NOT NULL AND {cust_sql}
          AND assignee IN ({support_ph})
        ORDER BY assignee
    """, tuple(cust_params) + support_analysts)
    products = query(f"""
        SELECT DISTINCT product_name FROM vw_ticket_analytics_core
        WHERE product_name IS NOT NULL AND {cust_sql}
        ORDER BY product_name
    """, tuple(cust_params))
    return (
        [r["assignee"] for r in assignees],
        [r["product_name"] for r in products],
    )


def get_deep_dive_kpis(assignees=None, products=None, months=12):
    """Return aggregate KPIs for selected analysts/products."""
    where, params = _deep_dive_base_conditions(assignees, products, months)
    return query_one(f"""
        SELECT
            COUNT(*)::int                                            AS total_closed,
            ROUND(AVG(v.days_opened), 1)                             AS avg_days_open,
            ROUND(AVG(v.overall_complexity), 2)                      AS avg_complexity,
            ROUND(AVG(v.hours_to_first_response), 1)                 AS avg_hours_first_response,
            COUNT(*) FILTER (WHERE v.frustrated = 'Yes')::int        AS frustrated_count,
            ROUND(100.0 * COUNT(*) FILTER (WHERE v.frustrated = 'Yes')
                  / NULLIF(COUNT(*), 0), 1)                          AS pct_frustrated,
            COUNT(*) FILTER (WHERE v.priority <= 3)::int             AS high_priority_count,
            ROUND(AVG(v.handoff_count), 1)                           AS avg_handoffs,
            COUNT(DISTINCT v.customer)::int                          AS distinct_customers,
            COUNT(DISTINCT v.assignee)::int                          AS distinct_analysts
        FROM vw_ticket_analytics_core v
        WHERE {where}
    """, tuple(params))


def get_deep_dive_severity_breakdown(assignees=None, products=None, months=12):
    """Return ticket counts by severity for selected filters."""
    where, params = _deep_dive_base_conditions(assignees, products, months)
    return query(f"""
        SELECT COALESCE(v.severity, 'Unknown') AS severity,
               COUNT(*)::int AS ticket_count
        FROM vw_ticket_analytics_core v
        WHERE {where}
        GROUP BY 1 ORDER BY ticket_count DESC
    """, tuple(params))


def get_deep_dive_action_mix(assignees=None, products=None, months=12):
    """Return action class distribution for selected filters."""
    where, params = _deep_dive_base_conditions(assignees, products, months)
    return query(f"""
        SELECT COALESCE(ta.action_class, 'unknown') AS action_class,
               COUNT(*)::int AS action_count
        FROM ticket_actions ta
        JOIN vw_ticket_analytics_core v ON v.ticket_id = ta.ticket_id
        WHERE ta.party = 'inh' AND NOT COALESCE(ta.is_empty, FALSE)
          AND {where}
        GROUP BY 1 ORDER BY action_count DESC
    """, tuple(params))


def get_deep_dive_volume_trend(assignees=None, products=None, months=12):
    """Return monthly closed ticket counts for selected filters."""
    where, params = _deep_dive_base_conditions(assignees, products, months)
    return query(f"""
        SELECT TO_CHAR(v.closed_at, 'YYYY-MM') AS month,
               COUNT(*)::int AS closed_count
        FROM vw_ticket_analytics_core v
        WHERE {where}
        GROUP BY 1 ORDER BY 1
    """, tuple(params))


def get_deep_dive_product_analyst_heatmap(assignees=None, products=None, months=12):
    """Return ticket counts by product × analyst for heatmap."""
    where, params = _deep_dive_base_conditions(assignees, products, months)
    return query(f"""
        SELECT v.assignee, v.product_name,
               COUNT(*)::int AS ticket_count,
               ROUND(AVG(v.days_opened), 1) AS avg_days_open,
               ROUND(AVG(v.overall_complexity), 1) AS avg_complexity,
               COUNT(*) FILTER (WHERE v.frustrated = 'Yes')::int AS frustrated
        FROM vw_ticket_analytics_core v
        WHERE v.assignee IS NOT NULL AND v.product_name IS NOT NULL
          AND {where}
        GROUP BY v.assignee, v.product_name
        ORDER BY ticket_count DESC
    """, tuple(params))


def get_deep_dive_tickets(assignees=None, products=None, months=12):
    """Return all tickets (open and closed) matching the deep-dive assignee/product filters."""
    conditions = ["TRUE"]
    params = []
    cust_sql, cust_params = _customer_exclusion_clause(column="v.customer")
    conditions.append(cust_sql)
    params.extend(cust_params)
    support_analysts = _get_support_analysts()
    support_ph = ",".join(["%s"] * len(support_analysts))
    conditions.append(f"v.assignee IN ({support_ph})")
    params.extend(support_analysts)
    if assignees:
        ph = ",".join(["%s"] * len(assignees))
        conditions.append(f"v.assignee IN ({ph})")
        params.extend(assignees)
    if products:
        ph = ",".join(["%s"] * len(products))
        conditions.append(f"v.product_name IN ({ph})")
        params.extend(products)
    where = " AND ".join(conditions)
    return query(f"""
        SELECT v.ticket_id, v.ticket_number, v.ticket_name, v.status,
               v.severity, v.product_name, v.assignee, v.customer,
               v.days_opened, v.priority, v.overall_complexity,
               v.frustrated, v.handoff_count, v.action_count,
               v.hours_to_first_response, v.date_modified, v.closed_at
        FROM vw_ticket_analytics_core v
        WHERE {where}
        ORDER BY v.date_modified DESC NULLS LAST
    """, tuple(params))


def get_deep_dive_resolution_distribution(assignees=None, products=None, months=12):
    """Return resolution time buckets for histogram."""
    where, params = _deep_dive_base_conditions(assignees, products, months)
    return query(f"""
        SELECT
            CASE
                WHEN v.days_opened < 1  THEN '< 1 day'
                WHEN v.days_opened < 7  THEN '1-7 days'
                WHEN v.days_opened < 30 THEN '7-30 days'
                WHEN v.days_opened < 90 THEN '30-90 days'
                ELSE '90+ days'
            END AS bucket,
            COUNT(*)::int AS ticket_count
        FROM vw_ticket_analytics_core v
        WHERE v.days_opened IS NOT NULL AND {where}
        GROUP BY 1
        ORDER BY MIN(v.days_opened)
    """, tuple(params))


def get_deep_dive_time_by_resource(assignees=None, products=None, months=12):
    """Return total hours entered in actions per analyst per month."""
    conditions = ["ta.created_at >= CURRENT_DATE - (%s || ' months')::interval"]
    params = [months]
    cust_sql, cust_params = _customer_exclusion_clause(column="v.customer")
    conditions.append(cust_sql)
    params.extend(cust_params)
    support_analysts = _get_support_analysts()
    support_ph = ",".join(["%s"] * len(support_analysts))
    # filter hours to entries made BY support analysts
    conditions.append(f"ta.creator_name IN ({support_ph})")
    params.extend(support_analysts)
    if assignees:
        ph = ",".join(["%s"] * len(assignees))
        conditions.append(f"v.assignee IN ({ph})")
        params.extend(assignees)
    if products:
        ph = ",".join(["%s"] * len(products))
        conditions.append(f"v.product_name IN ({ph})")
        params.extend(products)
    where = " AND ".join(conditions)
    return query(f"""
        SELECT
            TO_CHAR(ta.created_at, 'YYYY-MM') AS month,
            ta.creator_name                    AS assignee,
            ROUND(SUM(
                NULLIF(ta.source_payload->>'hours_spent', '')::numeric
            ), 2)                              AS total_hours
        FROM ticket_actions ta
        JOIN vw_ticket_analytics_core v ON v.ticket_id = ta.ticket_id
        WHERE NULLIF(ta.source_payload->>'hours_spent', '') IS NOT NULL
          AND ta.creator_name IS NOT NULL
          AND {where}
        GROUP BY 1, 2
        ORDER BY 1, 2
    """, tuple(params))


def get_deep_dive_avg_days_to_close(assignees=None, products=None, months=12):
    """Return average days to close per month for the deep-dive filters."""
    where, params = _deep_dive_base_conditions(assignees, products, months)
    return query(f"""
        SELECT
            TO_CHAR(v.closed_at, 'YYYY-MM') AS month,
            ROUND(AVG(v.days_opened), 1)     AS avg_days_to_close,
            COUNT(*)::int                    AS tickets_closed
        FROM vw_ticket_analytics_core v
        WHERE v.days_opened IS NOT NULL AND {where}
        GROUP BY 1
        ORDER BY 1
    """, tuple(params))


# ── Operations Overview KPIs (CS group) ──────────────────────────────

def get_ops_avg_days_to_close(months=6, group_name="Customer Support (CS)"):
    """Average days to close for tickets in the given group, per month."""
    cust_sql, cust_params = _customer_exclusion_clause(column="t.customer")
    return query(f"""
        SELECT
            TO_CHAR(t.closed_at, 'YYYY-MM') AS month,
            ROUND(AVG(EXTRACT(EPOCH FROM (t.closed_at - t.date_created)) / 86400.0), 1)
                AS avg_days_to_close,
            COUNT(*)::int AS tickets_closed
        FROM tickets t
        WHERE t.closed_at IS NOT NULL
          AND t.closed_at >= CURRENT_DATE - (%s || ' months')::interval
          AND t.date_created IS NOT NULL
          AND COALESCE(t.group_name, '') = %s
          AND COALESCE(t.status, '') != 'Open'
          AND {cust_sql}
        GROUP BY 1
        ORDER BY 1
    """, (str(months), group_name, *cust_params))


def get_ops_backlog_snapshot(group_name="Customer Support (CS)"):
    """Backlog count at Jan 1 of current year and now, for the given group."""
    cust_sql, cust_params = _customer_exclusion_clause(column="t.customer")
    jan1 = query_one(f"""
        SELECT COUNT(*) AS backlog
        FROM tickets t
        WHERE t.date_created IS NOT NULL
          AND t.date_created::date <= DATE_TRUNC('year', CURRENT_DATE)::date
          AND COALESCE(t.group_name, '') = %s
          AND COALESCE(t.status, '') != 'Open'
          AND {cust_sql}
          AND (
              (t.closed_at IS NOT NULL AND t.closed_at::date > DATE_TRUNC('year', CURRENT_DATE)::date)
              OR
              (t.closed_at IS NULL AND COALESCE(t.status, '') NOT IN ('Closed', 'Resolved'))
          )
    """, (group_name, *cust_params))

    now = query_one(f"""
        SELECT COUNT(*) AS backlog
        FROM tickets t
        WHERE t.date_created IS NOT NULL
          AND COALESCE(t.group_name, '') = %s
          AND COALESCE(t.status, '') != 'Open'
          AND {cust_sql}
          AND t.closed_at IS NULL
          AND COALESCE(t.status, '') NOT IN ('Closed', 'Resolved')
    """, (group_name, *cust_params))

    return {
        "jan1": (jan1 or {}).get("backlog", 0),
        "now": (now or {}).get("backlog", 0),
    }


def get_ops_most_improved_customers(months=3, group_name="Customer Support (CS)", top_n=5):
    """Customers whose open backlog dropped the most over the last N months."""
    cust_sql, cust_params = _customer_exclusion_clause(column="t.customer")
    return query(f"""
        WITH then_open AS (
            SELECT t.customer, COUNT(*) AS open_then
            FROM tickets t
            WHERE t.date_created IS NOT NULL
              AND t.date_created::date <= (CURRENT_DATE - (%s || ' months')::interval)::date
              AND COALESCE(t.group_name, '') = %s
              AND COALESCE(t.status, '') != 'Open'
              AND {cust_sql}
              AND t.customer IS NOT NULL AND t.customer != ''
              AND (
                  (t.closed_at IS NOT NULL
                   AND t.closed_at::date > (CURRENT_DATE - (%s || ' months')::interval)::date)
                  OR
                  (t.closed_at IS NULL
                   AND COALESCE(t.status, '') NOT IN ('Closed', 'Resolved'))
              )
            GROUP BY t.customer
        ),
        now_open AS (
            SELECT t.customer, COUNT(*) AS open_now
            FROM tickets t
            WHERE t.date_created IS NOT NULL
              AND COALESCE(t.group_name, '') = %s
              AND COALESCE(t.status, '') != 'Open'
              AND {cust_sql}
              AND t.customer IS NOT NULL AND t.customer != ''
              AND t.closed_at IS NULL
              AND COALESCE(t.status, '') NOT IN ('Closed', 'Resolved')
            GROUP BY t.customer
        )
        SELECT
            COALESCE(th.customer, nw.customer) AS customer,
            COALESCE(th.open_then, 0)::int     AS open_then,
            COALESCE(nw.open_now, 0)::int      AS open_now,
            (COALESCE(th.open_then, 0) - COALESCE(nw.open_now, 0))::int AS reduction
        FROM then_open th
        FULL OUTER JOIN now_open nw ON nw.customer = th.customer
        WHERE COALESCE(th.open_then, 0) - COALESCE(nw.open_now, 0) > 0
        ORDER BY reduction DESC
        LIMIT %s
    """, (str(months), group_name, *cust_params, str(months),
          group_name, *cust_params, top_n))
