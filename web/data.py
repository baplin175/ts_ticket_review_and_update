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
               product_name, assignee, customer,
               date_created, date_modified, days_opened, days_since_modified,
               action_count, customer_message_count, inhance_message_count,
               priority, priority_explanation,
               overall_complexity, frustrated
        FROM vw_ticket_analytics_core
        WHERE {cust_sql}
        ORDER BY date_modified DESC NULLS LAST
    """, tuple(cust_params))


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
               v.overall_complexity, v.frustrated, v.date_modified
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
    conditions = ["TRUE"]
    params = []

    if kpi_filter and kpi_filter in KPI_FILTERS:
        conditions.extend(KPI_FILTERS[kpi_filter])

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


def get_saved_reports():
    return query("""
        SELECT id, name, filter_model, created_at
        FROM saved_reports
        ORDER BY name
    """)


def save_report(name, filter_model):
    """Upsert a saved report by name and return the saved row."""
    return _execute_returning("""
        INSERT INTO saved_reports (name, filter_model)
        VALUES (%s, %s)
        ON CONFLICT (name) DO UPDATE SET filter_model = EXCLUDED.filter_model,
                                         created_at = now()
        RETURNING id, name, filter_model, created_at
    """, (name, json.dumps(filter_model)))


def delete_report(report_id):
    _execute("DELETE FROM saved_reports WHERE id = %s", (report_id,))


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
