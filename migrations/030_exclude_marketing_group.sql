-- Migration 030 — Exclude group_name='Marketing' from all analytics views.
--
-- Problem: Migration 020 only filtered on assignee='Marketing', but 475 of
-- 481 Marketing-group tickets have a non-Marketing assignee (e.g. individual
-- names).  These leak into all backlog/analytics views, inflating historical
-- counts by up to ~350 tickets.
--
-- Fix: Add COALESCE(t.group_name, '') != 'Marketing' alongside the existing
-- assignee filter in all five analytics views.

-- ════════════════════════════════════════════════════════════════════
-- 1. vw_ticket_analytics_core — master analytics join
-- ════════════════════════════════════════════════════════════════════
DROP VIEW IF EXISTS vw_ticket_analytics_core CASCADE;

CREATE OR REPLACE VIEW vw_ticket_analytics_core AS
SELECT
    t.ticket_id,
    t.ticket_number,
    t.ticket_name,
    t.status,
    t.severity,
    t.product_name,
    t.assignee,
    t.customer,
    t.date_created,
    t.date_modified,
    t.closed_at,
    t.days_opened,
    t.days_since_modified,
    -- metrics
    m.action_count,
    m.nonempty_action_count,
    m.customer_message_count,
    m.inhance_message_count,
    m.distinct_participant_count,
    m.first_response_at,
    m.last_human_activity_at,
    m.empty_action_ratio,
    m.handoff_count,
    m.hours_to_first_response,
    m.days_open,
    -- rollups
    r.latest_customer_text,
    r.latest_inhance_text,
    r.technical_core_text,
    r.summary_for_embedding,
    -- sentiment
    s.frustrated,
    s.frustrated_reason,
    -- priority
    p.priority,
    p.priority_explanation,
    -- complexity
    c.intrinsic_complexity,
    c.coordination_load,
    c.elapsed_drag,
    c.overall_complexity,
    -- issue summary
    iss.issue_summary,
    iss.cause_summary,
    iss.mechanism_summary,
    iss.resolution_summary
FROM tickets t
LEFT JOIN ticket_metrics m        ON m.ticket_id = t.ticket_id
LEFT JOIN ticket_thread_rollups r ON r.ticket_id = t.ticket_id
LEFT JOIN vw_latest_ticket_sentiment s   ON s.ticket_id = t.ticket_id
LEFT JOIN vw_latest_ticket_priority p    ON p.ticket_id = t.ticket_id
LEFT JOIN vw_latest_ticket_complexity c  ON c.ticket_id = t.ticket_id
LEFT JOIN vw_latest_ticket_issue_summary iss ON iss.ticket_id = t.ticket_id
WHERE COALESCE(t.status, '') != 'Open'
  AND COALESCE(t.assignee, '') != 'Marketing'
  AND COALESCE(t.group_name, '') != 'Marketing';

-- ════════════════════════════════════════════════════════════════════
-- 2. vw_backlog_aging_current — aging distribution
-- ════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW vw_backlog_aging_current AS
SELECT
    CASE
        WHEN age_days <  7  THEN '0-6'
        WHEN age_days < 14  THEN '7-13'
        WHEN age_days < 30  THEN '14-29'
        WHEN age_days < 60  THEN '30-59'
        WHEN age_days < 90  THEN '60-89'
        ELSE '90+'
    END AS age_bucket,
    COUNT(*) AS ticket_count
FROM (
    SELECT
        EXTRACT(DAY FROM now() - t.date_created)::int AS age_days
    FROM tickets t
    WHERE t.date_created IS NOT NULL
      AND t.closed_at IS NULL
      AND COALESCE(t.status, '') NOT IN ('Closed', 'Resolved', 'Open')
      AND COALESCE(t.assignee, '') != 'Marketing'
      AND COALESCE(t.group_name, '') != 'Marketing'
) sub
GROUP BY 1
ORDER BY MIN(age_days);

-- ════════════════════════════════════════════════════════════════════
-- 3. vw_backlog_daily — daily open backlog (live from tickets)
-- ════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW vw_backlog_daily AS
SELECT
    d.snapshot_date,
    COALESCE(t_count.open_backlog, 0)      AS open_backlog,
    COALESCE(s.high_priority_backlog, 0)   AS high_priority_backlog,
    COALESCE(s.high_complexity_backlog, 0) AS high_complexity_backlog
FROM (SELECT DISTINCT snapshot_date FROM daily_open_counts) d
LEFT JOIN LATERAL (
    SELECT COUNT(*) AS open_backlog
    FROM tickets t
    WHERE t.date_created IS NOT NULL
      AND t.date_created::date <= d.snapshot_date
      AND COALESCE(t.status, '') != 'Open'
      AND COALESCE(t.assignee, '') != 'Marketing'
      AND COALESCE(t.group_name, '') != 'Marketing'
      AND (
          (t.closed_at IS NOT NULL AND t.closed_at::date > d.snapshot_date)
          OR
          (t.closed_at IS NULL AND COALESCE(t.status, '') NOT IN ('Closed', 'Resolved'))
      )
) t_count ON TRUE
LEFT JOIN (
    SELECT snapshot_date,
           COUNT(*) FILTER (WHERE open_flag AND high_priority_flag)  AS high_priority_backlog,
           COUNT(*) FILTER (WHERE open_flag AND high_complexity_flag) AS high_complexity_backlog
    FROM ticket_snapshots_daily
    GROUP BY snapshot_date
) s ON s.snapshot_date = d.snapshot_date
ORDER BY d.snapshot_date DESC;

-- ════════════════════════════════════════════════════════════════════
-- 4. vw_backlog_daily_by_severity — severity breakdown over time
-- ════════════════════════════════════════════════════════════════════
DROP VIEW IF EXISTS vw_backlog_daily_by_severity;

CREATE OR REPLACE VIEW vw_backlog_daily_by_severity AS
SELECT
    d.snapshot_date,
    CASE
        WHEN t.severity LIKE '1%' OR LOWER(t.severity) LIKE '%high%'
        THEN 'High'
        WHEN t.severity LIKE '3%' OR LOWER(t.severity) LIKE '%low%'
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
 AND (
     (t.closed_at IS NOT NULL AND t.closed_at::date > d.snapshot_date)
     OR
     (t.closed_at IS NULL AND COALESCE(t.status, '') NOT IN ('Closed', 'Resolved'))
 )
GROUP BY d.snapshot_date, 2
ORDER BY d.snapshot_date DESC, severity_tier;

-- ════════════════════════════════════════════════════════════════════
-- 5. vw_backlog_product_severity_powman — product/severity breakdown
-- ════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW vw_backlog_product_severity_powman AS
SELECT
    CASE
        WHEN LOWER(t.product_name) LIKE 'pm%'
          OR LOWER(t.product_name) LIKE '%power%'
        THEN 'PowerMan'
        ELSE COALESCE(NULLIF(t.product_name, ''), 'Unknown')
    END AS product_name,
    CASE
        WHEN t.severity LIKE '1%' OR LOWER(t.severity) LIKE '%high%'
        THEN 'High'
        WHEN t.severity LIKE '3%' OR LOWER(t.severity) LIKE '%low%'
        THEN 'Low'
        ELSE 'Medium'
    END AS severity_tier,
    COUNT(*) AS ticket_count
FROM tickets t
WHERE t.closed_at IS NULL
  AND COALESCE(t.status, '') != 'Open'
  AND COALESCE(t.assignee, '') != 'Marketing'
  AND COALESCE(t.group_name, '') != 'Marketing'
GROUP BY 1, 2
ORDER BY product_name, severity_tier;
