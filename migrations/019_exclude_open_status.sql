-- Migration 019 — Exclude status='Open' tickets everywhere.
-- Tickets with status='Open' are user test tickets and must be ignored
-- in all analytics, views, and backlog calculations.

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
WHERE COALESCE(t.status, '') != 'Open';

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
) sub
GROUP BY 1
ORDER BY MIN(age_days);

-- ════════════════════════════════════════════════════════════════════
-- 3. vw_backlog_daily_by_severity — severity breakdown over time
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
 AND (
     (t.closed_at IS NOT NULL AND t.closed_at::date > d.snapshot_date)
     OR
     (t.closed_at IS NULL AND COALESCE(t.status, '') NOT IN ('Closed', 'Resolved'))
 )
GROUP BY d.snapshot_date, 2
ORDER BY d.snapshot_date DESC, severity_tier;

-- ════════════════════════════════════════════════════════════════════
-- 4. vw_backlog_product_severity_powman — product/severity breakdown
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
GROUP BY 1, 2
ORDER BY product_name, severity_tier;
