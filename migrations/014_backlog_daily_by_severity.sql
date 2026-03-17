-- Migration 014 — Daily backlog broken down by severity tier.
-- Computes per-day severity counts from tickets + daily_open_counts date spine.
-- Uses the same High / Medium / Low tiering as vw_backlog_product_severity_powman.

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
 AND (
     (t.closed_at IS NOT NULL AND t.closed_at::date > d.snapshot_date)
     OR
     (t.closed_at IS NULL AND COALESCE(t.status, '') NOT IN ('Closed', 'Resolved'))
 )
GROUP BY d.snapshot_date, 2
ORDER BY d.snapshot_date DESC, severity_tier;
