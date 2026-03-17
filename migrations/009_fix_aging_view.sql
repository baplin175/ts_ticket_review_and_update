-- Migration 009 — Rewrite vw_backlog_aging_current to use tickets table directly
-- with status-aware open-ticket logic (consistent with daily_open_counts).

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
      AND COALESCE(t.status, '') NOT IN ('Closed', 'Resolved')
) sub
GROUP BY 1
ORDER BY MIN(age_days);
