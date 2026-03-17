-- Migration 008 — Rewrite backlog views to source open counts from daily_open_counts.
-- HP/HC counts remain sourced from ticket_snapshots_daily (enrichment-dependent).
-- vw_backlog_aging_current stays on ticket_snapshots_daily (needs age_days).

-- Drop views that need column-type changes before recreating
DROP VIEW IF EXISTS vw_backlog_weekly_eow;
DROP VIEW IF EXISTS vw_backlog_weekly;
DROP VIEW IF EXISTS vw_backlog_weekly_from_dates;

-- ════════════════════════════════════════════════════════════════════
-- 1. vw_backlog_daily
-- ════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW vw_backlog_daily AS
SELECT
    d.snapshot_date,
    d.open_backlog,
    COALESCE(s.high_priority_backlog, 0)   AS high_priority_backlog,
    COALESCE(s.high_complexity_backlog, 0) AS high_complexity_backlog
FROM (
    SELECT snapshot_date,
           SUM(open_count) AS open_backlog
    FROM daily_open_counts
    GROUP BY snapshot_date
) d
LEFT JOIN (
    SELECT snapshot_date,
           COUNT(*) FILTER (WHERE open_flag AND high_priority_flag)  AS high_priority_backlog,
           COUNT(*) FILTER (WHERE open_flag AND high_complexity_flag) AS high_complexity_backlog
    FROM ticket_snapshots_daily
    GROUP BY snapshot_date
) s ON s.snapshot_date = d.snapshot_date
ORDER BY d.snapshot_date;

-- ════════════════════════════════════════════════════════════════════
-- 2. vw_backlog_weekly
-- ════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW vw_backlog_weekly AS
SELECT
    date_trunc('week', d.snapshot_date)::date                          AS week_start,
    MAX(d.snapshot_date)                                               AS latest_snapshot_in_week,
    ROUND(AVG(d.open_backlog)::numeric, 2)                             AS avg_open_backlog_ratio,
    SUM(d.open_backlog)                                                AS open_backlog_snapshot_rows,
    SUM(COALESCE(s.hp_ct, 0))                                         AS high_priority_backlog_snapshot_rows,
    SUM(COALESCE(s.hc_ct, 0))                                         AS high_complexity_backlog_snapshot_rows
FROM (
    SELECT snapshot_date,
           SUM(open_count) AS open_backlog
    FROM daily_open_counts
    GROUP BY snapshot_date
) d
LEFT JOIN (
    SELECT snapshot_date,
           COUNT(*) FILTER (WHERE open_flag AND high_priority_flag)  AS hp_ct,
           COUNT(*) FILTER (WHERE open_flag AND high_complexity_flag) AS hc_ct
    FROM ticket_snapshots_daily
    GROUP BY snapshot_date
) s ON s.snapshot_date = d.snapshot_date
GROUP BY date_trunc('week', d.snapshot_date)::date
ORDER BY week_start;

-- ════════════════════════════════════════════════════════════════════
-- 3. vw_backlog_weekly_eow
-- ════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW vw_backlog_weekly_eow AS
SELECT
    d.week_start,
    d.open_backlog,
    COALESCE(s.high_priority_backlog, 0)  AS high_priority_backlog,
    COALESCE(s.high_complexity_backlog, 0) AS high_complexity_backlog
FROM (
    -- End-of-week: use the latest snapshot_date per week from daily_open_counts
    SELECT DISTINCT ON (date_trunc('week', snapshot_date))
           date_trunc('week', snapshot_date)::date AS week_start,
           snapshot_date,
           SUM(open_count) OVER (PARTITION BY snapshot_date) AS open_backlog
    FROM daily_open_counts
    ORDER BY date_trunc('week', snapshot_date), snapshot_date DESC
) d
LEFT JOIN (
    SELECT DISTINCT ON (date_trunc('week', snapshot_date))
           date_trunc('week', snapshot_date)::date AS week_start,
           COUNT(*) FILTER (WHERE open_flag AND high_priority_flag)  AS high_priority_backlog,
           COUNT(*) FILTER (WHERE open_flag AND high_complexity_flag) AS high_complexity_backlog
    FROM ticket_snapshots_daily
    GROUP BY snapshot_date
    ORDER BY date_trunc('week', snapshot_date), snapshot_date DESC
) s ON s.week_start = d.week_start
ORDER BY d.week_start;

-- ════════════════════════════════════════════════════════════════════
-- 4. vw_backlog_aging_current  (unchanged — needs age_days from snapshots)
-- ════════════════════════════════════════════════════════════════════
-- No change needed; stays on ticket_snapshots_daily.

-- ════════════════════════════════════════════════════════════════════
-- 5. vw_backlog_weekly_from_dates — rewritten to use daily_open_counts
-- ════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW vw_backlog_weekly_from_dates AS
SELECT
    date_trunc('week', snapshot_date)::date AS week_start,
    SUM(open_count) AS open_backlog
FROM daily_open_counts
WHERE snapshot_date IN (
    -- Pick the latest date per week that exists in the table
    SELECT DISTINCT ON (date_trunc('week', snapshot_date))
           snapshot_date
    FROM daily_open_counts
    ORDER BY date_trunc('week', snapshot_date), snapshot_date DESC
)
GROUP BY date_trunc('week', snapshot_date)::date
ORDER BY week_start;
