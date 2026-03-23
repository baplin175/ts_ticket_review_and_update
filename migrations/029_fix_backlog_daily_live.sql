-- Migration 029 — Fix vw_backlog_daily to compute live from tickets table.
--
-- Problem: vw_backlog_daily reads from pre-computed daily_open_counts, but
-- vw_backlog_daily_by_severity computes live from the tickets table.  When
-- new historical tickets are imported (e.g. via CSV) the pre-computed counts
-- become stale, causing the severity stacked area to exceed the total line
-- in the Open Backlog Trend chart.
--
-- Fix: Rewrite vw_backlog_daily to compute live from the tickets table
-- using daily_open_counts only as a date spine, matching the approach
-- already used by vw_backlog_daily_by_severity.

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
