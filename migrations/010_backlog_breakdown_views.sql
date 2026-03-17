-- Migration 010 — Additional backlog breakdown views from daily_open_counts.

-- Daily backlog grouped by participant_type
CREATE OR REPLACE VIEW vw_backlog_daily_by_participant_type AS
SELECT
    snapshot_date,
    participant_type,
    SUM(open_count) AS open_backlog
FROM daily_open_counts
GROUP BY snapshot_date, participant_type
ORDER BY snapshot_date, participant_type;

-- Daily backlog grouped by participant_type + product
CREATE OR REPLACE VIEW vw_backlog_daily_by_participant_type_product AS
SELECT
    snapshot_date,
    participant_type,
    product_name,
    SUM(open_count) AS open_backlog
FROM daily_open_counts
GROUP BY snapshot_date, participant_type, product_name
ORDER BY snapshot_date, participant_type, product_name;
