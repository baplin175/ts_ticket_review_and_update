-- Backlog view with PM*/Power* products collapsed to "PowerMan"
CREATE OR REPLACE VIEW vw_backlog_daily_by_participant_type_product_powman AS
SELECT
    snapshot_date,
    participant_type,
    CASE
        WHEN LOWER(product_name) LIKE 'pm%'
          OR LOWER(product_name) LIKE '%power%'
        THEN 'PowerMan'
        ELSE product_name
    END AS product_name,
    SUM(open_count) AS open_backlog
FROM daily_open_counts
GROUP BY
    snapshot_date,
    participant_type,
    CASE
        WHEN LOWER(product_name) LIKE 'pm%'
          OR LOWER(product_name) LIKE '%power%'
        THEN 'PowerMan'
        ELSE product_name
    END
ORDER BY snapshot_date DESC, participant_type, product_name;
