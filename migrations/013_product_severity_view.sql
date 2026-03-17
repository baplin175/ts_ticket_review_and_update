-- Migration 013 — Add severity-aware product backlog view for the web dashboard.
-- Groups PM*/Power* products as "PowerMan", breaks down by severity tier.

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
GROUP BY 1, 2
ORDER BY product_name, severity_tier;
