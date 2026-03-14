-- Migration 004 — Analytics views for ticket intelligence reporting.
-- Idempotent: uses CREATE OR REPLACE VIEW throughout.
-- NOTE: Schema creation and search_path are set by db.py before this runs.

-- ════════════════════════════════════════════════════════════════════
-- 1. vw_latest_ticket_sentiment
--    Latest ticket_sentiment row per ticket_id by scored_at desc, id desc
-- ════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW vw_latest_ticket_sentiment AS
SELECT s.*
FROM ticket_sentiment s
INNER JOIN (
    SELECT ticket_id, MAX(id) AS max_id
    FROM ticket_sentiment
    WHERE (ticket_id, scored_at) IN (
        SELECT ticket_id, MAX(scored_at) FROM ticket_sentiment GROUP BY ticket_id
    )
    GROUP BY ticket_id
) latest ON s.id = latest.max_id;

-- ════════════════════════════════════════════════════════════════════
-- 2. vw_latest_ticket_priority
--    Latest ticket_priority_scores row per ticket_id by scored_at desc, id desc
-- ════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW vw_latest_ticket_priority AS
SELECT p.*
FROM ticket_priority_scores p
INNER JOIN (
    SELECT ticket_id, MAX(id) AS max_id
    FROM ticket_priority_scores
    WHERE (ticket_id, scored_at) IN (
        SELECT ticket_id, MAX(scored_at) FROM ticket_priority_scores GROUP BY ticket_id
    )
    GROUP BY ticket_id
) latest ON p.id = latest.max_id;

-- ════════════════════════════════════════════════════════════════════
-- 3. vw_latest_ticket_complexity
--    Latest ticket_complexity_scores row per ticket_id by scored_at desc, id desc
-- ════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW vw_latest_ticket_complexity AS
SELECT c.*
FROM ticket_complexity_scores c
INNER JOIN (
    SELECT ticket_id, MAX(id) AS max_id
    FROM ticket_complexity_scores
    WHERE (ticket_id, scored_at) IN (
        SELECT ticket_id, MAX(scored_at) FROM ticket_complexity_scores GROUP BY ticket_id
    )
    GROUP BY ticket_id
) latest ON c.id = latest.max_id;

-- ════════════════════════════════════════════════════════════════════
-- 4. vw_latest_ticket_issue_summary
--    Latest ticket_issue_summaries row per ticket_id by scored_at desc, id desc
-- ════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW vw_latest_ticket_issue_summary AS
SELECT s.*
FROM ticket_issue_summaries s
INNER JOIN (
    SELECT ticket_id, MAX(id) AS max_id
    FROM ticket_issue_summaries
    WHERE (ticket_id, scored_at) IN (
        SELECT ticket_id, MAX(scored_at) FROM ticket_issue_summaries GROUP BY ticket_id
    )
    GROUP BY ticket_id
) latest ON s.id = latest.max_id;

-- ════════════════════════════════════════════════════════════════════
-- 5. vw_ticket_analytics_core
--    Join tickets + metrics + rollups + latest enrichments
-- ════════════════════════════════════════════════════════════════════
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
    -- ticket_metrics
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
    -- ticket_thread_rollups
    r.latest_customer_text,
    r.latest_inhance_text,
    r.technical_core_text,
    r.summary_for_embedding,
    -- latest sentiment
    sen.frustrated,
    -- latest priority
    pri.priority,
    pri.priority_explanation,
    -- latest complexity
    cx.intrinsic_complexity,
    cx.coordination_load,
    cx.elapsed_drag,
    cx.overall_complexity,
    -- latest issue summary
    iss.issue_summary,
    iss.cause_summary,
    iss.mechanism_summary,
    iss.resolution_summary
FROM tickets t
LEFT JOIN ticket_metrics m ON m.ticket_id = t.ticket_id
LEFT JOIN ticket_thread_rollups r ON r.ticket_id = t.ticket_id
LEFT JOIN vw_latest_ticket_sentiment sen ON sen.ticket_id = t.ticket_id
LEFT JOIN vw_latest_ticket_priority pri ON pri.ticket_id = t.ticket_id
LEFT JOIN vw_latest_ticket_complexity cx ON cx.ticket_id = t.ticket_id
LEFT JOIN vw_latest_ticket_issue_summary iss ON iss.ticket_id = t.ticket_id;

-- ════════════════════════════════════════════════════════════════════
-- 6. vw_ticket_complexity_breakdown
--    Tickets joined with latest complexity scores
-- ════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW vw_ticket_complexity_breakdown AS
SELECT
    t.ticket_id,
    t.ticket_number,
    t.ticket_name,
    t.product_name,
    t.customer,
    cx.intrinsic_complexity,
    cx.coordination_load,
    cx.elapsed_drag,
    cx.overall_complexity,
    cx.confidence,
    cx.primary_complexity_drivers,
    cx.complexity_summary,
    cx.evidence,
    cx.noise_factors,
    cx.duration_vs_complexity_note,
    cx.scored_at
FROM tickets t
INNER JOIN vw_latest_ticket_complexity cx ON cx.ticket_id = t.ticket_id;

-- ════════════════════════════════════════════════════════════════════
-- 7. vw_ticket_wait_profile
--    Aggregate ticket_wait_states into minutes/percentages per state
-- ════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW vw_ticket_wait_profile AS
SELECT
    ticket_id,
    COALESCE(SUM(duration_minutes) FILTER (WHERE state_name = 'waiting_on_customer'), 0) AS waiting_on_customer_minutes,
    COALESCE(SUM(duration_minutes) FILTER (WHERE state_name = 'waiting_on_support'), 0)  AS waiting_on_support_minutes,
    COALESCE(SUM(duration_minutes) FILTER (WHERE state_name = 'waiting_on_dev'), 0)      AS waiting_on_dev_minutes,
    COALESCE(SUM(duration_minutes) FILTER (WHERE state_name = 'waiting_on_ps'), 0)       AS waiting_on_ps_minutes,
    COALESCE(SUM(duration_minutes) FILTER (WHERE state_name = 'active_work'), 0)         AS active_work_minutes,
    COALESCE(SUM(duration_minutes), 0)                                                    AS total_profiled_minutes,
    CASE WHEN COALESCE(SUM(duration_minutes), 0) > 0
         THEN ROUND(SUM(duration_minutes) FILTER (WHERE state_name = 'waiting_on_customer') * 100.0
                    / SUM(duration_minutes), 2)
         ELSE 0
    END AS pct_waiting_on_customer,
    CASE WHEN COALESCE(SUM(duration_minutes), 0) > 0
         THEN ROUND(SUM(duration_minutes) FILTER (WHERE state_name = 'active_work') * 100.0
                    / SUM(duration_minutes), 2)
         ELSE 0
    END AS pct_active_work
FROM ticket_wait_states
WHERE duration_minutes IS NOT NULL
GROUP BY ticket_id;

-- ════════════════════════════════════════════════════════════════════
-- 8. vw_customer_support_risk
--    90-day customer rollup using latest priority/complexity/sentiment
-- ════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW vw_customer_support_risk AS
SELECT
    t.customer,
    COUNT(*)                                                              AS ticket_count_90d,
    COUNT(*) FILTER (WHERE pri.priority <= 3)                             AS high_priority_count_90d,
    COUNT(*) FILTER (WHERE cx.overall_complexity >= 4)                    AS high_complexity_count_90d,
    ROUND(AVG(pri.priority), 2)                                          AS avg_priority_90d,
    ROUND(AVG(cx.overall_complexity), 2)                                  AS avg_complexity_90d,
    ROUND(AVG(cx.elapsed_drag), 2)                                        AS avg_elapsed_drag_90d,
    COUNT(*) FILTER (WHERE sen.frustrated = 'Yes')                        AS frustration_count_90d,
    (SELECT jsonb_agg(DISTINCT sub.cluster_label)
     FROM ticket_clusters sub
     WHERE sub.ticket_id = ANY(ARRAY_AGG(t.ticket_id))
       AND sub.cluster_label IS NOT NULL
    )                                                                     AS dominant_clusters
FROM tickets t
LEFT JOIN vw_latest_ticket_priority pri ON pri.ticket_id = t.ticket_id
LEFT JOIN vw_latest_ticket_complexity cx ON cx.ticket_id = t.ticket_id
LEFT JOIN vw_latest_ticket_sentiment sen ON sen.ticket_id = t.ticket_id
WHERE t.date_created >= (CURRENT_DATE - INTERVAL '90 days')
   OR (t.closed_at IS NULL)
GROUP BY t.customer
HAVING t.customer IS NOT NULL;

-- ════════════════════════════════════════════════════════════════════
-- 9. vw_product_pain_patterns
--    Group by product + latest cluster + mechanism
-- ════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW vw_product_pain_patterns AS
SELECT
    t.product_name,
    tc.cluster_id,
    tc.cluster_label,
    iss.mechanism_summary,
    COUNT(DISTINCT t.ticket_id)                     AS ticket_count,
    COUNT(DISTINCT t.customer)                       AS affected_customers,
    ROUND(AVG(cx.overall_complexity), 2)             AS avg_complexity,
    ROUND(AVG(cx.elapsed_drag), 2)                   AS avg_elapsed_drag
FROM tickets t
LEFT JOIN vw_latest_ticket_complexity cx ON cx.ticket_id = t.ticket_id
LEFT JOIN vw_latest_ticket_issue_summary iss ON iss.ticket_id = t.ticket_id
LEFT JOIN LATERAL (
    SELECT cluster_id, cluster_label
    FROM ticket_clusters
    WHERE ticket_id = t.ticket_id
    ORDER BY assigned_at DESC
    LIMIT 1
) tc ON TRUE
WHERE t.product_name IS NOT NULL
GROUP BY t.product_name, tc.cluster_id, tc.cluster_label, iss.mechanism_summary;

-- ════════════════════════════════════════════════════════════════════
-- 10. vw_intervention_opportunities
--     Group by intervention_type/target/product/cluster
-- ════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW vw_intervention_opportunities AS
SELECT
    ti.intervention_type,
    ti.intervention_target,
    t.product_name,
    tc.cluster_id,
    tc.cluster_label,
    COUNT(DISTINCT t.ticket_id)          AS affected_ticket_count,
    COUNT(DISTINCT t.customer)           AS affected_customer_count,
    ROUND(AVG(cx.overall_complexity), 2) AS avg_complexity,
    ROUND(AVG(cx.elapsed_drag), 2)       AS avg_elapsed_drag
FROM ticket_interventions ti
JOIN tickets t ON t.ticket_id = ti.ticket_id
LEFT JOIN vw_latest_ticket_complexity cx ON cx.ticket_id = t.ticket_id
LEFT JOIN LATERAL (
    SELECT cluster_id, cluster_label
    FROM ticket_clusters
    WHERE ticket_id = t.ticket_id
    ORDER BY assigned_at DESC
    LIMIT 1
) tc ON TRUE
GROUP BY ti.intervention_type, ti.intervention_target, t.product_name, tc.cluster_id, tc.cluster_label;

-- ════════════════════════════════════════════════════════════════════
-- 11. vw_backlog_daily
--     Daily backlog counts from ticket_snapshots_daily
-- ════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW vw_backlog_daily AS
SELECT
    snapshot_date,
    COUNT(*) FILTER (WHERE open_flag)                                     AS open_backlog,
    COUNT(*) FILTER (WHERE open_flag AND high_priority_flag)              AS high_priority_backlog,
    COUNT(*) FILTER (WHERE open_flag AND high_complexity_flag)            AS high_complexity_backlog
FROM ticket_snapshots_daily
GROUP BY snapshot_date
ORDER BY snapshot_date;

-- ════════════════════════════════════════════════════════════════════
-- 12. vw_backlog_weekly
--     Weekly aggregation from ticket_snapshots_daily
-- ════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW vw_backlog_weekly AS
SELECT
    DATE_TRUNC('week', snapshot_date)::DATE                              AS week_start,
    MAX(snapshot_date)                                                    AS latest_snapshot_in_week,
    ROUND(AVG(daily.open_backlog), 2)                                    AS avg_open_backlog_ratio,
    SUM(daily.open_backlog)                                              AS open_backlog_snapshot_rows,
    SUM(daily.high_priority_backlog)                                     AS high_priority_backlog_snapshot_rows,
    SUM(daily.high_complexity_backlog)                                   AS high_complexity_backlog_snapshot_rows
FROM (
    SELECT
        snapshot_date,
        COUNT(*) FILTER (WHERE open_flag)                                AS open_backlog,
        COUNT(*) FILTER (WHERE open_flag AND high_priority_flag)         AS high_priority_backlog,
        COUNT(*) FILTER (WHERE open_flag AND high_complexity_flag)       AS high_complexity_backlog
    FROM ticket_snapshots_daily
    GROUP BY snapshot_date
) daily
GROUP BY DATE_TRUNC('week', snapshot_date)
ORDER BY week_start;

-- ════════════════════════════════════════════════════════════════════
-- 13. vw_backlog_weekly_eow
--     End-of-week backlog using latest snapshot per ticket within each week
-- ════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW vw_backlog_weekly_eow AS
WITH weekly_latest AS (
    SELECT
        DATE_TRUNC('week', snapshot_date)::DATE AS week_start,
        ticket_id,
        open_flag,
        high_priority_flag,
        high_complexity_flag,
        ROW_NUMBER() OVER (
            PARTITION BY DATE_TRUNC('week', snapshot_date), ticket_id
            ORDER BY snapshot_date DESC
        ) AS rn
    FROM ticket_snapshots_daily
)
SELECT
    week_start,
    COUNT(*) FILTER (WHERE open_flag)                           AS open_backlog,
    COUNT(*) FILTER (WHERE open_flag AND high_priority_flag)    AS high_priority_backlog,
    COUNT(*) FILTER (WHERE open_flag AND high_complexity_flag)  AS high_complexity_backlog
FROM weekly_latest
WHERE rn = 1
GROUP BY week_start
ORDER BY week_start;

-- ════════════════════════════════════════════════════════════════════
-- 14. vw_backlog_aging_current
--     Age-bucket open tickets from the latest snapshot date
-- ════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW vw_backlog_aging_current AS
WITH latest_date AS (
    SELECT MAX(snapshot_date) AS max_date FROM ticket_snapshots_daily
)
SELECT
    CASE
        WHEN COALESCE(s.age_days, 0) BETWEEN  0 AND  6 THEN '0-6'
        WHEN COALESCE(s.age_days, 0) BETWEEN  7 AND 13 THEN '7-13'
        WHEN COALESCE(s.age_days, 0) BETWEEN 14 AND 29 THEN '14-29'
        WHEN COALESCE(s.age_days, 0) BETWEEN 30 AND 59 THEN '30-59'
        WHEN COALESCE(s.age_days, 0) BETWEEN 60 AND 89 THEN '60-89'
        ELSE '90+'
    END AS age_bucket,
    COUNT(*) AS ticket_count
FROM ticket_snapshots_daily s
CROSS JOIN latest_date ld
WHERE s.snapshot_date = ld.max_date
  AND s.open_flag
GROUP BY 1
ORDER BY MIN(COALESCE(s.age_days, 0));

-- ════════════════════════════════════════════════════════════════════
-- 15. vw_backlog_weekly_from_dates
--     Fallback weekly backlog approximation from tickets.date_created / closed_at
-- ════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW vw_backlog_weekly_from_dates AS
WITH week_series AS (
    SELECT generate_series(
        DATE_TRUNC('week', MIN(date_created))::DATE,
        CURRENT_DATE,
        '1 week'::INTERVAL
    )::DATE AS week_start
    FROM tickets
    WHERE date_created IS NOT NULL
)
SELECT
    ws.week_start,
    COUNT(*) FILTER (
        WHERE t.date_created <= (ws.week_start + INTERVAL '6 days')
          AND (t.closed_at IS NULL OR t.closed_at > (ws.week_start + INTERVAL '6 days'))
    ) AS open_backlog,
    COUNT(*) FILTER (
        WHERE t.date_created <= (ws.week_start + INTERVAL '6 days')
          AND (t.closed_at IS NULL OR t.closed_at > (ws.week_start + INTERVAL '6 days'))
          AND t.severity IN ('1 - Critical', '1 - Urgent', '2 - High Priority', '2 - High')
    ) AS high_priority_backlog,
    COUNT(*) FILTER (
        WHERE t.date_created <= (ws.week_start + INTERVAL '6 days')
          AND (t.closed_at IS NULL OR t.closed_at > (ws.week_start + INTERVAL '6 days'))
    ) AS total_tickets_in_window
FROM week_series ws
CROSS JOIN tickets t
WHERE t.date_created IS NOT NULL
GROUP BY ws.week_start
ORDER BY ws.week_start;
