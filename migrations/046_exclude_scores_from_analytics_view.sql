-- Migration 046 — Respect ticket_exclusions in vw_ticket_analytics_core.
--
-- When a ticket has exclude_sentiment / exclude_priority / exclude_complexity
-- set to TRUE in the ticket_exclusions table, the corresponding score fields
-- are returned as NULL so the ticket does not contribute to health rollups,
-- distress scores, or any other derived metrics for that stage.
--
-- Uses CREATE OR REPLACE (no CASCADE needed) since column names/types are
-- unchanged — only the expressions for the scored fields differ.

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
    t.group_name,
    t.date_created,
    t.date_modified,
    t.closed_at,
    t.days_opened,
    t.days_since_modified,
    t.do_number,
    wi.state AS do_status,
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
    -- sentiment (NULLed when ticket is excluded from sentiment scoring)
    CASE WHEN xe.exclude_sentiment THEN NULL ELSE s.frustrated          END AS frustrated,
    CASE WHEN xe.exclude_sentiment THEN NULL ELSE s.frustrated_reason   END AS frustrated_reason,
    -- priority (NULLed when ticket is excluded from priority scoring)
    CASE WHEN xe.exclude_priority  THEN NULL ELSE p.priority            END AS priority,
    CASE WHEN xe.exclude_priority  THEN NULL ELSE p.priority_explanation END AS priority_explanation,
    -- complexity (NULLed when ticket is excluded from complexity scoring)
    CASE WHEN xe.exclude_complexity THEN NULL ELSE c.intrinsic_complexity   END AS intrinsic_complexity,
    CASE WHEN xe.exclude_complexity THEN NULL ELSE c.coordination_load      END AS coordination_load,
    CASE WHEN xe.exclude_complexity THEN NULL ELSE c.elapsed_drag           END AS elapsed_drag,
    CASE WHEN xe.exclude_complexity THEN NULL ELSE c.overall_complexity     END AS overall_complexity,
    -- issue summary
    iss.issue_summary,
    iss.cause_summary,
    iss.mechanism_summary,
    iss.resolution_summary
FROM tickets t
LEFT JOIN work_items wi                  ON wi.work_item_id = t.do_number::integer
LEFT JOIN ticket_metrics m               ON m.ticket_id = t.ticket_id
LEFT JOIN ticket_thread_rollups r        ON r.ticket_id = t.ticket_id
LEFT JOIN vw_latest_ticket_sentiment s   ON s.ticket_id = t.ticket_id
LEFT JOIN vw_latest_ticket_priority p    ON p.ticket_id = t.ticket_id
LEFT JOIN vw_latest_ticket_complexity c  ON c.ticket_id = t.ticket_id
LEFT JOIN vw_latest_ticket_issue_summary iss ON iss.ticket_id = t.ticket_id
LEFT JOIN ticket_exclusions xe           ON xe.ticket_id = t.ticket_id
WHERE COALESCE(t.status, '') != 'Open'
  AND COALESCE(t.assignee, '') != 'Marketing'
  AND COALESCE(t.group_name, '') != 'Marketing';
