-- ═══════════════════════════════════════════════════════════════════
-- 042 — Add do_number column to tickets (DevOps work item reference)
-- ═══════════════════════════════════════════════════════════════════

ALTER TABLE tickets ADD COLUMN IF NOT EXISTS do_number TEXT;

-- Backfill from source_payload
UPDATE tickets
SET do_number = source_payload->>'DO'
WHERE source_payload->>'DO' IS NOT NULL
  AND (do_number IS NULL OR do_number = '');

CREATE INDEX IF NOT EXISTS idx_tickets_do_number ON tickets (do_number);

-- ── Rebuild views to include do_number ─────────────────────────────

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
    t.group_name,
    t.date_created,
    t.date_modified,
    t.closed_at,
    t.days_opened,
    t.days_since_modified,
    t.do_number,
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
WHERE COALESCE(t.status, '') != 'Open'
  AND COALESCE(t.assignee, '') != 'Marketing'
  AND COALESCE(t.group_name, '') != 'Marketing';

-- Recreate dependent view dropped by CASCADE
CREATE OR REPLACE VIEW vw_operational_open_tickets AS
SELECT v.*
FROM vw_ticket_analytics_core v
WHERE v.closed_at IS NULL
  AND COALESCE(v.status, '') NOT IN ('Closed', 'Resolved');
