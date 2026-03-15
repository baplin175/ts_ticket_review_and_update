-- Migration 004: Denormalize ticket_number into all ticket_id-bearing tables
-- ════════════════════════════════════════════════════════════════════
-- Adds a nullable TEXT column ticket_number to every table that references
-- ticket_id (except tickets itself which already has it, and
-- ticket_snapshots_daily which already has it).
-- Then backfills from the tickets table.
-- Finally recreates one view that needs ticket_number in its SELECT.

SET search_path = tickets_ai, public;

-- ── 1. ADD COLUMN ───────────────────────────────────────────────────

ALTER TABLE ticket_actions           ADD COLUMN IF NOT EXISTS ticket_number TEXT;
ALTER TABLE ticket_thread_rollups    ADD COLUMN IF NOT EXISTS ticket_number TEXT;
ALTER TABLE ticket_metrics           ADD COLUMN IF NOT EXISTS ticket_number TEXT;
ALTER TABLE ticket_sentiment         ADD COLUMN IF NOT EXISTS ticket_number TEXT;
ALTER TABLE ticket_priority_scores   ADD COLUMN IF NOT EXISTS ticket_number TEXT;
ALTER TABLE ticket_complexity_scores ADD COLUMN IF NOT EXISTS ticket_number TEXT;
ALTER TABLE ticket_wait_states       ADD COLUMN IF NOT EXISTS ticket_number TEXT;
ALTER TABLE ticket_participants      ADD COLUMN IF NOT EXISTS ticket_number TEXT;
ALTER TABLE ticket_handoffs          ADD COLUMN IF NOT EXISTS ticket_number TEXT;
ALTER TABLE ticket_issue_summaries   ADD COLUMN IF NOT EXISTS ticket_number TEXT;
ALTER TABLE ticket_embeddings        ADD COLUMN IF NOT EXISTS ticket_number TEXT;
ALTER TABLE ticket_clusters          ADD COLUMN IF NOT EXISTS ticket_number TEXT;
ALTER TABLE ticket_interventions     ADD COLUMN IF NOT EXISTS ticket_number TEXT;

-- ── 2. BACKFILL ─────────────────────────────────────────────────────

UPDATE ticket_actions a
   SET ticket_number = t.ticket_number
  FROM tickets t
 WHERE t.ticket_id = a.ticket_id AND a.ticket_number IS NULL;

UPDATE ticket_thread_rollups r
   SET ticket_number = t.ticket_number
  FROM tickets t
 WHERE t.ticket_id = r.ticket_id AND r.ticket_number IS NULL;

UPDATE ticket_metrics m
   SET ticket_number = t.ticket_number
  FROM tickets t
 WHERE t.ticket_id = m.ticket_id AND m.ticket_number IS NULL;

UPDATE ticket_sentiment s
   SET ticket_number = t.ticket_number
  FROM tickets t
 WHERE t.ticket_id = s.ticket_id AND s.ticket_number IS NULL;

UPDATE ticket_priority_scores p
   SET ticket_number = t.ticket_number
  FROM tickets t
 WHERE t.ticket_id = p.ticket_id AND p.ticket_number IS NULL;

UPDATE ticket_complexity_scores c
   SET ticket_number = t.ticket_number
  FROM tickets t
 WHERE t.ticket_id = c.ticket_id AND c.ticket_number IS NULL;

UPDATE ticket_wait_states ws
   SET ticket_number = t.ticket_number
  FROM tickets t
 WHERE t.ticket_id = ws.ticket_id AND ws.ticket_number IS NULL;

UPDATE ticket_participants tp
   SET ticket_number = t.ticket_number
  FROM tickets t
 WHERE t.ticket_id = tp.ticket_id AND tp.ticket_number IS NULL;

UPDATE ticket_handoffs h
   SET ticket_number = t.ticket_number
  FROM tickets t
 WHERE t.ticket_id = h.ticket_id AND h.ticket_number IS NULL;

UPDATE ticket_issue_summaries iss
   SET ticket_number = t.ticket_number
  FROM tickets t
 WHERE t.ticket_id = iss.ticket_id AND iss.ticket_number IS NULL;

UPDATE ticket_embeddings e
   SET ticket_number = t.ticket_number
  FROM tickets t
 WHERE t.ticket_id = e.ticket_id AND e.ticket_number IS NULL;

UPDATE ticket_clusters tc
   SET ticket_number = t.ticket_number
  FROM tickets t
 WHERE t.ticket_id = tc.ticket_id AND tc.ticket_number IS NULL;

UPDATE ticket_interventions iv
   SET ticket_number = t.ticket_number
  FROM tickets t
 WHERE t.ticket_id = iv.ticket_id AND iv.ticket_number IS NULL;

-- ── 3. RECREATE VIEW: vw_ticket_wait_profile ────────────────────────
-- Add ticket_number to SELECT + GROUP BY.  Must DROP first because
-- CREATE OR REPLACE cannot add columns to an existing view.

DROP VIEW IF EXISTS vw_ticket_wait_profile;

CREATE VIEW vw_ticket_wait_profile AS
SELECT
    ws.ticket_id,
    ws.ticket_number,
    COALESCE(SUM(ws.duration_minutes) FILTER (WHERE ws.state_name = 'waiting_on_customer'), 0) AS waiting_on_customer_minutes,
    COALESCE(SUM(ws.duration_minutes) FILTER (WHERE ws.state_name = 'waiting_on_support'), 0)  AS waiting_on_support_minutes,
    COALESCE(SUM(ws.duration_minutes) FILTER (WHERE ws.state_name = 'waiting_on_dev'), 0)      AS waiting_on_dev_minutes,
    COALESCE(SUM(ws.duration_minutes) FILTER (WHERE ws.state_name = 'waiting_on_ps'), 0)       AS waiting_on_ps_minutes,
    COALESCE(SUM(ws.duration_minutes) FILTER (WHERE ws.state_name = 'active_work'), 0)         AS active_work_minutes,
    COALESCE(SUM(ws.duration_minutes), 0) AS total_profiled_minutes,
    CASE WHEN COALESCE(SUM(ws.duration_minutes), 0) > 0
         THEN ROUND(SUM(ws.duration_minutes) FILTER (WHERE ws.state_name = 'waiting_on_customer')
                     / SUM(ws.duration_minutes) * 100, 2)
         ELSE 0 END AS pct_waiting_on_customer,
    CASE WHEN COALESCE(SUM(ws.duration_minutes), 0) > 0
         THEN ROUND(SUM(ws.duration_minutes) FILTER (WHERE ws.state_name = 'active_work')
                     / SUM(ws.duration_minutes) * 100, 2)
         ELSE 0 END AS pct_active_work
FROM ticket_wait_states ws
GROUP BY ws.ticket_id, ws.ticket_number;
