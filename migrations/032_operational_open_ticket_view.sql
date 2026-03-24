-- Migration 032 — Add canonical current-state operational open-ticket view.
--
-- Purpose: centralize the most duplicated "current operational open ticket"
-- business rules so selectors and the web layer stop re-embedding them.
--
-- Scope: current-state only. Historical backlog/snapshot logic remains in the
-- snapshot/backlog views because it has different time-sliced semantics.

CREATE OR REPLACE VIEW vw_operational_open_tickets AS
SELECT
    v.*,
    COALESCE(t.group_name, '') AS group_name
FROM vw_ticket_analytics_core v
JOIN tickets t ON t.ticket_id = v.ticket_id
WHERE v.closed_at IS NULL
  AND COALESCE(v.status, '') NOT IN ('Closed', 'Resolved');
