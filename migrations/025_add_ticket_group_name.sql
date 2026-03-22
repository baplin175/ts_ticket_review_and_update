-- Migration 025 — add ticket group_name to tickets.

ALTER TABLE tickets
    ADD COLUMN IF NOT EXISTS group_name TEXT;

CREATE INDEX IF NOT EXISTS idx_tickets_group_name
    ON tickets (group_name);
