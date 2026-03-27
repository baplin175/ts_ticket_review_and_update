-- Migration 045 — Ticket exclusions: prevent specific tickets from being
-- scored by priority, sentiment (frustration), and/or complexity pipelines.
-- Idempotent: safe to re-run.

CREATE TABLE IF NOT EXISTS ticket_exclusions (
    ticket_id           BIGINT      PRIMARY KEY REFERENCES tickets (ticket_id),
    exclude_priority    BOOLEAN     NOT NULL DEFAULT FALSE,
    exclude_sentiment   BOOLEAN     NOT NULL DEFAULT FALSE,
    exclude_complexity  BOOLEAN     NOT NULL DEFAULT FALSE,
    reason              TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE ticket_exclusions IS
    'Per-ticket opt-out from AI scoring stages. Set the relevant flag to TRUE to suppress that stage for the ticket.';
