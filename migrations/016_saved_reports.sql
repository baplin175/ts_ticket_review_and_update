-- Migration 016 — Saved filter reports for the ticket explorer.
-- Stores named filter presets that users can save and recall.

CREATE TABLE IF NOT EXISTS saved_reports (
    id              SERIAL        PRIMARY KEY,
    name            TEXT          NOT NULL,
    filter_model    JSONB         NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_saved_reports_name
    ON saved_reports (name);
