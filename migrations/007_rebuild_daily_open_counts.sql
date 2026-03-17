-- Migration 007 — Rebuild daily_open_counts to track by participant assignment.
-- The assigned participant on a given day is the one with the most recent
-- last_seen_at on or before that date (from ticket_participants).

DROP TABLE IF EXISTS daily_open_counts;

CREATE TABLE daily_open_counts (
    snapshot_date    DATE    NOT NULL,
    product_name     TEXT    NOT NULL DEFAULT '',
    status           TEXT    NOT NULL DEFAULT '',
    participant_id   TEXT    NOT NULL DEFAULT '',
    participant_name TEXT    NOT NULL DEFAULT '',
    participant_type TEXT    NOT NULL DEFAULT '',
    open_count       INT    NOT NULL DEFAULT 0,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (snapshot_date, product_name, status, participant_id)
);

CREATE INDEX IF NOT EXISTS idx_daily_open_counts_date
    ON daily_open_counts (snapshot_date);
CREATE INDEX IF NOT EXISTS idx_daily_open_counts_product
    ON daily_open_counts (snapshot_date, product_name);
CREATE INDEX IF NOT EXISTS idx_daily_open_counts_participant
    ON daily_open_counts (snapshot_date, participant_id);
