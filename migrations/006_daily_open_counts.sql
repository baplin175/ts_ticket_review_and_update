-- Migration 006 — daily_open_counts: aggregated daily counts of open tickets
-- broken down by product, status, and assigned_to.
-- Idempotent: uses IF NOT EXISTS throughout.

CREATE TABLE IF NOT EXISTS daily_open_counts (
    snapshot_date   DATE    NOT NULL,
    product_name    TEXT    NOT NULL DEFAULT '',
    status          TEXT    NOT NULL DEFAULT '',
    assigned_to     TEXT    NOT NULL DEFAULT '',
    open_count      INT    NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (snapshot_date, product_name, status, assigned_to)
);

CREATE INDEX IF NOT EXISTS idx_daily_open_counts_date
    ON daily_open_counts (snapshot_date);
CREATE INDEX IF NOT EXISTS idx_daily_open_counts_product
    ON daily_open_counts (snapshot_date, product_name);
