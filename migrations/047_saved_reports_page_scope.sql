-- Migration 047 — Scope saved_reports by page.
--
-- Adds a `page` column so the Tickets and Health Dashboards pages each
-- maintain an independent set of saved filter reports.  Existing rows
-- (all previously created for the Tickets page) default to 'tickets'.
--
-- The unique constraint was previously on (name) alone; it is now on
-- (name, page) so the same report name can appear on different pages.

ALTER TABLE saved_reports
    ADD COLUMN IF NOT EXISTS page TEXT NOT NULL DEFAULT 'tickets';

DROP INDEX IF EXISTS idx_saved_reports_name;

CREATE UNIQUE INDEX IF NOT EXISTS idx_saved_reports_name_page
    ON saved_reports (name, page);
