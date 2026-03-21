-- Migration 021 — Runtime-configurable dashboards, sections, and widgets.
-- V1 stores shared dashboards while preserving ownership fields for future
-- per-user or team-scoped dashboards.

CREATE TABLE IF NOT EXISTS dashboards (
    id              SERIAL        PRIMARY KEY,
    name            TEXT          NOT NULL,
    slug            TEXT          NOT NULL,
    description     TEXT,
    icon            TEXT,
    sort_order      INTEGER       NOT NULL DEFAULT 0,
    is_default      BOOLEAN       NOT NULL DEFAULT FALSE,
    is_active       BOOLEAN       NOT NULL DEFAULT TRUE,
    owner_type      TEXT          NOT NULL DEFAULT 'global',
    owner_id        TEXT,
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_dashboards_owner_slug
    ON dashboards (owner_type, COALESCE(owner_id, ''), slug);

CREATE INDEX IF NOT EXISTS idx_dashboards_active_sort
    ON dashboards (is_active, sort_order, name);

CREATE TABLE IF NOT EXISTS dashboard_sections (
    id              SERIAL        PRIMARY KEY,
    dashboard_id    INTEGER       NOT NULL REFERENCES dashboards(id) ON DELETE CASCADE,
    title           TEXT,
    description     TEXT,
    layout_columns  INTEGER       NOT NULL DEFAULT 1,
    sort_order      INTEGER       NOT NULL DEFAULT 0,
    is_active       BOOLEAN       NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dashboard_sections_dashboard_sort
    ON dashboard_sections (dashboard_id, is_active, sort_order);

CREATE TABLE IF NOT EXISTS dashboard_widgets (
    id                  SERIAL        PRIMARY KEY,
    section_id          INTEGER       NOT NULL REFERENCES dashboard_sections(id) ON DELETE CASCADE,
    widget_type         TEXT          NOT NULL,
    title               TEXT,
    query_key           TEXT,
    query_params_json   JSONB         NOT NULL DEFAULT '{}',
    display_config_json JSONB         NOT NULL DEFAULT '{}',
    sort_order          INTEGER       NOT NULL DEFAULT 0,
    is_active           BOOLEAN       NOT NULL DEFAULT TRUE,
    created_at          TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dashboard_widgets_section_sort
    ON dashboard_widgets (section_id, is_active, sort_order);
