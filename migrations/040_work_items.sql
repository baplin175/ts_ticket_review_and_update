-- ═══════════════════════════════════════════════════════════════════
-- 040 — Azure DevOps work_items table
-- ═══════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS work_items (
    work_item_id        INTEGER PRIMARY KEY,
    project             TEXT NOT NULL,
    work_item_type      TEXT,
    title               TEXT,
    state               TEXT,
    reason              TEXT,
    assigned_to         TEXT,
    assigned_to_email   TEXT,
    area_path           TEXT,
    iteration_path      TEXT,
    priority            INTEGER,
    severity            TEXT,
    created_date        TIMESTAMPTZ,
    changed_date        TIMESTAMPTZ,
    state_change_date   TIMESTAMPTZ,
    activated_date      TIMESTAMPTZ,
    board_column        TEXT,
    tags                TEXT,
    description         TEXT,
    completed_work      DOUBLE PRECISION,
    remaining_work      DOUBLE PRECISION,
    original_estimate   DOUBLE PRECISION,
    value_area          TEXT,
    billable            BOOLEAN,
    work_type           TEXT,
    comment_count       INTEGER,
    rev                 INTEGER,
    source_payload      JSONB,
    first_ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_ingested_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_work_items_project ON work_items (project);
CREATE INDEX IF NOT EXISTS idx_work_items_state ON work_items (state);
CREATE INDEX IF NOT EXISTS idx_work_items_work_item_type ON work_items (work_item_type);
CREATE INDEX IF NOT EXISTS idx_work_items_assigned_to ON work_items (assigned_to);
CREATE INDEX IF NOT EXISTS idx_work_items_changed_date ON work_items (changed_date);
CREATE INDEX IF NOT EXISTS idx_work_items_iteration_path ON work_items (iteration_path);
