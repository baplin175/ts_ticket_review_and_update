-- ═══════════════════════════════════════════════════════════════════
-- 041 — Azure DevOps work_item_updates (field-level change history)
-- ═══════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS work_item_updates (
    work_item_id        INTEGER NOT NULL,
    update_id           INTEGER NOT NULL,
    rev                 INTEGER,
    revised_by          TEXT,
    revised_by_email    TEXT,
    revised_date        TIMESTAMPTZ,
    field_changes       JSONB,
    relations_added     JSONB,
    relations_removed   JSONB,
    source_payload      JSONB,
    first_ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_ingested_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (work_item_id, update_id)
);

CREATE INDEX IF NOT EXISTS idx_work_item_updates_revised_date
    ON work_item_updates (revised_date);
CREATE INDEX IF NOT EXISTS idx_work_item_updates_revised_by
    ON work_item_updates (revised_by);
