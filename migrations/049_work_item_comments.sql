-- ═══════════════════════════════════════════════════════════════════
-- 049 — Azure DevOps work_item_comments table
-- ═══════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS work_item_comments (
    work_item_id      INTEGER NOT NULL,
    comment_id        INTEGER NOT NULL,
    created_date      TIMESTAMPTZ,
    modified_date     TIMESTAMPTZ,
    created_by        TEXT,
    created_by_email  TEXT,
    text              TEXT,
    source_payload    JSONB,
    first_ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (work_item_id, comment_id)
);

CREATE INDEX IF NOT EXISTS idx_work_item_comments_work_item_id
    ON work_item_comments (work_item_id);
CREATE INDEX IF NOT EXISTS idx_work_item_comments_created_date
    ON work_item_comments (created_date DESC);
