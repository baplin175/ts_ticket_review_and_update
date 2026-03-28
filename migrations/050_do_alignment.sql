-- ═══════════════════════════════════════════════════════════════════
-- 050 — DO / ticket alignment enrichment results
-- ═══════════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS ticket_do_alignment (
    ticket_id       INTEGER NOT NULL,
    ticket_number   TEXT,
    do_number       TEXT,
    do_state        TEXT,
    aligned         TEXT,
    mismatch_label  TEXT,
    explanation     TEXT,
    model_name      TEXT,
    prompt_name     TEXT,
    prompt_version  TEXT,
    input_hash      TEXT,
    scored_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    raw_response    JSONB
);

CREATE INDEX IF NOT EXISTS idx_ticket_do_alignment_ticket_id
    ON ticket_do_alignment (ticket_id);
CREATE INDEX IF NOT EXISTS idx_ticket_do_alignment_scored_at
    ON ticket_do_alignment (scored_at DESC);
CREATE INDEX IF NOT EXISTS idx_ticket_do_alignment_mismatch_label
    ON ticket_do_alignment (mismatch_label);

CREATE OR REPLACE VIEW vw_latest_ticket_do_alignment AS
SELECT DISTINCT ON (ticket_id)
    ticket_id, ticket_number, do_number, do_state,
    aligned, mismatch_label, explanation,
    model_name, prompt_name, prompt_version,
    input_hash, scored_at
FROM ticket_do_alignment
ORDER BY ticket_id, scored_at DESC;
