-- Migration 005 — LLM multi-pass pipeline results table.
-- Stores raw + parsed results for each LLM pass (Pass 1, Pass 2, …).
-- Idempotent: uses IF NOT EXISTS throughout.

-- ════════════════════════════════════════════════════════════════════
-- 1. ticket_llm_pass_results — stage results for each LLM pass
-- ════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS ticket_llm_pass_results (
    id                  BIGSERIAL       PRIMARY KEY,
    ticket_id           BIGINT          NOT NULL REFERENCES tickets (ticket_id),
    pass_name           TEXT            NOT NULL,          -- e.g. pass1_phenomenon
    input_text          TEXT,
    prompt_version      TEXT            NOT NULL,
    model_name          TEXT,
    raw_response_text   TEXT,
    parsed_json         JSONB,
    phenomenon          TEXT,                              -- projected column for pass1
    status              TEXT            NOT NULL DEFAULT 'pending',  -- pending / success / failed
    error_message       TEXT,
    started_at          TIMESTAMPTZ,
    completed_at        TIMESTAMPTZ,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT now()
);

-- Unique constraint: one successful result per ticket + pass_name + prompt_version.
-- A partial index on status = 'success' enforces at most one success row.
CREATE UNIQUE INDEX IF NOT EXISTS idx_llm_pass_unique_success
    ON ticket_llm_pass_results (ticket_id, pass_name, prompt_version)
    WHERE status = 'success';

-- Fast lookup for pending / failed tickets
CREATE INDEX IF NOT EXISTS idx_llm_pass_ticket_pass
    ON ticket_llm_pass_results (ticket_id, pass_name);

CREATE INDEX IF NOT EXISTS idx_llm_pass_status
    ON ticket_llm_pass_results (status);

-- ════════════════════════════════════════════════════════════════════
-- 2. View: vw_ticket_pass1_results — easy analytics query path
-- ════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW vw_ticket_pass1_results AS
SELECT
    t.ticket_id,
    r.full_thread_text,
    p.phenomenon,
    p.status            AS pass1_status,
    p.error_message     AS latest_error,
    p.prompt_version,
    p.model_name,
    p.completed_at      AS pass1_completed_at
FROM tickets t
LEFT JOIN ticket_thread_rollups r ON r.ticket_id = t.ticket_id
LEFT JOIN LATERAL (
    SELECT *
    FROM ticket_llm_pass_results lp
    WHERE lp.ticket_id = t.ticket_id
      AND lp.pass_name = 'pass1_phenomenon'
    ORDER BY
        CASE WHEN lp.status = 'success' THEN 0 ELSE 1 END,
        lp.updated_at DESC
    LIMIT 1
) p ON TRUE;
