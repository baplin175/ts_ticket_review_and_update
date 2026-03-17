-- Migration 015 — Pass 2: canonical failure grammar columns + analytics views.
-- Adds component/operation/unexpected_state/canonical_failure to the
-- generalized ticket_llm_pass_results table, plus views for Pass 2 analytics.
-- Idempotent: uses IF NOT EXISTS / ADD COLUMN IF NOT EXISTS throughout.

-- ════════════════════════════════════════════════════════════════════
-- 1. Add Pass 2 output columns to ticket_llm_pass_results
-- ════════════════════════════════════════════════════════════════════
ALTER TABLE ticket_llm_pass_results ADD COLUMN IF NOT EXISTS component         TEXT;
ALTER TABLE ticket_llm_pass_results ADD COLUMN IF NOT EXISTS operation         TEXT;
ALTER TABLE ticket_llm_pass_results ADD COLUMN IF NOT EXISTS unexpected_state  TEXT;
ALTER TABLE ticket_llm_pass_results ADD COLUMN IF NOT EXISTS canonical_failure TEXT;

-- ════════════════════════════════════════════════════════════════════
-- 2. View: vw_ticket_pass2_results — easy analytics for Pass 2
-- ════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW vw_ticket_pass2_results AS
SELECT
    t.ticket_id,
    r.full_thread_text,
    p1.phenomenon,
    p2.component,
    p2.operation,
    p2.unexpected_state,
    p2.canonical_failure,
    p1.p1_status         AS pass1_status,
    p2.p2_status         AS pass2_status,
    COALESCE(p2.p2_error, p1.p1_error) AS latest_error,
    p1.p1_prompt_version AS pass1_prompt_version,
    p2.p2_prompt_version AS pass2_prompt_version,
    p2.p2_model_name     AS model_name,
    p2.p2_completed_at   AS pass2_completed_at
FROM tickets t
LEFT JOIN ticket_thread_rollups r ON r.ticket_id = t.ticket_id
LEFT JOIN LATERAL (
    SELECT lp.phenomenon,
           lp.status         AS p1_status,
           lp.error_message  AS p1_error,
           lp.prompt_version AS p1_prompt_version
    FROM ticket_llm_pass_results lp
    WHERE lp.ticket_id = t.ticket_id
      AND lp.pass_name = 'pass1_phenomenon'
    ORDER BY
        CASE WHEN lp.status = 'success' THEN 0 ELSE 1 END,
        lp.updated_at DESC
    LIMIT 1
) p1 ON TRUE
LEFT JOIN LATERAL (
    SELECT lp.component,
           lp.operation,
           lp.unexpected_state,
           lp.canonical_failure,
           lp.status         AS p2_status,
           lp.error_message  AS p2_error,
           lp.prompt_version AS p2_prompt_version,
           lp.model_name     AS p2_model_name,
           lp.completed_at   AS p2_completed_at
    FROM ticket_llm_pass_results lp
    WHERE lp.ticket_id = t.ticket_id
      AND lp.pass_name = 'pass2_grammar'
    ORDER BY
        CASE WHEN lp.status = 'success' THEN 0 ELSE 1 END,
        lp.updated_at DESC
    LIMIT 1
) p2 ON TRUE;

-- ════════════════════════════════════════════════════════════════════
-- 3. View: vw_ticket_pass_pipeline — full pipeline status at a glance
-- ════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW vw_ticket_pass_pipeline AS
SELECT
    t.ticket_id,
    t.ticket_number,
    t.status           AS ticket_status,
    t.product_name,
    p1.phenomenon,
    p1.p1_status       AS pass1_status,
    p1.p1_completed_at AS pass1_completed_at,
    p2.component,
    p2.operation,
    p2.unexpected_state,
    p2.canonical_failure,
    p2.p2_status       AS pass2_status,
    p2.p2_completed_at AS pass2_completed_at,
    COALESCE(p2.p2_error, p1.p1_error) AS latest_error
FROM tickets t
LEFT JOIN LATERAL (
    SELECT lp.phenomenon,
           lp.status        AS p1_status,
           lp.error_message AS p1_error,
           lp.completed_at  AS p1_completed_at
    FROM ticket_llm_pass_results lp
    WHERE lp.ticket_id = t.ticket_id
      AND lp.pass_name = 'pass1_phenomenon'
    ORDER BY
        CASE WHEN lp.status = 'success' THEN 0 ELSE 1 END,
        lp.updated_at DESC
    LIMIT 1
) p1 ON TRUE
LEFT JOIN LATERAL (
    SELECT lp.component,
           lp.operation,
           lp.unexpected_state,
           lp.canonical_failure,
           lp.status        AS p2_status,
           lp.error_message AS p2_error,
           lp.completed_at  AS p2_completed_at
    FROM ticket_llm_pass_results lp
    WHERE lp.ticket_id = t.ticket_id
      AND lp.pass_name = 'pass2_grammar'
    ORDER BY
        CASE WHEN lp.status = 'success' THEN 0 ELSE 1 END,
        lp.updated_at DESC
    LIMIT 1
) p2 ON TRUE;
