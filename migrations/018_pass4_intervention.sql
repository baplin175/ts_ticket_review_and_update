-- Migration 018 — Pass 4: intervention mapping columns + analytics views.
-- Adds mechanism_class, intervention_type, intervention_action to the
-- generalized ticket_llm_pass_results table, plus views for Pass 4
-- analytics and an updated full-pipeline view.
-- Idempotent: uses ADD COLUMN IF NOT EXISTS and CREATE OR REPLACE VIEW.

-- ════════════════════════════════════════════════════════════════════
-- 1. Add Pass 4 output columns to ticket_llm_pass_results
-- ════════════════════════════════════════════════════════════════════
ALTER TABLE ticket_llm_pass_results ADD COLUMN IF NOT EXISTS mechanism_class TEXT;
ALTER TABLE ticket_llm_pass_results ADD COLUMN IF NOT EXISTS intervention_type TEXT;
ALTER TABLE ticket_llm_pass_results ADD COLUMN IF NOT EXISTS intervention_action TEXT;

-- ════════════════════════════════════════════════════════════════════
-- 2. View: vw_ticket_pass4_results — easy analytics for Pass 4
-- ════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW vw_ticket_pass4_results AS
SELECT
    t.ticket_id,
    t.ticket_number,
    t.product_name,
    p3.mechanism,
    p4.mechanism_class,
    p4.intervention_type,
    p4.intervention_action,
    p4.p4_status       AS pass4_status,
    p4.p4_error        AS pass4_error,
    p4.p4_model_name   AS model_name,
    p4.p4_completed_at AS pass4_completed_at
FROM tickets t
LEFT JOIN LATERAL (
    SELECT lp.mechanism
    FROM ticket_llm_pass_results lp
    WHERE lp.ticket_id = t.ticket_id
      AND lp.pass_name = 'pass3_mechanism'
    ORDER BY
        CASE WHEN lp.status = 'success' THEN 0 ELSE 1 END,
        lp.updated_at DESC
    LIMIT 1
) p3 ON TRUE
LEFT JOIN LATERAL (
    SELECT lp.mechanism_class,
           lp.intervention_type,
           lp.intervention_action,
           lp.status         AS p4_status,
           lp.error_message  AS p4_error,
           lp.model_name     AS p4_model_name,
           lp.completed_at   AS p4_completed_at
    FROM ticket_llm_pass_results lp
    WHERE lp.ticket_id = t.ticket_id
      AND lp.pass_name = 'pass4_intervention'
    ORDER BY
        CASE WHEN lp.status = 'success' THEN 0 ELSE 1 END,
        lp.updated_at DESC
    LIMIT 1
) p4 ON TRUE;

-- ════════════════════════════════════════════════════════════════════
-- 3. View: vw_intervention_roi — mechanism class and intervention
--    type counts for engineering ROI analysis
-- ════════════════════════════════════════════════════════════════════
CREATE OR REPLACE VIEW vw_intervention_roi AS
SELECT
    p4.mechanism_class,
    p4.intervention_type,
    COUNT(*)            AS ticket_count,
    MODE() WITHIN GROUP (ORDER BY p4.intervention_action) AS representative_action
FROM ticket_llm_pass_results p4
WHERE p4.pass_name = 'pass4_intervention'
  AND p4.status = 'success'
  AND p4.mechanism_class IS NOT NULL
GROUP BY p4.mechanism_class, p4.intervention_type
ORDER BY ticket_count DESC;

-- ════════════════════════════════════════════════════════════════════
-- 4. Replace vw_ticket_pass_pipeline to include Pass 4
-- ════════════════════════════════════════════════════════════════════
DROP VIEW IF EXISTS vw_ticket_pass_pipeline;
CREATE VIEW vw_ticket_pass_pipeline AS
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
    p3.mechanism,
    p3.p3_status       AS pass3_status,
    p3.p3_completed_at AS pass3_completed_at,
    p4.mechanism_class,
    p4.intervention_type,
    p4.intervention_action,
    p4.p4_status       AS pass4_status,
    p4.p4_completed_at AS pass4_completed_at,
    COALESCE(p4.p4_error, p3.p3_error, p2.p2_error, p1.p1_error) AS latest_error
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
) p2 ON TRUE
LEFT JOIN LATERAL (
    SELECT lp.mechanism,
           lp.status        AS p3_status,
           lp.error_message AS p3_error,
           lp.completed_at  AS p3_completed_at
    FROM ticket_llm_pass_results lp
    WHERE lp.ticket_id = t.ticket_id
      AND lp.pass_name = 'pass3_mechanism'
    ORDER BY
        CASE WHEN lp.status = 'success' THEN 0 ELSE 1 END,
        lp.updated_at DESC
    LIMIT 1
) p3 ON TRUE
LEFT JOIN LATERAL (
    SELECT lp.mechanism_class,
           lp.intervention_type,
           lp.intervention_action,
           lp.status        AS p4_status,
           lp.error_message AS p4_error,
           lp.completed_at  AS p4_completed_at
    FROM ticket_llm_pass_results lp
    WHERE lp.ticket_id = t.ticket_id
      AND lp.pass_name = 'pass4_intervention'
    ORDER BY
        CASE WHEN lp.status = 'success' THEN 0 ELSE 1 END,
        lp.updated_at DESC
    LIMIT 1
) p4 ON TRUE;
