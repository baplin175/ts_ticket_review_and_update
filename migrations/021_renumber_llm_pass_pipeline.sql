-- Migration 021 — Renumber active LLM passes after grammar extraction moved into Pass 1.
-- Active pipeline becomes:
--   Pass 1 = phenomenon + grammar
--   Pass 2 = mechanism inference
--   Pass 3 = intervention mapping

UPDATE ticket_llm_pass_results
   SET pass_name = 'pass2_mechanism'
 WHERE pass_name = 'pass3_mechanism';

UPDATE ticket_llm_pass_results
   SET pass_name = 'pass3_intervention'
 WHERE pass_name = 'pass4_intervention';

DROP VIEW IF EXISTS vw_ticket_pass_pipeline;
DROP VIEW IF EXISTS vw_ticket_pass3_results;
DROP VIEW IF EXISTS vw_ticket_pass2_results;
DROP VIEW IF EXISTS vw_ticket_pass4_results;

CREATE VIEW vw_ticket_pass2_results AS
SELECT
    t.ticket_id,
    r.full_thread_text,
    p1.phenomenon,
    p1.component,
    p1.operation,
    p1.unexpected_state,
    p1.canonical_failure,
    p2.mechanism,
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
           lp.component,
           lp.operation,
           lp.unexpected_state,
           lp.canonical_failure,
           lp.status         AS p1_status,
           lp.error_message  AS p1_error,
           lp.prompt_version AS p1_prompt_version
    FROM ticket_llm_pass_results lp
    WHERE lp.ticket_id = t.ticket_id
      AND lp.pass_name = 'pass1_phenomenon'
    ORDER BY CASE WHEN lp.status = 'success' THEN 0 ELSE 1 END, lp.updated_at DESC
    LIMIT 1
) p1 ON TRUE
LEFT JOIN LATERAL (
    SELECT lp.mechanism,
           lp.status         AS p2_status,
           lp.error_message  AS p2_error,
           lp.prompt_version AS p2_prompt_version,
           lp.model_name     AS p2_model_name,
           lp.completed_at   AS p2_completed_at
    FROM ticket_llm_pass_results lp
    WHERE lp.ticket_id = t.ticket_id
      AND lp.pass_name = 'pass2_mechanism'
    ORDER BY CASE WHEN lp.status = 'success' THEN 0 ELSE 1 END, lp.updated_at DESC
    LIMIT 1
) p2 ON TRUE;

CREATE VIEW vw_ticket_pass3_results AS
SELECT
    t.ticket_id,
    t.ticket_number,
    t.product_name,
    p2.mechanism,
    p3.mechanism_class,
    p3.intervention_type,
    p3.intervention_action,
    p3.p3_status       AS pass3_status,
    p3.p3_error        AS pass3_error,
    p3.p3_model_name   AS model_name,
    p3.p3_completed_at AS pass3_completed_at
FROM tickets t
LEFT JOIN LATERAL (
    SELECT lp.mechanism
    FROM ticket_llm_pass_results lp
    WHERE lp.ticket_id = t.ticket_id
      AND lp.pass_name = 'pass2_mechanism'
    ORDER BY CASE WHEN lp.status = 'success' THEN 0 ELSE 1 END, lp.updated_at DESC
    LIMIT 1
) p2 ON TRUE
LEFT JOIN LATERAL (
    SELECT lp.mechanism_class,
           lp.intervention_type,
           lp.intervention_action,
           lp.status         AS p3_status,
           lp.error_message  AS p3_error,
           lp.model_name     AS p3_model_name,
           lp.completed_at   AS p3_completed_at
    FROM ticket_llm_pass_results lp
    WHERE lp.ticket_id = t.ticket_id
      AND lp.pass_name = 'pass3_intervention'
    ORDER BY CASE WHEN lp.status = 'success' THEN 0 ELSE 1 END, lp.updated_at DESC
    LIMIT 1
) p3 ON TRUE;

CREATE OR REPLACE VIEW vw_intervention_roi AS
SELECT
    p3.mechanism_class,
    p3.intervention_type,
    COUNT(*) AS ticket_count,
    MODE() WITHIN GROUP (ORDER BY p3.intervention_action) AS representative_action
FROM ticket_llm_pass_results p3
WHERE p3.pass_name = 'pass3_intervention'
  AND p3.status = 'success'
  AND p3.mechanism_class IS NOT NULL
GROUP BY p3.mechanism_class, p3.intervention_type
ORDER BY ticket_count DESC;

CREATE VIEW vw_ticket_pass_pipeline AS
SELECT
    t.ticket_id,
    t.ticket_number,
    t.status           AS ticket_status,
    t.product_name,
    p1.phenomenon,
    p1.component,
    p1.operation,
    p1.unexpected_state,
    p1.canonical_failure,
    p1.p1_status       AS pass1_status,
    p1.p1_completed_at AS pass1_completed_at,
    p2.mechanism,
    p2.p2_status       AS pass2_status,
    p2.p2_completed_at AS pass2_completed_at,
    p3.mechanism_class,
    p3.intervention_type,
    p3.intervention_action,
    p3.p3_status       AS pass3_status,
    p3.p3_completed_at AS pass3_completed_at,
    COALESCE(p3.p3_error, p2.p2_error, p1.p1_error) AS latest_error
FROM tickets t
LEFT JOIN LATERAL (
    SELECT lp.phenomenon,
           lp.component,
           lp.operation,
           lp.unexpected_state,
           lp.canonical_failure,
           lp.status        AS p1_status,
           lp.error_message AS p1_error,
           lp.completed_at  AS p1_completed_at
    FROM ticket_llm_pass_results lp
    WHERE lp.ticket_id = t.ticket_id
      AND lp.pass_name = 'pass1_phenomenon'
    ORDER BY CASE WHEN lp.status = 'success' THEN 0 ELSE 1 END, lp.updated_at DESC
    LIMIT 1
) p1 ON TRUE
LEFT JOIN LATERAL (
    SELECT lp.mechanism,
           lp.status        AS p2_status,
           lp.error_message AS p2_error,
           lp.completed_at  AS p2_completed_at
    FROM ticket_llm_pass_results lp
    WHERE lp.ticket_id = t.ticket_id
      AND lp.pass_name = 'pass2_mechanism'
    ORDER BY CASE WHEN lp.status = 'success' THEN 0 ELSE 1 END, lp.updated_at DESC
    LIMIT 1
) p2 ON TRUE
LEFT JOIN LATERAL (
    SELECT lp.mechanism_class,
           lp.intervention_type,
           lp.intervention_action,
           lp.status        AS p3_status,
           lp.error_message AS p3_error,
           lp.completed_at  AS p3_completed_at
    FROM ticket_llm_pass_results lp
    WHERE lp.ticket_id = t.ticket_id
      AND lp.pass_name = 'pass3_intervention'
    ORDER BY CASE WHEN lp.status = 'success' THEN 0 ELSE 1 END, lp.updated_at DESC
    LIMIT 1
) p3 ON TRUE;
