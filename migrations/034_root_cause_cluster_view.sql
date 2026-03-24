-- View: flatten LLM pass results into one row per ticket for root-cause clustering.
-- Only includes tickets that have a non-null cluster_key (pass5).

CREATE OR REPLACE VIEW tickets_ai.v_root_cause_cluster AS
SELECT
    t.ticket_id,
    t.product_name,
    MAX(r.mechanism_class)      FILTER (WHERE r.pass_name = 'pass4_intervention') AS mechanism_class,
    MAX(r.cluster_key)          FILTER (WHERE r.pass_name = 'pass5_cluster_key')  AS cluster_key,
    MAX(r.mechanism)            FILTER (WHERE r.pass_name = 'pass3_mechanism')     AS mechanism,
    MAX(r.intervention_action)  FILTER (WHERE r.pass_name = 'pass4_intervention') AS intervention_action
FROM tickets_ai.tickets t
JOIN tickets_ai.ticket_llm_pass_results r
    ON r.ticket_id = t.ticket_id
WHERE r.status = 'success'
GROUP BY t.ticket_id, t.product_name
HAVING MAX(r.cluster_key) FILTER (WHERE r.pass_name = 'pass5_cluster_key') IS NOT NULL;
