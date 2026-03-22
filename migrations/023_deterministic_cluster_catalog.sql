-- Migration 023 — Extend cluster catalog for deterministic mechanism-class pipeline.
-- Adds structured cluster statistics needed by the web app and creates
-- convenience views for the latest completed deterministic cluster run.

SET search_path = tickets_ai, public;

ALTER TABLE cluster_catalog ADD COLUMN IF NOT EXISTS ticket_count INT;
ALTER TABLE cluster_catalog ADD COLUMN IF NOT EXISTS percent_of_total NUMERIC;
ALTER TABLE cluster_catalog ADD COLUMN IF NOT EXISTS customer_count INT;
ALTER TABLE cluster_catalog ADD COLUMN IF NOT EXISTS product_count INT;
ALTER TABLE cluster_catalog ADD COLUMN IF NOT EXISTS dominant_component TEXT;
ALTER TABLE cluster_catalog ADD COLUMN IF NOT EXISTS dominant_operation TEXT;
ALTER TABLE cluster_catalog ADD COLUMN IF NOT EXISTS dominant_intervention_type TEXT;
ALTER TABLE cluster_catalog ADD COLUMN IF NOT EXISTS example_ticket_ids JSONB;
ALTER TABLE cluster_catalog ADD COLUMN IF NOT EXISTS example_mechanisms JSONB;
ALTER TABLE cluster_catalog ADD COLUMN IF NOT EXISTS subclusters JSONB;
ALTER TABLE cluster_catalog ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

DROP VIEW IF EXISTS vw_latest_mechanism_cluster_catalog;
CREATE VIEW vw_latest_mechanism_cluster_catalog AS
WITH latest_run AS (
    SELECT cluster_run_id
    FROM cluster_runs
    WHERE cluster_method = 'mechanism_class_catalog'
      AND run_status = 'completed'
    ORDER BY created_at DESC
    LIMIT 1
)
SELECT
    cc.cluster_run_id,
    cc.cluster_id,
    cc.cluster_label,
    cc.cluster_description,
    cc.representative_tickets,
    cc.common_issue_pattern,
    cc.common_mechanism_pattern,
    cc.suggested_intervention_type,
    cc.ticket_count,
    cc.percent_of_total,
    cc.customer_count,
    cc.product_count,
    cc.dominant_component,
    cc.dominant_operation,
    cc.dominant_intervention_type,
    cc.example_ticket_ids,
    cc.example_mechanisms,
    cc.subclusters,
    cc.created_at,
    cc.updated_at
FROM cluster_catalog cc
JOIN latest_run lr ON lr.cluster_run_id = cc.cluster_run_id;

DROP VIEW IF EXISTS vw_latest_mechanism_ticket_clusters;
CREATE VIEW vw_latest_mechanism_ticket_clusters AS
WITH latest_run AS (
    SELECT cluster_run_id
    FROM cluster_runs
    WHERE cluster_method = 'mechanism_class_catalog'
      AND run_status = 'completed'
    ORDER BY created_at DESC
    LIMIT 1
)
SELECT
    tc.id,
    tc.ticket_id,
    tc.ticket_number,
    tc.cluster_run_id,
    tc.cluster_id,
    tc.cluster_label,
    tc.cluster_confidence,
    tc.cluster_method,
    tc.assigned_at
FROM ticket_clusters tc
JOIN latest_run lr ON lr.cluster_run_id = tc.cluster_run_id;
