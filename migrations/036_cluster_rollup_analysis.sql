-- ============================================================
-- PART 1 — cluster_key_rollup_map (L2 → L1 mapping table)
-- ============================================================

CREATE TABLE IF NOT EXISTS cluster_key_rollup_map (
    cluster_key     TEXT PRIMARY KEY,
    cluster_key_l1  TEXT NOT NULL,
    notes           TEXT,
    is_active       BOOLEAN DEFAULT TRUE
);

-- Seed initial L2 → L1 mappings
INSERT INTO cluster_key_rollup_map (cluster_key, cluster_key_l1)
VALUES
    ('incorrect_charge_calculation_logic',              'incorrect_calculation_logic'),
    ('wrong_meter_selected',                            'wrong_entity_selected'),
    ('missing_import_deduplication',                    'missing_idempotency_validation'),
    ('duplicate_transactions_created',                  'duplicate_records_created'),
    ('startup_dependency_on_stopped_service',           'missing_dependency'),
    ('missing_payment_batch_idempotency_validation',    'missing_idempotency_validation'),
    ('incorrect_meter_readings_used',                   'incorrect_calculation_logic'),
    ('incorrect_usage_reading_used',                    'incorrect_calculation_logic')
ON CONFLICT (cluster_key) DO NOTHING;


-- ============================================================
-- PART 2 — v_ticket_failure_flat (one-row-per-ticket flat view)
-- ============================================================

CREATE OR REPLACE VIEW tickets_ai.v_ticket_failure_flat AS
SELECT
    ticket_id,
    product_name,
    mechanism_class,
    cluster_key,
    mechanism,
    intervention_action
FROM tickets_ai.v_root_cause_cluster;


-- ============================================================
-- PART 3 — v_cluster_summary_l2 (L2 cluster summary)
-- ============================================================

CREATE OR REPLACE VIEW tickets_ai.v_cluster_summary_l2 AS
SELECT
    product_name,
    mechanism_class,
    cluster_key,
    COUNT(*) AS ticket_count
FROM tickets_ai.v_root_cause_cluster
GROUP BY product_name, mechanism_class, cluster_key;


-- ============================================================
-- PART 4 — v_cluster_summary_l1 (L1 rollup summary)
-- ============================================================

CREATE OR REPLACE VIEW tickets_ai.v_cluster_summary_l1 AS
SELECT
    rc.product_name,
    rc.mechanism_class,
    COALESCE(m.cluster_key_l1, rc.cluster_key) AS cluster_key_l1,
    COUNT(*) AS ticket_count
FROM tickets_ai.v_root_cause_cluster rc
LEFT JOIN tickets_ai.cluster_key_rollup_map m
    ON m.cluster_key = rc.cluster_key
   AND m.is_active = TRUE
GROUP BY rc.product_name, rc.mechanism_class, COALESCE(m.cluster_key_l1, rc.cluster_key);


-- ============================================================
-- PART 5 — v_cluster_examples (example tickets per cluster)
-- ============================================================

CREATE OR REPLACE VIEW tickets_ai.v_cluster_examples AS
SELECT
    rc.ticket_id,
    rc.product_name,
    rc.mechanism_class,
    COALESCE(m.cluster_key_l1, rc.cluster_key) AS cluster_key_l1,
    rc.cluster_key AS cluster_key_l2,
    rc.mechanism,
    rc.intervention_action
FROM tickets_ai.v_root_cause_cluster rc
LEFT JOIN tickets_ai.cluster_key_rollup_map m
    ON m.cluster_key = rc.cluster_key
   AND m.is_active = TRUE;


-- ============================================================
-- PART 6 — cluster_recommendations table
-- ============================================================

CREATE TABLE IF NOT EXISTS cluster_recommendations (
    id                          BIGSERIAL PRIMARY KEY,
    product_name                TEXT,
    mechanism_class             TEXT,
    cluster_key_l1              TEXT,
    ticket_count                INT,
    recommended_change          TEXT,
    where_to_implement          TEXT,
    why_it_prevents_recurrence  TEXT,
    confidence                  TEXT,
    source_model                TEXT,
    created_at                  TIMESTAMPTZ DEFAULT NOW()
);
