-- Migration 003 — Analytics extension: derived tables, snapshot/history,
-- clustering, interventions, health rollups, and views.
-- Idempotent: uses IF NOT EXISTS throughout.

-- ════════════════════════════════════════════════════════════════════
-- 1. ticket_wait_states
-- ════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS ticket_wait_states (
    segment_id              BIGSERIAL     PRIMARY KEY,
    ticket_id               BIGINT        NOT NULL REFERENCES tickets (ticket_id) ON DELETE CASCADE,
    state_name              TEXT          NOT NULL,
    start_at                TIMESTAMPTZ   NOT NULL,
    end_at                  TIMESTAMPTZ   NULL,
    duration_minutes        NUMERIC       NULL,
    inferred_from_action_ids JSONB        NULL,
    confidence              NUMERIC(5,4)  NULL,
    inference_method        TEXT          NULL,
    created_at              TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ   NOT NULL DEFAULT now(),
    CONSTRAINT chk_wait_state_end CHECK (end_at IS NULL OR end_at >= start_at)
);

CREATE INDEX IF NOT EXISTS idx_wait_states_ticket_start
    ON ticket_wait_states (ticket_id, start_at);
CREATE INDEX IF NOT EXISTS idx_wait_states_state_start
    ON ticket_wait_states (state_name, start_at);

-- ════════════════════════════════════════════════════════════════════
-- 2. ticket_participants
-- ════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS ticket_participants (
    ticket_id               BIGINT        NOT NULL REFERENCES tickets (ticket_id) ON DELETE CASCADE,
    participant_id          TEXT          NOT NULL,
    participant_name        TEXT          NULL,
    participant_type        TEXT          NOT NULL,
    first_seen_at           TIMESTAMPTZ   NULL,
    last_seen_at            TIMESTAMPTZ   NULL,
    action_count            INT           NOT NULL DEFAULT 0,
    first_response_flag     BOOLEAN       NOT NULL DEFAULT FALSE,
    created_at              TIMESTAMPTZ   NOT NULL DEFAULT now(),
    updated_at              TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (ticket_id, participant_id)
);

CREATE INDEX IF NOT EXISTS idx_participants_type
    ON ticket_participants (participant_type);
CREATE INDEX IF NOT EXISTS idx_participants_name
    ON ticket_participants (participant_name);

-- ════════════════════════════════════════════════════════════════════
-- 3. ticket_handoffs
-- ════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS ticket_handoffs (
    handoff_id              BIGSERIAL     PRIMARY KEY,
    ticket_id               BIGINT        NOT NULL REFERENCES tickets (ticket_id) ON DELETE CASCADE,
    from_party              TEXT          NULL,
    to_party                TEXT          NULL,
    from_participant_id     TEXT          NULL,
    to_participant_id       TEXT          NULL,
    handoff_at              TIMESTAMPTZ   NOT NULL,
    handoff_reason          TEXT          NULL,
    inferred_from_action_id BIGINT        NULL,
    confidence              NUMERIC(5,4)  NULL,
    created_at              TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_handoffs_ticket_at
    ON ticket_handoffs (ticket_id, handoff_at);
CREATE INDEX IF NOT EXISTS idx_handoffs_to_party_at
    ON ticket_handoffs (to_party, handoff_at);

-- ════════════════════════════════════════════════════════════════════
-- 4. ticket_snapshots_daily
-- ════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS ticket_snapshots_daily (
    snapshot_date           DATE          NOT NULL,
    ticket_id               BIGINT        NOT NULL REFERENCES tickets (ticket_id) ON DELETE CASCADE,
    ticket_number           TEXT          NULL,
    ticket_name             TEXT          NULL,
    status                  TEXT          NULL,
    owner                   TEXT          NULL,
    product_name            TEXT          NULL,
    customer                TEXT          NULL,
    open_flag               BOOLEAN       NOT NULL,
    age_days                NUMERIC       NULL,
    days_since_modified     NUMERIC       NULL,
    priority                INT           NULL,
    overall_complexity      INT           NULL,
    waiting_state           TEXT          NULL,
    high_priority_flag      BOOLEAN       NOT NULL DEFAULT FALSE,
    high_complexity_flag    BOOLEAN       NOT NULL DEFAULT FALSE,
    source_updated_at       TIMESTAMPTZ   NULL,
    created_at              TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (snapshot_date, ticket_id)
);

CREATE INDEX IF NOT EXISTS idx_snapshots_date
    ON ticket_snapshots_daily (snapshot_date);
CREATE INDEX IF NOT EXISTS idx_snapshots_date_open
    ON ticket_snapshots_daily (snapshot_date, open_flag);
CREATE INDEX IF NOT EXISTS idx_snapshots_date_product
    ON ticket_snapshots_daily (snapshot_date, product_name);
CREATE INDEX IF NOT EXISTS idx_snapshots_date_customer
    ON ticket_snapshots_daily (snapshot_date, customer);
CREATE INDEX IF NOT EXISTS idx_snapshots_date_owner
    ON ticket_snapshots_daily (snapshot_date, owner);

-- ════════════════════════════════════════════════════════════════════
-- 5. ticket_issue_summaries
-- ════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS ticket_issue_summaries (
    id                      BIGSERIAL     PRIMARY KEY,
    ticket_id               BIGINT        NOT NULL REFERENCES tickets (ticket_id) ON DELETE CASCADE,
    thread_hash             TEXT          NULL,
    technical_core_hash     TEXT          NULL,
    issue_summary           TEXT          NULL,
    cause_summary           TEXT          NULL,
    mechanism_summary       TEXT          NULL,
    resolution_summary      TEXT          NULL,
    model_name              TEXT          NULL,
    prompt_name             TEXT          NULL,
    prompt_version          TEXT          NULL,
    scored_at               TIMESTAMPTZ   NOT NULL DEFAULT now(),
    source_hash             TEXT          NULL,
    raw_response            JSONB         NULL
);

CREATE INDEX IF NOT EXISTS idx_issue_summaries_ticket_scored
    ON ticket_issue_summaries (ticket_id, scored_at DESC);

-- ════════════════════════════════════════════════════════════════════
-- 6. ticket_embeddings
-- ════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS ticket_embeddings (
    id                      BIGSERIAL     PRIMARY KEY,
    ticket_id               BIGINT        NOT NULL REFERENCES tickets (ticket_id) ON DELETE CASCADE,
    embedding_type          TEXT          NOT NULL,
    source_text_hash        TEXT          NOT NULL,
    model_name              TEXT          NOT NULL,
    created_at              TIMESTAMPTZ   NOT NULL DEFAULT now(),
    embedding_vector        JSONB         NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_embeddings_unique
    ON ticket_embeddings (ticket_id, embedding_type, source_text_hash, model_name);
CREATE INDEX IF NOT EXISTS idx_embeddings_ticket_type
    ON ticket_embeddings (ticket_id, embedding_type);

-- ════════════════════════════════════════════════════════════════════
-- 7. cluster_runs
-- ════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS cluster_runs (
    cluster_run_id          UUID          PRIMARY KEY,
    cluster_method          TEXT          NOT NULL,
    cluster_scope           TEXT          NOT NULL,
    embedding_model         TEXT          NULL,
    clustering_params       JSONB         NULL,
    run_status              TEXT          NOT NULL DEFAULT 'completed',
    created_at              TIMESTAMPTZ   NOT NULL DEFAULT now(),
    notes                   TEXT          NULL
);

-- ════════════════════════════════════════════════════════════════════
-- 8. ticket_clusters
-- ════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS ticket_clusters (
    id                      BIGSERIAL     PRIMARY KEY,
    ticket_id               BIGINT        NOT NULL REFERENCES tickets (ticket_id) ON DELETE CASCADE,
    cluster_run_id          UUID          NOT NULL REFERENCES cluster_runs (cluster_run_id) ON DELETE CASCADE,
    cluster_id              TEXT          NOT NULL,
    cluster_label           TEXT          NULL,
    cluster_confidence      NUMERIC(5,4)  NULL,
    cluster_method          TEXT          NULL,
    assigned_at             TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_clusters_ticket_assigned
    ON ticket_clusters (ticket_id, assigned_at DESC);
CREATE INDEX IF NOT EXISTS idx_clusters_run_cluster
    ON ticket_clusters (cluster_run_id, cluster_id);

-- ════════════════════════════════════════════════════════════════════
-- 9. cluster_catalog
-- ════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS cluster_catalog (
    id                          BIGSERIAL   PRIMARY KEY,
    cluster_run_id              UUID        NOT NULL REFERENCES cluster_runs (cluster_run_id) ON DELETE CASCADE,
    cluster_id                  TEXT        NOT NULL,
    cluster_label               TEXT        NULL,
    cluster_description         TEXT        NULL,
    representative_tickets      JSONB       NULL,
    common_issue_pattern        TEXT        NULL,
    common_mechanism_pattern    TEXT        NULL,
    suggested_intervention_type TEXT        NULL,
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_cluster_catalog_unique
    ON cluster_catalog (cluster_run_id, cluster_id);

-- ════════════════════════════════════════════════════════════════════
-- 10. ticket_interventions
-- ════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS ticket_interventions (
    id                      BIGSERIAL     PRIMARY KEY,
    ticket_id               BIGINT        NOT NULL REFERENCES tickets (ticket_id) ON DELETE CASCADE,
    intervention_type       TEXT          NOT NULL,
    intervention_target     TEXT          NULL,
    intervention_summary    TEXT          NULL,
    confidence              NUMERIC(5,4)  NULL,
    derived_from            TEXT          NULL,
    created_at              TIMESTAMPTZ   NOT NULL DEFAULT now(),
    raw_response            JSONB         NULL
);

CREATE INDEX IF NOT EXISTS idx_interventions_ticket_created
    ON ticket_interventions (ticket_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_interventions_type_target
    ON ticket_interventions (intervention_type, intervention_target);

-- ════════════════════════════════════════════════════════════════════
-- 11. customer_ticket_health
-- ════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS customer_ticket_health (
    as_of_date              DATE          NOT NULL,
    customer                TEXT          NOT NULL,
    open_ticket_count       INT           NOT NULL DEFAULT 0,
    high_priority_count     INT           NOT NULL DEFAULT 0,
    high_complexity_count   INT           NOT NULL DEFAULT 0,
    avg_complexity          NUMERIC       NULL,
    avg_elapsed_drag        NUMERIC       NULL,
    reopen_count_90d        INT           NOT NULL DEFAULT 0,
    frustration_count_90d   INT           NOT NULL DEFAULT 0,
    top_cluster_ids         JSONB         NULL,
    top_products            JSONB         NULL,
    ticket_load_pressure_score NUMERIC    NULL,
    created_at              TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (as_of_date, customer)
);

CREATE INDEX IF NOT EXISTS idx_cust_health_customer_date
    ON customer_ticket_health (customer, as_of_date);

-- ════════════════════════════════════════════════════════════════════
-- 12. product_ticket_health
-- ════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS product_ticket_health (
    as_of_date              DATE          NOT NULL,
    product_name            TEXT          NOT NULL,
    ticket_volume           INT           NOT NULL DEFAULT 0,
    avg_complexity          NUMERIC       NULL,
    avg_coordination_load   NUMERIC       NULL,
    avg_elapsed_drag        NUMERIC       NULL,
    top_clusters            JSONB         NULL,
    top_mechanisms          JSONB         NULL,
    dev_touched_rate        NUMERIC       NULL,
    customer_wait_rate      NUMERIC       NULL,
    created_at              TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (as_of_date, product_name)
);

CREATE INDEX IF NOT EXISTS idx_prod_health_product_date
    ON product_ticket_health (product_name, as_of_date);

-- ════════════════════════════════════════════════════════════════════
-- 13. enrichment_runs
-- ════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS enrichment_runs (
    enrichment_run_id       UUID          PRIMARY KEY,
    enrichment_type         TEXT          NOT NULL,
    started_at              TIMESTAMPTZ   NOT NULL DEFAULT now(),
    completed_at            TIMESTAMPTZ   NULL,
    status                  TEXT          NOT NULL,
    model_name              TEXT          NULL,
    prompt_version          TEXT          NULL,
    tickets_seen            INT           NOT NULL DEFAULT 0,
    tickets_scored          INT           NOT NULL DEFAULT 0,
    tickets_skipped         INT           NOT NULL DEFAULT 0,
    error_count             INT           NOT NULL DEFAULT 0,
    notes                   TEXT          NULL
);

CREATE INDEX IF NOT EXISTS idx_enrichment_runs_type_started
    ON enrichment_runs (enrichment_type, started_at DESC);


-- ════════════════════════════════════════════════════════════════════
-- VIEWS
-- ════════════════════════════════════════════════════════════════════

-- V1. Latest sentiment per ticket
CREATE OR REPLACE VIEW vw_latest_ticket_sentiment AS
SELECT DISTINCT ON (ticket_id) *
FROM ticket_sentiment
ORDER BY ticket_id, scored_at DESC, id DESC;

-- V2. Latest priority per ticket
CREATE OR REPLACE VIEW vw_latest_ticket_priority AS
SELECT DISTINCT ON (ticket_id) *
FROM ticket_priority_scores
ORDER BY ticket_id, scored_at DESC, id DESC;

-- V3. Latest complexity per ticket
CREATE OR REPLACE VIEW vw_latest_ticket_complexity AS
SELECT DISTINCT ON (ticket_id) *
FROM ticket_complexity_scores
ORDER BY ticket_id, scored_at DESC, id DESC;

-- V4. Latest issue summary per ticket
CREATE OR REPLACE VIEW vw_latest_ticket_issue_summary AS
SELECT DISTINCT ON (ticket_id) *
FROM ticket_issue_summaries
ORDER BY ticket_id, scored_at DESC, id DESC;

-- V5. Core analytics join
CREATE OR REPLACE VIEW vw_ticket_analytics_core AS
SELECT
    t.ticket_id,
    t.ticket_number,
    t.ticket_name,
    t.status,
    t.severity,
    t.product_name,
    t.assignee,
    t.customer,
    t.date_created,
    t.date_modified,
    t.closed_at,
    t.days_opened,
    t.days_since_modified,
    -- metrics
    m.action_count,
    m.nonempty_action_count,
    m.customer_message_count,
    m.inhance_message_count,
    m.distinct_participant_count,
    m.first_response_at,
    m.last_human_activity_at,
    m.empty_action_ratio,
    m.handoff_count,
    m.hours_to_first_response,
    m.days_open,
    -- rollups
    r.latest_customer_text,
    r.latest_inhance_text,
    r.technical_core_text,
    r.summary_for_embedding,
    -- sentiment
    s.frustrated,
    -- priority
    p.priority,
    p.priority_explanation,
    -- complexity
    c.intrinsic_complexity,
    c.coordination_load,
    c.elapsed_drag,
    c.overall_complexity,
    -- issue summary
    iss.issue_summary,
    iss.cause_summary,
    iss.mechanism_summary,
    iss.resolution_summary
FROM tickets t
LEFT JOIN ticket_metrics m        ON m.ticket_id = t.ticket_id
LEFT JOIN ticket_thread_rollups r ON r.ticket_id = t.ticket_id
LEFT JOIN vw_latest_ticket_sentiment s   ON s.ticket_id = t.ticket_id
LEFT JOIN vw_latest_ticket_priority p    ON p.ticket_id = t.ticket_id
LEFT JOIN vw_latest_ticket_complexity c  ON c.ticket_id = t.ticket_id
LEFT JOIN vw_latest_ticket_issue_summary iss ON iss.ticket_id = t.ticket_id;

-- V6. Complexity breakdown
CREATE OR REPLACE VIEW vw_ticket_complexity_breakdown AS
SELECT
    t.ticket_id,
    t.ticket_number,
    t.ticket_name,
    t.product_name,
    t.customer,
    c.intrinsic_complexity,
    c.coordination_load,
    c.elapsed_drag,
    c.overall_complexity,
    c.confidence,
    c.primary_complexity_drivers,
    c.complexity_summary,
    c.evidence,
    c.noise_factors,
    c.duration_vs_complexity_note,
    c.scored_at
FROM tickets t
JOIN vw_latest_ticket_complexity c ON c.ticket_id = t.ticket_id;

-- V7. Wait profile aggregation
CREATE OR REPLACE VIEW vw_ticket_wait_profile AS
SELECT
    ws.ticket_id,
    COALESCE(SUM(ws.duration_minutes) FILTER (WHERE ws.state_name = 'waiting_on_customer'), 0) AS waiting_on_customer_minutes,
    COALESCE(SUM(ws.duration_minutes) FILTER (WHERE ws.state_name = 'waiting_on_support'), 0)  AS waiting_on_support_minutes,
    COALESCE(SUM(ws.duration_minutes) FILTER (WHERE ws.state_name = 'waiting_on_dev'), 0)      AS waiting_on_dev_minutes,
    COALESCE(SUM(ws.duration_minutes) FILTER (WHERE ws.state_name = 'waiting_on_ps'), 0)       AS waiting_on_ps_minutes,
    COALESCE(SUM(ws.duration_minutes) FILTER (WHERE ws.state_name = 'active_work'), 0)         AS active_work_minutes,
    COALESCE(SUM(ws.duration_minutes), 0) AS total_profiled_minutes,
    CASE WHEN COALESCE(SUM(ws.duration_minutes), 0) > 0
         THEN ROUND(SUM(ws.duration_minutes) FILTER (WHERE ws.state_name = 'waiting_on_customer')
                     / SUM(ws.duration_minutes) * 100, 2)
         ELSE 0 END AS pct_waiting_on_customer,
    CASE WHEN COALESCE(SUM(ws.duration_minutes), 0) > 0
         THEN ROUND(SUM(ws.duration_minutes) FILTER (WHERE ws.state_name = 'active_work')
                     / SUM(ws.duration_minutes) * 100, 2)
         ELSE 0 END AS pct_active_work
FROM ticket_wait_states ws
GROUP BY ws.ticket_id;

-- V8. Customer support risk (90-day rollup)
CREATE OR REPLACE VIEW vw_customer_support_risk AS
SELECT
    t.customer,
    COUNT(*)                                                           AS ticket_count_90d,
    COUNT(*) FILTER (WHERE p.priority IS NOT NULL AND p.priority <= 3) AS high_priority_count_90d,
    COUNT(*) FILTER (WHERE c.overall_complexity >= 4)                  AS high_complexity_count_90d,
    ROUND(AVG(p.priority), 2)                                          AS avg_priority_90d,
    ROUND(AVG(c.overall_complexity), 2)                                AS avg_complexity_90d,
    ROUND(AVG(c.elapsed_drag), 2)                                      AS avg_elapsed_drag_90d,
    COUNT(*) FILTER (WHERE s.frustrated = 'Yes')                       AS frustration_count_90d,
    (SELECT jsonb_agg(DISTINCT tc.cluster_id)
       FROM ticket_clusters tc
       WHERE tc.ticket_id = ANY(array_agg(t.ticket_id))
    ) AS dominant_clusters
FROM tickets t
LEFT JOIN vw_latest_ticket_priority p   ON p.ticket_id = t.ticket_id
LEFT JOIN vw_latest_ticket_complexity c ON c.ticket_id = t.ticket_id
LEFT JOIN vw_latest_ticket_sentiment s  ON s.ticket_id = t.ticket_id
WHERE t.date_created >= (CURRENT_DATE - INTERVAL '90 days')
   OR (t.closed_at IS NULL)
   OR (t.closed_at >= CURRENT_DATE - INTERVAL '90 days')
GROUP BY t.customer
HAVING t.customer IS NOT NULL;

-- V9. Product pain patterns
CREATE OR REPLACE VIEW vw_product_pain_patterns AS
SELECT
    t.product_name,
    tc.cluster_id,
    tc.cluster_label,
    iss.mechanism_summary,
    COUNT(*)                                  AS ticket_count,
    COUNT(DISTINCT t.customer)                AS affected_customers,
    ROUND(AVG(c.overall_complexity), 2)       AS avg_complexity,
    ROUND(AVG(c.elapsed_drag), 2)             AS avg_elapsed_drag
FROM tickets t
LEFT JOIN vw_latest_ticket_complexity c       ON c.ticket_id = t.ticket_id
LEFT JOIN vw_latest_ticket_issue_summary iss  ON iss.ticket_id = t.ticket_id
LEFT JOIN LATERAL (
    SELECT tc2.cluster_id, tc2.cluster_label
    FROM ticket_clusters tc2
    WHERE tc2.ticket_id = t.ticket_id
    ORDER BY tc2.assigned_at DESC
    LIMIT 1
) tc ON TRUE
WHERE t.product_name IS NOT NULL
GROUP BY t.product_name, tc.cluster_id, tc.cluster_label, iss.mechanism_summary;

-- V10. Intervention opportunities
CREATE OR REPLACE VIEW vw_intervention_opportunities AS
SELECT
    iv.intervention_type,
    iv.intervention_target,
    t.product_name,
    tc.cluster_id,
    tc.cluster_label,
    COUNT(DISTINCT iv.ticket_id)              AS affected_ticket_count,
    COUNT(DISTINCT t.customer)                AS affected_customer_count,
    ROUND(AVG(c.overall_complexity), 2)       AS avg_complexity,
    ROUND(AVG(c.elapsed_drag), 2)             AS avg_elapsed_drag
FROM ticket_interventions iv
JOIN tickets t ON t.ticket_id = iv.ticket_id
LEFT JOIN vw_latest_ticket_complexity c ON c.ticket_id = iv.ticket_id
LEFT JOIN LATERAL (
    SELECT tc2.cluster_id, tc2.cluster_label
    FROM ticket_clusters tc2
    WHERE tc2.ticket_id = iv.ticket_id
    ORDER BY tc2.assigned_at DESC
    LIMIT 1
) tc ON TRUE
GROUP BY iv.intervention_type, iv.intervention_target, t.product_name,
         tc.cluster_id, tc.cluster_label;

-- V11. Backlog daily
CREATE OR REPLACE VIEW vw_backlog_daily AS
SELECT
    snapshot_date,
    COUNT(*) FILTER (WHERE open_flag)                                      AS open_backlog,
    COUNT(*) FILTER (WHERE open_flag AND high_priority_flag)               AS high_priority_backlog,
    COUNT(*) FILTER (WHERE open_flag AND high_complexity_flag)             AS high_complexity_backlog
FROM ticket_snapshots_daily
GROUP BY snapshot_date
ORDER BY snapshot_date;

-- V12. Backlog weekly
CREATE OR REPLACE VIEW vw_backlog_weekly AS
SELECT
    date_trunc('week', snapshot_date)::date                                AS week_start,
    MAX(snapshot_date)                                                     AS latest_snapshot_in_week,
    ROUND(AVG(open_ct)::numeric, 2)                                       AS avg_open_backlog_ratio,
    SUM(open_ct)                                                           AS open_backlog_snapshot_rows,
    SUM(hp_ct)                                                             AS high_priority_backlog_snapshot_rows,
    SUM(hc_ct)                                                             AS high_complexity_backlog_snapshot_rows
FROM (
    SELECT
        snapshot_date,
        COUNT(*) FILTER (WHERE open_flag)                                  AS open_ct,
        COUNT(*) FILTER (WHERE open_flag AND high_priority_flag)           AS hp_ct,
        COUNT(*) FILTER (WHERE open_flag AND high_complexity_flag)         AS hc_ct
    FROM ticket_snapshots_daily
    GROUP BY snapshot_date
) sub
GROUP BY date_trunc('week', snapshot_date)::date
ORDER BY week_start;

-- V13. Backlog weekly end-of-week
CREATE OR REPLACE VIEW vw_backlog_weekly_eow AS
SELECT
    date_trunc('week', snapshot_date)::date AS week_start,
    COUNT(*) FILTER (WHERE open_flag)                           AS open_backlog,
    COUNT(*) FILTER (WHERE open_flag AND high_priority_flag)    AS high_priority_backlog,
    COUNT(*) FILTER (WHERE open_flag AND high_complexity_flag)  AS high_complexity_backlog
FROM (
    SELECT DISTINCT ON (date_trunc('week', snapshot_date), ticket_id)
        snapshot_date, ticket_id, open_flag, high_priority_flag, high_complexity_flag
    FROM ticket_snapshots_daily
    ORDER BY date_trunc('week', snapshot_date), ticket_id, snapshot_date DESC
) latest_per_week
GROUP BY date_trunc('week', snapshot_date)::date
ORDER BY week_start;

-- V14. Backlog aging (current snapshot)
CREATE OR REPLACE VIEW vw_backlog_aging_current AS
SELECT
    CASE
        WHEN age_days <  7  THEN '0-6'
        WHEN age_days < 14  THEN '7-13'
        WHEN age_days < 30  THEN '14-29'
        WHEN age_days < 60  THEN '30-59'
        WHEN age_days < 90  THEN '60-89'
        ELSE '90+'
    END AS age_bucket,
    COUNT(*) AS ticket_count
FROM ticket_snapshots_daily
WHERE snapshot_date = (SELECT MAX(snapshot_date) FROM ticket_snapshots_daily)
  AND open_flag
GROUP BY 1
ORDER BY MIN(age_days);

-- V15. Backlog weekly from ticket dates (fallback)
CREATE OR REPLACE VIEW vw_backlog_weekly_from_dates AS
SELECT
    w.week_start,
    COUNT(*) FILTER (
        WHERE t.date_created <= w.week_start + INTERVAL '6 days'
          AND (t.closed_at IS NULL OR t.closed_at > w.week_start)
    ) AS open_backlog
FROM generate_series(
    (SELECT date_trunc('week', MIN(date_created))::date FROM tickets),
    CURRENT_DATE,
    '7 days'::interval
) AS w(week_start)
CROSS JOIN tickets t
WHERE t.date_created IS NOT NULL
GROUP BY w.week_start
ORDER BY w.week_start;
