-- Migration 003 — Analytics extension tables for wait-states, participants,
-- handoffs, daily snapshots, issue summaries, embeddings, clustering,
-- interventions, customer/product health, and enrichment tracking.
-- Idempotent: uses IF NOT EXISTS throughout so it can be re-run safely.
-- NOTE: Schema creation and search_path are set by db.py before this runs.

-- Ensure gen_random_uuid() is available.
DO $$ BEGIN CREATE EXTENSION IF NOT EXISTS pgcrypto; EXCEPTION WHEN others THEN NULL; END $$;

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

CREATE INDEX IF NOT EXISTS idx_ticket_wait_states_ticket_start
    ON ticket_wait_states (ticket_id, start_at);

CREATE INDEX IF NOT EXISTS idx_ticket_wait_states_state_start
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

CREATE INDEX IF NOT EXISTS idx_ticket_participants_type
    ON ticket_participants (participant_type);

CREATE INDEX IF NOT EXISTS idx_ticket_participants_name
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

CREATE INDEX IF NOT EXISTS idx_ticket_handoffs_ticket_at
    ON ticket_handoffs (ticket_id, handoff_at);

CREATE INDEX IF NOT EXISTS idx_ticket_handoffs_to_party_at
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

CREATE INDEX IF NOT EXISTS idx_snapshots_daily_date
    ON ticket_snapshots_daily (snapshot_date);

CREATE INDEX IF NOT EXISTS idx_snapshots_daily_date_open
    ON ticket_snapshots_daily (snapshot_date, open_flag);

CREATE INDEX IF NOT EXISTS idx_snapshots_daily_date_product
    ON ticket_snapshots_daily (snapshot_date, product_name);

CREATE INDEX IF NOT EXISTS idx_snapshots_daily_date_customer
    ON ticket_snapshots_daily (snapshot_date, customer);

CREATE INDEX IF NOT EXISTS idx_snapshots_daily_date_owner
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

CREATE INDEX IF NOT EXISTS idx_ticket_issue_summaries_ticket_scored
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

CREATE UNIQUE INDEX IF NOT EXISTS idx_ticket_embeddings_unique
    ON ticket_embeddings (ticket_id, embedding_type, source_text_hash, model_name);

CREATE INDEX IF NOT EXISTS idx_ticket_embeddings_ticket_type
    ON ticket_embeddings (ticket_id, embedding_type);

-- ════════════════════════════════════════════════════════════════════
-- 7. cluster_runs
-- ════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS cluster_runs (
    cluster_run_id          UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
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

CREATE INDEX IF NOT EXISTS idx_ticket_clusters_ticket_assigned
    ON ticket_clusters (ticket_id, assigned_at DESC);

CREATE INDEX IF NOT EXISTS idx_ticket_clusters_run_cluster
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
    created_at                  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (cluster_run_id, cluster_id)
);

CREATE INDEX IF NOT EXISTS idx_cluster_catalog_run_cluster
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

CREATE INDEX IF NOT EXISTS idx_ticket_interventions_ticket_created
    ON ticket_interventions (ticket_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_ticket_interventions_type_target
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

CREATE INDEX IF NOT EXISTS idx_customer_ticket_health_customer_date
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

CREATE INDEX IF NOT EXISTS idx_product_ticket_health_product_date
    ON product_ticket_health (product_name, as_of_date);

-- ════════════════════════════════════════════════════════════════════
-- 13. enrichment_runs
-- ════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS enrichment_runs (
    enrichment_run_id       UUID          PRIMARY KEY DEFAULT gen_random_uuid(),
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
