-- Migration 001 — Initial schema for TeamSupport ingestion + analytics pipeline.
-- Idempotent: uses IF NOT EXISTS throughout so it can be re-run safely.
-- NOTE: Schema creation and search_path are set by db.py before this runs.

-- ════════════════════════════════════════════════════════════════════
-- 1. tickets
-- ════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS tickets (
    ticket_id               BIGINT        PRIMARY KEY,
    ticket_number           TEXT,
    ticket_name             TEXT,
    status                  TEXT,
    severity                TEXT,
    product_name            TEXT,
    assignee                TEXT,
    customer                TEXT,
    date_created            TIMESTAMPTZ,
    date_modified           TIMESTAMPTZ,
    closed_at               TIMESTAMPTZ,
    days_opened             NUMERIC,
    days_since_modified     NUMERIC,
    source_updated_at       TIMESTAMPTZ,
    source_payload          JSONB,
    first_ingested_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    last_ingested_at        TIMESTAMPTZ   NOT NULL DEFAULT now(),
    last_seen_at            TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_tickets_ticket_number
    ON tickets (ticket_number);

CREATE INDEX IF NOT EXISTS idx_tickets_date_modified
    ON tickets (date_modified);

-- ════════════════════════════════════════════════════════════════════
-- 2. ticket_actions
-- ════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS ticket_actions (
    action_id               BIGINT        PRIMARY KEY,
    ticket_id               BIGINT        NOT NULL REFERENCES tickets (ticket_id),
    created_at              TIMESTAMPTZ,
    action_type             TEXT,
    creator_id              TEXT,
    creator_name            TEXT,
    party                   TEXT,
    is_visible              BOOLEAN,
    description             TEXT,
    cleaned_description     TEXT,
    action_class            TEXT,
    is_empty                BOOLEAN       NOT NULL DEFAULT FALSE,
    is_customer_visible     BOOLEAN,
    source_payload          JSONB,
    first_ingested_at       TIMESTAMPTZ   NOT NULL DEFAULT now(),
    last_ingested_at        TIMESTAMPTZ   NOT NULL DEFAULT now(),
    last_seen_at            TIMESTAMPTZ   NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ticket_actions_ticket_created
    ON ticket_actions (ticket_id, created_at);

-- ════════════════════════════════════════════════════════════════════
-- 3. sync_state
-- ════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS sync_state (
    source_name             TEXT          PRIMARY KEY,
    last_successful_sync_at TIMESTAMPTZ,
    last_attempted_sync_at  TIMESTAMPTZ,
    last_status             TEXT,
    last_error              TEXT,
    last_cursor             TEXT,
    updated_at              TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- ════════════════════════════════════════════════════════════════════
-- 4. ingest_runs
-- ════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS ingest_runs (
    ingest_run_id           UUID          PRIMARY KEY,
    source_name             TEXT          NOT NULL,
    started_at              TIMESTAMPTZ   NOT NULL DEFAULT now(),
    completed_at            TIMESTAMPTZ,
    status                  TEXT          NOT NULL DEFAULT 'running',
    tickets_seen            INT           DEFAULT 0,
    tickets_upserted        INT           DEFAULT 0,
    actions_seen            INT           DEFAULT 0,
    actions_upserted        INT           DEFAULT 0,
    error_text              TEXT,
    config_snapshot         JSONB
);

-- ════════════════════════════════════════════════════════════════════
-- 5. ticket_thread_rollups
-- ════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS ticket_thread_rollups (
    ticket_id               BIGINT        PRIMARY KEY REFERENCES tickets (ticket_id),
    full_thread_text        TEXT,
    customer_visible_text   TEXT,
    latest_customer_text    TEXT,
    latest_inhance_text     TEXT,
    technical_core_text     TEXT,
    summary_for_embedding   TEXT,
    thread_hash             TEXT,
    technical_core_hash     TEXT,
    updated_at              TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- ════════════════════════════════════════════════════════════════════
-- 6. ticket_metrics
-- ════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS ticket_metrics (
    ticket_id                   BIGINT    PRIMARY KEY REFERENCES tickets (ticket_id),
    action_count                INT,
    nonempty_action_count       INT,
    customer_message_count      INT,
    inhance_message_count       INT,
    distinct_participant_count  INT,
    first_response_at           TIMESTAMPTZ,
    last_human_activity_at      TIMESTAMPTZ,
    empty_action_ratio          NUMERIC,
    handoff_count               INT,
    updated_at                  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ════════════════════════════════════════════════════════════════════
-- 7. ticket_sentiment
-- ════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS ticket_sentiment (
    id                      BIGSERIAL     PRIMARY KEY,
    ticket_id               BIGINT        NOT NULL REFERENCES tickets (ticket_id),
    thread_hash             TEXT,
    model_name              TEXT,
    prompt_name             TEXT,
    prompt_version          TEXT,
    scored_at               TIMESTAMPTZ   NOT NULL DEFAULT now(),
    frustrated              TEXT,
    activity_id             TEXT,
    created_at              TIMESTAMPTZ,
    source_file             TEXT,
    raw_response            JSONB
);

CREATE INDEX IF NOT EXISTS idx_ticket_sentiment_ticket_scored
    ON ticket_sentiment (ticket_id, scored_at DESC);

-- ════════════════════════════════════════════════════════════════════
-- 8. ticket_priority_scores
-- ════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS ticket_priority_scores (
    id                      BIGSERIAL     PRIMARY KEY,
    ticket_id               BIGINT        NOT NULL REFERENCES tickets (ticket_id),
    thread_hash             TEXT,
    model_name              TEXT,
    prompt_name             TEXT,
    prompt_version          TEXT,
    scored_at               TIMESTAMPTZ   NOT NULL DEFAULT now(),
    priority                INT,
    priority_explanation    TEXT,
    raw_response            JSONB
);

CREATE INDEX IF NOT EXISTS idx_ticket_priority_ticket_scored
    ON ticket_priority_scores (ticket_id, scored_at DESC);

-- ════════════════════════════════════════════════════════════════════
-- 9. ticket_complexity_scores
-- ════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS ticket_complexity_scores (
    id                          BIGSERIAL   PRIMARY KEY,
    ticket_id                   BIGINT      NOT NULL REFERENCES tickets (ticket_id),
    technical_core_hash         TEXT,
    model_name                  TEXT,
    prompt_name                 TEXT,
    prompt_version              TEXT,
    scored_at                   TIMESTAMPTZ NOT NULL DEFAULT now(),
    intrinsic_complexity        INT,
    coordination_load           INT,
    elapsed_drag                INT,
    overall_complexity          INT,
    confidence                  NUMERIC,
    primary_complexity_drivers  JSONB,
    complexity_summary          TEXT,
    evidence                    JSONB,
    noise_factors               JSONB,
    duration_vs_complexity_note TEXT,
    raw_response                JSONB
);

CREATE INDEX IF NOT EXISTS idx_ticket_complexity_ticket_scored
    ON ticket_complexity_scores (ticket_id, scored_at DESC);
