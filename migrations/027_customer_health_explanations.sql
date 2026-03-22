-- Migration 027 — persisted customer health explanations.

SET search_path = tickets_ai, public;

CREATE TABLE IF NOT EXISTS customer_health_explanations (
    id                  BIGSERIAL PRIMARY KEY,
    customer            TEXT NOT NULL,
    as_of_date          DATE NOT NULL,
    group_filter_json   JSONB NOT NULL,
    group_filter_label  TEXT NOT NULL,
    model_name          TEXT,
    prompt_version      TEXT NOT NULL,
    explanation_text    TEXT NOT NULL,
    raw_context_json    JSONB,
    raw_response_text   TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_customer_health_explanations_customer_created
    ON customer_health_explanations (customer, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_customer_health_explanations_customer_date
    ON customer_health_explanations (customer, as_of_date DESC);
