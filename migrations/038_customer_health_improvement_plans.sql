CREATE TABLE IF NOT EXISTS customer_health_improvement_plans (
    id                  SERIAL PRIMARY KEY,
    customer            TEXT NOT NULL,
    as_of_date          DATE NOT NULL,
    group_filter_json   JSONB,
    group_filter_label  TEXT,
    target_band         TEXT NOT NULL,
    projected_score     NUMERIC,
    projected_band      TEXT,
    tickets_to_resolve  JSONB,
    model_name          TEXT,
    prompt_version      INT,
    plan_text           TEXT,
    raw_context_json    JSONB,
    raw_response_text   TEXT,
    created_at          TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_health_plans_customer
    ON customer_health_improvement_plans (customer);
