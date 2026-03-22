-- Migration 024 — Customer health history factors and contributor snapshots.

SET search_path = tickets_ai, public;

ALTER TABLE customer_ticket_health ADD COLUMN IF NOT EXISTS customer_health_score NUMERIC;
ALTER TABLE customer_ticket_health ADD COLUMN IF NOT EXISTS customer_health_band TEXT;
ALTER TABLE customer_ticket_health ADD COLUMN IF NOT EXISTS pressure_score NUMERIC;
ALTER TABLE customer_ticket_health ADD COLUMN IF NOT EXISTS aging_score NUMERIC;
ALTER TABLE customer_ticket_health ADD COLUMN IF NOT EXISTS friction_score NUMERIC;
ALTER TABLE customer_ticket_health ADD COLUMN IF NOT EXISTS concentration_score NUMERIC;
ALTER TABLE customer_ticket_health ADD COLUMN IF NOT EXISTS breadth_score NUMERIC;
ALTER TABLE customer_ticket_health ADD COLUMN IF NOT EXISTS factor_summary_json JSONB;
ALTER TABLE customer_ticket_health ADD COLUMN IF NOT EXISTS score_formula_version TEXT;
ALTER TABLE customer_ticket_health ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();

CREATE TABLE IF NOT EXISTS customer_health_ticket_contributors (
    as_of_date                   DATE          NOT NULL,
    customer                     TEXT          NOT NULL,
    ticket_id                    BIGINT        NOT NULL REFERENCES tickets (ticket_id) ON DELETE CASCADE,
    ticket_number                TEXT,
    ticket_name                  TEXT,
    product_name                 TEXT,
    status                       TEXT,
    severity                     TEXT,
    assignee                     TEXT,
    days_opened                  NUMERIC,
    date_modified                TIMESTAMPTZ,
    priority                     INT,
    overall_complexity           INT,
    frustrated                   TEXT,
    cluster_id                   TEXT,
    mechanism_class              TEXT,
    intervention_type            TEXT,
    pressure_contribution        NUMERIC       NOT NULL DEFAULT 0,
    aging_contribution           NUMERIC       NOT NULL DEFAULT 0,
    friction_contribution        NUMERIC       NOT NULL DEFAULT 0,
    concentration_contribution   NUMERIC       NOT NULL DEFAULT 0,
    breadth_contribution         NUMERIC       NOT NULL DEFAULT 0,
    total_contribution           NUMERIC       NOT NULL DEFAULT 0,
    score_formula_version        TEXT          NOT NULL,
    created_at                   TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (as_of_date, customer, ticket_id)
);

CREATE INDEX IF NOT EXISTS idx_customer_health_contrib_customer_date
    ON customer_health_ticket_contributors (customer, as_of_date);

CREATE INDEX IF NOT EXISTS idx_customer_health_contrib_date_total
    ON customer_health_ticket_contributors (as_of_date, total_contribution DESC);
