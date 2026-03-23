CREATE TABLE IF NOT EXISTS customer_attributes (
    customer_id          BIGINT PRIMARY KEY,
    customer_name        TEXT NOT NULL,
    is_active            BOOLEAN,
    key_acct             BOOLEAN,
    key_acct_raw         TEXT,
    default_support_group TEXT,
    date_created         TIMESTAMPTZ,
    date_modified        TIMESTAMPTZ,
    source_payload       JSONB,
    first_synced_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_synced_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_customer_attributes_name
    ON customer_attributes (customer_name);

CREATE INDEX IF NOT EXISTS idx_customer_attributes_key_acct
    ON customer_attributes (key_acct);
