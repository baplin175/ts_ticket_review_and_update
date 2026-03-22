-- Migration 026 — persist health rollups at the group_name level.

SET search_path = tickets_ai, public;

ALTER TABLE customer_ticket_health
    ADD COLUMN IF NOT EXISTS group_name TEXT;
UPDATE customer_ticket_health
SET group_name = ''
WHERE group_name IS NULL;
ALTER TABLE customer_ticket_health
    ALTER COLUMN group_name SET DEFAULT '';
ALTER TABLE customer_ticket_health
    ALTER COLUMN group_name SET NOT NULL;

ALTER TABLE product_ticket_health
    ADD COLUMN IF NOT EXISTS group_name TEXT;
UPDATE product_ticket_health
SET group_name = ''
WHERE group_name IS NULL;
ALTER TABLE product_ticket_health
    ALTER COLUMN group_name SET DEFAULT '';
ALTER TABLE product_ticket_health
    ALTER COLUMN group_name SET NOT NULL;

ALTER TABLE customer_health_ticket_contributors
    ADD COLUMN IF NOT EXISTS group_name TEXT;
UPDATE customer_health_ticket_contributors
SET group_name = ''
WHERE group_name IS NULL;
ALTER TABLE customer_health_ticket_contributors
    ALTER COLUMN group_name SET DEFAULT '';
ALTER TABLE customer_health_ticket_contributors
    ALTER COLUMN group_name SET NOT NULL;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_schema = current_schema()
          AND table_name = 'customer_ticket_health'
          AND constraint_name = 'customer_ticket_health_pkey'
    ) THEN
        ALTER TABLE customer_ticket_health DROP CONSTRAINT customer_ticket_health_pkey;
    END IF;
END $$;

ALTER TABLE customer_ticket_health
    ADD CONSTRAINT customer_ticket_health_pkey
    PRIMARY KEY (as_of_date, customer, group_name);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_schema = current_schema()
          AND table_name = 'product_ticket_health'
          AND constraint_name = 'product_ticket_health_pkey'
    ) THEN
        ALTER TABLE product_ticket_health DROP CONSTRAINT product_ticket_health_pkey;
    END IF;
END $$;

ALTER TABLE product_ticket_health
    ADD CONSTRAINT product_ticket_health_pkey
    PRIMARY KEY (as_of_date, product_name, group_name);

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_schema = current_schema()
          AND table_name = 'customer_health_ticket_contributors'
          AND constraint_name = 'customer_health_ticket_contributors_pkey'
    ) THEN
        ALTER TABLE customer_health_ticket_contributors
            DROP CONSTRAINT customer_health_ticket_contributors_pkey;
    END IF;
END $$;

ALTER TABLE customer_health_ticket_contributors
    ADD CONSTRAINT customer_health_ticket_contributors_pkey
    PRIMARY KEY (as_of_date, customer, group_name, ticket_id);

DROP INDEX IF EXISTS idx_cust_health_customer_date;
CREATE INDEX IF NOT EXISTS idx_cust_health_customer_group_date
    ON customer_ticket_health (customer, group_name, as_of_date);

DROP INDEX IF EXISTS idx_prod_health_product_date;
CREATE INDEX IF NOT EXISTS idx_prod_health_product_group_date
    ON product_ticket_health (product_name, group_name, as_of_date);

DROP INDEX IF EXISTS idx_customer_health_contrib_customer_date;
CREATE INDEX IF NOT EXISTS idx_customer_health_contrib_customer_group_date
    ON customer_health_ticket_contributors (customer, group_name, as_of_date);

CREATE INDEX IF NOT EXISTS idx_customer_health_contrib_group_date
    ON customer_health_ticket_contributors (group_name, as_of_date);
