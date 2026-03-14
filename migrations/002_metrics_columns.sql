-- Migration 002 — Add date_created, hours_to_first_response, days_open to ticket_metrics.
-- Idempotent: uses IF NOT EXISTS on each column add.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = 'ticket_metrics' AND column_name = 'date_created'
    ) THEN
        ALTER TABLE ticket_metrics ADD COLUMN date_created TIMESTAMPTZ;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = 'ticket_metrics' AND column_name = 'hours_to_first_response'
    ) THEN
        ALTER TABLE ticket_metrics ADD COLUMN hours_to_first_response NUMERIC;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = current_schema() AND table_name = 'ticket_metrics' AND column_name = 'days_open'
    ) THEN
        ALTER TABLE ticket_metrics ADD COLUMN days_open NUMERIC;
    END IF;
END
$$;
