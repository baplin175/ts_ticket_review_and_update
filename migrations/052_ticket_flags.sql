CREATE TABLE IF NOT EXISTS ticket_flags (
    ticket_id   BIGINT      PRIMARY KEY REFERENCES tickets (ticket_id),
    flag_review BOOLEAN     NOT NULL DEFAULT FALSE,
    flagged_at  TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
