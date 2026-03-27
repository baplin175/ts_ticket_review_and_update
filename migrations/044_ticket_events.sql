-- Ticket events: tracks user-initiated actions (e.g. Share to Teams) for audit / activity feed
CREATE TABLE IF NOT EXISTS ticket_events (
    id            SERIAL PRIMARY KEY,
    ticket_id     INTEGER NOT NULL REFERENCES tickets(ticket_id),
    event_type    TEXT NOT NULL,          -- e.g. 'teams_share'
    detail        JSONB DEFAULT '{}'::jsonb,
    created_by    TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ticket_events_ticket ON ticket_events(ticket_id);
