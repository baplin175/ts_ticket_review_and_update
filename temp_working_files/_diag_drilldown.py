#!/usr/bin/env python3
"""Diagnose drill-down query — run from project root."""
import db

# Check migration status
row = db.fetch_one("SELECT MAX(filename) FROM _migrations WHERE filename LIKE '019%%'")
print("Migration 019 applied:", row)

# Test base drilldown query
rows = db.fetch_all("""
    SELECT count(*)
    FROM tickets t
    WHERE t.closed_at IS NULL
      AND COALESCE(t.status, '') NOT IN ('Closed', 'Resolved', 'Open')
""")
print("Open non-test ticket count:", rows)

# Test the full drilldown join
rows2 = db.fetch_all("""
    SELECT count(*)
    FROM tickets t
    JOIN vw_ticket_analytics_core v ON v.ticket_id = t.ticket_id
    WHERE t.closed_at IS NULL
      AND COALESCE(t.status, '') NOT IN ('Closed', 'Resolved', 'Open')
""")
print("Drilldown join count:", rows2)

# Test with a specific age bucket
rows3 = db.fetch_all("""
    SELECT count(*)
    FROM tickets t
    JOIN vw_ticket_analytics_core v ON v.ticket_id = t.ticket_id
    WHERE t.closed_at IS NULL
      AND COALESCE(t.status, '') NOT IN ('Closed', 'Resolved', 'Open')
      AND EXTRACT(DAY FROM now() - t.date_created)::int >= 30
      AND EXTRACT(DAY FROM now() - t.date_created)::int < 60
""")
print("Drilldown 30-59 bucket:", rows3)

# Also test what the aging view returns
rows4 = db.fetch_all("SELECT * FROM vw_backlog_aging_current")
print("Aging view:", rows4)
