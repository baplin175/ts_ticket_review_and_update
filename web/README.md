# TS Ticket Analytics — Web Dashboard

## Quick Start

```bash
# Install dashboard dependencies (from project root)
pip install -e '.[web]'

# Start the dashboard
python3 -m web.app
```

Open **http://localhost:8050** in your browser.

## Options

```bash
# Custom port
WEB_PORT=9000 python3 -m web.app

# Disable debug/hot-reload
WEB_DEBUG=0 python3 -m web.app

# Production (gunicorn)
gunicorn web.app:server -b 0.0.0.0:8050
```

## Requirements

- `DATABASE_URL` must be set (reads from the `tickets_ai` schema)
- Postgres must have migrations applied (`python3 db.py migrate`)
- Tickets should be ingested before the dashboard shows meaningful data
- `run_ingest.py sync` already rebuilds rollups and analytics for touched tickets
