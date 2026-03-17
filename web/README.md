# TS Ticket Analytics — Web Dashboard

## Quick Start

```bash
# Install dependencies (from project root)
pip install -r web_requirements.txt

# Start the dashboard
python3 web/app.py
```

Open **http://localhost:8050** in your browser.

## Options

```bash
# Custom port
WEB_PORT=9000 python3 web/app.py

# Disable debug/hot-reload
WEB_DEBUG=0 python3 web/app.py

# Production (gunicorn)
gunicorn web.app:server -b 0.0.0.0:8050
```

## Requirements

- `DATABASE_URL` must be set (reads from the `tickets_ai` schema)
- Postgres must have migrations applied (`python3 db.py migrate`)
- Tickets and rollups should be ingested before the dashboard shows meaningful data
