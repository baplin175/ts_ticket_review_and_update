# TeamSupport Ticket Review & Update

Ingestion + analytics pipeline for TeamSupport tickets: fetches ticket data, cleans activities, runs LLM-based enrichment (sentiment, priority, complexity), and writes results back to TeamSupport.

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# JSON-only mode (no database required)
TARGET_TICKET=29696 python run_all.py

# With Postgres (canonical DB mode)
export DATABASE_URL="postgresql://user:pass@localhost:5432/Work"
python db.py migrate
python run_ingest.py sync --ticket 29696
python run_rollups.py all --ticket 29696
python run_export.py all --ticket 29696
```

## Running Tests

```bash
pip install pytest
python -m pytest tests/ -v
```

See [SOLUTION.md](SOLUTION.md) for full architecture docs, configuration, and operational notes.