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

## Pass 1 — Phenomenon Extraction

Pass 1 reads `full_thread_text` from `ticket_thread_rollups`, sends it to the Matcha LLM endpoint, and extracts a structured `phenomenon` statement describing the observable system behavior. Results are stored in `ticket_llm_pass_results`.

**Requires** `DATABASE_URL` to be set.

```bash
# Apply migrations (creates ticket_llm_pass_results table + view)
python db.py migrate

# Run all pending tickets (with optional limit)
python run_ticket_pass1.py --limit 100

# Run for specific ticket(s)
python run_ticket_pass1.py --ticket-id 99784
python run_ticket_pass1.py --ticket-id 99784,98154,100289

# Run only for tickets created after a date
python run_ticket_pass1.py --since 2026-03-01

# Rerun only previously failed tickets
python run_ticket_pass1.py --failed-only

# Force rerun (overwrite existing success results)
python run_ticket_pass1.py --force

# Combine flags
python run_ticket_pass1.py --limit 50 --force
```

Results can be queried via the `vw_ticket_pass1_results` view:

```sql
SELECT ticket_id, phenomenon, pass1_status, latest_error
FROM vw_ticket_pass1_results
WHERE pass1_status = 'success';
```

See [SOLUTION.md](SOLUTION.md) for full architecture docs, configuration, and operational notes.