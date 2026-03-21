# TeamSupport Ticket Review & Update

Ingestion + analytics pipeline for TeamSupport tickets: fetches ticket data, cleans activities, runs LLM-based enrichment (sentiment, priority, complexity), and writes results back to TeamSupport.

## Supported Surfaces

- Canonical production path: DB-backed ingestion + rollups + DB enrichment
- Dashboard: Dash app in [`web/`](web/)
- CSV pipeline app: standalone Flask app in [`pipeline/`](pipeline/)
- Legacy compatibility path: JSON-only orchestration via `run_all.py`

## Quick Start

```bash
# Install core dependencies
pip install -e .
pip install -e '.[dev]'

# With Postgres (canonical DB mode)
export DATABASE_URL="postgresql://user:pass@localhost:5432/Work"
python db.py migrate
python run_ingest.py sync --ticket 29696
python run_enrich_db.py --limit 1 --status Open
python run_export.py all --ticket 29696
```

`run_ingest.py sync` already rebuilds rollups and analytics for touched tickets. Use `run_rollups.py` for manual rebuilds or recovery operations.

## Legacy Compatibility Path

```bash
# JSON-only mode (no database required)
TARGET_TICKET=29696 python run_all.py
```

Use this only when you explicitly need the legacy JSON artifact flow or TeamSupport write-back compatibility.

## Dashboard

```bash
pip install -e '.[web]'
python3 -m web.app
```

## Running Tests

```bash
python3 -m pytest tests/ -v
```

## Dependency Notes

- `pyproject.toml` is the primary dependency definition.
- `requirements.txt` and `web_requirements.txt` remain as compatibility install lists.

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

Pass 1 also performs the grammar extraction that used to live in the old Pass 2 script. The active numbered sequence is now:

- Pass 1: phenomenon + canonical failure grammar
- Pass 2: mechanism inference
- Pass 3: intervention mapping

## Legacy Grammar Pass

```bash
python run_ticket_pass2.py --limit 100
```

`run_ticket_pass2.py` is deprecated and retained only for backward compatibility.
