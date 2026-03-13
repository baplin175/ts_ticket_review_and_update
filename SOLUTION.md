# Solution Overview

## Purpose

Pull open-ticket activities from TeamSupport, cleanse the text, classify each activity by party (inHANCE team vs customer), write them to a timestamped JSON file, run sentiment analysis, AI priority scoring, and complexity analysis via Matcha LLM.

## Architecture

```
config.py              — Centralised settings (API creds, limits, target ticket, output dir, stage toggles, DATABASE_URL)
ts_client.py           — TeamSupport REST API client (fetch tickets, activities, inHANCE users, update_ticket with auto LastInhComment/LastCustComment)
activity_cleaner.py    — Text-cleaning pipeline (HTML→text, boilerplate/signature removal)
matcha_client.py       — Matcha LLM API client (send prompts, extract responses)
db.py                  — Postgres data-access layer (connection pool, migration runner, upsert helpers, enrichment persistence)
run_ingest.py          — DB-backed incremental ingestion CLI (sync, resync, status)
action_classifier.py   — Deterministic rule-based action classification (no LLM)
run_rollups.py         — Rebuild action classification, thread rollups, and metrics from DB state
run_all.py             — Orchestrator: runs all stages in sequence, merges fields, single API call per ticket (--force, --no-writeback)
run_export.py          — Export canonical DB state to timestamped JSON artifacts
run_pull_activities.py — Part 1: fetch, clean, classify activities → activities JSON
run_sentiment.py       — Part 2: sentiment analysis via Matcha (DB persistence + hash-based skipping when DB available)
run_priority.py        — Part 3: AI priority scoring via Matcha (DB persistence + hash-based skipping when DB available)
run_complexity.py      — Part 4: complexity analysis via Matcha (DB persistence + hash-based skipping when DB available)
migrations/            — Numbered SQL migration files applied by db.py
```

## Data Flow

1. **Fetch tickets** — `ts_client.fetch_open_tickets(ticket_number=...)` queries the TeamSupport `/Tickets` endpoint. When `TARGET_TICKET` is set, the ticket is fetched by number regardless of open/closed status. When no target is set, only open tickets are returned (`isClosed=False`) with full pagination. If the API returns 403 (rate limit), the pipeline falls back to `Activities.csv`.
2. **CSV fallback** — When the API is rate-limited or unavailable, `run_pull_activities.py` reads `Activities.csv` from the project root. Party classification uses cached inHANCE names from prior activity JSON files; if none exist, party is set to `"unknown"`. Some metadata fields (`ticket_id`, `status`, `severity`, `assignee`, `date_created`, `date_modified`) are unavailable from CSV.
2. **Limit tickets** — When not targeting a specific ticket, `config.MAX_TICKETS` (default **5**) caps how many are processed. Set to `0` for unlimited.
3. **Fetch activities per ticket** — `ts_client.fetch_all_activities(ticket_id)` pages through `/Tickets/{id}/Actions`.
4. **Load inHANCE user IDs** — `ts_client.fetch_inhance_user_ids()` calls `/Users?Organization=inHANCE` once and caches the set of CS-team user IDs.
5. **Cleanse & classify each activity** — `activity_cleaner.clean_activity_dict(action)` runs the full pipeline:
   - HTML → plain text conversion (with double-encoded entity handling)
   - Mojibake / encoding repair
   - Boilerplate, header, and inline-noise removal
   - Email signature and quoted-reply stripping
   - Line deduplication and whitespace normalisation
   - Creator name extraction and party classification (`inh` = inHANCE team, `cust` = customer)
   - Visibility flag extraction (`is_visible` from `IsVisibleOnPortal`)
   - **Only public/visible activities** (`is_visible=True`) are kept; private actions are filtered out
6. **Recalculate date_modified** — After filtering, `date_modified` is recalculated as the `created_at` of the last visible activity where `party` is `inh` or `cust`. `days_since_modified` is recomputed accordingly. This ensures the date reflects the last meaningful human interaction, not system events.
7. **Write JSON** — Output is written to `output/activities_YYYYMMDD_HHMMSS.json` as an array of ticket objects. Each ticket object contains metadata at the top level and an `activities` array of cleaned activity records — ticket-level fields are stored once, not repeated per activity.

### Part 2 — Sentiment Analysis

7. **Extract customer comments** — `run_sentiment.py` reads the latest activities JSON, filters to `party=cust` with non-empty descriptions, and takes the last N (default **4**).
8. **Build prompt** — The instructions from `prompts/sentiment.md` are combined with the customer comments as a JSON input block.
9. **Call Matcha** — `matcha_client.call_matcha()` sends the prompt to the Matcha LLM API with retry logic for transient failures.
10. **Write sentiment JSON** — The response is written to `output/sentiment_YYYYMMDD_HHMMSS.json`.

## Activities JSON Schema

The output file is an array of ticket objects. Each ticket object:

### Ticket-level fields

| Field                | Description                                         |
|----------------------|-----------------------------------------------------|
| `ticket_id`          | TeamSupport internal ID                             |
| `ticket_number`      | Human-readable ticket number                        |
| `ticket_name`        | Ticket title / name                                 |
| `date_created`       | Date the ticket was created                         |
| `date_modified`      | Date the ticket was last modified                   |
| `days_opened`        | Number of days the ticket has been open             |
| `days_since_modified`| Computed: days since `date_modified`                |
| `status`             | Ticket status (e.g. "In-Progress")                  |
| `severity`           | Severity level (e.g. "3 - Low Priority")            |
| `product_name`       | Product associated with the ticket                  |
| `assignee`           | Ticket assignee (support staff)                     |
| `customer`           | Primary customer / organisation                     |
| `activities`         | Array of activity objects (see below)               |

### Activity-level fields (each entry in `activities`)

| Field           | Description                                         |
|-----------------|-----------------------------------------------------|
| `action_id`     | Activity / action ID                                |
| `created_at`    | Timestamp of the activity                           |
| `action_type`   | e.g. "Comment", "Email", etc.                       |
| `creator_id`    | ID of the user who created the activity             |
| `creator_name`  | Display name of the creator                         |
| `party`         | `inh` (inHANCE CS team) or `cust` (customer)       |
| `is_visible`    | `true` if visible on portal (public), `false` if private |
| `description`   | Cleaned plain-text body                             |

### Part 3 — AI Priority Scoring

11. **Build input** — `run_priority.py` reads the latest activities JSON, extracts ticket metadata + all activities into the input format expected by `prompts/ai_priority.md`.
12. **Call Matcha** — The full prompt (instructions + ticket data) is sent to Matcha with a 600s timeout.
13. **Parse response** — Matcha returns a JSON array with `priority` (1–10), `priority_explanation`, and verbatim pass-through fields.
14. **Collect fields** — `run_priority.py` returns the computed fields (`AIPriority`, `AIPriExpln`, `AILastUpdate`) per ticket without calling the API. When run standalone, it writes back directly; when run via the orchestrator, write-back is deferred.
15. **Save locally** — Results are saved to `output/priority_YYYYMMDD_HHMMSS.json`.

## Priority JSON Output Schema

| Field                  | Description                                         |
|------------------------|-----------------------------------------------------|
| `source_file`          | Activities JSON filename used as input              |
| `tickets_sent`         | Number of tickets sent to Matcha                    |
| `writeback_count`      | Number of tickets successfully written back to TS   |
| `results`              | Array of priority result objects (see below)        |

### Per-ticket result fields

| Field                  | Description                                         |
|------------------------|-----------------------------------------------------|
| `ticket_number`        | Ticket number                                       |
| `ticket_name`          | Ticket title                                        |
| `severity`             | Verbatim from input                                 |
| `priority`             | AI-assigned priority (1–10, 1 = most urgent)        |
| `priority_explanation` | 1–2 sentence justification                          |
| `days_opened`          | Verbatim from input                                 |
| `days_since_modified`  | Verbatim from input                                 |
| `assignee`             | Verbatim from input                                 |
| `customer`             | Verbatim from input                                 |

### TeamSupport fields updated

| TS Field         | Source                     |
|------------------|----------------------------|
| `AIPriority`     | `priority` from Matcha     |
| `AIPriExpln`     | `priority_explanation`     |
| `AILastUpdate`   | UTC timestamp of write-back|

## Sentiment JSON Schema

| Field           | Description                                         |
|-----------------|-----------------------------------------------------|
| `ticket_number` | Ticket number analysed                              |
| `comments_sent` | Number of customer comments sent to Matcha          |
| `source_file`   | Activities JSON filename used as input              |
| `frustrated`    | `"Yes"` or `"No"`                                   |
| `activity_id`   | ID of first frustration activity (or `null`)        |
| `created_at`    | Timestamp of first frustration activity (or `null`) |

### Part 4 — Complexity Analysis

16. **Build ticket history** — `run_complexity.py` reads the latest activities JSON and builds a text representation of each ticket (metadata + chronological activity history) for the `prompts/complexity.md` template.
17. **Call Matcha** — The prompt (with `{{TICKET_HISTORY}}` replaced) is sent to Matcha per ticket.
18. **Parse response** — Matcha returns a JSON object with `intrinsic_complexity`, `coordination_load`, `elapsed_drag`, `overall_complexity` (1–5 scale), plus `confidence`, drivers, evidence, and noise factors.
19. **Collect fields** — `run_complexity.py` returns the computed fields (`Complexity`, `COORDINATIONLOAD`, `ELAPSEDDRAG`, `INTRINSICCOMPLEXITY`) per ticket without calling the API. When run standalone, it writes back directly; when run via the orchestrator, write-back is deferred.
20. **Save locally** — Results are saved to `output/complexity_YYYYMMDD_HHMMSS.json`.

### Consolidated Write-Back

21. **Single API call per ticket** — When running via `run_all.py`, the orchestrator collects all fields from enabled stages (priority + complexity) and merges them into a single `update_ticket()` call per ticket. `LastInhComment` and `LastCustComment` are injected automatically by `update_ticket()` — derived from the activities list by scanning for the most recent `party=="inh"` and `party=="cust"` entries. If the API returns 403 or the ticket has no `ticket_id` (CSV-sourced), the payload is saved to `output/api_payloads_dry_run.json` for verification.

## Complexity JSON Output Schema

| Field                       | Description                                           |
|-----------------------------|-------------------------------------------------------|
| `source_file`               | Activities JSON filename used as input                |
| `tickets_scored`            | Number of tickets scored                              |
| `writeback_count`           | Number of tickets successfully written back to TS     |
| `results`                   | Array of per-ticket complexity objects                 |

### Per-ticket complexity fields

| Field                       | Description                                           |
|-----------------------------|-------------------------------------------------------|
| `ticket_id`                 | TeamSupport internal ID                               |
| `ticket_number`             | Ticket number                                         |
| `ticket_name`               | Ticket title                                          |
| `intrinsic_complexity`      | Technical difficulty (1–5)                             |
| `coordination_load`         | Coordination burden (1–5)                              |
| `elapsed_drag`              | Delay/noise inflation (1–5)                            |
| `overall_complexity`        | Final rollup score (1–5, weighted toward intrinsic)    |
| `confidence`                | 0.00–1.00 confidence in the estimate                   |
| `primary_complexity_drivers`| Short phrases describing main drivers                 |
| `complexity_summary`        | 3–6 sentence explanation                               |
| `evidence`                  | Strongest concrete evidence from the ticket           |
| `noise_factors`             | Factors that inflated duration without real complexity |
| `duration_vs_complexity_note`| One sentence distinguishing time from work           |

## Configuration

All settings live in `config.py` and can be overridden with environment variables:

| Env Var         | Default                | Purpose                              |
|-----------------|------------------------|--------------------------------------|
| `TS_BASE`       | TeamSupport NA2 URL    | API base URL                         |
| `TS_KEY`        | `9980809a-…57a2`       | API key                              |
| `TS_USER_ID`       | `1189708`              | API user ID                          |
| `MATCHA_URL`       | Matcha completions URL | Matcha LLM endpoint                  |
| `MATCHA_API_KEY`   | *(set)*                | Matcha API key                       |
| `MATCHA_MISSION_ID`| `27301`                | Matcha mission ID                    |
| `MAX_TICKETS`      | `5`                    | Max tickets to pull (0=unlimited)    |
| `TARGET_TICKET`    | *(empty)*              | Comma-delimited ticket number(s) to target |
| `RUN_SENTIMENT`    | `1`                    | Run sentiment analysis (0 = skip)    |
| `RUN_PRIORITY`     | `1`                    | Run AI priority scoring (0 = skip)   |
| `RUN_COMPLEXITY`   | `1`                    | Run complexity analysis (0 = skip)   |
| `LOG_TO_FILE`      | `1`                    | Save pipeline logs to output dir     |
| `LOG_API_CALLS`    | `1`                    | Save API call log to output dir      |
| `CUST_COMMENT_COUNT`| `4`                   | Customer comments sent to Matcha     |
| `OUTPUT_DIR`       | `./output`             | Where JSONL files are written        |
| `DATABASE_URL`     | *(empty)*              | Postgres DSN; empty = JSON-only mode |
| `DATABASE_SCHEMA`  | `tickets_ai`           | Postgres schema for all pipeline tables |

## Logging

When `LOG_TO_FILE=1` (default), all pipeline stdout/stderr is teed to `output/pipeline_YYYYMMDD_HHMMSS.log`.

When `LOG_API_CALLS=1` (default), every TeamSupport and Matcha API call (GET, PUT, POST) is recorded in `output/api_calls.json` with timestamp, method, URL, params/payload, and HTTP status code. Matcha entries include `input_length` (prompt size) instead of the full prompt text.

## Dry-Run Payloads

When the TeamSupport API is rate-limited (403) or the ticket has no `ticket_id` (CSV-sourced data), write-back payloads are saved to `output/api_payloads_dry_run.json` instead of being sent to the API. Each entry contains:

| Field       | Description                              |
|-------------|------------------------------------------|
| `timestamp` | UTC time the payload was generated       |
| `method`    | HTTP method (`PUT`)                      |
| `url`       | TeamSupport API URL that would be called |
| `payload`   | Full request body (`{"Ticket": {...}}`)  |

This allows verification of payload contents when the API is unavailable.

## Database (Postgres)

### Overview

When `DATABASE_URL` is set, the pipeline can persist tickets, actions, enrichment results, and sync state to a Postgres database via `db.py`. When `DATABASE_URL` is empty (the default), the pipeline runs in JSON-only mode — all existing scripts work unchanged.

### Setup

```bash
# 1. Set the connection string (local dev example)
export DATABASE_URL="postgresql://user:pass@localhost:5432/Work"

# 2. (Optional) Override the schema name — default is tickets_ai
# export DATABASE_SCHEMA="tickets_ai"

# 3. Apply migrations (idempotent — safe to re-run)
#    Creates the tickets_ai schema and all tables inside it.
python db.py migrate
```

### Schema

All tables are created inside the `tickets_ai` Postgres schema (configurable via `DATABASE_SCHEMA`). Migrations live in `migrations/` and are applied in filename order. The `_migrations` tracking table also lives in `tickets_ai`.

| Table                      | Purpose                                                |
|----------------------------|--------------------------------------------------------|
| `tickets`                  | Canonical ticket rows (upserted by `ticket_id`)       |
| `ticket_actions`           | Canonical action rows (upserted by `action_id`)       |
| `sync_state`               | Per-source watermark / cursor tracking                 |
| `ingest_runs`              | Audit log of each ingestion run                        |
| `ticket_thread_rollups`    | Concatenated thread texts and hashes per ticket        |
| `ticket_metrics`           | Computed counts and timestamps per ticket              |
| `ticket_sentiment`         | Sentiment enrichment results (append-only)             |
| `ticket_priority_scores`   | AI priority enrichment results (append-only)           |
| `ticket_complexity_scores` | Complexity enrichment results (append-only)            |

### Upsert helpers

`db.py` provides idempotent helpers used by future ingestion phases:

- `upsert_ticket(ticket_dict)` — INSERT ON CONFLICT UPDATE by `ticket_id`
- `upsert_action(action_dict)` — INSERT ON CONFLICT UPDATE by `action_id`
- `upsert_sync_state(source_name, status=..., ...)` — track sync progress
- `create_ingest_run(source_name)` / `complete_ingest_run(run_id, ...)` — audit trail

### Enrichment persistence helpers

- `insert_sentiment(ticket_id, ...)` — append to `ticket_sentiment`
- `insert_priority(ticket_id, ...)` — append to `ticket_priority_scores`
- `insert_complexity(ticket_id, ...)` — append to `ticket_complexity_scores` (full rich schema)
- `get_latest_enrichment_hash(ticket_id, enrichment_type)` — return most recent content hash for skip checks
- `get_current_hashes(ticket_id)` — return `thread_hash` and `technical_core_hash` from `ticket_thread_rollups`
- `load_ticket_with_actions(ticket_id)` — load ticket + actions in JSON-pipeline shape
- `ticket_ids_for_numbers(ticket_numbers)` — batch resolve ticket_number → ticket_id

All helpers are no-ops when the DB is unavailable. Repeated calls with the same primary key are safe.

## Running

### JSON Pipeline (existing)

```bash
# Default: pulls 5 tickets
python run_pull_activities.py

# Target a specific ticket (or multiple, comma-delimited)
TARGET_TICKET=29696 python run_pull_activities.py

# Target multiple tickets
TARGET_TICKET=29696,110554 python run_pull_activities.py

# Override limit
MAX_TICKETS=20 python run_pull_activities.py

# Unlimited
MAX_TICKETS=0 python run_pull_activities.py

# Part 2: Sentiment analysis (requires activities JSON from Part 1)
TARGET_TICKET=29696 python run_sentiment.py

# Override number of customer comments sent
TARGET_TICKET=29696 CUST_COMMENT_COUNT=6 python run_sentiment.py

# Part 3: AI priority scoring + write-back to TeamSupport
TARGET_TICKET=29696 python run_priority.py

# Part 4: Complexity analysis
python run_complexity.py

# Skip specific stages
RUN_SENTIMENT=0 python run_sentiment.py   # skips sentiment
RUN_PRIORITY=0 python run_priority.py     # skips priority
RUN_COMPLEXITY=0 python run_complexity.py  # skips complexity

# Orchestrator: run all stages in sequence
TARGET_TICKET=110554 python run_all.py

# Orchestrator with --force (rerun enrichments even if hashes unchanged)
TARGET_TICKET=110554 python run_all.py --force

# Orchestrator dry-run (skip TS write-back, just score and persist to DB)
TARGET_TICKET=110554 python run_all.py --no-writeback

# Orchestrator with stages skipped
TARGET_TICKET=110554 RUN_SENTIMENT=0 python run_all.py

# Orchestrator: pull only (skip all analysis)
TARGET_TICKET=110554 RUN_SENTIMENT=0 RUN_PRIORITY=0 RUN_COMPLEXITY=0 python run_all.py
```

### DB Ingestion (run_ingest.py)

Requires `DATABASE_URL` to be set. Does not replace the JSON pipeline above.

```bash
# Incremental sync — fetches open tickets (respects MAX_TICKETS)
DATABASE_URL="postgresql://user:pass@localhost:5432/Work" python run_ingest.py sync

# Resync a single ticket by number
python run_ingest.py sync --ticket 29696

# Resync multiple tickets
python run_ingest.py sync --ticket 29696,110554

# Replay tickets modified in a recent window
python run_ingest.py sync --since 2026-03-01

# Fetch all open tickets (ignore MAX_TICKETS)
python run_ingest.py sync --all

# Dry-run — fetch from TS API but don't write to DB
python run_ingest.py sync --ticket 29696 --dry-run --verbose

# Show sync state and recent ingest runs
python run_ingest.py status
```

**Notes:**
- Upserts are idempotent — repeated runs converge to the same state.
- Raw TS payloads are stored in `source_payload` JSONB columns.
- `ingest_runs` records start/finish/counts/errors for each run.
- `sync_state` tracks last successful sync.
- The TS API does not support server-side `DateModifiedSince` filtering; `--since` does a full fetch and filters locally.

### Analytical Derived Layer (run_rollups.py)

Requires `DATABASE_URL` to be set and tickets/actions to be ingested via `run_ingest.py`. No TeamSupport API calls — reads entirely from DB.

```bash
# Classify all actions using deterministic rules
python run_rollups.py classify

# Classify actions for a single ticket
python run_rollups.py classify --ticket 29696

# Rebuild thread rollups (concatenated texts + hashes)
python run_rollups.py rollups

# Rebuild metrics (counts, timestamps, handoffs)
python run_rollups.py metrics

# Run all three stages: classify → rollups → metrics
python run_rollups.py all

# All stages for a single ticket
python run_rollups.py all --ticket 29696
```

**Action classes** (assigned by `action_classifier.py`):

| Class                      | Meaning                                    |
|----------------------------|--------------------------------------------|
| `technical_work`           | Code, SQL, configs, bugs, testing, deploy  |
| `customer_problem_statement` | Customer describing their issue/request  |
| `status_update`            | Brief progress notes, FYI, checking in     |
| `scheduling`               | Meeting coordination, timeslots            |
| `waiting_on_customer`      | inHANCE awaiting file/review/approval      |
| `delivery_confirmation`    | Confirming deployment/completion           |
| `administrative_noise`     | Signatures, greetings, one-word ACKs       |
| `system_noise`             | Auto-generated, system events, empty       |
| `unknown`                  | No patterns matched                        |

**Thread rollup fields** (per ticket in `ticket_thread_rollups`):
- `full_thread_text` — all non-empty actions concatenated
- `customer_visible_text` — non-noise actions only
- `technical_core_text` — technical substance only
- `latest_customer_text` / `latest_inhance_text` — most recent message per party
- `summary_for_embedding` — customer_visible capped at ~4000 chars
- `thread_hash` / `technical_core_hash` — SHA-256 for change detection

**Metrics** (per ticket in `ticket_metrics`):
- `action_count`, `nonempty_action_count`, `customer_message_count`, `inhance_message_count`
- `distinct_participant_count`, `first_response_at`, `last_human_activity_at`
- `empty_action_ratio`, `handoff_count`

### Enrichment Persistence (Phase 4)

When `DATABASE_URL` is set, `run_sentiment.py`, `run_priority.py`, and `run_complexity.py` gain:

1. **DB as data source** — Can read ticket data from DB instead of requiring a JSON activities file.
2. **Hash-based skipping** — Before calling Matcha, each script checks whether the relevant content hash has changed since the last enrichment run:
   - Sentiment and priority compare `thread_hash` from `ticket_thread_rollups` against the `thread_hash` stored in their most recent enrichment row.
   - Complexity compares `technical_core_hash` from `ticket_thread_rollups` against the `technical_core_hash` stored in its most recent enrichment row.
   - If the hash matches, the ticket is skipped (no LLM call). Use `--force` to override.
3. **Durable persistence** — Results are appended (not upserted) to the enrichment tables, preserving full scoring history.
4. **JSON artifact emission** — JSON output files are still generated for compatibility.

**Important:** Rollups must be built before enrichment for hashes to exist. Run `python run_rollups.py all` after ingestion.

```bash
# Sentiment — reads from DB, skips unchanged, persists to ticket_sentiment
TARGET_TICKET=29696 RUN_SENTIMENT=1 python run_sentiment.py

# Sentiment — force rerun even if hash unchanged
TARGET_TICKET=29696 RUN_SENTIMENT=1 python run_sentiment.py --force

# Priority — reads from DB, skips unchanged, persists to ticket_priority_scores
TARGET_TICKET=29696 python run_priority.py
TARGET_TICKET=29696 python run_priority.py --force

# Complexity — reads from DB, skips unchanged on technical_core_hash, persists to ticket_complexity_scores
TARGET_TICKET=29696 python run_complexity.py
TARGET_TICKET=29696 python run_complexity.py --force
```

**Enrichment table metadata stored per row:**

| Field             | Description                                      |
|-------------------|--------------------------------------------------|
| `ticket_id`       | FK to tickets                                    |
| `thread_hash` / `technical_core_hash` | Content hash at time of scoring |
| `model_name`      | e.g. `matcha-27301`                              |
| `prompt_name`     | e.g. `sentiment`, `ai_priority`, `complexity`    |
| `prompt_version`  | Version string for the prompt                    |
| `scored_at`       | Timestamp of scoring (auto-set)                  |
| `raw_response`    | Full Matcha response (JSONB)                     |

**Complexity fields persisted** (in `ticket_complexity_scores`):
`intrinsic_complexity`, `coordination_load`, `elapsed_drag`, `overall_complexity`, `confidence`, `primary_complexity_drivers` (JSONB), `complexity_summary`, `evidence` (JSONB), `noise_factors` (JSONB), `duration_vs_complexity_note`.

### JSON Artifact Export (run_export.py)

Export canonical DB state to the same timestamped JSON artifacts the original pipeline produces. Requires `DATABASE_URL`.

```bash
# Export activities JSON from DB
python run_export.py activities
python run_export.py activities --ticket 29696

# Export latest enrichment scores
python run_export.py sentiment
python run_export.py priority
python run_export.py complexity

# Export all artifact types at once
python run_export.py all
python run_export.py all --ticket 29696
```

JSON artifacts are now **exports from the canonical DB**, not the primary data store. The enrichment scripts also still emit JSON artifacts inline during scoring.

---

## Quickstart — How to Run This System

### Prerequisites

- Python 3.13+
- Postgres (local or remote) — optional, enables DB mode
- TeamSupport API credentials (set in environment or `config.py`)
- Matcha API credentials (set in environment or `config.py`)

```bash
pip install -r requirements.txt
```

### Mode 1: JSON-Only (original workflow, no Postgres)

```bash
# Pull activities and run all stages
TARGET_TICKET=29696 python run_all.py

# Or run stages individually
TARGET_TICKET=29696 python run_pull_activities.py
TARGET_TICKET=29696 python run_sentiment.py
TARGET_TICKET=29696 python run_priority.py
TARGET_TICKET=29696 python run_complexity.py
```

No `DATABASE_URL` needed. JSON artifacts are the only output.

### Mode 2: DB-Backed (canonical Postgres store)

```bash
# 1. Set up database
export DATABASE_URL="postgresql://user:pass@localhost:5432/Work"
python db.py migrate

# 2. Ingest tickets from TeamSupport into DB
python run_ingest.py sync --ticket 29696
# or: python run_ingest.py sync --all

# 3. Build rollups (classification + thread texts + hashes + metrics)
python run_rollups.py all
# or for one ticket: python run_rollups.py all --ticket 29696

# 4. Run enrichments (reads from DB, skips unchanged, persists to DB)
TARGET_TICKET=29696 python run_sentiment.py
TARGET_TICKET=29696 python run_priority.py
TARGET_TICKET=29696 python run_complexity.py

# 5. (Optional) Export JSON artifacts from DB
python run_export.py all

# 6. (Optional) Write back to TeamSupport
TARGET_TICKET=29696 python run_all.py          # full pipeline with write-back
TARGET_TICKET=29696 python run_all.py --no-writeback  # dry-run
```

### Common Operational Workflows

| Workflow | Command |
|----------|---------|
| **Incremental sync** | `python run_ingest.py sync` |
| **Single-ticket resync** | `python run_ingest.py sync --ticket 29696` |
| **Check sync status** | `python run_ingest.py status` |
| **Rebuild rollups/metrics** | `python run_rollups.py all` |
| **Rebuild for one ticket** | `python run_rollups.py all --ticket 29696` |
| **Rerun enrichments** | `TARGET_TICKET=29696 python run_sentiment.py` |
| **Force rerun (skip hash check)** | `TARGET_TICKET=29696 python run_sentiment.py --force` |
| **Force all enrichments** | `TARGET_TICKET=29696 python run_all.py --force` |
| **Export JSON from DB** | `python run_export.py all` |
| **Dry-run write-back** | `TARGET_TICKET=29696 python run_all.py --no-writeback` |
| **Full pipeline + write-back** | `TARGET_TICKET=29696 python run_all.py` |

### Canonical DB Concept

The Postgres database is the **canonical store** for all ticket data, action history, rollups, metrics, and enrichment results. JSON files are **artifacts/exports** — convenient snapshots, not the source of truth.

- **Idempotent upserts**: Re-ingesting a ticket converges to the same state. No duplicates.
- **Append-only enrichment**: Enrichment tables (`ticket_sentiment`, `ticket_priority_scores`, `ticket_complexity_scores`) are append-only. Each scoring run adds a new row. Historical scores are never deleted.
- **Hash-based skipping**: Content hashes (`thread_hash`, `technical_core_hash`) in `ticket_thread_rollups` are compared against the most recent enrichment row. If unchanged, the LLM call is skipped. Use `--force` to override.
- **Replay/resync**: Run `python run_ingest.py sync --ticket <num>` to re-fetch from the TS API, then `python run_rollups.py all --ticket <num>` to rebuild derived data. Enrichments will automatically detect the hash change and rescore on next run.

### Compatibility Notes

- **No breaking changes**: All original JSON-only workflows work identically when `DATABASE_URL` is not set.
- **JSON artifacts**: Still generated by enrichment scripts and by `run_export.py`. Output schemas are unchanged.
- **Write-back**: `ts_client.update_ticket()` continues to inject `LastInhComment` and `LastCustComment` automatically from the activity history.
- **run_all.py**: Now accepts `--force` and `--no-writeback` flags. Without flags, behaviour is identical to the original.
