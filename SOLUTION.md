# Solution Overview

## Purpose

Pull open-ticket activities from TeamSupport, cleanse the text, classify each activity by party (inHANCE team vs customer), write them to a timestamped JSON file, run sentiment analysis, AI priority scoring, and complexity analysis via Matcha LLM.

## Architecture

```
config.py              — Centralised settings (API creds, limits, target ticket, output dir, stage toggles, DATABASE_URL, SAFETY_BUFFER_MINUTES, INITIAL_BACKFILL_DAYS)
ts_client.py           — TeamSupport REST API client (fetch tickets, activities, inHANCE users, all-users name→ID mapping, update_ticket with auto LastInhComment/LastCustComment, fetch_ticket_by_id)
activity_cleaner.py    — Text-cleaning pipeline (HTML→text, boilerplate/signature removal)
matcha_client.py       — Matcha LLM API client (send prompts, extract responses)
db.py                  — Postgres data-access layer (connection pool, migration runner, upsert helpers, enrichment persistence, get_sync_state watermark reader)
run_ingest.py          — DB-backed incremental ingestion CLI (watermark-based sync, single-ticket resync, replay mode, status)
action_classifier.py   — Deterministic rule-based action classification (no LLM; action_type='Description' → customer_problem_statement)
run_rollups.py         — Rebuild action classification, thread rollups, and metrics from DB state
run_all.py             — Orchestrator: runs all stages in sequence, merges fields, single API call per ticket (--force, --no-writeback)
run_enrich_db.py       — DB-only enrichment: score all closed (or filtered) tickets from Postgres, no TS API calls
run_csv_import.py      — Bulk-import Activities.csv into DB (synthetic action IDs, streaming, idempotent)
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
| `TS_WRITEBACK`     | `0`                    | Enable TeamSupport write-back (`1` = on, `0` = off) |
| `SKIP_OUTPUT_FILES`| `0`                    | Skip JSON artifact files when DB is active (`1` = skip) |

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
| `ticket_wait_states`       | Wait-state segments per ticket (003)                   |
| `ticket_participants`      | Per-ticket participant roster with counts (003)        |
| `ticket_handoffs`          | Inferred handoff events between parties (003)          |
| `ticket_snapshots_daily`   | Daily point-in-time ticket snapshots (003)             |
| `ticket_issue_summaries`   | LLM-generated issue/cause/mechanism summaries (003)    |
| `ticket_embeddings`        | Stored embedding vectors per ticket (003)              |
| `cluster_runs`             | Clustering run metadata (003)                          |
| `ticket_clusters`          | Ticket-to-cluster assignments (003)                    |
| `cluster_catalog`          | Cluster labels and descriptions (003)                  |
| `ticket_interventions`     | Recommended interventions per ticket (003)             |
| `customer_ticket_health`   | Daily customer-level health aggregates (003)           |
| `product_ticket_health`    | Daily product-level health aggregates (003)            |
| `enrichment_runs`          | Enrichment run audit log (003)                         |

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
# Incremental sync — reads watermark from sync_state, applies safety buffer,
# fetches open tickets, and post-filters to those modified since the watermark.
DATABASE_URL="postgresql://user:pass@localhost:5432/Work" python run_ingest.py sync

# Resync a single ticket by TicketNumber
python run_ingest.py sync --ticket 29696

# Resync multiple tickets by TicketNumber
python run_ingest.py sync --ticket 29696,110554

# Resync a single ticket by internal TicketID
python run_ingest.py sync --ticket-id 12345

# Replay tickets modified in a recent window (explicit date)
python run_ingest.py sync --since 2026-03-01

# Replay the last N days (shorthand for --since)
python run_ingest.py sync --days 7

# Fetch all open tickets (ignore MAX_TICKETS)
python run_ingest.py sync --all

# Dry-run — fetch from TS API but don't write to DB
python run_ingest.py sync --ticket 29696 --dry-run --verbose

# Show sync state and recent ingest runs
python run_ingest.py status
```

**Incremental sync details:**

- **Watermark**: On each normal sync, `sync_state.last_successful_sync_at` is
  read.  If present, `SAFETY_BUFFER_MINUTES` (default 10) is subtracted to
  produce the effective `from_ts`.  Tickets with `DateModified < from_ts` are
  skipped.  The overlap is safe because all writes use `ON CONFLICT … DO UPDATE`
  (idempotent upserts).
- **First run / no watermark**: If no watermark exists, the sync fetches all
  open tickets (full backfill).  Set `INITIAL_BACKFILL_DAYS` to limit to a
  recent window instead.
- **Watermark advancement**: `last_successful_sync_at` is advanced **only**
  when a normal incremental sync completes successfully.  Targeted syncs
  (`--ticket`, `--ticket-id`, `--since`, `--days`) never advance the watermark,
  because they process a subset and cannot vouch for completeness.
- **Failure safety**: If a run fails, the watermark stays put.  The next run
  replays the same window, converging to correctness without duplicates.
- **Replay safety**: Re-running the same time window is idempotent — it
  produces the same DB state without duplicate rows.

**Notes:**
- Upserts are idempotent — repeated runs converge to the same state.
- Raw TS payloads are stored in `source_payload` JSONB columns.
- `ingest_runs` records start/finish/counts/errors for each run.
- `sync_state` tracks last successful sync.
- The TS API does not support server-side `DateModifiedSince` filtering; the sync fetches all qualifying tickets and filters locally.

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
- `full_thread_text` — all non-empty actions concatenated; each prefixed with `[YYYY-MM-DD HH:MM | CreatorName]` for LLM sequence interpretation
- `customer_visible_text` — non-noise actions only (same timestamped prefix)
- `technical_core_text` — technical substance only (same timestamped prefix)
- `latest_customer_text` / `latest_inhance_text` — most recent message per party
- `summary_for_embedding` — customer_visible capped at ~4000 chars
- `thread_hash` / `technical_core_hash` — SHA-256 for change detection

**Metrics** (per ticket in `ticket_metrics`):
- `action_count`, `nonempty_action_count`, `customer_message_count`, `inhance_message_count`
- `distinct_participant_count`, `first_response_at`, `last_human_activity_at`
- `empty_action_ratio`, `handoff_count`
- `date_created`, `hours_to_first_response`, `days_open`

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

### Mode 3: DB-Only Enrichment (no TS API calls)

Score all closed tickets using data already in Postgres. Only Matcha LLM calls are made. Hash-based skipping ensures only new/changed tickets are scored.

```bash
# Score all closed tickets (priority + complexity)
python run_enrich_db.py

# Limit to first 100 tickets
python run_enrich_db.py --limit 100

# Priority only, batch size 10
python run_enrich_db.py --priority-only --batch-size 10

# Complexity only
python run_enrich_db.py --complexity-only

# Include sentiment
python run_enrich_db.py --sentiment

# Force rescore (ignore hash-based skip)
python run_enrich_db.py --force --limit 50

# Target Open tickets instead of Closed
python run_enrich_db.py --status Open
```

**Key behaviours:**
- **No TS API calls** — reads entirely from the Postgres DB.
- **Hash-based skipping** — tickets already scored with the same content hash are skipped automatically. Use `--force` to override.
- **Batched priority** — priority scoring sends `--batch-size` tickets (default 20) per Matcha call.
- **Per-ticket complexity** — complexity processes one ticket per Matcha call (inherent to the prompt design).
- **Resilient** — errors in one batch don't stop the pipeline; processing continues with the next batch.

### Mode 4: CSV Bulk Import (initial DB population without API)

Use this when you have a large TeamSupport CSV export and want to populate the DB without burning API requests.

```bash
# 1. Set up database
export DATABASE_URL="postgresql://user:pass@localhost:5432/Work"
python db.py migrate

# 2. Bulk-import from CSV
python run_csv_import.py                     # all rows
python run_csv_import.py --ticket 109683     # specific tickets
python run_csv_import.py --dry-run            # preview without writing
python run_csv_import.py --verbose            # per-ticket detail

# 3. Build rollups (required before enrichments)
python run_rollups.py all

# 4. Run enrichments
TARGET_TICKET=109683 python run_sentiment.py
TARGET_TICKET=109683 python run_priority.py
TARGET_TICKET=109683 python run_complexity.py
```

**CSV column mapping:**

| CSV Column | DB Column | Notes |
|---|---|---|
| `Ticket ID` | `tickets.ticket_id` | Primary key (BIGINT) |
| `Ticket Number` | `tickets.ticket_number` | Display number |
| `Ticket Name` | `tickets.ticket_name` | Subject line |
| `Ticket Product Name` | `tickets.product_name` | |
| `Primary Customer` | `tickets.customer` | |
| `Severity` | `tickets.severity` | |
| `Date Ticket Created` | `tickets.date_created` | Parsed to UTC |
| `Is Closed` | `tickets.status` | Mapped to Open/Closed |
| `Group Name` | `tickets.assignee` | |
| `Action Description` | `ticket_actions.description` | Raw + cleaned |
| `Action Type` | `ticket_actions.action_type` | |
| `Date Action Created` | `ticket_actions.created_at` | Parsed to UTC |
| `Action Creator Name` | `ticket_actions.creator_name` | |

**Key behaviours:**
- **No Action ID in CSV**: Synthetic deterministic `action_id` generated from SHA-256 of `(ticket_id, date_created, description[:200])`. Re-importing the same CSV produces identical IDs.
- **Creator ID resolution**: `creator_id` is resolved via `ts_client.fetch_all_users()` which provides a name→ID mapping from the TS API. Party detection uses the inHANCE user ID set for authoritative `inh`/`cust` classification.
- **Streaming**: CSV is processed row-by-row; the entire file is never loaded into memory.
- **Idempotent**: Re-running the import converges to the same state (upsert semantics).
- **Ingest run tracking**: Each import is recorded in `ingest_runs` with source `csv_import`.
- **Bonus columns** (`Ticket Source`, `Ticket Type`, `Action Hours Spent`, `Action Source`) are stored in `source_payload` JSONB.

### Common Operational Workflows

| Workflow | Command |
|----------|---------|
| **CSV bulk import** | `python run_csv_import.py` |
| **CSV import (specific tickets)** | `python run_csv_import.py --ticket 109683` |
| **CSV import dry-run** | `python run_csv_import.py --dry-run` |
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
| **Score all closed tickets** | `python run_enrich_db.py` |
| **Score closed (limit 100)** | `python run_enrich_db.py --limit 100` |
| **Priority only (DB)** | `python run_enrich_db.py --priority-only` |
| **Complexity only (DB)** | `python run_enrich_db.py --complexity-only` |

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

### Write-Back & Output Controls

Two config flags control side-effects:

- **`TS_WRITEBACK`** (default `0`): When `0`, no enrichment data is written back to TeamSupport — even via `run_all.py`. The `--no-writeback` CLI flag always wins (overrides `TS_WRITEBACK=1`). When run standalone, `run_priority.py` and `run_complexity.py` also respect this default (their `write_back` parameter defaults to `TS_WRITEBACK` when not explicitly passed).

- **`SKIP_OUTPUT_FILES`** (default `0`): When `1`, **no** files are written to the `output/` directory during enrichment — this includes activities JSON, enrichment result JSON, pipeline log files, and API call logs. This is useful when the DB is the canonical store and file artifacts are unwanted. When `0`, all files are written as usual.

## Tests

```bash
pip install pytest
python -m pytest tests/ -v
```

Test coverage includes:
- **Action classifier** (18 tests): all classification categories, noise/substance helpers
- **Activity cleaner** (9 tests): HTML conversion, boilerplate removal, party classification
- **DB upserts** (11 tests): idempotent INSERT ON CONFLICT, first_ingested_at stability, sync_state, ingest_runs
- **Incremental sync** (9 tests): watermark reading, safety buffer filtering, watermark advancement on success, no advancement on targeted/replay/failed syncs, empty result handling
- **Hash-based skipping** (9 tests): all three enrichment types, force override, missing rollups
- **Write-back** (5 tests): LastInhComment/LastCustComment timestamp derivation
- **Operational fixes** (11 tests): cache poisoning, atomic writes, retry logic, transaction batching

## Audit Summary (2026-03-13)

### Architecture as Implemented

The codebase implements a dual-mode pipeline:

1. **JSON-only mode** (DATABASE_URL unset): `run_pull_activities.py` → `run_sentiment.py` → `run_priority.py` → `run_complexity.py` → TeamSupport write-back. All data flows through timestamped JSON files.
2. **DB-backed mode** (DATABASE_URL set): `run_ingest.py` → `run_rollups.py` → enrichment scripts read/write DB. JSON files become exports/artifacts.

Both modes coexist cleanly. The orchestrator (`run_all.py`) uses mode 1 for pulling, but enrichment scripts auto-detect DB availability and read from DB when possible.

### Verified ✓

| Item | Status |
|------|--------|
| Postgres-backed canonical store | ✓ Implemented and used by run_ingest, run_rollups, run_export, and enrichment scripts |
| Idempotent upserts (tickets + actions) | ✓ ON CONFLICT DO UPDATE; first_ingested_at stable; last_ingested_at/last_seen_at update |
| Incremental sync safety | ✓ sync_state only advances on success; ingest_runs record failures |
| sync_state / ingest_runs | ✓ Correctly implemented with proper failure tracking |
| Raw + normalized storage | ✓ source_payload JSONB stores raw; normalized columns extracted |
| Rollups rebuiltable from DB | ✓ run_rollups.py reads only from DB; no API calls |
| Action classification | ✓ Deterministic rule-based; 9 categories; well-documented |
| Enrichment persistence | ✓ Append-only tables with full metadata and raw response |
| Hash-based skipping | ✓ thread_hash for sentiment/priority; technical_core_hash for complexity; --force override |
| JSON artifacts still work | ✓ Still emitted by enrichment scripts; run_export.py generates from DB |
| TeamSupport write-back | ✓ LastInhComment/LastCustComment auto-injected by update_ticket() |
| CLI coherence | ✓ All scripts have --help, consistent argument patterns |

### Issues Found and Fixed

1. **Missing .gitignore** — `.DS_Store` and `__pycache__/*.pyc` were committed to the repository. Added `.gitignore` and removed tracked artifacts.

2. **No auto-migrate in DB-backed scripts** — `run_ingest.py sync`, `run_rollups.py`, and `run_export.py` all require the database schema to exist, but none called `db.migrate()`. A fresh `DATABASE_URL` against a new database would fail. Added `db.migrate()` calls after the `db._is_enabled()` check in each script.

3. **No test suite** — Added 53 tests covering action classification, activity cleaning, DB upsert idempotency, hash-based skip logic, and write-back timestamp derivation.

4. **Minimal README.md** — Expanded with quick start instructions and a pointer to SOLUTION.md.

### Remaining Notes (Not Bugs)

- **API keys in config.py defaults**: `TS_KEY` and `MATCHA_API_KEY` have hardcoded defaults. These are already in git history and appear to be development/test credentials. Production deployments should override via environment variables.

- **`db._is_enabled()` used externally**: The underscore-prefixed function is called from multiple scripts. This is a minor code smell but not a bug; the function is a stable utility.

- **Python version**: SOLUTION.md states Python 3.13+ but the code uses `str | None` syntax which requires Python 3.10+. No functional issue.

- **`run_all.py` uses JSON pipeline for pull**: The orchestrator calls `run_pull_activities.py` (JSON mode) and does not call `run_ingest.py`. This is intentional — the orchestrator serves the JSON-based workflow while `run_ingest.py` serves the DB-backed workflow.

## Operational Failure Mode Fixes

Red-team review of the pipeline identified and fixed the following production failure modes:

### 1. inHANCE User Cache Poisoning (ts_client.py)

**Problem:** When the `/Users?Organization=inHANCE` API call failed (timeout, 403, network error), `_INHANCE_IDS` was set to an empty set and cached permanently. All subsequent `is_inhance_user()` calls returned `False`, silently misclassifying every inHANCE support agent as a customer for the entire process lifetime.

**Fix:** On API failure, return an empty set but do **not** cache it. The next call retries the API.

### 2. COALESCE Masks Real NULL Updates (db.py)

**Problem:** `upsert_ticket` used `COALESCE(EXCLUDED.assignee, tickets.assignee)` for all fields. If a ticket's assignee was genuinely removed in TeamSupport (set to NULL), the DB would silently retain the stale old value. Same for status, severity, customer, and other mutable fields.

**Fix:** Mutable fields (status, severity, assignee, customer, ticket_name, product_name, date_modified, closed_at, days_opened, days_since_modified) now use `EXCLUDED.value` directly. COALESCE is only used for immutable fields (date_created, ticket_number) and externally-managed fields (action_class, source_payload). Same fix applied to `upsert_action`.

### 3. Non-Atomic JSON File Writes (ts_client.py)

**Problem:** `_log_api_call()` and `save_dry_run_payload()` performed read-modify-write on shared JSON files (`api_calls.json`, `api_payloads_dry_run.json`). If the process crashed mid-write or two processes ran concurrently, the file would be corrupted (truncated or partially written).

**Fix:** Write to a temporary file (`.tmp` suffix) then atomically swap with `os.replace()`. On POSIX systems this is an atomic rename operation.

### 4. Matcha Client Missing 5xx Retry (matcha_client.py)

**Problem:** `call_matcha()` only retried on `ConnectionError` and `Timeout` exceptions. HTTP 500/502/503 server errors (common transient failures) were not retried — a single server hiccup would fail the entire enrichment run.

**Fix:** Added 5xx status code check with exponential backoff retry, consistent with the existing retry logic for connection/timeout errors.

### 5. Per-Ticket Transaction Batching (db.py, run_ingest.py)

**Problem:** `run_ingest.py` called `upsert_ticket()` followed by `upsert_action()` for each action separately. Each was a separate DB transaction. If the process crashed after upserting 5 of 10 actions, the ticket would have partial data in the DB.

**Fix:** Added `upsert_ticket_with_actions()` that executes the ticket upsert and all its action upserts within a single database transaction. If any action fails, the entire batch (including the ticket) is rolled back.

### 6. Rate-Limited Write-Backs Counted as Successes (run_all.py, run_priority.py, run_complexity.py)

**Problem:** When the TeamSupport API returned 403 (rate-limited), the write-back code incremented `updated += 1`, treating the rate-limited response as a successful update. This overstated success counts in logs and could mask systematic rate-limiting issues.

**Fix:** Rate-limited responses are now tracked separately as `deferred` and reported distinctly in log output (e.g., "3/5 updated, 2 deferred (rate-limited)").

### Tests

All fixes are covered by `tests/test_operational_fixes.py` (11 tests).
