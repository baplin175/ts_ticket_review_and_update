# Solution Overview

## Purpose

Pull open-ticket activities from TeamSupport, cleanse the text, classify each activity by party (inHANCE team vs customer), write them to a timestamped JSON file, run sentiment analysis, AI priority scoring, and complexity analysis via Matcha LLM.

## Domain Model

See [DOMAIN_MODEL.md](DOMAIN_MODEL.md) for the complete operational domain model, including system purpose, end-to-end data pipeline, table semantics, LLM pipeline design, failure ontology, clustering system, intervention model, and product/domain knowledge.

## Architecture

```
config.py              — Centralised settings (API creds, limits, target ticket, output dir, stage toggles, FORCE_ENRICHMENT, DATABASE_URL, SAFETY_BUFFER_MINUTES, INITIAL_BACKFILL_DAYS)
ts_client.py           — TeamSupport REST API client (fetch tickets, activities, inHANCE users, all-users name→ID mapping, update_ticket with auto LastInhComment/LastCustComment, fetch_ticket_by_id)
activity_cleaner.py    — Text-cleaning pipeline (HTML→text, boilerplate/signature removal)
matcha_client.py       — Matcha LLM API client (send prompts, extract responses)
db.py                  — Postgres data-access layer (connection pool, migration runner, upsert helpers, enrichment persistence, get_sync_state watermark reader)
run_ingest.py          — DB-backed incremental ingestion CLI (watermark-based sync with automatic created-since merge for opened+closed tickets, single-ticket resync, replay mode, --sentiment flag for post-sync enrichment, status)
action_classifier.py   — Deterministic rule-based action classification (no LLM; action_type='Description' → customer_problem_statement)
run_rollups.py         — Rebuild action classification, thread rollups, metrics, and daily open counts from DB state; run_full_rollups() auto-triggers after any new migration
run_all.py             — Orchestrator: runs all stages in sequence, merges fields, single API call per ticket (--force, --no-writeback)
run_enrich_db.py       — DB-only enrichment: score tickets from Postgres (priority + complexity + sentiment by default), no TS API calls
run_csv_import.py      — Bulk-import Activities.csv into DB (synthetic action IDs, streaming, idempotent, derives closed_at from Days Closed)
run_export.py          — Export canonical DB state to timestamped JSON artifacts
run_pull_activities.py — Part 1: fetch, clean, classify activities → activities JSON
run_sentiment.py       — Part 2: sentiment analysis via Matcha (DB persistence + hash-based skipping when DB available)
run_priority.py        — Part 3: AI priority scoring via Matcha (DB persistence + hash-based skipping when DB available)
run_complexity.py      — Part 4: complexity analysis via Matcha (DB persistence + hash-based skipping when DB available)
run_ticket_pass1.py    — Pass 1: phenomenon extraction from full_thread_text via Matcha (DB-only, idempotent, prompt-versioned)
pass1_parser.py        — Pass 1 response parser (strict JSON validation, phenomenon extraction; null phenomenon accepted as valid "no observable behavior")
run_ticket_pass2.py    — Pass 2: canonical failure grammar from Pass 1 phenomenon via Matcha (DB-only, idempotent, prompt-versioned)
pass2_parser.py        — Pass 2 response parser (strict JSON validation, operation normalization, canonical_failure reconstruction)
run_ticket_pass3.py    — Pass 3: failure mechanism inference from Pass 2 canonical_failure via Matcha (DB-only, idempotent, prompt-versioned)
pass3_parser.py        — Pass 3 response parser (strict JSON validation, mechanism extraction, lightweight restatement/admin-text rejection)
migrations/            — Numbered SQL migration files applied by db.py
web/app.py             — Dash + Mantine web dashboard entry point (read-only, live Postgres queries)
web/dashboard.yaml     — YAML-driven dashboard configuration (pages, nav, queries, grids, charts, stat cards)
web/renderer.py        — YAML config renderer: parses dashboard.yaml, builds Dash layouts for YAML-driven pages, imports custom page modules
web/data.py            — Data access layer for the web dashboard (SELECT-only for analytics, plus dashboard-local CRUD for saved reports; no TS writes)
web/pages/overview.py  — Overview page: KPI stat cards, backlog trend chart, aging distribution, open-by-product, chart drill-down (custom)
web/pages/tickets.py   — Ticket explorer: AG Grid with filters, sorting, row-click navigation to detail, saved filter reports (custom)
web/pages/ticket_detail.py — Ticket detail: metadata header, thread timeline, score cards, wait profile chart, issue summary (custom)
web/pages/health.py    — Health dashboards: Customer and Product health AG Grid tables (kept for reference; now YAML-driven)
web/pages/root_cause.py — Root Cause analysis: AG Grid of pass-processed tickets, detail view with Pass 1 (phenomenon), Pass 2 (grammar decomposition), Pass 3 placeholder, cleaned thread text (custom)
web/pages/config_view.py — Pipeline config viewer (read-only display of all settings + sync status, custom)
web_requirements.txt   — Python dependencies for the web dashboard (dash, dash-mantine-components, dash-ag-grid, PyYAML, etc.)
```

## Data Dictionary

All database objects live in the `tickets_ai` schema. Every table that references a ticket carries both `ticket_id` (integer PK from TeamSupport) and `ticket_number` (human-readable, denormalised in Phase 8).

### Tables (23)

| Table | Category | Purpose |
|-------|----------|---------|
| `tickets` | Source truth | Canonical ticket rows keyed by `ticket_id` |
| `ticket_actions` | Source truth | Canonical action/activity rows keyed by `action_id` |
| `sync_state` | Source truth | Per-source watermark / cursor tracking |
| `ingest_runs` | Source truth | Audit log of each ingestion run |
| `ticket_thread_rollups` | Derived | Concatenated thread texts and content hashes per ticket |
| `ticket_metrics` | Derived | Computed counts, timestamps, and response-time metrics per ticket |
| `ticket_participants` | Derived | Each unique participant per ticket with action counts |
| `ticket_handoffs` | Derived | Inferred transitions when party/participant changes between actions |
| `ticket_wait_states` | Derived | Deterministic wait segments from action stream and lifecycle |
| `ticket_sentiment` | Enrichment | LLM sentiment scores (append-only, hash-gated); includes `frustrated_reason` |
| `ticket_priority_scores` | Enrichment | LLM priority scores (append-only, hash-gated) |
| `ticket_complexity_scores` | Enrichment | LLM complexity scores (append-only, hash-gated) |
| `ticket_issue_summaries` | Enrichment | LLM issue/cause/mechanism/resolution summaries (schema-only) |
| `ticket_embeddings` | Enrichment | Vector embeddings for similarity search (schema-only) |
| `ticket_interventions` | Enrichment | Recommended interventions per ticket (schema-only) |
| `enrichment_runs` | Enrichment | Audit log for enrichment batches (schema-only) |
| `cluster_runs` | Clustering | Clustering run metadata (schema-only) |
| `ticket_clusters` | Clustering | Ticket-to-cluster assignments (schema-only) |
| `cluster_catalog` | Clustering | Cluster descriptions and patterns (schema-only) |
| `ticket_snapshots_daily` | Snapshot/Health | Point-in-time snapshot of ticket state on a given date |
| `customer_ticket_health` | Snapshot/Health | Per-customer health rollup (open/HP/HC counts, pressure score) |
| `product_ticket_health` | Snapshot/Health | Per-product health rollup (volume, complexity, dev-touched rate) |
| `daily_open_counts` | Snapshot/Health | Aggregated daily counts of open tickets by product, status, and last-active participant (from ticket_participants) |
| `saved_reports` | Dashboard | Named saved filter presets for the ticket explorer grid |

### Views (17)

| View | Purpose |
|------|---------|
| `vw_latest_ticket_sentiment` | Latest sentiment row per ticket |
| `vw_latest_ticket_priority` | Latest priority row per ticket |
| `vw_latest_ticket_complexity` | Latest complexity row per ticket |
| `vw_latest_ticket_issue_summary` | Latest issue summary per ticket |
| `vw_ticket_analytics_core` | Master join: tickets + metrics + rollups + 4 latest views |
| `vw_ticket_complexity_breakdown` | Detailed complexity dimension breakdown |
| `vw_ticket_wait_profile` | Per-ticket wait-time aggregation by wait type |
| `vw_customer_support_risk` | 90-day customer risk rollup |
| `vw_product_pain_patterns` | Product-level pain/complexity grouping |
| `vw_intervention_opportunities` | Intervention impact scoring and grouping |
| `vw_backlog_daily` | Daily open backlog from daily_open_counts; HP/HC from snapshots (newest first) |
| `vw_backlog_weekly` | Weekly backlog averages from daily_open_counts; HP/HC from snapshots (newest first) |
| `vw_backlog_weekly_eow` | End-of-week backlog from daily_open_counts; HP/HC from snapshots (newest first) |
| `vw_backlog_daily_by_severity` | Daily open backlog by severity tier (High/Medium/Low) from tickets + daily_open_counts date spine |
| `vw_backlog_aging_current` | Current backlog by age buckets from tickets table (status-aware) |
| `vw_backlog_daily_by_participant_type` | Daily open backlog grouped by participant type (newest first) |
| `vw_backlog_daily_by_participant_type_product` | Daily open backlog grouped by participant type + product (newest first) |
| `vw_backlog_daily_by_participant_type_product_powman` | Same as above but products starting with "PM" or containing "Power" (case-insensitive) collapsed to "PowerMan" |
| `vw_backlog_weekly_from_dates` | Weekly backlog from daily_open_counts (newest first) |
| `vw_ticket_pass1_results` | Latest Pass 1 phenomenon result per ticket |
| `vw_ticket_pass2_results` | Latest Pass 2 grammar result per ticket (joined with Pass 1 phenomenon) |
| `vw_ticket_pass3_results` | Latest Pass 3 mechanism result per ticket (joined with Pass 1 + Pass 2) |
| `vw_ticket_pass_pipeline` | Full pipeline status: Pass 1 + Pass 2 + Pass 3 side-by-side per ticket |

## Data Flow

1. **Fetch tickets** — `ts_client.fetch_open_tickets(ticket_number=...)` queries the TeamSupport `/Tickets` endpoint. When `TARGET_TICKET` is set, the ticket is fetched by number regardless of open/closed status. When no target is set, only open tickets are returned (`isClosed=False`) with full pagination. If the API returns 403 (rate limit), the pipeline falls back to `Activities.csv`.
2. **CSV fallback** — When the API is rate-limited or unavailable, `run_pull_activities.py` reads `Activities.csv` from the project root. Party classification uses cached inHANCE names from prior activity JSON files; if none exist, party is set to `"unknown"`. Some metadata fields (`ticket_id`, `status`, `severity`, `assignee`, `date_created`, `date_modified`) are unavailable from CSV.
2. **Limit tickets** — When not targeting a specific ticket, `config.MAX_TICKETS` (default **5**) caps how many are processed. Set to `0` for unlimited.
3. **Fetch activities per ticket** — `ts_client.fetch_all_activities(ticket_id)` pages through `/Tickets/{id}/Actions`. Deduplicates by action ID to guard against the TS API recycling pages past the real end, with a 500-page safety cap.
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
9a. **Parse response** — `run_sentiment.py` strips markdown code fences (`` ```json `` blocks) from Matcha responses before JSON parsing, with regex fallback extraction. This matches the robust parsing already used in `run_priority.py` and `run_complexity.py`. The response now includes `frustrated_reason` — a one-sentence explanation of the frustration classification.
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

# Use TARGET_TICKET from config.py (fallback when --ticket is not provided)
TARGET_TICKET=29696 python run_ingest.py sync

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

# Only sync tickets CREATED since the last watermark (includes open + closed)
python run_ingest.py sync --new-only

# Only sync tickets CREATED since a specific date (includes open + closed)
python run_ingest.py sync --new-only --since 2026-03-01

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
- The TS API supports server-side date filtering on any date field using the format `YYYYMMDDHHMMSS` (UTC, 24-hour). The normal sync still post-filters by `DateModified` locally; `--new-only` uses server-side `DateCreated` filtering.

**New-only mode (`--new-only`):**

Uses the TeamSupport server-side `DateCreated` filter to fetch only tickets
(open **and** closed) created after the watermark or `--since` date. This is
efficient — only newly-created tickets are returned by the API, not the entire
ticket archive. Captures tickets that were created and closed between sync runs.
Does not advance the watermark.

**Closed-ticket reconciliation:**

When a full unrestricted sync completes (no `--ticket`/`--since`/`--days` and
`MAX_TICKETS=0` or `--all`), the sync automatically detects tickets that are
marked open in the DB (`closed_at IS NULL`) but were **not** returned by
TeamSupport. These are presumed closed in TS. Each missing ticket is re-fetched
individually via `fetch_ticket_by_id()` and upserted, updating `status`,
`closed_at`, and all actions. Reconciled tickets are included in the post-sync
rollup rebuild.

- Reconciliation only runs on full syncs — partial syncs (`--ticket`, `--since`,
  `--days`, `MAX_TICKETS > 0`) are excluded because missing tickets are expected.
- Use `--no-reconcile` to skip reconciliation on a full sync.

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

Score all closed tickets using data already in Postgres. Only Matcha LLM calls are made. Hash-based skipping ensures only new/changed tickets are scored. Sentiment analysis is included by default.

```bash
# Score all closed tickets (priority + complexity + sentiment)
python run_enrich_db.py

# Limit to first 100 tickets
python run_enrich_db.py --limit 100

# Priority only, batch size 10
python run_enrich_db.py --priority-only --batch-size 10

# Complexity only
python run_enrich_db.py --complexity-only

# Exclude sentiment
python run_enrich_db.py --no-sentiment

# Force rescore (ignore hash-based skip)
python run_enrich_db.py --force --limit 50

# Target Open tickets instead of Closed
# (matches all non-closed tickets via closed_at IS NULL)
python run_enrich_db.py --status Open
```

**Key behaviours:**
- **No TS API calls** — reads entirely from the Postgres DB.
- **Hash-based skipping** — tickets already scored with the same content hash are skipped automatically. Use `--force` to override.
- **Ticket counter logging** — each enrichment script logs `ticket count X/N` per ticket processed, enabling validation that the run stays within expected bounds.
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
| `Days Closed` | `tickets.closed_at` | Integer; derived as `csv_export_date - timedelta(days=N)` |
| `Group Name` | `tickets.assignee` | |
| `Action Description` | `ticket_actions.description` | Raw + cleaned |
| `Action Type` | `ticket_actions.action_type` | |
| `Date Action Created` | `ticket_actions.created_at` | Parsed to UTC |
| `Action Creator Name` | `ticket_actions.creator_name` | |

**Key behaviours:**
- **No Action ID in CSV**: Synthetic deterministic `action_id` generated from SHA-256 of `(ticket_id, date_created, description[:200])`. Re-importing the same CSV produces identical IDs.
- **closed_at derivation**: CSV contains `Days Closed` (integer relative to export date) instead of an absolute close date. The import pre-scans all rows to find the latest `Date Ticket Created`, infers that as the CSV export date, then computes `closed_at = export_date - timedelta(days=Days_Closed)` for each closed ticket.
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
| **Exclude sentiment (DB)** | `python run_enrich_db.py --no-sentiment` |
| **Score open tickets (DB)** | `python run_enrich_db.py --status Open` |

### Canonical DB Concept

The Postgres database is the **canonical store** for all ticket data, action history, rollups, metrics, and enrichment results. JSON files are **artifacts/exports** — convenient snapshots, not the source of truth.

- **Idempotent upserts**: Re-ingesting a ticket converges to the same state. No duplicates.
- **Append-only enrichment**: Enrichment tables (`ticket_sentiment`, `ticket_priority_scores`, `ticket_complexity_scores`) are append-only. Each scoring run adds a new row. Historical scores are never deleted.
- **Hash-based skipping**: Content hashes (`thread_hash`, `technical_core_hash`) in `ticket_thread_rollups` are compared against the most recent enrichment row. If unchanged, the LLM call is skipped. Use `--force` to override.
- **Replay/resync**: Run `python run_ingest.py sync --ticket <num>` to re-fetch from the TS API; rollups and analytics are automatically rebuilt for all touched tickets. Enrichments will automatically detect the hash change and rescore on next run.
- **Post-sync rollup rebuild**: Both `run_ingest.py` (API sync) and `run_csv_import.py` (CSV import) automatically rebuild all rollups and analytics (classify → rollups → metrics → participants → handoffs → wait_states → snapshot → health → daily_open_counts) for every upserted ticket after a successful sync. No manual `run_rollups.py` step is required.

### Compatibility Notes

- **No breaking changes**: All original JSON-only workflows work identically when `DATABASE_URL` is not set.
- **JSON artifacts**: Still generated by enrichment scripts and by `run_export.py`. Output schemas are unchanged.
- **Write-back**: `ts_client.update_ticket()` continues to inject `LastInhComment` and `LastCustComment` automatically from the activity history.
- **run_all.py**: Now accepts `--force` and `--no-writeback` flags. Without flags, behaviour is identical to the original.

### Write-Back & Output Controls

Two config flags control side-effects:

- **`TS_WRITEBACK`** (default `0`): When `0`, no enrichment data is written back to TeamSupport — this is a **hard lock** that the `--no-writeback` CLI flag cannot override. `TS_WRITEBACK=0` in config always wins, regardless of CLI flags. When `1`, write-back is enabled but can be suppressed for a specific run using `--no-writeback`. When run standalone, `run_priority.py` and `run_complexity.py` also respect this default (their `write_back` parameter defaults to `TS_WRITEBACK` when not explicitly passed).

- **`SKIP_OUTPUT_FILES`** (default `0`): When `1`, **no** files are written to the `output/` directory during enrichment — this includes activities JSON, enrichment result JSON, pipeline log files, and API call logs. This is useful when the DB is the canonical store and file artifacts are unwanted. When `0`, all files are written as usual.

## Web Dashboard

A live analytics dashboard built with Dash + Dash Mantine Components + AG Grid. Reads directly from the Postgres database — **no writes to the DB or TeamSupport**. All queries are SELECT-only through `web/data.py`.

### Running

```bash
# Install dashboard dependencies
pip install -r web_requirements.txt

# Start the dashboard (default: http://localhost:8050)
python3 web/app.py

# Override port or disable debug mode
WEB_PORT=9000 WEB_DEBUG=0 python3 web/app.py

# For production (gunicorn)
gunicorn web.app:server -b 0.0.0.0:8050
```

Requires `DATABASE_URL` to be set (reads from the `tickets_ai` schema).

### Pages

| Page | URL | Data Source | Description |
|------|-----|------------|-------------|
| Overview | `/` | `vw_ticket_analytics_core`, `vw_backlog_daily`, `vw_backlog_daily_by_severity`, `vw_backlog_aging_current` | KPI stat cards (open/HP/HC/frustrated), backlog trend stacked-area chart (severity breakdown: High=red, Medium=amber, Low=blue with total line overlay), aging distribution bar chart, open-by-product breakdown. **Click any bar** to drill down into the underlying tickets in a modal grid. |
| Tickets | `/tickets` | `vw_ticket_analytics_core` | AG Grid explorer with column filters, sorting, floating filters. Click any row to navigate to detail. |
| Ticket Detail | `/ticket/{id}` | `vw_ticket_analytics_core`, `ticket_actions`, `vw_ticket_wait_profile` | Metadata header, tabbed view: Thread (chronological action cards with party-colored borders), Scores (priority/complexity/sentiment cards), Wait Profile (horizontal bar chart of time per state), Summary (issue/cause/mechanism/resolution) |
| Health | `/health` | `customer_ticket_health`, `product_ticket_health` | Tabbed AG Grid tables: Customer Health (pressure score, frustration, HP/HC counts) and Product Health (volume, complexity, dev-touched rate, customer wait rate) |
| Config | `/config` | `config.*` attributes, `sync_state` | Read-only display of all pipeline settings grouped by category, plus sync status |

### Architecture

```
web/
├── app.py              ← Entry point: Mantine AppShell, routing, nav callbacks
├── data.py             ← Read-only query layer (imports db.py, returns serialised dicts)
└── pages/
    ├── overview.py     ← KPI cards + Plotly charts
    ├── tickets.py      ← AG Grid ticket list
    ├── ticket_detail.py ← Full ticket view with tabs
    ├── health.py       ← Customer + Product health grids
    └── config_view.py  ← Pipeline config display
```

- **No modifications to existing code** — the dashboard imports `db.get_conn()`/`db.put_conn()` and `config.*` read-only.
- **No TeamSupport API calls** — purely database-driven.
- **Dash 4.x** with `suppress_callback_exceptions=True` for dynamic page content.
- **Mantine v7** theming via `dash-mantine-components` for a polished UI.
- **AG Grid** (`dash-ag-grid`) for high-performance data tables with built-in sort/filter/pagination.
- **Chart drill-down** — clicking a bar in the Aging, Open-by-Product, or per-product Aging breakdown charts opens a modal with matching tickets in an AG Grid. Per-product aging bars filter by both product and age bucket. Click a row in the modal to navigate to the ticket detail page. Powered by `data.get_drilldown_tickets()` which filters by product (with PowerMan consolidation), severity tier, and/or age bucket.
- **KPI card drill-down** — clicking any of the four overview stat cards (Open Tickets, High Priority, High Complexity, Frustrated) opens the same drill-down modal with the corresponding filtered ticket list.
- **CSV export** — every AG Grid table (YAML-driven and code-driven) has an "Export CSV" button that triggers AG Grid's built-in CSV download. Callbacks are registered dynamically from the YAML config + known code-driven grid IDs.

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
- **Post-sync rollups** (4 tests): CSV import returns upserted IDs, rollup rebuild triggered after import, skipped on dry-run
- **Pass 1 — phenomenon** (35 tests): parser, selection logic, idempotency, DB persistence, malformed handling, success flow, prompt template
- **Pass 2 — grammar** (51 tests): parser, operation normalization, canonical failure reconstruction, selection logic, idempotency, DB persistence, malformed handling, success flow, prompt template

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

---

## Phase 7 — Analytics Extension

### Overview

Phase 7 adds 13 new tables, 15 views, and 6 rebuild functions that turn the raw ticket/action data into a query-ready analytics layer. The extension integrates into the existing migration, rollup, and ingestion patterns — no parallel pipeline.

### Table Categories

The full schema (`tickets_ai`) is now organized into five categories:

| Category | Tables | Purpose |
|---|---|---|
| **Source truth** | `tickets`, `ticket_actions`, `sync_state`, `ingest_runs` | Raw data from TeamSupport API / CSV import |
| **Derived (per-ticket)** | `ticket_thread_rollups`, `ticket_metrics`, `ticket_participants`, `ticket_handoffs`, `ticket_wait_states` | Deterministic rebuilds from action stream |
| **Enrichment (append-only)** | `ticket_sentiment`, `ticket_priority_scores`, `ticket_complexity_scores`, `ticket_issue_summaries`, `ticket_embeddings`, `ticket_interventions`, `enrichment_runs` | LLM-scored analytics, append-only with hash-based skip |
| **Clustering** | `cluster_runs`, `ticket_clusters`, `cluster_catalog` | Grouping tickets by similarity; populated by future clustering pipeline |
| **Snapshot / Health** | `ticket_snapshots_daily`, `customer_ticket_health`, `product_ticket_health` | Point-in-time backlog snapshots and customer/product health rollups |

### New Derived Tables

#### ticket_participants
Populated from `ticket_actions`. Tracks each unique participant per ticket:
- `participant_id`: `creator_id` when available, else synthetic `{party}:{creator_name}`
- `participant_type`: `inhance` or `customer` (derived from `party`)
- `first_response_flag`: TRUE for the inh participant who sent the first response after the first customer action
- Full-refresh per ticket (DELETE + INSERT)

#### ticket_handoffs
Inferred from transitions in the action stream:
- A handoff occurs when a different party or participant sends the next action
- `handoff_reason`: `party_switch:inh->cust` or `participant_switch_within_inh`
- `confidence`: 0.8 (heuristic-based)
- Full-refresh per ticket

#### ticket_wait_states
Deterministic wait segments inferred from action stream and lifecycle:
- Each action opens a new segment; the previous segment is closed at the new action's timestamp
- State mapping: `action_class` → state name, with party-based fallback
  - `customer_problem_statement` → `waiting_on_support`
  - `technical_work`, `status_update`, `delivery_confirmation` → `active_work`
  - `waiting_on_customer` → `waiting_on_customer`
  - `scheduling` → `scheduled`
  - Fallback: cust action → `waiting_on_support`, inh action → `waiting_on_customer`
- Final segment is closed with `tickets.closed_at` if ticket is closed; otherwise left open (`end_at IS NULL`)
- Guard: if `closed_at` predates the last action, `end_at` is clamped to `start_at` (zero-duration close) to satisfy the `chk_wait_state_end` constraint
- `confidence`: 0.75, `inference_method`: `action_class_heuristic`
- Full-refresh per ticket

### Snapshot / Health Tables

#### ticket_snapshots_daily
Purpose: **Support historical backlog analysis** such as "backlog per week over the year."

Each row is a point-in-time snapshot of a ticket's state on a given date. On each normal run, today's rows are created/upserted for all touched tickets (or all tickets when run manually). Joins latest priority score, complexity score, and wait state.

- `open_flag`: TRUE if status is not Closed/Resolved
- `high_priority_flag`: TRUE if priority ≤ 3
- `high_complexity_flag`: TRUE if overall_complexity ≥ 4
- `waiting_state`: latest inferred state from `ticket_wait_states`

#### customer_ticket_health
Per-customer health rollup for a given date. Full-refresh. Includes:
- Open/high-priority/high-complexity ticket counts
- Average complexity and elapsed drag
- Frustration count (from sentiment analysis)
- Top cluster IDs and products (JSONB arrays)
- `ticket_load_pressure_score`: `open + 2×high_priority + 1.5×high_complexity + 3×frustration` (simple first-pass formula)
- `reopen_count_90d`: placeholder (0) — not currently inferable from available data

#### product_ticket_health
Per-product health rollup for a given date. Full-refresh. Includes:
- Ticket volume, average complexity/coordination_load/elapsed_drag
- `dev_touched_rate`: fraction of tickets with at least one `technical_work` action
- `customer_wait_rate`: fraction of tickets whose latest wait state is `waiting_on_customer`
- Top clusters and mechanisms (JSONB arrays)

### Enrichment / Clustering Tables (Schema-Only)

These tables are created by the migration but are **not yet populated** by the normal run. They are designed for future enrichment stages:

- `ticket_issue_summaries`: LLM-generated issue/cause/mechanism/resolution summaries
- `ticket_embeddings`: Vector embeddings for similarity search
- `cluster_runs`, `ticket_clusters`, `cluster_catalog`: Ticket clustering results
- `ticket_interventions`: Recommended interventions per ticket
- `enrichment_runs`: Audit log for enrichment batches (UUID PK generated in Python)

### Views

15 views provide query-ready analytics:

| View | Source | Purpose |
|---|---|---|
| `vw_latest_ticket_sentiment` | `ticket_sentiment` | Latest sentiment per ticket |
| `vw_latest_ticket_priority` | `ticket_priority_scores` | Latest priority per ticket |
| `vw_latest_ticket_complexity` | `ticket_complexity_scores` | Latest complexity per ticket |
| `vw_latest_ticket_issue_summary` | `ticket_issue_summaries` | Latest issue summary per ticket |
| `vw_ticket_analytics_core` | tickets + metrics + rollups + 4 latest views | Master analytics join |
| `vw_ticket_complexity_breakdown` | tickets + latest complexity | Detailed complexity dimensions |
| `vw_ticket_wait_profile` | `ticket_wait_states` | Per-ticket wait time aggregation |
| `vw_customer_support_risk` | tickets + priority/complexity/sentiment + clusters | 90-day customer risk rollup |
| `vw_product_pain_patterns` | tickets + clusters + issue summaries | Product-level pain grouping |
| `vw_intervention_opportunities` | interventions + tickets + clusters | Intervention impact grouping |
| `vw_backlog_daily` | `ticket_snapshots_daily` | Daily open/HP/HC backlog counts |
| `vw_backlog_daily_by_severity` | `tickets` + `daily_open_counts` | Daily open backlog by severity tier (High/Medium/Low) |
| `vw_backlog_weekly` | `ticket_snapshots_daily` | Weekly backlog averages |
| `vw_backlog_weekly_eow` | `ticket_snapshots_daily` | End-of-week backlog from latest snapshot per week |
| `vw_backlog_aging_current` | `ticket_snapshots_daily` | Age buckets (0-6, 7-13, 14-29, 30-59, 60-89, 90+) |
| `vw_backlog_weekly_from_dates` | `tickets` | Fallback weekly backlog from ticket created/closed dates |

#### How "Backlog Per Week Over the Year" Is Answered

Three approaches, depending on data availability:

1. **`vw_backlog_weekly`** — Averages daily snapshot counts within each week. Requires daily snapshots to have been running.
2. **`vw_backlog_weekly_eow`** — Uses only the latest snapshot per ticket per week. Best for end-of-week point-in-time view.
3. **`vw_backlog_weekly_from_dates`** — Fallback that uses only `tickets.date_created` and `tickets.closed_at` with a generated weekly series. Works even with zero snapshots but is less accurate (doesn't capture status changes mid-life).

### Normal Run Flow

After `run_ingest.py sync` completes successfully, the post-sync hook automatically runs:

```
1. sync changed tickets/actions         (run_ingest.py _sync)
2. classify actions for touched tickets  (run_rollups.classify_actions)
3. rebuild thread rollups               (run_rollups.rebuild_rollups)
4. rebuild ticket_metrics               (run_rollups.rebuild_metrics)
5. rebuild ticket_participants          (run_rollups.rebuild_ticket_participants)
6. rebuild ticket_handoffs              (run_rollups.rebuild_ticket_handoffs)
7. rebuild ticket_wait_states           (run_rollups.rebuild_ticket_wait_states)
8. write today's ticket_snapshots_daily (run_rollups.snapshot_tickets_daily)
9. refresh customer_ticket_health       (run_rollups.rebuild_customer_ticket_health)
10. refresh product_ticket_health       (run_rollups.rebuild_product_ticket_health)
```

Steps 2-7 are scoped to the touched ticket_ids. Steps 9-10 are full-refresh for the current date (they aggregate across all customers/products).

### Manual CLI Commands

```bash
# Run all analytics for a single ticket
python run_rollups.py analytics --ticket 109683

# Run everything (classify + rollups + metrics + analytics)
python run_rollups.py full --ticket 109683

# Run everything for all tickets (no --ticket flag)
python run_rollups.py full

# Individual stages
python run_rollups.py participants --ticket 109683
python run_rollups.py handoffs --ticket 109683
python run_rollups.py wait_states --ticket 109683
python run_rollups.py snapshot --ticket 109683
python run_rollups.py health
```

### Incremental vs Full-Refresh

| Function | Scope | Strategy |
|---|---|---|
| `classify_actions` | Per ticket_id list | Full-refresh per ticket |
| `rebuild_rollups` | Per ticket_id list | Full-refresh per ticket |
| `rebuild_metrics` | Per ticket_id list | Full-refresh per ticket |
| `rebuild_ticket_participants` | Per ticket_id list | Full-refresh per ticket (DELETE + INSERT) |
| `rebuild_ticket_handoffs` | Per ticket_id list | Full-refresh per ticket (DELETE + INSERT) |
| `rebuild_ticket_wait_states` | Per ticket_id list | Full-refresh per ticket (DELETE + INSERT) |
| `snapshot_tickets_daily` | Per ticket_id list or all | Upsert (ON CONFLICT DO UPDATE) |
| `rebuild_customer_ticket_health` | All customers | Full-refresh for given date |
| `rebuild_product_ticket_health` | All products | Full-refresh for given date |

All per-ticket rebuilds use full-refresh in this first pass. This is safe because hash-based skipping in the enrichment layer prevents redundant LLM calls, and the derived tables are deterministic from the action stream.

### Migration

All new tables and views are in `migrations/003_analytics.sql`. Idempotent (`IF NOT EXISTS` / `CREATE OR REPLACE VIEW`). Applied automatically by `db.migrate()` which runs at the start of every command.

---

## Phase 8: `ticket_number` Denormalization

### Overview

Denormalized `ticket_number` (from `tickets` table) into every table and view that references `ticket_id`. This makes `ticket_number` available directly in query results without requiring a JOIN to `tickets` — useful for reporting, debugging, and API responses.

### Migration (`004_add_ticket_number.sql`)

- **13 ALTER TABLE** statements adding `ticket_number TEXT` column (IF NOT EXISTS for idempotency)
- **13 backfill UPDATEs** populating `ticket_number` from the `tickets` table for existing rows
- **1 view recreation** (`vw_ticket_wait_profile`) — DROP + CREATE to add `ticket_number` to GROUP BY

Tables modified: `ticket_actions`, `ticket_thread_rollups`, `ticket_metrics`, `ticket_sentiment`, `ticket_priority_scores`, `ticket_complexity_scores`, `ticket_wait_states`, `ticket_participants`, `ticket_handoffs`, `ticket_issue_summaries`, `ticket_embeddings`, `ticket_clusters`, `ticket_interventions`.

### Write Path Changes

All functions that INSERT or UPDATE rows into the above tables now include `ticket_number`:

| File | Function | Change |
|---|---|---|
| `db.py` | `ticket_numbers_for_ids()` | **New** — bulk reverse-mapping `{ticket_id: ticket_number}` |
| `db.py` | `upsert_action()` | Added `ticket_number` to INSERT + ON CONFLICT |
| `db.py` | `upsert_ticket_with_actions()` | Added `ticket_number` to action INSERT |
| `db.py` | `insert_sentiment()` | Added `ticket_number` keyword parameter |
| `db.py` | `insert_priority()` | Added `ticket_number` keyword parameter |
| `db.py` | `insert_complexity()` | Added `ticket_number` keyword parameter |
| `run_rollups.py` | `rebuild_rollups()` | Fetches `tnum_map`, includes in upsert |
| `run_rollups.py` | `rebuild_metrics()` | Fetches `tnum_map`, includes in upsert |
| `run_rollups.py` | `rebuild_ticket_participants()` | Fetches `tnum_map`, includes in bulk_insert |
| `run_rollups.py` | `rebuild_ticket_handoffs()` | Fetches `tnum_map`, includes in bulk_insert |
| `run_rollups.py` | `rebuild_ticket_wait_states()` | Fetches `tnum_map`, includes in bulk_insert |
| `run_sentiment.py` | `_persist_to_db()` | Accepts + passes `ticket_number` |
| `run_priority.py` | `_persist_to_db()` | Accepts + passes `ticket_number` |
| `run_complexity.py` | `_persist_to_db()` | Accepts + passes `ticket_number` |
| `run_ingest.py` | `_sync()` loop | Sets `action_row["ticket_number"] = tnum` |
| `run_csv_import.py` | `run_import()` | Adds `ticket_number` to action dict |

### Views

- **4 `vw_latest_*` views** — auto-inherit via `SELECT *` (no change needed)
- **`vw_ticket_analytics_core`** — already had `t.ticket_number` (no change)
- **`vw_ticket_complexity_breakdown`** — already had `t.ticket_number` (no change)
- **`vw_ticket_wait_profile`** — rebuilt with `ticket_number` in SELECT + GROUP BY
- **Aggregate views** (`vw_customer_support_risk`, `vw_product_pain_patterns`, `vw_intervention_opportunities`) — excluded (group by customer/product, not per-ticket)

---

## Pass 1 — Phenomenon Extraction (LLM Multi-Pass Pipeline)

### Overview

Pass 1 extracts the observable system behavior (phenomenon) from each ticket's `full_thread_text` using the Matcha LLM endpoint. This is the first stage of a planned multi-pass LLM pipeline.

### Architecture

| Component | File | Purpose |
|-----------|------|---------|
| Migration | `migrations/005_llm_pass_results.sql` | Creates `ticket_llm_pass_results` table + `vw_ticket_pass1_results` view |
| Prompt | `prompts/pass1_phenomenon.txt` | Pass 1 prompt template with `{{input_text}}` placeholder |
| Parser | `pass1_parser.py` | Strict JSON validation for `{"phenomenon": "..."}` responses |
| Orchestrator | `run_ticket_pass1.py` | CLI entrypoint + batch processing logic |
| DB helpers | `db.py` | `insert_pass_result`, `update_pass_result`, `delete_prior_failed_pass`, `get_latest_pass_result`, `fetch_pending_pass1_tickets` |
| Tests | `tests/test_pass1.py` | 35 focused tests (parser, selection, idempotency, persistence, malformed handling) |

### Data Flow

```
ticket_thread_rollups.full_thread_text
    → load prompt template (prompts/pass1_phenomenon.txt)
    → substitute {{input_text}} with full_thread_text
    → call Matcha endpoint
    → parse JSON response → extract "phenomenon"
    → store in ticket_llm_pass_results (raw + parsed + status)
```

### Table: ticket_llm_pass_results

| Column | Type | Description |
|--------|------|-------------|
| id | BIGSERIAL PK | Row identifier |
| ticket_id | BIGINT FK | References tickets(ticket_id) |
| pass_name | TEXT | Stage name (e.g. `pass1_phenomenon`, `pass2_grammar`) |
| input_text | TEXT | The text sent to the model (full_thread_text for Pass 1, phenomenon for Pass 2) |
| prompt_version | TEXT | Version identifier for the prompt |
| model_name | TEXT | Model identifier (e.g. `matcha-27301`) |
| raw_response_text | TEXT | Raw text response from Matcha |
| parsed_json | JSONB | Parsed JSON payload |
| phenomenon | TEXT | Pass 1 output; NULL for other passes |
| component | TEXT | Pass 2 output: subsystem/module involved |
| operation | TEXT | Pass 2 output: normalized operation verb |
| unexpected_state | TEXT | Pass 2 output: unexpected system outcome |
| canonical_failure | TEXT | Pass 2 output: `<Component> + <Operation> + <Unexpected State>` |
| status | TEXT | `pending` / `success` / `failed` |
| error_message | TEXT | Error details on failure |
| started_at | TIMESTAMPTZ | When processing began |
| completed_at | TIMESTAMPTZ | When processing finished |
| created_at | TIMESTAMPTZ | Row creation time |
| updated_at | TIMESTAMPTZ | Last update time |

**Uniqueness:** A partial unique index `(ticket_id, pass_name, prompt_version) WHERE status = 'success'` ensures at most one successful result per ticket per pass per prompt version.

### Idempotency

- Rerunning without `--force` skips tickets that already have a successful result for the current prompt version.
- Prior `pending`/`failed` rows are cleaned up before each new attempt.
- The `--force` flag removes existing success rows to allow a fresh run.

### CLI Usage

```bash
python run_ticket_pass1.py --limit 100
python run_ticket_pass1.py --ticket-id 99784
python run_ticket_pass1.py --ticket-id 99784,98154,100289
python run_ticket_pass1.py --since 2026-03-01
python run_ticket_pass1.py --failed-only
python run_ticket_pass1.py --force
```

### Analytics View: vw_ticket_pass1_results

```sql
SELECT ticket_id, phenomenon, pass1_status, latest_error
FROM vw_ticket_pass1_results
WHERE pass1_status = 'success';
```

### Extending to Pass 3+

The `ticket_llm_pass_results` table is designed for multi-pass use:
- Each pass uses a distinct `pass_name` (e.g. `pass3_resolution`)
- The same table, DB helpers, and CLI patterns can be reused
- New parsers can be added per pass (e.g. `pass3_parser.py`)
- New prompt files go in `prompts/` (e.g. `pass3_resolution.txt`)
- New columns can be added via migration for pass-specific projected fields

---

## Pass 2 — Canonical Failure Grammar (LLM Multi-Pass Pipeline)

### Overview

Pass 2 converts each Pass 1 phenomenon into the standardized operational grammar:

`<Component> + <Operation> + <Unexpected State>`

This produces structured, normalized failure descriptions suitable for aggregation and root-cause analysis.

### Architecture

| Component | File | Purpose |
|-----------|------|---------||
| Migration | `migrations/015_pass2_grammar.sql` | Adds `component`, `operation`, `unexpected_state`, `canonical_failure` columns + `vw_ticket_pass2_results` and `vw_ticket_pass_pipeline` views |
| Prompt | `prompts/pass2_grammar.txt` | Pass 2 prompt template with `{{input_text}}` placeholder (receives phenomenon) |
| Parser | `pass2_parser.py` | Strict JSON validation, operation normalization via synonym map, canonical_failure reconstruction |
| Orchestrator | `run_ticket_pass2.py` | CLI entrypoint + batch processing logic |
| DB helpers | `db.py` | Extended `update_pass_result` (component/operation/unexpected_state/canonical_failure), `fetch_pending_pass2_tickets` |
| Tests | `tests/test_pass2.py` | 51 focused tests (parser, normalization, reconstruction, selection, idempotency, persistence, malformed handling) |

### Data Flow

```
ticket_llm_pass_results.phenomenon (from successful Pass 1)
    → load prompt template (prompts/pass2_grammar.txt)
    → substitute {{input_text}} with phenomenon
    → call Matcha endpoint
    → parse JSON response → extract component, operation, unexpected_state
    → normalize operation verb (synonym mapping)
    → reconstruct canonical_failure from parsed fields
    → store in ticket_llm_pass_results (raw + parsed + status + projected columns)
```

### Pass 2 Output Fields

| Field | Description | Example |
|-------|-------------|----------|
| component | Subsystem/module involved | `WebShare AutoPay transfer job` |
| operation | Normalized verb from vocabulary | `transfer` |
| unexpected_state | Unexpected system outcome | `payments remain in web tables` |
| canonical_failure | Reconstructed `component + operation + unexpected_state` | `WebShare AutoPay transfer job + transfer + payments remain in web tables` |

### Operation Normalization

Operations are normalized to a fixed vocabulary: `post`, `import`, `export`, `print`, `load`, `transfer`, `calculate`, `attach`, `generate`, `recover`, `create`, `update`.

Near-synonyms are mapped automatically (e.g. `upload` → `import`, `build` → `generate`, `modify` → `update`). Unknown operations cause a validation failure.

### Selection Logic

- Requires a successful Pass 1 result with non-null, non-empty phenomenon for the configured Pass 1 prompt version
- Excludes tickets with an existing successful Pass 2 result for the current Pass 2 prompt version (unless `--force`)
- `--failed-only` restricts to tickets with a prior failed Pass 2 attempt

### CLI Usage

```bash
python run_ticket_pass2.py --limit 100
python run_ticket_pass2.py --ticket-id 99784
python run_ticket_pass2.py --ticket-id 99784,98154,100289
python run_ticket_pass2.py --failed-only
python run_ticket_pass2.py --force
```

### Analytics Views

```sql
-- Pass 2 results with Pass 1 phenomenon
SELECT ticket_id, phenomenon, component, operation, unexpected_state,
       canonical_failure, pass2_status
FROM vw_ticket_pass2_results
WHERE pass2_status = 'success';

-- Full pipeline status at a glance
SELECT ticket_id, ticket_number, phenomenon, pass1_status,
       canonical_failure, pass2_status
FROM vw_ticket_pass_pipeline
WHERE pass1_status = 'success';
```

## analytics_queries.py — Operational Analytics Module

`analytics_queries.py` exposes 10 read-only SQL queries as Python string
constants plus thin wrapper functions that execute them against an existing
`psycopg2` connection.

### SQL Constants

| Constant | Purpose |
|---|---|
| `SQL_ROOT_CAUSE_DISTRIBUTION` | Count tickets by `root_cause_class` with percentage of total |
| `SQL_ROOT_CAUSE_SEVERITY` | Count tickets grouped by `root_cause_class` × `severity` |
| `SQL_FUNCTIONAL_AREA_DISTRIBUTION` | Count tickets grouped by `functional_area` |
| `SQL_PREVENTABLE_VS_ENGINEERING` | Bucket into *engineering_required* vs *preventable_operational* |
| `SQL_TICKET_AGING_BY_CAUSE` | Average `days_opened` by `root_cause_class` (join to `tickets`) |
| `SQL_FRUSTRATION_BY_CAUSE` | Frustrated-ticket rate by root cause (join to `ticket_sentiment`) |
| `SQL_PRODUCT_RELIABILITY` | Ticket count by `product_name` (join to `tickets`) |
| `SQL_INTEGRATION_FAILURE_RATE` | Integration-related ticket count and percentage |
| `SQL_HIGH_PRIORITY_BY_CAUSE` | High-priority (≤ 3) tickets by root cause (join to `ticket_priority_scores`) |
| `SQL_TOP_FAILURE_MECHANISMS` | Top 20 mechanisms from Pass 3 results |

### Helper Functions

| Function | Description |
|---|---|
| `run_query(conn, sql, params, as_df)` | Execute SQL; return `list[dict]` or `pandas.DataFrame` |
| `root_cause_distribution(conn, as_df)` | Wrapper for `SQL_ROOT_CAUSE_DISTRIBUTION` |
| `root_cause_severity(conn, as_df)` | Wrapper for `SQL_ROOT_CAUSE_SEVERITY` |
| `functional_area_distribution(conn, as_df)` | Wrapper for `SQL_FUNCTIONAL_AREA_DISTRIBUTION` |
| `preventable_vs_engineering(conn, as_df)` | Wrapper for `SQL_PREVENTABLE_VS_ENGINEERING` |
| `ticket_aging_by_cause(conn, as_df)` | Wrapper for `SQL_TICKET_AGING_BY_CAUSE` |
| `frustration_by_cause(conn, as_df)` | Wrapper for `SQL_FRUSTRATION_BY_CAUSE` |
| `product_reliability(conn, as_df)` | Wrapper for `SQL_PRODUCT_RELIABILITY` |
| `integration_failure_rate(conn, as_df)` | Wrapper for `SQL_INTEGRATION_FAILURE_RATE` |
| `high_priority_by_cause(conn, as_df)` | Wrapper for `SQL_HIGH_PRIORITY_BY_CAUSE` |
| `top_failure_mechanisms(conn, as_df)` | Wrapper for `SQL_TOP_FAILURE_MECHANISMS` |

### Usage

```python
import psycopg2
import analytics_queries

conn = psycopg2.connect("postgresql://user:pass@localhost:5432/Work")
rows = analytics_queries.root_cause_distribution(conn)
df   = analytics_queries.top_failure_mechanisms(conn, as_df=True)
conn.close()
```
