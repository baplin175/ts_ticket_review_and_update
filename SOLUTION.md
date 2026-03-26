# Solution Overview

## Purpose

Ingest TeamSupport tickets and customer metadata into Postgres, derive operational analytics and health rollups, run Matcha-backed enrichment/classification passes, and surface the results in the web dashboard. JSON exports remain supported as artifacts and compatibility outputs, but the database is now the canonical runtime store.

## Supported Modes

- Canonical path: Postgres-backed ingestion via `run_ingest.py`, derived rebuilds via automatic post-sync hooks, and DB-backed enrichment via `run_enrich_db.py`
- Dashboard: `python -m web.app`
- CSV pipeline app: standalone Flask service in `pipeline/`
- Prompt administration: DB-backed prompt store with version history, editable from the Configuration page
- Legacy compatibility path: `run_all.py` and the JSON-only orchestration flow

## Active Pass Sequence

- Pass 1: phenomenon extraction plus canonical failure grammar
- Pass 2: mechanism inference
- Pass 3: intervention mapping
- Pass 4: cluster key normalization

The old grammar-only pass remains in the codebase as a deprecated compatibility path, but it is not part of the active numbered sequence.

## Domain Model

See [DOMAIN_MODEL.md](DOMAIN_MODEL.md) for the complete operational domain model, including system purpose, end-to-end data pipeline, table semantics, LLM pipeline design, failure ontology, clustering system, intervention model, and product/domain knowledge.

## Architecture

```
config.py              — Centralised settings (API creds, limits, target ticket, output dir, stage toggles, FORCE_ENRICHMENT, DATABASE_URL, SAFETY_BUFFER_MINUTES, INITIAL_BACKFILL_DAYS, MATCHA_RESPONSE_LLM optional model override)
ts_client.py           — TeamSupport REST API client (fetch tickets, activities, inHANCE users, all-users name→ID mapping, update_ticket with auto LastInhComment/LastCustComment, fetch_ticket_by_id)
activity_cleaner.py    — Text-cleaning pipeline (HTML→text, boilerplate/signature removal)
matcha_client.py       — Matcha LLM API client (send prompts, extract responses, optional responseLLM override via config)
prompt_store.py        — DB-backed prompt/version store and seed loader; runtime source of truth for prompts used by enrichment and RCA passes
db.py                  — Postgres data-access layer (connection pool, migration runner, upsert helpers, enrichment persistence, get_sync_state watermark reader, watermark-safe upsert_sync_state with optional watermark_at override)
run_ingest.py          — DB-backed incremental ingestion CLI (watermark-based sync with automatic created-since merge for opened+closed tickets, single-ticket resync, replay mode, customer metadata refresh, --sentiment flag for post-sync sentiment, --enrich flag for post-sync enrichment (sentiment + priority + complexity + health rollups), status; MAX_TICKETS-safe watermark: sorts tickets by DateModified ascending and advances only to min DateModified of processed tickets when set is truncated, ensuring incremental progress without skipping)
run_sync_customer_attributes.py — One-off/customer-metadata sync from TeamSupport `/Customers` into `customer_attributes` (captures `KeyAcct`, active flag, support group, raw payload)
action_classifier.py   — Deterministic rule-based action classification (no LLM; action_type='Description' → customer_problem_statement)
run_rollups.py         — Rebuild action classification, thread rollups, metrics, and daily open counts from DB state; run_full_rollups() auto-triggers after any new migration; run_analytics_for_tickets() snapshots ALL tickets (not just touched ones) so aging/backlog views always have complete data for today
run_all.py             — Legacy compatibility orchestrator: runs the JSON-oriented stages in sequence, merges fields, single API call per ticket (--force, --no-writeback)
run_enrich_db.py       — DB-only enrichment: score tickets from Postgres (priority + complexity + sentiment by default), no TS API calls
run_csv_import.py      — Bulk-import Activities.csv into DB (synthetic action IDs, streaming, idempotent, derives closed_at from Days Closed)
run_csv_pipe_import.py — Import CSV pipeline results (pass1 / pass3 / pass4 / pass5) into ticket_llm_pass_results; matches the DB-backed runner schema; --force overwrites existing success rows; supports custom filenames via --pass1/--pass3/--pass4/--pass5 flags
run_export.py          — Export canonical DB state to timestamped JSON artifacts
run_pull_activities.py — Part 1: fetch, clean, classify activities → activities JSON
run_sentiment.py       — Part 2: sentiment analysis via Matcha (DB persistence + hash-based skipping when DB available)
run_priority.py        — Part 3: AI priority scoring via Matcha (DB persistence + hash-based skipping when DB available)
run_complexity.py      — Part 4: complexity analysis via Matcha (DB persistence + hash-based skipping when DB available)
run_ticket_pass1.py    — Pass 1: phenomenon extraction + grammar decomposition from full_thread_text via Matcha (DB-only, idempotent, prompt-versioned; absorbs the former grammar pass; includes ticket_name context, violation warning stripping, confidence gate)
pass1_parser.py        — Pass 1 response parser (strict JSON validation, phenomenon + confidence + grammar field extraction with operation normalization; null phenomenon accepted as valid "no observable behavior"; LOW confidence auto-nulls)
run_ticket_pass2.py    — Legacy grammar pass: canonical failure grammar from Pass 1 phenomenon via Matcha (DEPRECATED — grammar now extracted in Pass 1; kept for backward compatibility)
pass2_parser.py        — Pass 2 response parser (strict JSON validation, operation normalization, canonical_failure reconstruction; normalize_operation reused by Pass 1 v2 parser)
run_ticket_pass3.py    — Pass 2: failure mechanism inference from Pass 1 canonical_failure + full_thread_text via Matcha (DB-only, idempotent, prompt-versioned; insufficient_evidence rule prevents fabrication)
pass3_parser.py        — Pass 3 response parser (strict JSON validation, mechanism extraction, phrase-level admin-text rejection allowing technical "customer" references)
run_pass4.py           — Pass 3: intervention mapping from Pass 2 mechanism via Matcha (DB-only, idempotent, prompt-versioned, ROI aggregation, --aggregate-only mode; invalidates stale results for tickets missing required upstream mechanism version)
pass4/mechanism_classes.py  — Normalized mechanism class taxonomy (14 classes including 'other' catch-all with proposed_class for taxonomy expansion)
pass4/intervention_types.py — Normalized intervention type taxonomy (8 types including 'other' catch-all with proposed_type for taxonomy expansion)
pass4/mechanism_classifier.py — Pass 4 response parser (strict JSON validation, taxonomy enforcement with soft 'other' + proposed_class/proposed_type, action validation)
pass4/intervention_mapper.py  — Pass 4 per-ticket LLM orchestrator (prompt build, Matcha call, parse, DB persist)
pass4/intervention_aggregator.py — ROI aggregation engine (mechanism class counts, intervention type counts, top engineering fixes, JSON artifact export)
run_pass5.py           — Pass 4: cluster key normalization from Pass 2 mechanism via Matcha (DB-only, idempotent, prompt-versioned; normalizes mechanism text into reusable snake_case clustering keys)
pass5/cluster_key_parser.py  — Pass 5 response parser (plain-text validation, snake_case enforcement, max 6-word limit, XML tag stripping)
pass5/cluster_key_mapper.py  — Pass 5 per-ticket LLM orchestrator (prompt build, Matcha call, parse, DB persist)
build_cluster_catalog.py — Deterministic cluster/catalog pipeline: loads `ticket_llm_pass_results`, reshapes the native row-per-pass table into analytical wide rows, filters to successful intervention mappings, clusters by `mechanism_class`, computes `(component, operation)` subclusters, writes CSV/JSON artifacts, and persists the latest cluster run into `cluster_runs`, `ticket_clusters`, and `cluster_catalog`
migrations/            — Numbered SQL migration files applied by db.py
web/app.py             — Dash + Mantine web dashboard entry point (read-only, live Postgres queries; run with `python -m web.app`); includes manual dark mode toggle in the header (sun/moon switch with localStorage persistence via clientside callback that toggles `body.dark-mode` CSS class); `render_sidebar_nav` renders nested collapsible `dmc.NavLink` children for pages that declare a `children` list in dashboard.yaml, with `opened` driven by the current pathname so the parent auto-expands when a sub-route is active
web/assets/dark_mode.css — Dark mode CSS: comprehensive `body.dark-mode` scoped overrides for Mantine AppShell, navbar, cards, tabs, inputs, modals, AG Grid (quartz + alpine themes via CSS custom properties), Plotly chart backgrounds, and scrollbars
web/dashboard.yaml     — YAML-driven dashboard configuration (pages, nav, queries, grids, charts, stat cards)
web/renderer.py        — YAML config renderer: parses dashboard.yaml, builds Dash layouts for YAML-driven pages, imports custom page modules
web/data.py            — Data access layer for the web dashboard (SELECT-only for analytics, plus dashboard-local CRUD for saved reports; no TS writes); root cause analytics now read from the persisted deterministic cluster views `vw_latest_mechanism_cluster_catalog` and `vw_latest_mechanism_ticket_clusters` instead of recomputing distributions from raw pass rows; health queries now anchor on active TeamSupport customers from `customer_attributes`, expose `key_account`, exclude Marketing and Sales by default, and support customer-level group-filtered history/explanations; EXCLUDED_CUSTOMERS tuple (currently "InHance Internal") filters internal tickets from all dashboard counts: overview KPIs, ticket list, health grid, backlog trend, drill-downs, and root cause stats/detail; get_filtered_backlog_daily() reconstructs daily backlog trend from tickets table with dynamic WHERE clauses for overview filters; get_tickets_by_customers() returns open tickets for a list of customer names (health drill-down); get_tickets_by_fixes() returns tickets matching (mechanism_class, intervention_type) pairs (engineering fixes drill-down); get_top_clusters_for_customer(customer_names, top_n, open_only) returns top N issue clusters aggregated across selected customers by joining tickets → pass4/pass5 results → cluster_key_rollup_map, with configurable open-only vs all-tickets scope; Cluster rollup analysis queries: get_top_clusters(product_names, top_n) returns top N L1 clusters per product via window function; get_cluster_examples(product, mechanism_class, cluster_key_l1) returns example tickets for a cluster; save_cluster_recommendation() persists engineering recommendations; get_cluster_recommendations() fetches saved recommendations; get_cluster_summary_l2/l1() return aggregate cluster summaries at L2 and L1 granularity; Operations queries are scoped to users whose TeamSupport title contains "Support" (fetched from TS API at startup, cached for process lifetime via _get_support_analysts()): get_analyst_scorecard() computes per-analyst closure count, avg complexity, own-work ratio, zero-contribution closes, and low-contribution % from ticket_actions join; get_analyst_complexity_distribution() returns complexity level counts per analyst; get_analyst_monthly_closures() returns monthly closure counts per analyst; get_analyst_swooper_tickets() returns tickets where an analyst closed but contributed < 25% of InHance actions; get_analyst_action_profile() returns technical work % and scheduling % of each analyst's own actions on closed tickets; get_analyst_severity_profile() returns high-severity (Sev 1) and low-severity closure percentages per analyst; get_analyst_reassignment_profile() returns average within-InHance handoffs per closed ticket by analyst and severity; Operations Overview KPIs (CS group): get_ops_avg_days_to_close(months, group_name) returns monthly avg days-to-close for the group; get_ops_backlog_snapshot(group_name) returns backlog count at Jan 1 vs now; get_ops_most_improved_customers(months, group_name, top_n) returns customers with largest backlog reduction over the period
web/pages/overview.py  — Overview page: KPI stat cards, backlog trend chart, aging distribution, open-by-product, frustrated-by-group chart with click-to-drill-down, chart drill-down; multi-select filter bar (status, severity, product, assignee, customer, group, frustrated) — empty = all open tickets (original DB views), selected values = include only those; all visualizations update including backlog trend (reconstructed from tickets table with filter-matching SQL); clearing filters reverts to original DB-view data; filters update KPI values in-place (no card recreation to avoid drill-down trigger); drill-downs use ticket store when filters active for consistent counts (custom)
web/pages/tickets.py   — Ticket explorer: AG Grid with filters, sorting, row-click navigation to detail, saved filter reports, background sync via subprocess with live log panel and auto-dismiss; tabbed view with Open (default, excludes Closed), All Tickets, and user-saved report tabs; saved reports are tabs rather than chips, can be deleted from the page, and the current tab/filter state is preserved when navigating to ticket detail and back; saved report tabs are reorderable via chevron-left/chevron-right buttons that swap sort_order with the adjacent tab (migration 037); rowSelection uses string format "single" for compatibility; filter persistence via browser sessionStorage — AG Grid filterModel and active tab are saved to a session-scoped dcc.Store on every change and restored automatically when navigating back from ticket detail, so filters survive page transitions until the user clicks Clear Filters; sync-progress-panel hidden via display:none when empty to avoid blank gap in stack layout (custom)
web/pages/ticket_detail.py — Ticket detail: metadata header, thread timeline (newest-first), score cards, wait profile chart, issue summary, refresh button (re-syncs single ticket from TeamSupport via run_ingest.py --ticket-id, then re-renders page); ticket number in the header links to the TeamSupport UI, and the back link preserves the originating Tickets tab/report context (custom)
web/health_explainer.py — Customer health explanation service: builds Matcha payloads from current customer health, selected groups, prior snapshot, and top contributors; loads the active explanation prompt from the DB prompt store; persists generated explanations
web/health_planner.py — Customer health improvement plan service: calls simulate_improvement_to_band for a target band, builds a Matcha payload from the simulation output and contributor details, persists plans to customer_health_improvement_plans versioned by as_of_date (same pattern as explanations)
web/pages/health.py    — Health dashboards: Customer and Product health AG Grid tables; Customer Health now shows one row per active TeamSupport customer, includes `Key Account`, computes default distress across all groups except Marketing/Sales, and supports multi-row "View Tickets" drill-down plus single-customer history modal with group selector, Matcha-backed Explain button, and persisted explanation history; View Tickets drill-down modal now includes a Top Issue Clusters panel (top 3 clusters by ticket count for the selected customer(s)) with an open-only/all-tickets toggle, showing both L1 cluster key and mechanism class per card; Customer Health grid columns use flex sizing with minWidth so all columns fill the available viewport width; column order places Distress and Band immediately after Customer (before Key Acct) for quick triage visibility; grid now surfaces all five distress sub-dimensions (Pressure, Aging, Friction, Concentration, Breadth) matching the Health History dialog breakdown, each with color-coded highlights when above threshold; Generate Plan button produces a greedy target-band simulation (tickets sorted by total_contribution, removed until score falls below threshold) plus a Matcha-generated narrative plan, persisted with as_of_date versioning identical to explanations; target band is user-selectable (at_risk / watch / healthy) via a modal Select; `/health/plans` sub-page (`plans_layout`) renders an AG Grid of all saved plans across all customers with row-click detail modal (custom)
web/pages/operations.py — Operations dashboard: analyst activity metrics covering workload distribution, skill mix, and contribution patterns. Overview KPI cards (CS group): Avg Days to Close (current month + past 6 months weighted average), CS Backlog at Jan 1 vs Now (with delta badge), and Most Improved Customers table (top 5 customers whose open backlog decreased most over last 3 months, showing open-then/open-now/reduction). Analyst Scorecard AG Grid (tickets closed, avg days open, high sev %, technical %, scheduling %, high priority/frustrated counts, own-work ratio, avg other contributors, zero-contribution closes, % low-contribution closes) with highlighted rows for metrics differing notably from team averages and row-click drill-down into ticket details. Technical vs Scheduling Work chart (horizontal grouped bar of technical work % and scheduling % per analyst compared to team averages). High-Severity Closure Share chart (horizontal bar of high-sev % per analyst compared to team average). Avg Handoffs per Ticket by Severity chart (grouped horizontal bar showing average within-InHance analyst handoffs per closed ticket, broken down by severity per analyst). Collaboration Ratio chart (horizontal bar of own-work ratio). Monthly Closures by Analyst line chart (12 months, top 10). Ticket detail drill-down modal shows tickets where the analyst closed with under 25% of InHance actions (custom)
web/pages/deep_dive.py — Deep Dive page: per-analyst / per-product operational analytics with multi-select filter bar (analyst, product, time range defaulting to 12 months). KPI stat cards (tickets closed, avg resolution days, avg complexity, frustrated count/%, avg first response hours). Tickets by Severity bar chart, Action Mix donut (InHance action class distribution), Monthly Closure Trend line chart, Resolution Time Distribution histogram (< 1d / 1-7d / 7-30d / 30-90d / 90+d), Resolution Time by Analyst grouped bar chart (avg vs median days per analyst, hover shows ticket count / first response time / frustrated %), Workload Heatmap (analyst × product with hover showing avg days/complexity/frustrated), and filterable AG Grid of matching tickets with CSV export. All charts and grid update dynamically when filters change (custom)
web/pages/root_cause.py — Root Cause Analytics dashboard: tabbed layout (Dashboard + Detail + Glossary). Dashboard tab: KPI stat cards (tickets analyzed, mechanisms found, interventions mapped, pipeline completion %, top mechanism), pipeline completion funnel chart, mechanism class distribution (horizontal bar), intervention type breakdown (donut), component treemap, operation verb frequency bar chart, Sankey flow diagram (Component → Mechanism Class → Intervention Type), root cause by product (stacked bar), Top Failure Patterns by Product section (grouped horizontal bar chart of top 5 L1 clusters per product + AG Grid with row-click drill-down modal showing example tickets from v_cluster_examples), top engineering fixes AG Grid (ROI-ranked from the latest deterministic cluster run) with multi-row checkbox selection and "View Tickets" drill-down modal; Cluster Catalog AG Grid with Subcluster Breakdown chart — segmented control toggles between "By Component" (aggregated, default) and "Component → Operation" (granular) views to avoid an oversized Other bucket when component/operation pairs are too fragmented. Detail tab: original AG Grid of pass-processed tickets with CSV export, row-click expands Pass 1/2/3 cards + cleaned thread text. Glossary tab: reference guide for pipeline passes, canonical failure grammar fields, all 14 mechanism classes, all 8 intervention types, and dashboard metrics (custom)
web/pages/config_view.py — Pipeline config editor plus prompt administration UI (editable settings with Save button writes to config.py + live reload; MATCHA_RESPONSE_LLM model override, toggles, text fields, sync status; Prompts section edits DB-backed prompts by creating new versions rather than overwriting; custom)
web_requirements.txt   — Python dependencies for the web dashboard (dash, dash-mantine-components, dash-ag-grid, PyYAML, etc.)
pipeline/__init__.py   — Standalone CSV pipeline package (no DB dependency, self-contained)
pipeline/csv_runner.py — CSV-only pipeline orchestrator: Pass 1→3→4→5 using Matcha LLM + existing parsers, per-row error isolation, background job manager with disk + blob-persisted state tracking, progress callbacks; all output CSVs include `prompt_version` column for traceability; `_load_prompt` warns when falling back to flat-file prompts; uploads each pass CSV to Azure Blob Storage on completion so results survive container restarts; per-job run.log captured via JobLogger (timestamps, per-row failures, tracebacks) and uploaded to blob on completion
pipeline/blob_store.py — Azure Blob Storage helper: uploads/downloads output CSVs and job state JSON to the `csv-pipeline-results` container; graceful fallback when no AZURE_STORAGE_CONNECTION_STRING is set (local-dev mode); list_blobs() for browsing all blobs by prefix, upload_text() for plain-text log uploads, download_blob_bytes() for raw blob access
pipeline/app.py        — Standalone Flask web app (port 5001): upload CSV, run pipeline as background job, poll status via JSON API, download result CSVs (pass1/pass3/pass4/pass5_results.csv); download route falls back to blob storage when local file is missing after container restart; /files browser page for navigating Azure Blob Storage with inline preview of .log and .json files; /api/files and /api/files/download/<path> endpoints
pipeline/templates/index.html — Single-page HTML/JS UI for CSV upload, progress bar with pass indicator, download links, link to blob storage browser; job list includes Log links for completed/failed jobs
pipeline/templates/files.html — Blob Storage browser UI: folder navigation, breadcrumbs, file listing with size/date, inline preview for .log and .json files, direct download for CSVs
pipeline/azure.env     — Deployment resource names (RESOURCE_GROUP, ACR_NAME, APP_NAME, IMAGE) sourced by deploy.sh
pipeline/Dockerfile    — Container image: python:3.13-slim + gunicorn + flask + requests + azure-storage-blob; copies pipeline code + parsers + pass5/ + prompt_store + prompts (no DB files); serves on port 80 with 600s timeout for long LLM runs
pipeline/deploy.sh     — Azure Container Apps deployment script: sources pipeline/azure.env for resource names; full deploy (no args) or --build-only for quick rebuild + update; Matcha credentials + AZURE_STORAGE_CONNECTION_STRING passed via az containerapp update --set-env-vars
run_export_pipeline_input.py — Export up to 1000 PM/PowerManager/Impresa tickets without RCA (no successful pass1_phenomenon) and less than 18 months old as a CSV-pipeline-compatible input file (ticket_id, ticket_name, full_thread_text)
```

## Data Dictionary

All database objects live in the `tickets_ai` schema. Every table that references a ticket carries both `ticket_id` (integer PK from TeamSupport) and `ticket_number` (human-readable, denormalised in Phase 8).

### Tables

| Table | Category | Purpose |
|-------|----------|---------|
| `tickets` | Source truth | Canonical ticket rows keyed by `ticket_id` |
| `ticket_actions` | Source truth | Canonical action/activity rows keyed by `action_id` |
| `sync_state` | Source truth | Per-source watermark / cursor tracking |
| `ingest_runs` | Source truth | Audit log of each ingestion run |
| `customer_attributes` | Source truth | TeamSupport customer metadata (`is_active`, `KeyAcct`, support group, raw payload) used by customer health and dashboard labeling |
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
| `cluster_runs` | Clustering | Metadata for deterministic cluster rebuilds (`cluster_method='mechanism_class_catalog'`) |
| `ticket_clusters` | Clustering | Ticket-to-cluster assignments for the latest deterministic mechanism-class run |
| `cluster_catalog` | Clustering | Persisted cluster catalog with counts, dominant fields, examples, and subcluster JSON |
| `ticket_snapshots_daily` | Snapshot/Health | Point-in-time snapshot of ticket state on a given date |
| `customer_ticket_health` | Snapshot/Health | Persisted per-customer-per-group health rollup for a given date |
| `customer_health_ticket_contributors` | Snapshot/Health | Ticket-level health contributor rows used to aggregate customer distress, history, drill-down, and explanations |
| `product_ticket_health` | Snapshot/Health | Persisted per-product-per-group health rollup (volume, complexity, dev-touched rate) |
| `daily_open_counts` | Snapshot/Health | Aggregated daily counts of open tickets by product, status, and last-active participant (from ticket_participants) |
| `customer_health_explanations` | Dashboard | Persisted Matcha-generated explanations for customer distress snapshots, including selected group filters |
| `saved_reports` | Dashboard | Named saved filter presets for the ticket explorer grid; `sort_order` column controls tab display order, reorderable via move-left/move-right buttons |
| `prompts` / `prompt_versions` | Configuration | DB-backed prompt catalog, active-version tracking, and immutable prompt revision history |

### Views

| View | Purpose |
|------|---------|
| `vw_latest_ticket_sentiment` | Latest sentiment row per ticket |
| `vw_latest_ticket_priority` | Latest priority row per ticket |
| `vw_latest_ticket_complexity` | Latest complexity row per ticket |
| `vw_latest_ticket_issue_summary` | Latest issue summary per ticket |
| `vw_ticket_analytics_core` | Master join: tickets + metrics + rollups + 4 latest views; excludes status='Open' test tickets |
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
| `vw_ticket_pass4_results` | Latest Pass 4 intervention mapping per ticket (mechanism_class, intervention_type, intervention_action) |
| `vw_intervention_roi` | Engineering ROI: ticket counts by mechanism_class × intervention_type from raw Pass 4 success rows |
| `vw_latest_mechanism_cluster_catalog` | Latest persisted deterministic cluster catalog for the root-cause dashboard |
| `vw_latest_mechanism_ticket_clusters` | Ticket-to-cluster assignments for the latest deterministic cluster run |
| `vw_ticket_pass_pipeline` | Full legacy/internal pipeline status: Pass 1 + legacy grammar + mechanism + intervention side-by-side per ticket |
| `v_root_cause_cluster` | Flattened one-row-per-ticket view joining mechanism_class (Pass 3), cluster_key (Pass 4), mechanism (Pass 2), and intervention_action (Pass 3) with product_name; only tickets with a non-null cluster_key |
| `v_ticket_failure_flat` | Flat passthrough of `v_root_cause_cluster` for one-row-per-ticket failure inspection |
| `v_cluster_summary_l2` | Aggregate ticket counts grouped by product_name, mechanism_class, and cluster_key (L2 granularity) |
| `v_cluster_summary_l1` | Aggregate ticket counts rolled up to broader L1 buckets via `cluster_key_rollup_map`; unmapped L2 keys retain their original value |
| `v_cluster_examples` | Example tickets per cluster with both L1 and L2 keys, mechanism, and intervention_action |
| `cluster_key_rollup_map` | L2→L1 mapping table: maps fine-grained cluster_key values to broader reusable buckets; supports notes and is_active flag |
| `cluster_recommendations` | Persisted per-cluster engineering recommendations with confidence, source_model, and timestamps |

Note: the active user-facing pass numbering is now:

- Pass 1 = phenomenon + grammar decomposition (`run_ticket_pass1.py`)
- Pass 2 = mechanism inference (`run_ticket_pass3.py`)
- Pass 3 = intervention mapping (`run_pass4.py`)
- Pass 4 = cluster key normalization (`run_pass5.py`)

`run_ticket_pass2.py` remains only as a backward-compatibility path for historical grammar rows and prompt versions.

## Data Flow

1. **Fetch tickets** — `ts_client.fetch_open_tickets(ticket_number=...)` queries the TeamSupport `/Tickets` endpoint. When `TARGET_TICKET` is set, the ticket is fetched by number regardless of open/closed status. When no target is set, only open tickets are returned (`isClosed=False`) with full pagination. If the API returns 403 (rate limit), the pipeline falls back to `Activities.csv`.
2. **CSV fallback** — When the API is rate-limited or unavailable, `run_pull_activities.py` reads `Activities.csv` from the project root. Party classification uses cached inHANCE names from prior activity JSON files; if none exist, party is set to `"unknown"`. Some metadata fields (`ticket_id`, `status`, `severity`, `assignee`, `date_created`, `date_modified`) are unavailable from CSV.
2. **Limit tickets** — When not targeting a specific ticket, `config.MAX_TICKETS` (default **0** = unlimited) caps how many are processed.
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
8. **Build prompt** — The active `sentiment` prompt is loaded from the DB prompt store and combined with the customer comments as a JSON input block.
9. **Call Matcha** — `matcha_client.call_matcha()` sends the prompt to the Matcha LLM API with retry logic for transient failures.
9a. **Parse response** — `run_sentiment.py` strips markdown code fences (`` ```json `` blocks) from Matcha responses before JSON parsing, with regex fallback extraction. Unlike `run_priority.py` and `run_complexity.py` (which skip fence-stripping and go directly to regex extraction), sentiment has an explicit fence-removal step before the shared regex fallback. The response includes `frustrated_reason` — a one-sentence explanation of the frustration classification.
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
| `party`         | `inh` (inHANCE CS team), `cust` (customer), or `unknown` (CSV fallback when no cached inHANCE names exist) |
| `is_visible`    | `true` if visible on portal (public), `false` if private |
| `description`   | Cleaned plain-text body                             |

### Part 3 — AI Priority Scoring

11. **Build input** — `run_priority.py` reads the latest activities JSON, extracts ticket metadata + all activities into the input format expected by the active DB-backed `ai_priority` prompt.
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
| `frustrated_reason` | One-sentence explanation of frustration classification (or `null` when not frustrated) |
| `activity_id`   | ID of first frustration activity (or `null`)        |
| `created_at`    | Timestamp of first frustration activity (or `null`) |

### Part 4 — Complexity Analysis

16. **Build ticket history** — `run_complexity.py` reads the latest activities JSON and builds a text representation of each ticket (metadata + chronological activity history) for the active DB-backed `complexity` prompt template.
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
| `MAX_TICKETS`      | `0`                    | Max tickets to pull (0=unlimited)    |
| `TARGET_TICKET`    | *(empty)*              | Comma-delimited ticket number(s) to target |
| `RUN_SENTIMENT`    | `1`                    | Run sentiment analysis (0 = skip)    |
| `RUN_PRIORITY`     | `1`                    | Run AI priority scoring (0 = skip)   |
| `RUN_COMPLEXITY`   | `1`                    | Run complexity analysis (0 = skip)   |
| `LOG_TO_FILE`      | `1`                    | Save pipeline logs to output dir     |
| `LOG_API_CALLS`    | `1`                    | Save API call log to output dir      |
| `CUST_COMMENT_COUNT`| `4`                   | Customer comments sent to Matcha     |
| `OUTPUT_DIR`       | *(absolute path to `output/`)* | Where JSONL files are written |
| `DATABASE_URL`     | *(local Postgres DSN)* | Postgres DSN; empty = JSON-only mode |
| `DATABASE_SCHEMA`  | `tickets_ai`           | Postgres schema for all pipeline tables |
| `TS_WRITEBACK`     | `0`                    | Enable TeamSupport write-back (`1` = on, `0` = off) |
| `FORCE_ENRICHMENT` | `1`                    | Bypass hash-based skipping on first run (`1` = force, `0` = incremental) |
| `SKIP_OUTPUT_FILES`| `1`                    | Skip JSON artifact files when DB is active (`1` = skip) |

## Logging

When `LOG_TO_FILE=1` (default), all pipeline stdout/stderr is teed to `output/pipeline_YYYYMMDD_HHMMSS.log`.

When `LOG_API_CALLS=1` (default), every TeamSupport and Matcha API call (GET, PUT, POST) is recorded in `output/api_calls.json` with timestamp, method, URL, params/payload, and HTTP status code. Matcha entries include the full payload (including the prompt text); no redaction is applied.

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

When `DATABASE_URL` is set, the pipeline can persist tickets, actions, enrichment results, and sync state to a Postgres database via `db.py`. The default `config.py` ships with a local Postgres DSN; set `DATABASE_URL` to an empty string to run in JSON-only mode (all existing scripts work unchanged).

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

### Deterministic Cluster Catalog

The clustering pipeline is now implemented and persisted in Postgres.

- Entry point: `python build_cluster_catalog.py`
- Source: `tickets_ai.ticket_llm_pass_results`
- Scope: successful intervention-mapping tickets only
- Cluster ID: `mechanism_class`
- Subclusters: `(component, operation)`
- Method: deterministic grouping only; no embeddings and no ML

The script supports both a wide analytical source shape with human-readable columns such as `Ticket #`, `Pass 4`, and `Mechanism Class`, and the repo's native row-per-pass table shape. When pointed at the native table, it reshapes the latest pass rows per ticket into that same analytical wide shape before computing the catalog.

Each rebuild:

- writes `cluster_catalog.csv`, `cluster_catalog.json`, and `ticket_cluster_mapping.csv`
- creates a fresh `cluster_runs` row with `cluster_method='mechanism_class_catalog'`
- replaces prior persisted rows for that method
- populates `ticket_clusters` with one assignment per ticket
- populates `cluster_catalog` with counts, dominant fields, examples, and subcluster JSON

The root-cause dashboard now reads from:

- `vw_latest_mechanism_cluster_catalog`
- `vw_latest_mechanism_ticket_clusters`

This gives the dashboard a stable persisted cluster snapshot instead of recomputing its distributions directly from raw Pass 4 rows on each page load.

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

**Important:** Rollups must be built before enrichment for hashes to exist. In normal operation they are rebuilt automatically after `run_ingest.py` and `run_csv_import.py`; run `python run_rollups.py all` only for manual rebuilds or repair work.

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

# 3. Rollups/health rebuild automatically after sync.
#    Use this only for a manual rebuild or repair:
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

# 3. Rollups rebuild automatically after import.
#    Use this only if you need a manual rebuild before enrichments:
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
| `Assigned To` | `tickets.assignee` | |
| `Group Name` | `tickets.group_name` | |
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

- **`SKIP_OUTPUT_FILES`** (default `1`): When `1`, **no** files are written to the `output/` directory during enrichment — this includes activities JSON, enrichment result JSON, pipeline log files, and API call logs. This is useful when the DB is the canonical store and file artifacts are unwanted. When `0`, all files are written as usual.

## Web Dashboard

A live analytics dashboard built with Dash + Dash Mantine Components + AG Grid. Reads directly from the Postgres database — **no writes to the DB or TeamSupport**. All queries are SELECT-only through `web/data.py`.

### Running

```bash
# Install dashboard dependencies
pip install -r web_requirements.txt

# Start the dashboard (default: http://localhost:8050)
python3 -m web.app

# Override port or disable debug mode
WEB_PORT=9000 WEB_DEBUG=0 python3 -m web.app

# For production (gunicorn)
gunicorn web.app:server -b 0.0.0.0:8050
```

Requires `DATABASE_URL` to be set (reads from the `tickets_ai` schema).

### Pages

| Page | URL | Data Source | Description |
|------|-----|------------|-------------|
| Overview | `/` | `vw_ticket_analytics_core`, `vw_backlog_daily`, `vw_backlog_daily_by_severity`, `vw_backlog_aging_current` | KPI stat cards (open/HP/HC/frustrated), backlog trend stacked-area chart (severity breakdown: High=red, Medium=amber, Low=blue with total line overlay), aging distribution bar chart, open-by-product breakdown. **Click any bar** to drill down into the underlying tickets in a modal grid. |
| Tickets | `/tickets` | `vw_ticket_analytics_core`, `saved_reports` | AG Grid explorer with column filters, sorting, floating filters, saved-report tabs, report deletion, and tab-preserving navigation to detail. Click any row to navigate to detail. |
| Ticket Detail | `/ticket/{id}` | `vw_ticket_analytics_core`, `ticket_actions`, `vw_ticket_wait_profile` | Metadata header, TeamSupport ticket link, tabbed view: Thread (chronological action cards with party-colored borders), Scores (priority/complexity/sentiment cards), Wait Profile (horizontal bar chart of time per state), Summary (issue/cause/mechanism/resolution) |
| Health | `/health` | `customer_health_ticket_contributors`, `product_ticket_health`, `customer_attributes`, `customer_health_explanations` | Customer distress grid and history/explanation workflow. Customer Health shows all active TeamSupport customers, includes `Key Account`, and defaults to all non-Marketing/non-Sales groups with zero-ticket rows when applicable. |
| Root Cause | `/root-cause` | `vw_latest_mechanism_cluster_catalog`, `vw_latest_mechanism_ticket_clusters`, latest pass rows from `ticket_llm_pass_results` | Persisted deterministic root-cause cluster analytics plus per-ticket pass drill-down |
| Config | `/config` | `config.*` attributes, `sync_state`, `prompts`, `prompt_versions` | Editable pipeline settings plus DB-backed prompt administration with immutable prompt version history |

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
- **Deterministic cluster catalog** (5 tests): success filtering, dominant-value selection, catalog construction, mapping construction, DB serializer payloads
- **DB upserts** (11 tests): idempotent INSERT ON CONFLICT, first_ingested_at stability, sync_state, ingest_runs
- **Incremental sync** (9 tests): watermark reading, safety buffer filtering, watermark advancement on success, no advancement on targeted/replay/failed syncs, empty result handling
- **Hash-based skipping** (9 tests): all three enrichment types, force override, missing rollups
- **Write-back** (5 tests): LastInhComment/LastCustComment timestamp derivation
- **Operational fixes** (11 tests): cache poisoning, atomic writes, retry logic, transaction batching
- **Post-sync rollups** (4 tests): CSV import returns upserted IDs, rollup rebuild triggered after import, skipped on dry-run
- **Pass 1 — phenomenon** (35 tests): parser, selection logic, idempotency, DB persistence, malformed handling, success flow, prompt template
- **Pass 2 — grammar** (51 tests): parser, operation normalization, canonical failure reconstruction, selection logic, idempotency, DB persistence, malformed handling, success flow, prompt template
- **Web dashboard data** includes regression coverage for the persisted root-cause cluster views

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
| **Clustering** | `cluster_runs`, `ticket_clusters`, `cluster_catalog` | Persisted deterministic mechanism-class cluster catalog and ticket assignments |
| **Snapshot / Health** | `ticket_snapshots_daily`, `customer_ticket_health`, `product_ticket_health`, `customer_health_improvement_plans` | Point-in-time backlog snapshots, customer/product health rollups, and versioned improvement plans (target band, projected score/band, ticket list, AI narrative, as_of_date) |

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

### Enrichment / Clustering Tables

Some enrichment tables remain schema-only, but the deterministic cluster catalog is now populated by the normal clustering rebuild:

- `ticket_issue_summaries`: LLM-generated issue/cause/mechanism/resolution summaries
- `ticket_embeddings`: Vector embeddings for similarity search
- `cluster_runs`, `ticket_clusters`, `cluster_catalog`: Persisted deterministic mechanism-class cluster results produced by `build_cluster_catalog.py`
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

## Pass 1 v2 — Phenomenon Extraction + Grammar Decomposition (LLM Multi-Pass Pipeline)

### Overview

Pass 1 v2 extracts the observable system behavior (phenomenon) from each ticket's `full_thread_text` using the Matcha LLM endpoint, and simultaneously decomposes it into the standardized operational grammar `<Component> + <Operation> + <Unexpected State>`. This merges what was formerly two separate passes (Pass 1 + Pass 2) into a single LLM call.

Key improvements in v2:
- **Merged grammar extraction** — component, operation, unexpected_state, and canonical_failure are extracted alongside the phenomenon in a single pass, eliminating the need for a separate Pass 2 call
- **Ticket name context** — the ticket title is provided as a separate field with explicit guidance to NOT extract phenomena from titles alone
- **Violation warning stripping** — automated SLA violation lines are stripped from thread text before sending to the LLM
- **Confidence gate** — the model assesses confidence (HIGH/MEDIUM/LOW); LOW confidence auto-nulls the phenomenon and grammar fields, replacing the prior 6 categorical null rules

### Architecture

| Component | File | Purpose |
|-----------|------|---------|
| Migration | `migrations/005_llm_pass_results.sql` | Creates `ticket_llm_pass_results` table + `vw_ticket_pass1_results` view |
| Prompt | `prompt_versions` (`pass1_phenomenon` key; seeded from `prompts/pass1_phenomenon.txt`) | Active Pass 1 v2 prompt template with `{{input_text}}` and `{{ticket_name}}` placeholders |
| Parser | `pass1_parser.py` | Strict JSON validation for phenomenon + confidence + grammar fields; imports `normalize_operation` from `pass2_parser` |
| Orchestrator | `run_ticket_pass1.py` | CLI entrypoint + batch processing logic; strips violation warnings, passes ticket_name |
| DB helpers | `db.py` | `insert_pass_result`, `update_pass_result`, `delete_prior_failed_pass`, `get_latest_pass_result`, `fetch_pending_pass1_tickets` (now returns 3-tuple with ticket_name) |
| Tests | `tests/test_pass1.py` | Focused tests (parser, selection, idempotency, persistence, malformed handling) |

### Data Flow

```
tickets.ticket_name + ticket_thread_rollups.full_thread_text
    → strip violation/SLA warning lines
    → load active prompt text from DB prompt store (`pass1_phenomenon`)
    → substitute {{ticket_name}} and {{input_text}}
    → call Matcha endpoint
    → parse JSON response → extract phenomenon, confidence, component, operation, unexpected_state
    → normalize operation verb (via pass2_parser.normalize_operation)
    → reconstruct canonical_failure from parsed fields
    → store in ticket_llm_pass_results (raw + parsed + status + all projected columns)
```

### Table: ticket_llm_pass_results

| Column | Type | Description |
|--------|------|-------------|
| id | BIGSERIAL PK | Row identifier |
| ticket_id | BIGINT FK | References tickets(ticket_id) |
| ticket_number | TEXT | Human-readable ticket number (denormalized from tickets on insert) |
| product_name | TEXT | Product name (denormalized from tickets on insert) |
| pass_name | TEXT | Stage name (e.g. `pass1_phenomenon`, `pass2_grammar`) |
| input_text | TEXT | The text sent to the model (full_thread_text for Pass 1, phenomenon for Pass 2) |
| prompt_version | TEXT | Version identifier for the prompt |
| model_name | TEXT | Model identifier (e.g. `matcha-27301`) |
| raw_response_text | TEXT | Raw text response from Matcha |
| parsed_json | JSONB | Parsed JSON payload |
| phenomenon | TEXT | Pass 1 output; NULL for other passes |
| component | TEXT | Pass 1 v2 output (formerly Pass 2): subsystem/module involved |
| operation | TEXT | Pass 1 v2 output (formerly Pass 2): normalized operation verb |
| unexpected_state | TEXT | Pass 1 v2 output (formerly Pass 2): unexpected system outcome |
| canonical_failure | TEXT | Pass 1 v2 output (formerly Pass 2): `<Component> + <Operation> + <Unexpected State>` |
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
- New or edited prompts are versioned in the DB prompt store. Seed copies can still live under `prompts/`, but runtime reads use the active DB version.
- New columns can be added via migration for pass-specific projected fields

---

## Pass 2 — Canonical Failure Grammar (DEPRECATED)

### Overview

**Pass 2 is deprecated.** Grammar extraction (component, operation, unexpected_state, canonical_failure) is now performed in Pass 1 v2 in a single LLM call. Pass 2 code remains functional for backward compatibility with v1 results.

Previously, Pass 2 converted each Pass 1 phenomenon into the standardized operational grammar:

`<Component> + <Operation> + <Unexpected State>`

This produces structured, normalized failure descriptions suitable for aggregation and root-cause analysis.

### Architecture

| Component | File | Purpose |
|-----------|------|---------||
| Migration | `migrations/015_pass2_grammar.sql` | Adds `component`, `operation`, `unexpected_state`, `canonical_failure` columns + `vw_ticket_pass2_results` and `vw_ticket_pass_pipeline` views |
| Prompt | `prompt_versions` (`pass2_grammar` key; legacy seed in `prompts/pass2_grammar.txt`) | Pass 2 prompt template with `{{input_text}}` placeholder (receives phenomenon) |
| Parser | `pass2_parser.py` | Strict JSON validation, operation normalization via synonym map, canonical_failure reconstruction |
| Orchestrator | `run_ticket_pass2.py` | CLI entrypoint + batch processing logic |
| DB helpers | `db.py` | Extended `update_pass_result` (component/operation/unexpected_state/canonical_failure), `fetch_pending_pass2_tickets` |
| Tests | `tests/test_pass2.py` | 51 focused tests (parser, normalization, reconstruction, selection, idempotency, persistence, malformed handling) |

### Data Flow

```
ticket_llm_pass_results.phenomenon (from successful Pass 1)
    → load active prompt text from DB prompt store (`pass2_grammar`)
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

Near-synonyms are mapped automatically (e.g. `upload` → `import`, `build` → `generate`, `modify` → `update`, `resequence` → `update`). Unknown operations cause a validation failure.

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

---

## Pass 3 — Failure Mechanism Inference (LLM Multi-Pass Pipeline)

### Overview

Pass 3 infers the underlying system mechanism that explains each canonical failure from Pass 2. Unlike earlier passes, Pass 3 receives both the canonical failure string AND the original ticket thread text, enabling evidence-grounded mechanism inference rather than pure speculation.

Pass 3 also classifies each failure into a category (software_defect, configuration, user_training, data_issue) and reports whether the mechanism is grounded in thread evidence or inferred.

### Architecture

| Component | File | Purpose |
|-----------|------|---------|
| Migration | `migrations/017_pass3_mechanism.sql` | Adds `mechanism` column + `vw_ticket_pass3_results` view |
| Prompt | `prompt_versions` (`pass3_mechanism` key; seeded from `prompts/pass3_mechanism.txt`) | Pass 3 prompt template with `{{input_text}}` and `{{thread_context}}` placeholders |
| Parser | `pass3_parser.py` | Strict JSON validation for `{"mechanism": "...", "category": "...", "evidence": "..."}` responses |
| Orchestrator | `run_ticket_pass3.py` | CLI entrypoint + batch processing logic |
| DB helpers | `db.py` | `fetch_pending_pass3_tickets` (returns 3-tuple with thread context), `update_pass_result` |
| Tests | `tests/test_pass3.py` | 50 focused tests (parser, category/evidence validation, selection, thread context, idempotency, persistence, malformed handling) |

### Data Flow

```
ticket_llm_pass_results.canonical_failure (from successful Pass 2)
  + ticket_thread_rollups.technical_core_text (original ticket thread)
    → load active prompt text from DB prompt store (`pass3_mechanism`)
    → substitute {{input_text}} with canonical_failure
    → substitute {{thread_context}} with technical_core_text (capped at 3000 chars)
    → call Matcha endpoint
    → parse JSON response → extract mechanism, category, evidence
    → store in ticket_llm_pass_results (raw + parsed + status + mechanism column)
```

### Pass 3 Output Fields

| Field | Description | Example |
|-------|-------------|---------|
| mechanism | Internal system behavior explaining the failure | `Billing calculation logic applies service charge rule twice during bill generation` |
| category | Failure classification | `software_defect` |
| evidence | Grounding indicator | `from_thread` |

### Category Values

| Category | Meaning |
|----------|---------|
| `software_defect` | Bug or logic error in the software |
| `configuration` | System or environment configuration needed adjustment |
| `user_training` | Resolved by educating the user on existing functionality |
| `data_issue` | Resolved by correcting data, not code |

### Evidence Values

| Evidence | Meaning |
|----------|---------|
| `from_thread` | Mechanism is grounded in observable thread content |
| `inferred` | Mechanism is inferred from canonical failure alone (thread was sparse) |

### Thread Context Integration

Pass 3 receives `technical_core_text` from `ticket_thread_rollups` as additional context alongside the canonical failure. This allows the LLM to:

- Ground mechanisms in actual resolution evidence from the thread
- Identify non-software-defect resolutions (training, configuration)
- Avoid fabricating specific technical details when the thread doesn't support them

The thread context is capped at 3000 characters and provided via a `{{thread_context}}` placeholder. When no thread text is available, the placeholder is filled with `"(no thread context available)"`.

### Selection Logic

- Requires a successful Pass 2 result with non-null, non-empty canonical_failure
- LEFT JOINs `ticket_thread_rollups` to include `technical_core_text` (defaults to empty string if missing)
- Excludes tickets with an existing successful Pass 3 result for the current prompt version (unless `--force`)
- `--failed-only` restricts to tickets with a prior failed Pass 3 attempt

### Prompt Version

Current prompt version: **2** (upgraded from v1 to include thread context, category, and evidence fields).

### CLI Usage

```bash
python run_ticket_pass3.py --limit 100
python run_ticket_pass3.py --ticket-id 99784
python run_ticket_pass3.py --ticket-id 99784,98154,100289
python run_ticket_pass3.py --failed-only
python run_ticket_pass3.py --force
```

### Quality Improvements (v2 Prompt)

The v2 prompt addresses issues identified in a 30-ticket quality evaluation:

1. **Thread-grounded inference** — Mechanisms are now based on actual ticket resolutions instead of speculative technical explanations
2. **Non-software-defect categorization** — Tickets resolved via user training or configuration are categorized correctly instead of being forced into a software-defect mechanism
3. **Evidence transparency** — Downstream consumers can distinguish grounded vs speculative mechanisms
4. **Reduced hallucination** — The prompt explicitly instructs: "NEVER fabricate specific technical details when the thread does not support them"

### Companion Prompt Improvements (v2)

Pass 1 and Pass 2 prompts were also improved in the same release:

- **Pass 1**: Strengthened null-return guidance with 6 explicit scenarios where phenomenon should be null (empty threads, administrative-only content, user-training resolutions). Added multi-issue ticket guidance to prefer the original reported problem.
- **Pass 2**: Added `load` vs `display` operation disambiguation with positive/negative examples. Tightened `unexpected_state` to be shorter than the phenomenon and avoid echoing redundant context.

### Pass 4 Dependency Update

`run_pass4.py` `PASS3_PROMPT_VERSION` updated from `"1"` to `"2"` so that Pass 4 reads mechanisms from Pass 3 v2 results.

### 30-Ticket Evaluation Results (Post v2 Prompt Changes)

All 4 passes were rerun with `--force` against 30 tickets from `root_c_extraction.csv`:

| Pass | Metric | Result |
|------|--------|--------|
| Pass 1 | Phenomena extracted | 22/30 (8 null returns) |
| Pass 2 | Grammars produced | 22/22 (scoped to Pass 1 successes) |
| Pass 2 | `load` operations | 9 of 22 (vs 9/30 in CSV baseline) |
| Pass 2 | Operation match vs CSV | 18/22 |
| Pass 3 v2 | Mechanisms produced | 30/30 |
| Pass 3 v2 | Evidence: from_thread | 22 |
| Pass 3 v2 | Evidence: inferred | 8 |
| Pass 3 v2 | Categories | 24 software_defect, 4 configuration, 1 data_issue, 1 user_training |
| Pass 4 | Interventions produced | 29/30 (1 expected failure: user_training ticket) |
| Pass 4 | Mechanism class match vs CSV | 15/29 |
| Pass 4 | Intervention type match vs CSV | 19/29 |

**Key observations:**

- **Pass 1 null-guidance** is working but may be over-suppressing: 8 tickets with real phenomena returned null. All 8 have thread text (391–32,369 chars), so the model is choosing null based on content ambiguity rather than text absence.
- **Pass 3 evidence grounding** works well: 22/30 mechanisms are `from_thread`, with thread-specific details (e.g., "missing GEO code API key", "Impresa and GP databases must reside on same SQL server/instance").
- **Pass 3 non-defect categorization** is functioning: 1 ticket correctly classified as `user_training` (mechanism: `user_knowledge_gap`), 4 as `configuration`.
- **Pass 4 divergence from CSV** is expected: the new v2 mechanisms differ from v1, producing different (often more specific) mechanism_class and intervention_type assignments. The CSV baseline was generated from v1 mechanisms.

## Exclude status='Open' Test Tickets (Migration 019)

Tickets with `status='Open'` are user test tickets and must be excluded from all analytics, views, backlog calculations, and exports.

### Migration 019 — `019_exclude_open_status.sql`

Rebuilds the following views with `status != 'Open'` filters:

- **`vw_ticket_analytics_core`** — added `WHERE COALESCE(t.status, '') != 'Open'`
- **`vw_backlog_aging_current`** — added `'Open'` to the `NOT IN` list
- **`vw_backlog_daily_by_severity`** — added `AND COALESCE(t.status, '') != 'Open'` to the join condition
- **`vw_backlog_product_severity_powman`** — added `AND COALESCE(t.status, '') != 'Open'`

### Python Code Changes

| File | Function/Location | Change |
|------|-------------------|--------|
| `db.py` | `fetch_ticket_numbers_by_status()` | Added `'Open'` to all three `NOT IN` branches |
| `db.py` | `get_open_ticket_ids()` | Added `AND COALESCE(status, '') != 'Open'` |
| `run_rollups.py` | `is_open` check (snapshot daily) | Added `"open"` to the `not in` tuple |
| `run_rollups.py` | Customer health query | Added `'Open'` to all `NOT IN` lists |
| `run_rollups.py` | `daily_open_counts` query | Added `'Open'` to `NOT IN` list |
| `web/data.py` | `get_open_by_status()` | Added `AND COALESCE(status, '') != 'Open'` |
| `web/data.py` | `get_drilldown_tickets()` | Added `'Open'` to `NOT IN` list |
| `web/data.py` | `get_root_cause_tickets()` | Added `AND COALESCE(t.status, '') != 'Open'` |
| `web/data.py` | `get_root_cause_stats()` | Added `AND COALESCE(t.status, '') != 'Open'` |
| `export_1000_no_rc.py` | SQL query | Added `AND COALESCE(t.status, '') != 'Open'` |
| `run_export_pipeline_input.py` | SQL query | Added `AND COALESCE(t.status, '') != 'Open'` |

**Design decision:** Ingestion (`run_ingest.py` / `ts_client.py`) still stores status='Open' tickets in the DB to keep a complete mirror of TeamSupport. Exclusion happens at the query/view layer.

## Exclude assignee='Marketing' Tickets (Migration 020)

Tickets assigned to "Marketing" are not real support tickets and must be excluded from all analytics, views, backlog calculations, and exports — same treatment as status='Open' test tickets.

### Migration 020 — `020_exclude_marketing_assignee.sql`

Rebuilds the same four views from migration 019 with an additional `AND COALESCE(t.assignee, '') != 'Marketing'` filter:

- **`vw_ticket_analytics_core`**, **`vw_backlog_aging_current`**, **`vw_backlog_daily_by_severity`**, **`vw_backlog_product_severity_powman`**

### Python Code Changes

All locations updated in migration 019 received an additional `assignee != 'Marketing'` condition:

| File | Function/Location | Change |
|------|-------------------|--------|
| `db.py` | `fetch_ticket_numbers_by_status()` | Added `AND COALESCE(t.assignee, '') != 'Marketing'` to all branches |
| `db.py` | `get_open_ticket_ids()` | Added `AND COALESCE(assignee, '') != 'Marketing'` |
| `run_rollups.py` | `is_open` check (snapshot daily) | Added `assignee != "Marketing"` to condition |
| `run_rollups.py` | Customer health query | Added `AND COALESCE(t.assignee, '') != 'Marketing'` to FILTER clauses |
| `run_rollups.py` | `daily_open_counts` query | Added `AND COALESCE(t.assignee, '') != 'Marketing'` |
| `web/data.py` | `get_open_by_status()` | Added `AND COALESCE(assignee, '') != 'Marketing'` |
| `web/data.py` | `get_drilldown_tickets()` | Added `COALESCE(t.assignee, '') != 'Marketing'` to conditions |
| `web/data.py` | `get_root_cause_tickets()` | Added `AND COALESCE(t.assignee, '') != 'Marketing'` |
| `web/data.py` | `get_root_cause_stats()` | Added `AND COALESCE(t.assignee, '') != 'Marketing'` |
| `export_1000_no_rc.py` | SQL query | Added `AND COALESCE(t.assignee, '') != 'Marketing'` |
| `run_export_pipeline_input.py` | SQL query | Added `AND COALESCE(t.assignee, '') != 'Marketing'` |

After applying: force-rebuilt all `daily_open_counts` dates to purge Marketing tickets from historical snapshots.
## Fix: Reconcile Compares Against All TS Open IDs (not just upserted)

### Problem

`reconcile_closed` was receiving only `upserted_ids` — the ticket IDs that were actually written to the DB during the current sync.  During a normal incremental sync where no tickets have been modified since the watermark, the modified-since post-filter reduces the processing set to 0, so `upserted_ids` is empty.  The reconcile then computes `missing = db_open_ids - {} = ALL db_open_ids`, falsely flagging every DB-open ticket (404 in the observed log) as "missing from TS" and wastefully re-fetching each one individually.

### Root Cause

The reconcile needs the set of ticket IDs that **TeamSupport considers open** (the full 590 fetched via `fetch_open_tickets()`), not the subset that passed the modified-since filter and were upserted.

### Fix

| File | Change |
|------|--------|
| `run_ingest.py` | Added `fetched_open_ids` list, populated from `raw_tickets` **before** the modified-since filter.  Included in the `_sync` result dict.  Passed to `_reconcile_closed` instead of `upserted_ids`. |
| `ingest/reconcile.py` | Renamed parameter from `upserted_ids` to `synced_open_ids`.  Updated docstring to clarify the parameter should contain all IDs TS reported as open. |

After this fix, the reconcile correctly computes `missing = db_open_ids − ts_open_ids`, which should be a small set of genuinely closed tickets rather than the entire DB open set.

## Fix: Reconcile Handles Deleted/Merged Tickets (400 from TS)

### Problem

When reconcile re-fetches tickets that are open in the DB but missing from the TS open-ticket list, some tickets return **400 Bad Request** from the TS single-ticket endpoint.  This means the ticket has been **deleted or merged** in TeamSupport — it no longer exists at all.  The previous code treated this as a generic error, logged it, and left the ticket marked as open in the DB.  This caused the same errors to recur on every sync indefinitely.

### Fix

| File | Change |
|------|--------|
| `ingest/reconcile.py` | Added `_mark_deleted()` helper that sets `status='Deleted'` and `closed_at=now` for a ticket.  The main reconcile loop now catches `requests.exceptions.HTTPError` with status 400 specifically, calls `_mark_deleted`, and counts the ticket as reconciled so it no longer appears in future runs. |

## Fix: Open Backlog Trend Severity Exceeds Total (Migration 029)

### Problem

The Open Backlog Trend chart's stacked severity areas (Low/Medium/High) extended far above the Total Open line.  Root cause: the two data sources used different computation methods:

- **Total Open line** → `vw_backlog_daily` → read from pre-computed `daily_open_counts` table (stale after historical ticket imports)
- **Severity stacked areas** → `vw_backlog_daily_by_severity` → computed live from `tickets` table (accurate)

When historical closed tickets were imported (e.g., via CSV), the live query found ~655 tickets open on a date while the stale pre-computed counts still said ~307.

### Fix — Migration `029_fix_backlog_daily_live.sql`

Rewrote `vw_backlog_daily` to compute the `open_backlog` column live from the `tickets` table (using `daily_open_counts` only as a date spine), matching the approach already used klog_daily_by_severity`.  The HP/HC columns continue to come from `ticket_snapshots_daily` via LEFT JOIN.

After migration: both views now produce identical total counts for every date (verified: diff = 0 across all dates).

## Exclude group_name='Marketing' Tickets (Migration 030)

### Problem

Migration 020 excluded tickets with `assignee = 'Marketing'`, but only 6 of 481 Marketing-group tickets actually have that assignee — the other 475 have individual names (e.g. "Ben Aplin", "Jimmy Biondo").  These leaked into all analytics views and historical backlog counts, inflating some dates by up to ~350 tickets.

### Migration 030 — `030_exclude_marketing_group.sql`

Rebuilt all five analytics views with an additional `AND COALESCE(t.group_name, '') != 'Marketing'` filter:

- **`vw_ticket_analytics_core`**, **`vw_backlog_aging_current`**, **`vw_backlog_daily`**, **`vw_backlog_daily_by_severity`**, **`vw_backlog_product_severity_powman`**

### Python Code Changes

All locations with `assignee != 'Marketing'` received a matching `group_name != 'Marketing'` condition:

| File | Function/Location | Change |
|------|---------------|--------|
| `db.py` | `get_open_ticket_ids()` | Added `AND COALESCE(group_name, '') != 'Marketing'` |
| `db.py` | `fetch_ticket_numbers_by_status()` | Added `AND COALESCE(t.group_name, '') != 'Marketing'` to all three branches |
| `run_rollups.py` | `rebuild_daily_snapshots` open_flag SQL | Added `AND COALESCE(t.group_name, '') != 'Marketing'` |
| `run_rollups.py` | `snapshot_tickets_daily` Python `is_open` | Added `group_name` to SELECT; added `(group_name or "") != "Marketing"` to condition |
| `run_rollups.py` | `rebuild_daily_open_counts` SQL | 
dded `AND COALESCE(t.group_name, '') != 'Marketing'` |
| `web/data.py` | `get_filtered_backlog_daily()` | Added `AND COALESCE(t.group_name, '') != 'Marketing'` to both total and severity queries |
| `web/data.py` | `get_open_by_status()` | Added `AND COALESCE(group_name, '') != 'Marketing'` |
| `export_1000_no_rc.py` | SQL query | Added `AND COALESCE(t.group_name, '') != 'Marketing'` |
| `run_export_pipeline_input.py` | SQL query | Added `AND COALESCE(t.group_name, '') != 'Marketing'` |

After applying: verified 2025-01-01 counts dropped from 655 → 307 (consistent between daily and severity views), confirming 348 marketing-group tickets excluded.
