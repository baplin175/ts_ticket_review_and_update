# Operational Domain Model — Support Analytics System

---

## 1. SYSTEM PURPOSE

This system is a support-ticket analytics pipeline built for inHANCE, a Harris Computer subsidiary that develops municipal utility software products (PowerMan, Impresa, and related tools). It solves the operational problem of understanding, triaging, and systematically improving the support experience across a large volume of customer tickets managed in TeamSupport.

The pipeline ingests raw tickets and their activity histories from the TeamSupport API (or CSV export) into a Postgres database (`tickets_ai` schema). Each ticket's conversation thread is cleaned (HTML→text, signature/boilerplate removal, mojibake repair), classified by action type (technical work, customer problem, scheduling, noise, etc.), and rolled up into structured thread summaries.

Multiple LLM enrichment passes then extract structured failure descriptions: Pass 1 extracts the observable phenomenon, Pass 2 decomposes it into a canonical failure grammar (Component + Operation + Unexpected State), and Pass 3 infers the underlying system mechanism. Separately, LLM scoring assigns sentiment (customer frustration), operational priority (1–10 triage scale), and complexity ratings (intrinsic difficulty vs. elapsed drag).

The schema further supports embedding generation, clustering of similar failures, a cluster catalog with suggested interventions, and daily backlog snapshots with customer/product health scoring. A Flask-based web dashboard provides operational visibility into backlog trends, aging distributions, root cause analysis, ticket drill-downs, and health metrics.

The system enables support managers to: identify high-priority unresolved tickets, detect customer frustration early, understand recurring failure patterns across products, separate true technical complexity from elapsed drag, and drive systematic interventions (software fixes, documentation improvements, configuration corrections) based on clustered root cause analysis.

---

## 2. DATA PIPELINE (END-TO-END)

### Lifecycle of a ticket

A ticket enters the system from the TeamSupport API or a CSV export, passes through cleaning and classification, gets rolled up into thread summaries, receives LLM-based scoring and failure analysis, and ultimately feeds into clustering and intervention logic. A web dashboard consumes all derived tables for operational reporting.

### Pipeline Stages

| Stage | Input Tables | Output Tables | Purpose | Implemented In |
|-------|-------------|---------------|---------|----------------|
| 1. API Ingestion | TeamSupport API (open tickets + actions) | `tickets`, `ticket_actions`, `sync_state`, `ingest_runs` | Fetch tickets and their activity histories from TeamSupport. Watermark-based incremental sync with safety buffer overlap. Idempotent upserts. | `run_ingest.py`, `ts_client.py`, `db.py` |
| 1b. CSV Import | `Activities.csv` file | `tickets`, `ticket_actions`, `ingest_runs` | Bulk import from TeamSupport CSV export. Generates synthetic deterministic action IDs. Idempotent. | `run_csv_import.py` |
| 1c. JSON Pipeline | TeamSupport API | `output/activities_*.json` | Legacy/parallel path: fetch open tickets + activities → JSON files for downstream LLM scoring without DB. | `run_pull_activities.py` |
| 2. Activity Cleaning | Raw `ticket_actions.description` | `ticket_actions.cleaned_description` | HTML→text conversion, email signature stripping, boilerplate removal (external email warnings, confidentiality notices, reply headers), mojibake repair, deduplication. | `activity_cleaner.py` |
| 3. Action Classification | `ticket_actions.cleaned_description`, `party`, `action_type` | `ticket_actions.action_class` | Rule-based deterministic classification into: `technical_work`, `customer_problem_statement`, `status_update`, `scheduling`, `waiting_on_customer`, `delivery_confirmation`, `administrative_noise`, `system_noise`, `unknown`. First-match priority ordering. | `action_classifier.py`, `run_rollups.py` (classify command) |
| 4. Thread Rollups | `ticket_actions` (all actions per ticket) | `ticket_thread_rollups` | Build multiple text views per ticket: `full_thread_text` (all non-empty), `customer_visible_text` (non-noise), `technical_core_text` (technical substance only), `latest_customer_text`, `latest_inhance_text`, `summary_for_embedding` (capped at 4000 chars). Computes SHA-256 hashes (`thread_hash`, `technical_core_hash`) for change detection. | `run_rollups.py` (rollups command) |
| 5. Metrics | `ticket_actions`, `tickets` | `ticket_metrics` | Compute per-ticket operational metrics: action counts (total, non-empty, customer, inhance), distinct participants, first response time, last human activity, empty action ratio, handoff count, hours to first response, days open. | `run_rollups.py` (metrics command) |
| 6a. Participants | `ticket_actions` | `ticket_participants` | Extract distinct participants per ticket with action counts, first/last seen, party type, first-response flag. | `run_rollups.py` (participants command) |
| 6b. Handoffs | `ticket_actions` | `ticket_handoffs` | Detect party transitions (inh→cust, cust→inh) as handoff events with timestamps. | `run_rollups.py` (handoffs command) |
| 6c. Wait States | `ticket_actions` | `ticket_wait_states` | Infer waiting periods (waiting-on-customer, waiting-on-inhance) from action sequences with duration and confidence. | `run_rollups.py` (wait_states command) |
| 6d. Daily Snapshots | `tickets`, `ticket_priority_scores`, `ticket_complexity_scores` | `ticket_snapshots_daily` | Point-in-time snapshot of each ticket's state (status, owner, product, age, priority, complexity, high-priority/high-complexity flags). | `run_rollups.py` (snapshot command) |
| 6e. Daily Open Counts | `tickets`, `ticket_participants` | `daily_open_counts` | Aggregated daily counts of open tickets broken down by product, status, and participant (assignee). | `run_rollups.py` (daily_open_counts command) |
| 6f. Health Scores | `tickets`, `ticket_metrics`, scoring tables | `customer_ticket_health`, `product_ticket_health` | Aggregate health scoring per customer and per product. Includes ticket load pressure, frustration rates, complexity distributions. | `run_rollups.py` (health command) |
| 7. Sentiment | `ticket_actions` (customer comments) | `ticket_sentiment` | LLM (Matcha) classifies customer frustration (Yes/No) from last N customer comments. Detects explicit negative sentiment, repeated complaints, sarcasm, status-update requests (auto-frustrated). Hash-based skip if thread unchanged. | `run_sentiment.py`, `prompts/sentiment.md` |
| 8. Priority | `tickets`, `ticket_actions` (full history) | `ticket_priority_scores` | LLM (Matcha) assigns urgency priority 1–10 with explanation. Hard override rules: unresponded breach (>2 days, no staff response)→1; multiple unanswered requests→1.5; violation language in most recent action→1.5; critical operational impact→1.5. Hash-based skip. | `run_priority.py`, `prompts/ai_priority.md` |
| 9. Complexity | `tickets`, `ticket_actions` (full history) | `ticket_complexity_scores` | LLM (Matcha) scores intrinsic complexity, coordination load, elapsed drag, and overall complexity (1–5 each). Separates true technical difficulty from thread noise and customer delays. Hash-based skip on `technical_core_hash`. | `run_complexity.py`, `prompts/complexity.md` |
| 10. Pass 1 — Phenomenon | `ticket_thread_rollups.full_thread_text` | `ticket_llm_pass_results` (pass_name=`pass1_phenomenon`) | LLM extracts a single normalized sentence describing the observable system behavior (the phenomenon). Ignores support workflow, project updates, administrative notes. Returns null if no system behavior is described. | `run_ticket_pass1.py`, `pass1_parser.py`, `prompts/pass1_phenomenon.txt` |
| 11. Pass 2 — Grammar | `ticket_llm_pass_results.phenomenon` (from Pass 1) | `ticket_llm_pass_results` (pass_name=`pass2_grammar`) | LLM decomposes the phenomenon into canonical failure grammar: Component + Operation + Unexpected State → Canonical Failure. Operations are normalized to a fixed 12-verb vocabulary. | `run_ticket_pass2.py`, `pass2_parser.py`, `prompts/pass2_grammar.txt` |
| 12. Pass 3 — Mechanism | `ticket_llm_pass_results.canonical_failure` (from Pass 2) | `ticket_llm_pass_results` (pass_name=`pass3_mechanism`) | LLM infers the most plausible internal system mechanism that would produce the canonical failure. Focuses on technical system behaviors (validation failures, logic errors, integration errors). | `run_ticket_pass3.py`, `pass3_parser.py`, `prompts/pass3_mechanism.txt` |
| 13. Embeddings | `ticket_thread_rollups.summary_for_embedding` | `ticket_embeddings` | Generate vector embeddings for clustering. Schema is prepared (JSONB vectors with type, source hash, model name) but embedding generation code is not yet present in the repository. | Schema: `003_analytics.sql` |
| 14. Clustering | `ticket_embeddings` | `cluster_runs`, `ticket_clusters`, `cluster_catalog` | Group tickets by failure similarity. Schema supports method/scope/params tracking, cluster assignment with confidence, and a catalog with labels, descriptions, representative tickets, and suggested intervention types. Clustering execution code is not yet present in the repository. | Schema: `003_analytics.sql` |
| 15. Interventions | `cluster_catalog`, `ticket_clusters` | `ticket_interventions` | Map clusters to actionable interventions. The `ticket_interventions` table is referenced in migration 004 (ticket_number column added) and the `cluster_catalog` table includes `suggested_intervention_type`. Full intervention logic is not yet implemented. | Schema: `003_analytics.sql`, `004_add_ticket_number.sql` |
| 16. Orchestration | All of the above | All output tables | `run_all.py` orchestrates the JSON pipeline: pull activities → sentiment → priority → complexity → consolidated TeamSupport write-back. `run_rollups.py full` runs the DB pipeline: classify → rollups → metrics → analytics. | `run_all.py`, `run_rollups.py` |
| 17. Export | All DB tables | `output/*.json` | Export canonical DB state back to JSON artifacts for portability/archival. | `run_export.py` |
| 18. Web Dashboard | All derived tables + views | HTTP responses | Flask app with overview (KPIs, backlog trends, aging), ticket list/detail, root cause analysis (LLM pass results), health scores, config/sync status. | `web/app.py`, `web/data.py`, `web/pages/*.py` |

---

## 3. TABLE SEMANTICS

### `tickets`

| Attribute | Value |
|-----------|-------|
| **Purpose** | Canonical record of each support ticket. One row per ticket across the system's lifetime. |
| **Population Method** | Upserted from TeamSupport API (`run_ingest.py`) or CSV import (`run_csv_import.py`). Idempotent on `ticket_id`. |
| **Type** | Raw (source of truth from TeamSupport) |
| **Key Columns** | `ticket_id` (PK, TS internal ID), `ticket_number` (human-facing number), `ticket_name` (subject), `status` (Open/Closed/Resolved), `severity` (0–3 or label), `product_name` (e.g. PowerMan, Impresa), `assignee`, `customer`, `date_created`, `date_modified`, `closed_at`, `days_opened`, `days_since_modified`, `source_payload` (full raw TS JSON), `first_ingested_at` / `last_ingested_at` / `last_seen_at` (pipeline tracking) |
| **Downstream Consumers** | All derived tables, all rollup scripts, all LLM scoring, web dashboard, backlog views |

### `ticket_actions`

| Attribute | Value |
|-----------|-------|
| **Purpose** | Individual activities/actions within a ticket (comments, emails, descriptions, status changes). |
| **Population Method** | Upserted from TS API (paginated, per-ticket) or CSV import with synthetic action IDs. |
| **Type** | Raw (with derived `cleaned_description` and `action_class`) |
| **Key Columns** | `action_id` (PK), `ticket_id` (FK), `created_at`, `action_type` (Comment/Email/Description/etc.), `creator_name`, `party` (inh/cust — determined from TS user org), `is_visible` (public vs private), `description` (raw HTML/text), `cleaned_description` (after cleaning pipeline), `action_class` (deterministic classification), `is_empty` (no meaningful text), `source_payload` |
| **Downstream Consumers** | Thread rollups, metrics, participants, handoffs, wait states, sentiment, priority, complexity scoring |

### `ticket_thread_rollups`

| Attribute | Value |
|-----------|-------|
| **Purpose** | Pre-computed text views of a ticket's conversation thread at varying levels of filtering. |
| **Population Method** | Derived by `run_rollups.py` from `ticket_actions` using `action_class` to filter noise. |
| **Type** | Derived |
| **Key Columns** | `ticket_id` (PK), `full_thread_text` (all non-empty actions with timestamps/names), `customer_visible_text` (excludes noise classes), `technical_core_text` (only `technical_work`, `customer_problem_statement`, `delivery_confirmation`), `latest_customer_text`, `latest_inhance_text`, `summary_for_embedding` (customer_visible capped at 4000 chars), `thread_hash` (SHA-256 of full_thread), `technical_core_hash` (SHA-256 of technical_core) |
| **Downstream Consumers** | Pass 1 (uses `full_thread_text`), sentiment (via action queries), priority/complexity (via action queries), embedding generation (uses `summary_for_embedding`), hash-based skip checks for all enrichments |

### `ticket_metrics`

| Attribute | Value |
|-----------|-------|
| **Purpose** | Quantitative operational metrics derived from action history. |
| **Population Method** | Computed by `run_rollups.py` from `ticket_actions`. |
| **Type** | Derived |
| **Key Columns** | `ticket_id` (PK), `action_count`, `nonempty_action_count`, `customer_message_count`, `inhance_message_count`, `distinct_participant_count`, `first_response_at` (first inh action after first cust action), `last_human_activity_at`, `empty_action_ratio`, `handoff_count` (party switches), `hours_to_first_response`, `days_open`, `date_created` |
| **Downstream Consumers** | `vw_ticket_analytics_core` (main analytics view), web dashboard, priority/complexity scoring context |

### `ticket_llm_pass_results`

| Attribute | Value |
|-----------|-------|
| **Purpose** | Generalized storage for all LLM pipeline pass results (Pass 1, 2, 3). Each row represents one LLM invocation for one ticket. |
| **Population Method** | Inserted by `run_ticket_pass1.py`, `run_ticket_pass2.py`, `run_ticket_pass3.py`. Starts as `pending`, updated to `success` or `failed`. |
| **Type** | LLM-derived |
| **Key Columns** | `ticket_id`, `pass_name` (pass1_phenomenon / pass2_grammar / pass3_mechanism), `prompt_version`, `model_name`, `input_text` (what was sent to LLM), `raw_response_text`, `parsed_json`, `phenomenon` (Pass 1 output), `component` / `operation` / `unexpected_state` / `canonical_failure` (Pass 2 outputs), `mechanism` (Pass 3 output), `status` (pending/success/failed), `error_message`, `started_at` / `completed_at` |
| **Downstream Consumers** | Pass 2 reads Pass 1's `phenomenon`; Pass 3 reads Pass 2's `canonical_failure`; web dashboard root cause views; clustering (future); `vw_ticket_pass_pipeline` view |

### `ticket_embeddings`

| Attribute | Value |
|-----------|-------|
| **Purpose** | Vector embeddings of ticket text for similarity-based clustering. |
| **Population Method** | Schema prepared; embedding generation code not yet present in repository. Intended to embed `summary_for_embedding` from rollups. |
| **Type** | LLM-derived (planned) |
| **Key Columns** | `ticket_id`, `embedding_type` (identifies what text was embedded), `source_text_hash` (for dedup), `model_name`, `embedding_vector` (JSONB), `created_at` |
| **Downstream Consumers** | Clustering pipeline (planned) |

### `ticket_clusters`

| Attribute | Value |
|-----------|-------|
| **Purpose** | Assignment of tickets to failure clusters within a specific clustering run. |
| **Population Method** | Schema prepared; clustering code not yet present in repository. |
| **Type** | Analytics (planned) |
| **Key Columns** | `ticket_id`, `cluster_run_id` (FK to `cluster_runs`), `cluster_id`, `cluster_label`, `cluster_confidence`, `cluster_method` |
| **Downstream Consumers** | `cluster_catalog`, intervention mapping (planned) |

### `cluster_catalog`

| Attribute | Value |
|-----------|-------|
| **Purpose** | Human-readable catalog describing each cluster's meaning, failure patterns, and suggested interventions. |
| **Population Method** | Schema prepared; catalog generation code not yet present. Intended to be populated per cluster run. |
| **Type** | Analytics (planned) |
| **Key Columns** | `cluster_run_id`, `cluster_id`, `cluster_label`, `cluster_description`, `representative_tickets` (JSONB), `common_issue_pattern`, `common_mechanism_pattern`, `suggested_intervention_type` |
| **Downstream Consumers** | Intervention decisions, web dashboard (planned) |

### `ticket_interventions`

| Attribute | Value |
|-----------|-------|
| **Purpose** | Actionable intervention records that map ticket analysis or cluster findings to specific corrective actions. |
| **Population Method** | Schema referenced in migration 004 (ticket_number column added); `CREATE TABLE` statement not found in current migrations. Table exists conceptually but may be created externally or is planned. |
| **Type** | Analytics (planned) |
| **Key Columns** | `ticket_id`, `ticket_number` (denormalized) — other columns inferred from schema intent |
| **Downstream Consumers** | Operational decision-making, web dashboard (planned) |

### `ticket_sentiment`

| Attribute | Value |
|-----------|-------|
| **Purpose** | LLM-scored customer frustration assessment per ticket. |
| **Population Method** | Matcha LLM call via `run_sentiment.py`. Analyzes last N customer comments. |
| **Type** | LLM-derived |
| **Key Columns** | `ticket_id`, `thread_hash` (for skip detection), `frustrated` (Yes/No), `frustrated_reason` (one-sentence explanation), `activity_id` (first frustrated activity), `created_at` (timestamp of that activity), `model_name`, `prompt_version` |
| **Downstream Consumers** | `vw_ticket_analytics_core`, web dashboard KPIs, priority scoring (frustration signal) |

### `ticket_priority_scores`

| Attribute | Value |
|-----------|-------|
| **Purpose** | LLM-assigned urgency priority for triage. |
| **Population Method** | Matcha LLM call via `run_priority.py`. Batched (multiple tickets per prompt). |
| **Type** | LLM-derived |
| **Key Columns** | `ticket_id`, `thread_hash`, `priority` (1–10, 1 = highest urgency), `priority_explanation` (1–2 sentence justification), `model_name`, `prompt_version` |
| **Downstream Consumers** | `vw_ticket_analytics_core`, web dashboard, `ticket_snapshots_daily` (high_priority_flag when priority ≤ 3), TeamSupport write-back |

### `ticket_complexity_scores`

| Attribute | Value |
|-----------|-------|
| **Purpose** | LLM-assessed complexity decomposition separating true difficulty from noise. |
| **Population Method** | Matcha LLM call via `run_complexity.py`. One ticket per prompt. |
| **Type** | LLM-derived |
| **Key Columns** | `ticket_id`, `technical_core_hash`, `intrinsic_complexity` (1–5), `coordination_load` (1–5), `elapsed_drag` (1–5), `overall_complexity` (1–5, weighted toward intrinsic), `confidence` (0.00–1.00), `primary_complexity_drivers` (JSONB array), `complexity_summary`, `evidence` (JSONB), `noise_factors` (JSONB), `duration_vs_complexity_note` |
| **Downstream Consumers** | `vw_ticket_analytics_core`, web dashboard, `ticket_snapshots_daily` (high_complexity_flag when overall ≥ 4), TeamSupport write-back |

### Supporting Tables

| Table | Purpose | Type |
|-------|---------|------|
| `sync_state` | Watermark tracking for incremental TeamSupport sync (last successful sync timestamp per source) | Infrastructure |
| `ingest_runs` | Audit log of each ingestion run (start/end times, counts, errors, config snapshot) | Infrastructure |
| `ticket_wait_states` | Inferred waiting periods (waiting-on-customer, waiting-on-inhance) with duration and confidence | Derived |
| `ticket_participants` | Distinct participants per ticket with action counts, party type, first/last seen | Derived |
| `ticket_handoffs` | Party transition events (inh↔cust) with timestamps | Derived |
| `ticket_snapshots_daily` | Point-in-time snapshot of each ticket's state per day (for trend analysis) | Analytics |
| `daily_open_counts` | Aggregated daily open ticket counts by product, status, and participant | Analytics |
| `ticket_issue_summaries` | LLM-generated issue/cause/mechanism/resolution summaries (separate from the 3-pass pipeline) | LLM-derived |
| `customer_ticket_health` | Aggregate customer health scores (ticket load, frustration, complexity) | Analytics |
| `product_ticket_health` | Aggregate product health scores (volume, severity distribution) | Analytics |
| `saved_reports` | Named filter presets for the web dashboard ticket explorer | Application |
| `_migrations` | Migration tracking (which SQL files have been applied) | Infrastructure |

---

## 4. LLM PIPELINE DESIGN

All LLM calls use **Matcha**, a Harris Computer internal LLM API (`matcha.harriscomputer.com/rest/api/v1/completions`), configured via a mission ID. The system uses retry logic with exponential backoff (3 retries, 10s initial backoff).

### Pass 1 — Phenomenon Extraction

| Attribute | Value |
|-----------|-------|
| **Pass Name** | `pass1_phenomenon` |
| **Purpose** | Extract the single observable system behavior described in the ticket. Not diagnosis, not root cause — only what the system did or failed to do, visible to the user. |
| **Input Text Source** | `ticket_thread_rollups.full_thread_text` |
| **Output Fields** | `phenomenon` (single normalized sentence, or null if no system behavior described) |
| **Prompt Location** | `prompts/pass1_phenomenon.txt` |
| **Model Used** | Matcha (mission_id from config, stored as `matcha-{MATCHA_MISSION_ID}`) |
| **How Results Are Stored** | Inserted into `ticket_llm_pass_results` with `pass_name = 'pass1_phenomenon'`. Status transitions: pending → success/failed. Unique constraint ensures one success per ticket+pass+prompt_version. |
| **Parser** | `pass1_parser.py` — validates JSON structure, rejects empty/missing phenomenon. |

### Pass 2 — Canonical Failure Grammar

| Attribute | Value |
|-----------|-------|
| **Pass Name** | `pass2_grammar` |
| **Purpose** | Decompose the phenomenon into a standardized operational failure grammar for cross-ticket clustering and comparison. |
| **Input Text Source** | `phenomenon` from a successful Pass 1 result |
| **Output Fields** | `component`, `operation` (normalized from 12-verb vocabulary), `unexpected_state`, `canonical_failure` (reconstructed as `Component + Operation + Unexpected State`) |
| **Prompt Location** | `prompts/pass2_grammar.txt` |
| **Model Used** | Matcha |
| **How Results Are Stored** | Inserted into `ticket_llm_pass_results` with `pass_name = 'pass2_grammar'`. |
| **Parser** | `pass2_parser.py` — validates JSON, normalizes operation through synonym map (e.g. upload→import, display→load, save→update), reconstructs canonical_failure from parsed fields. |

### Pass 3 — Mechanism Inference

| Attribute | Value |
|-----------|-------|
| **Pass Name** | `pass3_mechanism` |
| **Purpose** | Infer the most plausible internal system mechanism that would produce the observed failure. |
| **Input Text Source** | `canonical_failure` from a successful Pass 2 result |
| **Output Fields** | `mechanism` (single sentence describing internal system behavior — validation failure, config mismatch, integration error, etc.) |
| **Prompt Location** | `prompts/pass3_mechanism.txt` |
| **Model Used** | Matcha |
| **How Results Are Stored** | Inserted into `ticket_llm_pass_results` with `pass_name = 'pass3_mechanism'`. |
| **Parser** | `pass3_parser.py` — validates JSON, rejects exact restatements of canonical failure, rejects administrative/support-workflow language (words like "ticket", "customer", "agent", "troubleshoot", "escalat*"). |

### Sentiment Scoring

| Attribute | Value |
|-----------|-------|
| **Purpose** | Detect customer frustration from recent customer comments. |
| **Input Text Source** | Last N (default 4) customer-authored actions from `ticket_actions` (DB) or activities JSON |
| **Output Fields** | `frustrated` (Yes/No), `frustrated_reason` (one-sentence justification or null), `activity_id` (first frustrated activity), `created_at` |
| **Prompt Location** | `prompts/sentiment.md` |
| **Model Used** | Matcha |
| **How Results Are Stored** | Inserted into `ticket_sentiment` table with `thread_hash` for skip detection. |
| **Special Rules** | Status-update requests are always classified as frustrated. Closed tickets override to "No". Test/placeholder content is non-frustrated unless it includes a status request. |

### Priority Scoring

| Attribute | Value |
|-----------|-------|
| **Purpose** | Assign urgency priority (1–10) for operational triage. |
| **Input Text Source** | Full ticket metadata + chronological activity history |
| **Output Fields** | `priority` (1–10), `priority_explanation` (1–2 sentences), `severity` (verbatim), `days_opened` (verbatim), `days_since_modified` (verbatim) |
| **Prompt Location** | `prompts/ai_priority.md` |
| **Model Used** | Matcha |
| **How Results Are Stored** | Inserted into `ticket_priority_scores` with `thread_hash`. Batched (multiple tickets per LLM call). |
| **Special Rules** | Hard override: unresponded >2 days → priority 1; multiple unanswered customer requests → 1.5; "in violation and is greater" in most recent action → 1.5; production outage/data loss → 1.5. |

### Complexity Scoring

| Attribute | Value |
|-----------|-------|
| **Purpose** | Estimate true work complexity independent of elapsed time and thread noise. |
| **Input Text Source** | Full ticket metadata + chronological activity history |
| **Output Fields** | `intrinsic_complexity` (1–5), `coordination_load` (1–5), `elapsed_drag` (1–5), `overall_complexity` (1–5), `confidence` (0.00–1.00), `primary_complexity_drivers`, `complexity_summary`, `evidence`, `noise_factors`, `duration_vs_complexity_note` |
| **Prompt Location** | `prompts/complexity.md` |
| **Model Used** | Matcha |
| **How Results Are Stored** | Inserted into `ticket_complexity_scores` with `technical_core_hash`. One ticket per LLM call. |

---

## 5. FAILURE ONTOLOGY

The system implements a structured failure description language designed for cross-ticket comparison and clustering.

### Definitions

**Phenomenon**
The observable system behavior the user or system encountered. This is the raw "what happened" — not a diagnosis, not an explanation. It describes what the system actually did or failed to do, visible at the application surface.

Example: *"Meter import overwrites current readings with previous period data"*

**Component**
The system module, subsystem, workflow, report, integration, screen, process, or interface involved. Prefer subsystem/workflow names over individual field labels.

Examples: *Invoice Cloud integration*, *Billing module*, *Meter import process*, *Work Order screen*, *Warranty date calculation*

**Operation**
The action the system was performing, normalized to a fixed 12-verb vocabulary:

| Verb | Covers |
|------|--------|
| `post` | posting |
| `import` | importing, upload, ingest, read |
| `export` | exporting, download, extract |
| `print` | printing |
| `load` | loading, open, launch, display, show, view, render, validate, test |
| `transfer` | send, transmit, move, sync, synchronize |
| `calculate` | compute, sum, tally |
| `attach` | link, connect |
| `generate` | build, produce, compile |
| `recover` | restore, rollback |
| `create` | insert, add |
| `update` | modify, edit, change, save, write, delete, remove |

**Unexpected State**
A concise, factual description of the incorrect observable behavior. No root cause, no troubleshooting steps.

Example: *"service charge prints twice"*, *"map does not appear"*, *"changes do not save when using return or tab"*

**Canonical Failure**
The structured composition: `<Component> + <Operation> + <Unexpected State>`

Example: *"Invoice Cloud integration + import + files fail to upload"*

**Mechanism**
The inferred internal system behavior that explains the failure. Implementation-neutral and generalizable.

Mechanism categories include:
- Data validation failure
- Configuration mismatch
- Integration mapping error
- Schema mismatch
- Missing dependency
- Incorrect business rule logic
- Background job failure
- File format incompatibility
- Permission or access restriction
- Synchronization failure
- Cache or state inconsistency
- Calculation logic error
- Field mapping error
- Interface communication failure
- Scheduling or job execution failure

Example: *"Billing calculation logic applies service charge rule twice during bill generation"*

### Grammar Structure

```
full_thread_text
    → [Pass 1] phenomenon
        → [Pass 2] Component + Operation + Unexpected State = Canonical Failure
            → [Pass 3] Mechanism
```

The passes are strictly sequential and dependency-chained. Pass 2 requires a successful Pass 1 phenomenon. Pass 3 requires a successful Pass 2 canonical_failure.

### Ontology Hierarchy (from concrete to abstract)

```
Phenomenon (what the system did)
  ↓ decomposition
Canonical Failure = Component + Operation + Unexpected State
  ↓ inference
Mechanism (why the system did it)
  ↓ clustering (planned)
Failure Pattern (recurring mechanism across tickets)
  ↓ intervention (planned)
Corrective Action (what to do about it)
```

---

## 6. CLUSTERING SYSTEM

### Current State

The clustering system has a fully specified schema but the execution code (embedding generation, clustering algorithm, cluster labeling) is **not yet present in the repository**. The schema anticipates the full pipeline.

### Intended Architecture

| Attribute | Value |
|-----------|-------|
| **Embedding Source** | `ticket_thread_rollups.summary_for_embedding` — the customer-visible (non-noise) thread text, capped at 4000 characters |
| **Embedding Model** | Configurable per run; stored in `ticket_embeddings.model_name` and `cluster_runs.embedding_model` |
| **Embedding Storage** | `ticket_embeddings` table with JSONB vector, unique on (ticket_id, embedding_type, source_text_hash, model_name) |
| **Clustering Method** | Configurable per run; stored in `cluster_runs.cluster_method` with `clustering_params` (JSONB) |
| **Cluster Metadata Tables** | `cluster_runs` (run metadata), `ticket_clusters` (ticket↔cluster assignment with confidence), `cluster_catalog` (human-readable cluster descriptions) |
| **Cluster Label Generation** | Stored in `cluster_catalog.cluster_label` and `cluster_catalog.cluster_description` — intended to be LLM-generated from representative tickets |
| **Cluster Interpretation Logic** | `cluster_catalog` stores `common_issue_pattern`, `common_mechanism_pattern`, `representative_tickets` (JSONB), and `suggested_intervention_type` per cluster |

### Schema Design

The schema supports multiple concurrent clustering runs (`cluster_run_id` as UUID FK) so that different methods, scopes, or parameters can be compared. Each ticket can belong to different clusters in different runs. The `cluster_catalog` is unique on (cluster_run_id, cluster_id), enabling per-run catalogs.

---

## 7. INTERVENTION MODEL

### Current State

The intervention model is partially specified in the schema. The `ticket_interventions` table is referenced (migration 004 adds a `ticket_number` column to it) but its `CREATE TABLE` statement is not present in the current migration set, suggesting it was created externally or is planned for a future migration. The `cluster_catalog` table provides the primary intervention-recommendation mechanism.

### Intervention Pathway

```
Cluster Catalog
  ├── common_issue_pattern     → What failure recurs
  ├── common_mechanism_pattern → Why it recurs
  └── suggested_intervention_type → What to do about it
```

### Intended Intervention Classes

Based on the schema field `suggested_intervention_type` and the domain context (municipal utility software support), the system is designed to support these intervention classes:

| Intervention Class | Description |
|-------------------|-------------|
| **Software fix** | Bug fix, patch, or code change to address a defect in product logic |
| **Feature addition** | New capability or enhancement to address a gap in product functionality |
| **Configuration correction** | Adjust system settings, parameters, mappings, or rules to resolve misconfiguration |
| **Documentation improvement** | Update or create user documentation, knowledge base articles, or training materials |
| **Customer training** | Provide guidance, walkthroughs, or training to address user misunderstanding |
| **Operational process change** | Modify support workflows, escalation paths, or operational procedures |

### Storage

- `cluster_catalog.suggested_intervention_type`: Per-cluster recommendation
- `ticket_interventions`: Per-ticket intervention assignment (planned — table referenced but not fully defined in available migrations)
- `ticket_issue_summaries`: Per-ticket summaries with `issue_summary`, `cause_summary`, `mechanism_summary`, `resolution_summary` fields

---

## 8. PRODUCT / DOMAIN KNOWLEDGE

### Products / Systems Mentioned

| Product | Details |
|---------|---------|
| **PowerMan** | Primary product. Municipal utility management software. Variants referenced as PM*, Power*. Has sub-modules for billing, meters, work orders, accounts, payments. The web dashboard consolidates PM*/Power* variants under "PowerMan". |
| **Impresa** | Companion or related product. Referenced in prompts (e.g., "Impresa → QuickBooks interface", "Renaming a field in Impresa does not save"). |
| **Invoice Cloud** | Third-party payment/invoicing integration. Referenced in prompts for file upload failures. |
| **Crystal Reports** | Reporting tool used for generating bills and reports. Referenced in prompts for report generation failures. |
| **Neptune** | Meter reading system. Referenced for meter import file format. |
| **QuickBooks** | Accounting system receiving data from Impresa. Referenced integration interface. |
| **TeamSupport** | The ticketing system that is the data source. NA2 region (`app.na2.teamsupport.com`). |
| **Matcha** | Harris Computer internal LLM API used for all scoring and extraction. |

### Typical User Roles

- **Customer**: Municipal utility staff using PowerMan/Impresa. Submits tickets, reports issues, provides files.
- **inHANCE support staff**: Customer success / support engineers who respond to tickets, debug issues, implement fixes. Identified via TeamSupport Organization=inHANCE.
- **Support managers**: Primary consumers of the analytics dashboard. Use priority, complexity, and root cause data for triage and resource allocation.
- **Developers**: Receive escalated tickets for code fixes, stored procedures, configuration changes.

### Common Workflows

- **Meter import/export**: Uploading Neptune files, meter readings, data transformation between systems
- **Billing**: Bill generation, service charges, level billing, deposit handling, AR batch posting
- **Payment processing**: AutoPay, payment plans, payment display, Invoice Cloud integration
- **Work orders**: Work order screens, map display, service location management
- **Reporting**: Crystal Reports generation, GL/AP reports, bank reconciliation
- **Data integration**: Impresa↔QuickBooks sync, file transfers, EDI processing
- **Account management**: Customer account screens, field renaming, warranty date calculations

### Typical Failure Types

- File import/export failures (format mismatches, upload errors)
- Display/rendering issues (maps not appearing, incorrect values shown)
- Calculation errors (service charges, warranty dates, billing amounts)
- Save/persistence failures (changes lost on tab/return, fields not updating)
- Integration failures (Invoice Cloud upload, QuickBooks sync, API errors)
- Report generation failures (Crystal Reports on specific workstations)
- Data integrity issues (duplicates, overwrites, incorrect matching)

---

## 9. OPEN QUESTIONS / UNKNOWNS

1. **Embedding generation code is missing.** The `ticket_embeddings` table schema exists with model name and vector storage, but no code in the repository generates embeddings. What embedding model is intended? Is embedding generation handled by an external service or a separate repository?

2. **Clustering execution code is missing.** The `cluster_runs`, `ticket_clusters`, and `cluster_catalog` schemas are defined, but no clustering algorithm code exists. What clustering method is planned (HDBSCAN, k-means, etc.)? How are cluster labels generated?

3. **`ticket_interventions` CREATE TABLE is missing.** Migration 004 adds a `ticket_number` column to this table, but the original `CREATE TABLE` is not in any of the 17 migration files. Was this table created manually, in an earlier migration that was subsequently removed, or is it planned?

4. **Issue summary generation is undefined.** The `ticket_issue_summaries` table has columns for `issue_summary`, `cause_summary`, `mechanism_summary`, and `resolution_summary`, but no code populates it. Is this generated by a separate LLM pass that combines Pass 1–3 results, or is it planned?

5. **Customer/product health scoring logic.** The `customer_ticket_health` and `product_ticket_health` tables are referenced in the web dashboard and rollups, but their `CREATE TABLE` statements are not visible in the migration files examined. What health metrics are computed and how?

6. **TeamSupport write-back field mapping.** The system writes back priority and complexity fields to TeamSupport, but what specific TS custom fields do these map to? What is the expected behavior when `TS_WRITEBACK=1` — which fields are updated and how do they appear in the TeamSupport UI?

7. **Matcha mission configuration.** The system uses a single `MATCHA_MISSION_ID` for all LLM calls. Does the mission define the base model, system prompt, temperature, and other parameters? Are different missions used for different pass types in production?

8. **Pass dependency failure handling.** If Pass 1 returns `null` (no phenomenon), the ticket is excluded from Pass 2 and Pass 3. But there is no retry escalation or fallback for tickets where the LLM fails to extract a phenomenon from a technically substantive thread. How are these gaps monitored?

9. **Operation verb vocabulary constraints.** Pass 2 normalizes operations to exactly 12 verbs. Some semantic overloading exists (e.g., `load` covers display, validate, test, open, render). Would expanding the vocabulary improve clustering fidelity?

10. **Backlog view date range.** The web dashboard queries `vw_backlog_daily` with a hardcoded filter `WHERE snapshot_date >= '2024-07-01'`. Is this intentional (system start date) or should it be configurable?

11. **Multi-product ticket handling.** A ticket has a single `product_name`. How are tickets handled that involve cross-product issues (e.g., Impresa→QuickBooks integration where the failure could be attributed to either product)?

12. **Cluster-to-intervention workflow.** The schema stores `suggested_intervention_type` on `cluster_catalog`, but who acts on this? Is there an intended workflow where cluster analysis produces work items in an external system, or is the dashboard the terminal consumer?
