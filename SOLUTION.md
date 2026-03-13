# Solution Overview

## Purpose

Pull open-ticket activities from TeamSupport, cleanse the text, classify each activity by party (inHANCE team vs customer), write them to a timestamped JSON file, run sentiment analysis, AI priority scoring, and complexity analysis via Matcha LLM.

## Architecture

```
config.py              — Centralised settings (API creds, limits, target ticket, output dir, stage toggles)
ts_client.py           — TeamSupport REST API client (fetch tickets, activities, inHANCE users, update_ticket with auto LastInhComment/LastCustComment)
activity_cleaner.py    — Text-cleaning pipeline (HTML→text, boilerplate/signature removal)
matcha_client.py       — Matcha LLM API client (send prompts, extract responses)
run_all.py             — Orchestrator: runs all stages in sequence, passes activities file between stages
run_pull_activities.py — Part 1: fetch, clean, classify activities → activities JSON
run_sentiment.py       — Part 2: send customer comments to Matcha for sentiment → sentiment JSON
run_priority.py        — Part 3: AI priority scoring via Matcha → write back to TeamSupport
run_complexity.py      — Part 4: complexity analysis via Matcha → complexity JSON
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
6. **Write JSON** — Output is written to `output/activities_YYYYMMDD_HHMMSS.json` as an array of ticket objects. Each ticket object contains metadata at the top level and an `activities` array of cleaned activity records — ticket-level fields are stored once, not repeated per activity.

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
| `description`   | Cleaned plain-text body                             |

### Part 3 — AI Priority Scoring

11. **Build input** — `run_priority.py` reads the latest activities JSON, extracts ticket metadata + all activities into the input format expected by `prompts/ai_priority.md`.
12. **Call Matcha** — The full prompt (instructions + ticket data) is sent to Matcha with a 600s timeout.
13. **Parse response** — Matcha returns a JSON array with `priority` (1–10), `priority_explanation`, and verbatim pass-through fields.
14. **Write back to TeamSupport** — `ts_client.update_ticket()` updates custom fields `AIPriority`, `AIPriExpln`, `AILastUpdate` on each ticket. `LastInhComment` and `LastCustComment` are injected automatically by `update_ticket()` for every TeamSupport write — derived from the activities list by scanning for the most recent `party=="inh"` and `party=="cust"` entries. If the API returns 403 or the ticket has no `ticket_id` (CSV-sourced), the payload is saved to `output/api_payloads_dry_run.json` for verification.
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
19. **Write back to TeamSupport** — `ts_client.update_ticket()` updates `Complexity`, `COORDINATIONLOAD`, `ELAPSEDDRAG`, `INTRINSICCOMPLEXITY` on each ticket. `LastInhComment` and `LastCustComment` are included automatically. If the API is rate-limited or ticket_id is unavailable, payloads are saved to the dry-run file.
20. **Save locally** — Results are saved to `output/complexity_YYYYMMDD_HHMMSS.json`.

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
| `CUST_COMMENT_COUNT`| `4`                   | Customer comments sent to Matcha     |
| `OUTPUT_DIR`       | `./output`             | Where JSONL files are written        |

## Dry-Run Payloads

When the TeamSupport API is rate-limited (403) or the ticket has no `ticket_id` (CSV-sourced data), write-back payloads are saved to `output/api_payloads_dry_run.json` instead of being sent to the API. Each entry contains:

| Field       | Description                              |
|-------------|------------------------------------------|
| `timestamp` | UTC time the payload was generated       |
| `method`    | HTTP method (`PUT`)                      |
| `url`       | TeamSupport API URL that would be called |
| `payload`   | Full request body (`{"Ticket": {...}}`)  |

This allows verification of payload contents when the API is unavailable.

## Running

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

# Orchestrator with stages skipped
TARGET_TICKET=110554 RUN_SENTIMENT=0 python run_all.py

# Orchestrator: pull only (skip all analysis)
TARGET_TICKET=110554 RUN_SENTIMENT=0 RUN_PRIORITY=0 RUN_COMPLEXITY=0 python run_all.py
```
