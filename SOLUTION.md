# Solution Overview

## Purpose

Pull open-ticket activities from TeamSupport, cleanse the text, classify each activity by party (inHANCE team vs customer), write them to a timestamped JSONL file, and run sentiment analysis on customer comments via Matcha LLM.

## Architecture

```
config.py              — Centralised settings (API creds, limits, target ticket, output dir)
ts_client.py           — TeamSupport REST API client (fetch tickets, activities, inHANCE users)
activity_cleaner.py    — Text-cleaning pipeline (HTML→text, boilerplate/signature removal)
matcha_client.py       — Matcha LLM API client (send prompts, extract responses)
run_pull_activities.py — Part 1: fetch, clean, classify activities → activities JSONL
run_sentiment.py       — Part 2: send customer comments to Matcha for sentiment → sentiment JSONL
```

## Data Flow

1. **Fetch open tickets** — `ts_client.fetch_open_tickets(ticket_number=...)` queries the TeamSupport `/Tickets` endpoint with `isClosed=False`. When `TARGET_TICKET` is set, the ticket number is passed as an API filter (`TicketNumber=...`) so only that ticket is fetched — no full paginated sweep.
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
6. **Write JSONL** — All cleansed activities are written to `output/activities_YYYYMMDD_HHMMSS.jsonl`, one JSON object per line.

### Part 2 — Sentiment Analysis

7. **Extract customer comments** — `run_sentiment.py` reads the latest activities JSONL, filters to `party=cust` with non-empty descriptions, and takes the last N (default **4**).
8. **Build prompt** — The instructions from `prompts/sentiment.md` are combined with the customer comments as a JSON input block.
9. **Call Matcha** — `matcha_client.call_matcha()` sends the prompt to the Matcha LLM API with retry logic for transient failures.
10. **Write sentiment JSONL** — The response is written to `output/sentiment_YYYYMMDD_HHMMSS.jsonl`.

## JSONL Record Schema

| Field           | Description                                         |
|-----------------|-----------------------------------------------------|
| `ticket_id`     | TeamSupport internal ID                             |
| `ticket_number` | Human-readable ticket number                        |
| `ticket_name`   | Ticket title / name                                 |
| `action_id`     | Activity / action ID                                |
| `created_at`    | Timestamp of the activity                           |
| `action_type`   | e.g. "Comment", "Email", etc.                       |
| `creator_id`    | ID of the user who created the activity             |
| `creator_name`  | Display name of the creator                         |
| `party`         | `inh` (inHANCE CS team) or `cust` (customer)       |
| `description`   | Cleaned plain-text body                             |

## Sentiment JSONL Record Schema

| Field           | Description                                         |
|-----------------|-----------------------------------------------------|
| `ticket_number` | Ticket number analysed                              |
| `comments_sent` | Number of customer comments sent to Matcha          |
| `source_file`   | Activities JSONL filename used as input             |
| `frustrated`    | `"Yes"` or `"No"`                                   |
| `activity_id`   | ID of first frustration activity (or `null`)        |
| `created_at`    | Timestamp of first frustration activity (or `null`) |

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
| `TARGET_TICKET`    | *(empty)*              | Pull/analyse only this ticket number |
| `CUST_COMMENT_COUNT`| `4`                   | Customer comments sent to Matcha     |
| `OUTPUT_DIR`       | `./output`             | Where JSONL files are written        |

## Running

```bash
# Default: pulls 5 tickets
python run_pull_activities.py

# Target a specific ticket
TARGET_TICKET=29696 python run_pull_activities.py

# Override limit
MAX_TICKETS=20 python run_pull_activities.py

# Unlimited
MAX_TICKETS=0 python run_pull_activities.py

# Part 2: Sentiment analysis (requires activities JSONL from Part 1)
TARGET_TICKET=29696 python run_sentiment.py

# Override number of customer comments sent
TARGET_TICKET=29696 CUST_COMMENT_COUNT=6 python run_sentiment.py
```
