"""
Configuration for the ticket activity pull pipeline.

All settings can be overridden via environment variables.
"""

import os

_PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

# ── TeamSupport API credentials ──────────────────────────────────────
TS_BASE = os.getenv("TS_BASE", "https://app.na2.teamsupport.com/api/json")
#TS_KEY = os.getenv("TS_KEY", "")
#TS_USER_ID = os.getenv("TS_USER_ID", "")
TS_KEY = os.getenv("TS_KEY", "9980809a-174b-49a8-9469-bd5c14a657a2")
TS_USER_ID = os.getenv("TS_USER_ID", "1189708")

# ── Matcha LLM API ──────────────────────────────────────────────────
MATCHA_URL = os.getenv("MATCHA_URL", "https://matcha.harriscomputer.com/rest/api/v1/completions")
MATCHA_API_KEY = os.getenv("MATCHA_API_KEY", "15c0db915567455e98b90f1ecc22e088")
MATCHA_MISSION_ID = os.getenv("MATCHA_MISSION_ID", "27301")
#MATCHA_API_KEY = os.getenv("MATCHA_API_KEY", "")
#MATCHA_MISSION_ID = os.getenv("MATCHA_MISSION_ID", "")

# Optional LLM override (Matcha model id). Set to a model id to override
# the mission default, or None to use the mission's configured LLM.
MATCHA_RESPONSE_LLM = 87

# ── Pull limits ──────────────────────────────────────────────────────
# Maximum number of tickets to pull (for testing). Set to 0 for unlimited.
MAX_TICKETS = int(os.getenv("MAX_TICKETS", "0"))

# ── Target specific ticket(s) (by TicketNumber) ────────────────────
# When set, only these tickets are processed (overrides MAX_TICKETS).
# Accepts a comma-delimited list, e.g. "29696,110554".
_TARGET_RAW = os.getenv("TARGET_TICKET", "").strip()
TARGET_TICKETS = [t.strip() for t in _TARGET_RAW.split(",") if t.strip()]

# ── Output ───────────────────────────────────────────────────────────
OUTPUT_DIR = os.getenv("OUTPUT_DIR", "/Users/baplin/ts_ticket_review_and_update/ts_ticket_review_and_update/output")

# ── Stage toggles (1 = run, 0 = skip) ───────────────────────────────
RUN_SENTIMENT = os.getenv("RUN_SENTIMENT", "1").strip() == "1"
RUN_PRIORITY = os.getenv("RUN_PRIORITY", "1").strip() == "1"
RUN_COMPLEXITY = os.getenv("RUN_COMPLEXITY", "1").strip() == "1"

# Force enrichment on first run — bypass hash-based skipping so every
# ticket is scored even if hashes already match.  Set to "1" for the
# initial enrichment pass, then revert to "0" for incremental runs.
FORCE_ENRICHMENT = os.getenv("FORCE_ENRICHMENT", "1").strip() == "1"

# ── Write-back / output controls ────────────────────────────────────
# Write enrichment results back to TeamSupport (1 = yes, 0 = no).
TS_WRITEBACK = os.getenv("TS_WRITEBACK", "0").strip() == "1"
# Skip writing JSON artifact files when DB persistence is active (1 = skip, 0 = write).
SKIP_OUTPUT_FILES = os.getenv("SKIP_OUTPUT_FILES", "1").strip() == "1"

# ── Logging / diagnostics ────────────────────────────────────────────
LOG_TO_FILE = os.getenv("LOG_TO_FILE", "1").strip() == "1"
LOG_API_CALLS = os.getenv("LOG_API_CALLS", "1").strip() == "1"

# ── Incremental sync ─────────────────────────────────────────────────
# Safety buffer (minutes) subtracted from last_successful_sync_at when
# querying for changed tickets.  Overlap is safe because upserts are
# idempotent; it guards against clock skew and in-flight writes.
SAFETY_BUFFER_MINUTES = int(os.getenv("SAFETY_BUFFER_MINUTES", "10"))

# Default number of days to look back when no prior sync_state exists
# (initial backfill window).  Set to 0 for full backfill (all open tickets).
INITIAL_BACKFILL_DAYS = int(os.getenv("INITIAL_BACKFILL_DAYS", "0"))

# ── Database (Postgres) ──────────────────────────────────────────────
# Optional. When empty/unset, the pipeline runs in JSON-only mode.
# Local dev: DATABASE_URL=postgresql://user:pass@localhost:5432/Work
#DATABASE_URL = os.getenv("DATABASE_URL", "")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:kaplah@localhost:5432/Work")
# All pipeline tables are created in this schema (not public).
DATABASE_SCHEMA = os.getenv("DATABASE_SCHEMA", "tickets_ai")
