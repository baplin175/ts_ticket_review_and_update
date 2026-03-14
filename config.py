"""
Configuration for the ticket activity pull pipeline.

All settings can be overridden via environment variables.
"""

import os

# ── TeamSupport API credentials ──────────────────────────────────────
TS_BASE = os.getenv("TS_BASE", "https://app.na2.teamsupport.com/api/json")
TS_KEY = os.getenv("TS_KEY", "9980809a-174b-49a8-9469-bd5c14a657a2")
TS_USER_ID = os.getenv("TS_USER_ID", "1189708")

# ── Matcha LLM API ──────────────────────────────────────────────────
MATCHA_URL = os.getenv("MATCHA_URL", "https://matcha.harriscomputer.com/rest/api/v1/completions")
MATCHA_API_KEY = os.getenv("MATCHA_API_KEY", "15c0db915567455e98b90f1ecc22e088")
MATCHA_MISSION_ID = os.getenv("MATCHA_MISSION_ID", "27301")

# ── Pull limits ──────────────────────────────────────────────────────
# Maximum number of tickets to pull (for testing). Set to 0 for unlimited.
MAX_TICKETS = int(os.getenv("MAX_TICKETS", "5"))

# ── Target specific ticket(s) (by TicketNumber) ────────────────────
# When set, only these tickets are processed (overrides MAX_TICKETS).
# Accepts a comma-delimited list, e.g. "29696,110554".
_TARGET_RAW = os.getenv("TARGET_TICKET", "109683,108476,108182,108098").strip()
TARGET_TICKETS = [t.strip() for t in _TARGET_RAW.split(",") if t.strip()]

# ── Output ───────────────────────────────────────────────────────────
OUTPUT_DIR = os.getenv("OUTPUT_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "output"))

# ── Stage toggles (1 = run, 0 = skip) ───────────────────────────────
RUN_SENTIMENT = os.getenv("RUN_SENTIMENT", "0").strip() == "1"
RUN_PRIORITY = os.getenv("RUN_PRIORITY", "1").strip() == "1"
RUN_COMPLEXITY = os.getenv("RUN_COMPLEXITY", "1").strip() == "1"

# ── Write-back / output controls ────────────────────────────────────
# Write enrichment results back to TeamSupport (1 = yes, 0 = no).
TS_WRITEBACK = os.getenv("TS_WRITEBACK", "0").strip() == "1"
# Skip writing JSON artifact files when DB persistence is active (1 = skip, 0 = write).
SKIP_OUTPUT_FILES = os.getenv("SKIP_OUTPUT_FILES", "1").strip() == "1"

# ── Logging / diagnostics ────────────────────────────────────────────
LOG_TO_FILE = os.getenv("LOG_TO_FILE", "1").strip() == "1"
LOG_API_CALLS = os.getenv("LOG_API_CALLS", "1").strip() == "1"

# ── Database (Postgres) ──────────────────────────────────────────────
# Optional. When empty/unset, the pipeline runs in JSON-only mode.
# Local dev: DATABASE_URL=postgresql://user:pass@localhost:5432/Work
DATABASE_URL = os.getenv("DATABASE_URL", "")
# All pipeline tables are created in this schema (not public).
DATABASE_SCHEMA = os.getenv("DATABASE_SCHEMA", "tickets_ai")
