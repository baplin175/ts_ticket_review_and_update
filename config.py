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

# ── Target a specific ticket (by TicketNumber) ──────────────────────
# When set, only this ticket is processed (overrides MAX_TICKETS).
TARGET_TICKET = os.getenv("TARGET_TICKET", "").strip()

# ── Output ───────────────────────────────────────────────────────────
OUTPUT_DIR = os.getenv("OUTPUT_DIR", os.path.join(os.path.dirname(os.path.abspath(__file__)), "output"))
