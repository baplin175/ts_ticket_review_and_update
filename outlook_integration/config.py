"""
Configuration for Microsoft Graph API / Outlook integration.

All settings are loaded from environment variables or a .env file
located alongside this module.
"""

import os
from pathlib import Path

_MODULE_DIR = Path(__file__).resolve().parent

# Load .env from the outlook_integration folder if python-dotenv is available.
try:
    from dotenv import load_dotenv
    _env_path = _MODULE_DIR / ".env"
    if _env_path.exists():
        load_dotenv(_env_path)
except ImportError:
    pass

# ── Azure AD App Registration ───────────────────────────────────────
CLIENT_ID: str = os.getenv("GRAPH_CLIENT_ID", "")
CLIENT_SECRET: str = os.getenv("GRAPH_CLIENT_SECRET", "")
TENANT_ID: str = os.getenv("GRAPH_TENANT_ID", "")

# ── Auth ─────────────────────────────────────────────────────────────
# Scopes for delegated (user) auth.  Adjust as needed.
DELEGATED_SCOPES: list[str] = [
    "Mail.ReadWrite",
    "Mail.Send",
    "Calendars.ReadWrite",
    "User.Read",
]

# Scopes for app-only (client credentials) auth.
# Client-credential flows always use .default on the resource.
APP_SCOPES: list[str] = ["https://graph.microsoft.com/.default"]

# ── Graph API base URL ───────────────────────────────────────────────
GRAPH_BASE_URL: str = "https://graph.microsoft.com/v1.0"

# ── Token cache ──────────────────────────────────────────────────────
# Persisted token cache so the user doesn't have to re-authenticate
# every time.  Set to "" to disable persistence.
TOKEN_CACHE_PATH: str = os.getenv(
    "GRAPH_TOKEN_CACHE_PATH",
    str(_MODULE_DIR / ".token_cache.bin"),
)

# ── Authority ────────────────────────────────────────────────────────
AUTHORITY: str = f"https://login.microsoftonline.com/{TENANT_ID}" if TENANT_ID else ""
