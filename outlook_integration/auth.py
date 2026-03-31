"""
Authentication helpers for Microsoft Graph API.

Supports two flows:
  1. Device-code flow  — interactive, delegated (user) permissions
  2. Client-credentials — app-only, no user interaction

Both flows use MSAL with an optional persistent token cache so tokens
survive across process restarts.
"""

from __future__ import annotations

import atexit
import sys
from typing import Optional

import msal

from . import config


# ── Token cache persistence ─────────────────────────────────────────

def _build_cache() -> msal.SerializableTokenCache:
    """Return an MSAL token cache backed by a local file (if configured)."""
    cache = msal.SerializableTokenCache()
    cache_path = config.TOKEN_CACHE_PATH
    if cache_path:
        try:
            with open(cache_path, "r") as fh:
                cache.deserialize(fh.read())
        except FileNotFoundError:
            pass

        def _persist():
            if cache.has_state_changed:
                with open(cache_path, "w") as fh:
                    fh.write(cache.serialize())

        atexit.register(_persist)
    return cache


_cache: Optional[msal.SerializableTokenCache] = None


def _get_cache() -> msal.SerializableTokenCache:
    global _cache
    if _cache is None:
        _cache = _build_cache()
    return _cache


# ── Delegated auth (device-code flow) ───────────────────────────────

def _build_public_app() -> msal.PublicClientApplication:
    if not config.CLIENT_ID or not config.AUTHORITY:
        raise ValueError(
            "GRAPH_CLIENT_ID and GRAPH_TENANT_ID must be set for delegated auth. "
            "Check your .env file in the outlook_integration folder."
        )
    return msal.PublicClientApplication(
        config.CLIENT_ID,
        authority=config.AUTHORITY,
        token_cache=_get_cache(),
    )


def get_delegated_token(scopes: Optional[list[str]] = None) -> str:
    """
    Acquire a delegated access token via device-code flow.

    On the first call the user is prompted to open a URL and enter a code.
    Subsequent calls within the token lifetime are served from the cache.

    Returns the raw access-token string.
    """
    scopes = scopes or config.DELEGATED_SCOPES
    app = _build_public_app()

    # Try the cache first.
    accounts = app.get_accounts()
    if accounts:
        result = app.acquire_token_silent(scopes, account=accounts[0])
        if result and "access_token" in result:
            return result["access_token"]

    # Fall back to device-code flow.
    flow = app.initiate_device_flow(scopes=scopes)
    if "user_code" not in flow:
        raise RuntimeError(f"Device-code flow failed: {flow.get('error_description', flow)}")

    print(flow["message"], file=sys.stderr)
    result = app.acquire_token_by_device_flow(flow)

    if "access_token" not in result:
        raise RuntimeError(
            f"Token acquisition failed: "
            f"{result.get('error')}: {result.get('error_description')}"
        )
    return result["access_token"]


# ── App-only auth (client credentials) ──────────────────────────────

def _build_confidential_app() -> msal.ConfidentialClientApplication:
    if not config.CLIENT_ID or not config.CLIENT_SECRET or not config.AUTHORITY:
        raise ValueError(
            "GRAPH_CLIENT_ID, GRAPH_CLIENT_SECRET, and GRAPH_TENANT_ID must all be set "
            "for app-only auth.  Check your .env file in the outlook_integration folder."
        )
    return msal.ConfidentialClientApplication(
        config.CLIENT_ID,
        authority=config.AUTHORITY,
        client_credential=config.CLIENT_SECRET,
        token_cache=_get_cache(),
    )


def get_app_token(scopes: Optional[list[str]] = None) -> str:
    """
    Acquire an app-only access token via client-credentials grant.

    Returns the raw access-token string.
    """
    scopes = scopes or config.APP_SCOPES
    app = _build_confidential_app()
    result = app.acquire_token_for_client(scopes=scopes)

    if "access_token" not in result:
        raise RuntimeError(
            f"Client-credential token acquisition failed: "
            f"{result.get('error')}: {result.get('error_description')}"
        )
    return result["access_token"]
