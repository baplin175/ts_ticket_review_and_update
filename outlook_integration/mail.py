"""
Microsoft Graph API — Mail operations.

Provides helpers to read, search, and send emails via the Graph API.
Works with both delegated and app-only tokens.
"""

from __future__ import annotations

import json
from typing import Any, Optional

import requests

from . import config


# ── Internal helpers ─────────────────────────────────────────────────

def _headers(token: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def _raise_on_error(resp: requests.Response) -> None:
    if not resp.ok:
        detail = resp.text[:500]
        raise RuntimeError(f"Graph API {resp.status_code}: {detail}")


def _user_path(user_id: Optional[str]) -> str:
    """Return '/me' for delegated or '/users/{id}' for app-only."""
    return f"/users/{user_id}" if user_id else "/me"


# ── Read / list ──────────────────────────────────────────────────────

def list_messages(
    token: str,
    *,
    user_id: Optional[str] = None,
    folder: str = "inbox",
    top: int = 25,
    skip: int = 0,
    select: Optional[list[str]] = None,
    filter_query: Optional[str] = None,
    order_by: str = "receivedDateTime desc",
) -> list[dict[str, Any]]:
    """
    List mail messages from a folder.

    Parameters
    ----------
    token : access token (delegated or app-only).
    user_id : required for app-only; omit for delegated (/me).
    folder : mail folder name (default ``inbox``).
    top / skip : paging.
    select : list of fields to return (default: common set).
    filter_query : OData $filter expression.
    order_by : OData $orderby expression.

    Returns a list of message dicts.
    """
    base = _user_path(user_id)
    url = f"{config.GRAPH_BASE_URL}{base}/mailFolders/{folder}/messages"

    params: dict[str, str] = {
        "$top": str(top),
        "$skip": str(skip),
        "$orderby": order_by,
    }
    if select:
        params["$select"] = ",".join(select)
    if filter_query:
        params["$filter"] = filter_query

    resp = requests.get(url, headers=_headers(token), params=params)
    _raise_on_error(resp)
    return resp.json().get("value", [])


def get_message(
    token: str,
    message_id: str,
    *,
    user_id: Optional[str] = None,
    select: Optional[list[str]] = None,
) -> dict[str, Any]:
    """Fetch a single message by ID."""
    base = _user_path(user_id)
    url = f"{config.GRAPH_BASE_URL}{base}/messages/{message_id}"

    params: dict[str, str] = {}
    if select:
        params["$select"] = ",".join(select)

    resp = requests.get(url, headers=_headers(token), params=params)
    _raise_on_error(resp)
    return resp.json()


def search_messages(
    token: str,
    query: str,
    *,
    user_id: Optional[str] = None,
    top: int = 25,
) -> list[dict[str, Any]]:
    """
    Full-text search via the ``$search`` query parameter.

    ``query`` uses KQL syntax, e.g.:
      - ``"subject:weekly report"``
      - ``"from:alice@contoso.com"``
      - ``"hasAttachments:true AND subject:invoice"``
    """
    base = _user_path(user_id)
    url = f"{config.GRAPH_BASE_URL}{base}/messages"

    params: dict[str, str] = {
        "$search": f'"{query}"',
        "$top": str(top),
    }
    resp = requests.get(url, headers=_headers(token), params=params)
    _raise_on_error(resp)
    return resp.json().get("value", [])


# ── Send ─────────────────────────────────────────────────────────────

def send_mail(
    token: str,
    *,
    subject: str,
    body: str,
    to: list[str],
    cc: Optional[list[str]] = None,
    bcc: Optional[list[str]] = None,
    body_type: str = "HTML",
    save_to_sent: bool = True,
    user_id: Optional[str] = None,
) -> None:
    """
    Send an email.

    Parameters
    ----------
    to / cc / bcc : lists of email address strings.
    body_type : ``"HTML"`` or ``"Text"``.
    save_to_sent : whether to save a copy in Sent Items.
    """
    base = _user_path(user_id)
    url = f"{config.GRAPH_BASE_URL}{base}/sendMail"

    def _recip(addr: str) -> dict:
        return {"emailAddress": {"address": addr}}

    message: dict[str, Any] = {
        "subject": subject,
        "body": {
            "contentType": body_type,
            "content": body,
        },
        "toRecipients": [_recip(a) for a in to],
    }
    if cc:
        message["ccRecipients"] = [_recip(a) for a in cc]
    if bcc:
        message["bccRecipients"] = [_recip(a) for a in bcc]

    payload = {
        "message": message,
        "saveToSentItems": save_to_sent,
    }
    resp = requests.post(url, headers=_headers(token), data=json.dumps(payload))
    _raise_on_error(resp)


def reply_to_message(
    token: str,
    message_id: str,
    *,
    comment: str,
    user_id: Optional[str] = None,
) -> None:
    """Reply to an existing message (reply-all)."""
    base = _user_path(user_id)
    url = f"{config.GRAPH_BASE_URL}{base}/messages/{message_id}/replyAll"

    payload = {"comment": comment}
    resp = requests.post(url, headers=_headers(token), data=json.dumps(payload))
    _raise_on_error(resp)


# ── Folders ──────────────────────────────────────────────────────────

def list_mail_folders(
    token: str,
    *,
    user_id: Optional[str] = None,
) -> list[dict[str, Any]]:
    """List all mail folders for the user."""
    base = _user_path(user_id)
    url = f"{config.GRAPH_BASE_URL}{base}/mailFolders"

    resp = requests.get(url, headers=_headers(token))
    _raise_on_error(resp)
    return resp.json().get("value", [])
