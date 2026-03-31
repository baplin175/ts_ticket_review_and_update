"""
Microsoft Graph API — Calendar operations.

Provides helpers to read and create calendar events via the Graph API.
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
    return f"/users/{user_id}" if user_id else "/me"


# ── List / get events ───────────────────────────────────────────────

def list_events(
    token: str,
    *,
    user_id: Optional[str] = None,
    calendar_id: Optional[str] = None,
    top: int = 25,
    skip: int = 0,
    select: Optional[list[str]] = None,
    filter_query: Optional[str] = None,
    order_by: str = "start/dateTime asc",
) -> list[dict[str, Any]]:
    """
    List calendar events.

    Parameters
    ----------
    calendar_id : specific calendar; omit for the default calendar.
    filter_query : OData $filter, e.g.
        ``"start/dateTime ge '2025-01-01T00:00:00Z'"``
    """
    base = _user_path(user_id)
    if calendar_id:
        url = f"{config.GRAPH_BASE_URL}{base}/calendars/{calendar_id}/events"
    else:
        url = f"{config.GRAPH_BASE_URL}{base}/events"

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


def get_event(
    token: str,
    event_id: str,
    *,
    user_id: Optional[str] = None,
    select: Optional[list[str]] = None,
) -> dict[str, Any]:
    """Fetch a single event by ID."""
    base = _user_path(user_id)
    url = f"{config.GRAPH_BASE_URL}{base}/events/{event_id}"

    params: dict[str, str] = {}
    if select:
        params["$select"] = ",".join(select)

    resp = requests.get(url, headers=_headers(token), params=params)
    _raise_on_error(resp)
    return resp.json()


def calendar_view(
    token: str,
    start: str,
    end: str,
    *,
    user_id: Optional[str] = None,
    top: int = 50,
    select: Optional[list[str]] = None,
) -> list[dict[str, Any]]:
    """
    Return events in a time window (calendarView).

    ``start`` and ``end`` are ISO 8601 datetime strings,
    e.g. ``"2025-03-01T00:00:00Z"``.
    """
    base = _user_path(user_id)
    url = f"{config.GRAPH_BASE_URL}{base}/calendarView"

    params: dict[str, str] = {
        "startDateTime": start,
        "endDateTime": end,
        "$top": str(top),
    }
    if select:
        params["$select"] = ",".join(select)

    resp = requests.get(url, headers=_headers(token), params=params)
    _raise_on_error(resp)
    return resp.json().get("value", [])


# ── Create / update / delete events ─────────────────────────────────

def create_event(
    token: str,
    *,
    subject: str,
    start: str,
    end: str,
    start_tz: str = "UTC",
    end_tz: str = "UTC",
    body: Optional[str] = None,
    body_type: str = "HTML",
    location: Optional[str] = None,
    attendees: Optional[list[str]] = None,
    is_online_meeting: bool = False,
    user_id: Optional[str] = None,
    calendar_id: Optional[str] = None,
) -> dict[str, Any]:
    """
    Create a new calendar event.

    Parameters
    ----------
    start / end : ISO 8601 datetime strings (without offset — tz comes
                  from ``start_tz`` / ``end_tz``).
    attendees : list of email address strings.
    is_online_meeting : create a Teams meeting link.
    """
    base = _user_path(user_id)
    if calendar_id:
        url = f"{config.GRAPH_BASE_URL}{base}/calendars/{calendar_id}/events"
    else:
        url = f"{config.GRAPH_BASE_URL}{base}/events"

    event: dict[str, Any] = {
        "subject": subject,
        "start": {"dateTime": start, "timeZone": start_tz},
        "end": {"dateTime": end, "timeZone": end_tz},
        "isOnlineMeeting": is_online_meeting,
    }
    if is_online_meeting:
        event["onlineMeetingProvider"] = "teamsForBusiness"

    if body:
        event["body"] = {"contentType": body_type, "content": body}
    if location:
        event["location"] = {"displayName": location}
    if attendees:
        event["attendees"] = [
            {
                "emailAddress": {"address": addr},
                "type": "required",
            }
            for addr in attendees
        ]

    resp = requests.post(url, headers=_headers(token), data=json.dumps(event))
    _raise_on_error(resp)
    return resp.json()


def update_event(
    token: str,
    event_id: str,
    updates: dict[str, Any],
    *,
    user_id: Optional[str] = None,
) -> dict[str, Any]:
    """
    Patch an existing event.

    ``updates`` is a dict of Graph event fields to modify, e.g.
    ``{"subject": "New Title"}``.
    """
    base = _user_path(user_id)
    url = f"{config.GRAPH_BASE_URL}{base}/events/{event_id}"

    resp = requests.patch(url, headers=_headers(token), data=json.dumps(updates))
    _raise_on_error(resp)
    return resp.json()


def delete_event(
    token: str,
    event_id: str,
    *,
    user_id: Optional[str] = None,
) -> None:
    """Delete a calendar event."""
    base = _user_path(user_id)
    url = f"{config.GRAPH_BASE_URL}{base}/events/{event_id}"

    resp = requests.delete(url, headers=_headers(token))
    _raise_on_error(resp)


# ── Calendars ────────────────────────────────────────────────────────

def list_calendars(
    token: str,
    *,
    user_id: Optional[str] = None,
) -> list[dict[str, Any]]:
    """List all calendars for the user."""
    base = _user_path(user_id)
    url = f"{config.GRAPH_BASE_URL}{base}/calendars"

    resp = requests.get(url, headers=_headers(token))
    _raise_on_error(resp)
    return resp.json().get("value", [])
