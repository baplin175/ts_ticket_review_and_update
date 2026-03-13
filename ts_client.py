"""
TeamSupport API client — fetch open tickets and their activities.
"""

import base64
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List

import requests

from config import OUTPUT_DIR, TS_BASE, TS_KEY, TS_USER_ID


def _ts_headers() -> Dict[str, str]:
    auth = base64.b64encode(f"{TS_USER_ID}:{TS_KEY}".encode("ascii")).decode("ascii")
    return {
        "Authorization": f"Basic {auth}",
        "Accept": "application/json",
    }


def ts_get(url: str, params=None) -> Any:
    r = requests.get(url, headers=_ts_headers(), params=params or {}, timeout=60)
    r.raise_for_status()
    return r.json()


def ts_put(url: str, payload: Dict[str, Any]) -> Any:
    headers = {**_ts_headers(), "Content-Type": "application/json"}
    r = requests.put(url, headers=headers, json=payload, timeout=30)
    r.raise_for_status()
    return r.json()


# ── Normalisation helpers ────────────────────────────────────────────

def _normalize_ticket_list(data: Any) -> List[Dict[str, Any]]:
    if not isinstance(data, dict):
        return []
    t = data.get("Tickets")
    if isinstance(t, list):
        return t
    if isinstance(t, dict):
        if t.get("RecordsReturned") == "0":
            return []
        if any(k in t for k in ("ID", "TicketID", "TicketNumber", "Name")):
            return [t]
    if "Ticket" in data:
        tt = data["Ticket"]
        return tt if isinstance(tt, list) else [tt]
    return []


def _normalize_action_list(data: Any) -> List[Dict[str, Any]]:
    if not isinstance(data, dict):
        return []
    a = data.get("Actions")
    if isinstance(a, list):
        return a
    if isinstance(a, dict):
        if a.get("RecordsReturned") == "0":
            return []
        if any(k in a for k in ("ID", "ActionID", "DateCreated")):
            return [a]
    if "Action" in data:
        aa = data["Action"]
        return aa if isinstance(aa, list) else [aa]
    return []


# ── Public API ───────────────────────────────────────────────────────

def fetch_open_tickets(ticket_numbers: List[str] | None = None) -> List[Dict[str, Any]]:
    """Return open tickets (paginated).

    If *ticket_numbers* is provided, each ticket is fetched individually
    regardless of open/closed status, avoiding a full paginated sweep.
    """
    if ticket_numbers:
        all_targeted: List[Dict[str, Any]] = []
        for tn in ticket_numbers:
            data = ts_get(
                f"{TS_BASE}/Tickets",
                params={"TicketNumber": tn},
            )
            items = _normalize_ticket_list(data)
            all_targeted.extend(items)
        print(f"[ts] Fetched {len(all_targeted)} ticket(s) for number(s) {', '.join(ticket_numbers)}.", flush=True)
        return all_targeted

    all_tickets: List[Dict[str, Any]] = []
    page, page_size = 1, 500

    while True:
        params = {
            "isClosed": "False",
            "pageNumber": page,
            "pageSize": page_size,
        }
        data = ts_get(f"{TS_BASE}/Tickets", params=params)
        page_items = _normalize_ticket_list(data)
        if not page_items:
            break
        all_tickets.extend(page_items)
        if len(page_items) < page_size:
            break
        page += 1

    print(f"[ts] Fetched {len(all_tickets)} open ticket(s).", flush=True)
    return all_tickets


def fetch_all_activities(ticket_id: str) -> List[Dict[str, Any]]:
    """Return every activity/action for a ticket (paginated, oldest→newest)."""
    all_actions: List[Dict[str, Any]] = []
    page, page_size = 1, 100

    while True:
        params = [("page", page), ("pageSize", page_size)]
        data = ts_get(f"{TS_BASE}/Tickets/{ticket_id}/Actions", params=params)
        page_items = _normalize_action_list(data)
        if not page_items:
            break
        all_actions.extend(page_items)
        if len(page_items) < page_size:
            break
        page += 1

    return all_actions


# ── inHANCE (CS team) user ID cache ─────────────────────────────────

_INHANCE_IDS: set | None = None


def _normalize_users_list(data: Any) -> List[Dict[str, Any]]:
    if not isinstance(data, dict):
        return []
    u = data.get("Users") or data.get("User")
    if isinstance(u, list):
        return u
    if isinstance(u, dict):
        if u.get("RecordsReturned") == "0":
            return []
        if any(k in u for k in ("ID", "UserID", "Name", "Email")):
            return [u]
    return []


def fetch_inhance_user_ids() -> set:
    """Fetch all user IDs belonging to Organization=inHANCE (one API call, cached)."""
    global _INHANCE_IDS
    if _INHANCE_IDS is not None:
        return _INHANCE_IDS
    try:
        data = ts_get(f"{TS_BASE}/Users", params={"Organization": "inHANCE"})
    except Exception as e:
        print(f"[ts] Failed to fetch inHANCE users: {e}", flush=True)
        _INHANCE_IDS = set()
        return _INHANCE_IDS
    users = _normalize_users_list(data)
    _INHANCE_IDS = set()
    for u in users:
        uid = str(u.get("ID") or u.get("Id") or u.get("UserID") or "").strip()
        if uid:
            _INHANCE_IDS.add(uid)
    print(f"[ts] Loaded {len(_INHANCE_IDS)} inHANCE user ID(s).", flush=True)
    return _INHANCE_IDS


def is_inhance_user(creator_id: str) -> bool:
    """Return True if the creator_id belongs to an inHANCE org user."""
    return creator_id in fetch_inhance_user_ids()


def ticket_id(ticket: Dict[str, Any]) -> str:
    """Extract the internal ID from a ticket dict."""
    return str(
        ticket.get("ID")
        or ticket.get("Id")
        or ticket.get("TicketID")
        or ticket.get("TicketId")
        or ""
    ).strip()


# ── Ticket update with last-comment timestamps ──────────────────────

def _last_comment_timestamps(activities: List[Dict[str, Any]]) -> tuple:
    """Return (last_inh_comment, last_cust_comment) timestamps from an activities list."""
    last_inh = ""
    last_cust = ""
    for a in activities:
        ts = a.get("created_at", "")
        if not ts:
            continue
        if a.get("party") == "inh":
            last_inh = ts
        elif a.get("party") == "cust":
            last_cust = ts
    return last_inh, last_cust


def update_ticket(tid: str, fields: Dict[str, Any], activities: List[Dict[str, Any]]) -> Any:
    """PUT fields to a ticket, automatically injecting LastInhComment / LastCustComment.

    *activities* should be the cleaned activities list (dicts with ``party`` and
    ``created_at`` keys) so the last comment timestamps can be derived.

    If *tid* is empty (e.g. CSV-sourced data), the payload is saved as a dry-run
    file without calling the API.  If the API returns 403 (rate-limited), the
    payload is also written to the dry-run file.
    """
    last_inh, last_cust = _last_comment_timestamps(activities)
    if last_inh:
        fields.setdefault("LastInhComment", last_inh)
    if last_cust:
        fields.setdefault("LastCustComment", last_cust)
    payload = {"Ticket": fields}

    if not tid:
        save_dry_run_payload(tid, payload)
        print(f"[ts] Dry-run (no ticket_id): payload saved.", flush=True)
        return None

    try:
        return ts_put(f"{TS_BASE}/Tickets/{tid}", payload)
    except requests.exceptions.HTTPError as e:
        if e.response is not None and e.response.status_code == 403:
            save_dry_run_payload(tid, payload)
            raise
        raise


def save_dry_run_payload(tid: str, payload: Dict[str, Any]) -> None:
    """Append a payload to the dry-run output file for later review."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    dry_run_path = os.path.join(OUTPUT_DIR, "api_payloads_dry_run.json")
    entry = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "method": "PUT",
        "url": f"{TS_BASE}/Tickets/{tid}",
        "payload": payload,
    }
    # Append to existing array or start a new one
    existing = []
    if os.path.exists(dry_run_path):
        try:
            with open(dry_run_path, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except (json.JSONDecodeError, OSError):
            existing = []
    existing.append(entry)
    with open(dry_run_path, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False, indent=2)
    print(f"[ts] Dry-run: payload for ticket {tid} saved to {dry_run_path}", flush=True)
