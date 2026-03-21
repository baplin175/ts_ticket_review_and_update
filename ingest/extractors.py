"""Shared TeamSupport -> DB extraction helpers for ingestion."""

from __future__ import annotations

from datetime import datetime, timezone

from ts_client import ticket_id as extract_ticket_id


def parse_ts_datetime(value):
    """Parse a TeamSupport datetime string into a timezone-aware datetime."""
    if not value:
        return None
    v = str(value).strip()
    if not v:
        return None
    try:
        if v.endswith("Z"):
            return datetime.fromisoformat(v.replace("Z", "+00:00"))
        dt = datetime.fromisoformat(v)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass
    try:
        return datetime.strptime(v, "%m/%d/%Y %I:%M %p").replace(tzinfo=timezone.utc)
    except Exception:
        return None


def extract_ticket_row(ticket_raw: dict, now: datetime) -> dict:
    """Build a dict suitable for db.upsert_ticket from a raw TS ticket dict."""
    tid = extract_ticket_id(ticket_raw)
    ticket_number = str(ticket_raw.get("TicketNumber") or "").strip()
    ticket_name = str(ticket_raw.get("Name") or ticket_raw.get("TicketName") or "").strip()

    date_created = parse_ts_datetime(str(ticket_raw.get("DateCreated") or "").strip())
    date_modified = parse_ts_datetime(str(ticket_raw.get("DateModified") or "").strip())
    closed_at = parse_ts_datetime(str(ticket_raw.get("DateClosed") or "").strip())

    days_opened_raw = ticket_raw.get("DaysOpened")
    days_opened = None
    if days_opened_raw is not None and str(days_opened_raw).strip():
        try:
            days_opened = float(str(days_opened_raw).strip())
        except ValueError:
            pass

    days_since_modified = None
    if date_modified:
        days_since_modified = (now - date_modified).days

    status = str(ticket_raw.get("Status") or "").strip() or None
    severity = str(ticket_raw.get("Severity") or "").strip() or None
    product_name = str(ticket_raw.get("ProductName") or ticket_raw.get("Product") or "").strip() or None
    assignee = str(
        ticket_raw.get("UserName")
        or ticket_raw.get("AssignedTo")
        or ticket_raw.get("AssignedToName")
        or ticket_raw.get("Assignee")
        or ticket_raw.get("AssigneeName")
        or ticket_raw.get("OwnerName")
        or ticket_raw.get("Owner")
        or ticket_raw.get("AssignedToUserName")
        or ""
    ).strip() or None
    customer = str(ticket_raw.get("PrimaryCustomer") or "").strip() or None

    return {
        "ticket_id": int(tid) if tid else None,
        "ticket_number": ticket_number or None,
        "ticket_name": ticket_name or None,
        "status": status,
        "severity": severity,
        "product_name": product_name,
        "assignee": assignee,
        "customer": customer,
        "date_created": date_created,
        "date_modified": date_modified,
        "closed_at": closed_at,
        "days_opened": days_opened,
        "days_since_modified": days_since_modified,
        "source_updated_at": date_modified,
        "source_payload": ticket_raw,
    }


def extract_action_row(action_raw: dict, tid: int, cleaned: dict) -> dict:
    """Build a dict suitable for db.upsert_action from raw + cleaned action dicts."""
    action_id_str = cleaned.get("action_id") or ""
    action_id = int(action_id_str) if action_id_str else None
    raw_desc = action_raw.get("Description") or action_raw.get("Text") or ""
    cleaned_desc = cleaned.get("description") or ""

    return {
        "action_id": action_id,
        "ticket_id": tid,
        "created_at": parse_ts_datetime(cleaned.get("created_at")),
        "action_type": cleaned.get("action_type") or None,
        "creator_id": cleaned.get("creator_id") or None,
        "creator_name": cleaned.get("creator_name") or None,
        "party": cleaned.get("party") or None,
        "is_visible": cleaned.get("is_visible"),
        "description": raw_desc or None,
        "cleaned_description": cleaned_desc or None,
        "action_class": None,
        "is_empty": not cleaned_desc.strip(),
        "is_customer_visible": cleaned.get("is_visible"),
        "source_payload": action_raw,
    }
