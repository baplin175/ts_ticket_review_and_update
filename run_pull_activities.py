"""
Part 1 — Pull open-ticket activities from TeamSupport, cleanse them,
and write everything to a single timestamped JSON file.

Usage:
    python run_pull_activities.py              # pulls up to MAX_TICKETS (default 5)
    MAX_TICKETS=20 python run_pull_activities.py   # override via env
    MAX_TICKETS=0  python run_pull_activities.py   # unlimited
"""

import json
import os
import sys
from datetime import datetime, timezone

from config import MAX_TICKETS, OUTPUT_DIR, TARGET_TICKETS
from ts_client import fetch_open_tickets, fetch_all_activities, ticket_id
from activity_cleaner import clean_activity_dict


def _log(msg: str) -> None:
    print(msg, flush=True)


def _run_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _parse_ts_datetime(value: str):
    """Parse a TeamSupport datetime string into a timezone-aware datetime."""
    if not value:
        return None
    v = value.strip()
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


def _ticket_meta(ticket: dict) -> dict:
    """Extract ticket-level metadata fields for the JSON record."""
    date_created = str(ticket.get("DateCreated") or "").strip()
    date_modified = str(ticket.get("DateModified") or "").strip()
    days_opened = str(ticket.get("DaysOpened") or "").strip()
    status = str(ticket.get("Status") or "").strip()
    severity = str(ticket.get("Severity") or "").strip()
    product_name = str(
        ticket.get("ProductName") or ticket.get("Product") or ""
    ).strip()
    assignee = str(
        ticket.get("UserName")
        or ticket.get("AssignedTo")
        or ticket.get("AssignedToName")
        or ticket.get("Assignee")
        or ticket.get("AssigneeName")
        or ticket.get("OwnerName")
        or ticket.get("Owner")
        or ticket.get("AssignedToUserName")
        or ""
    ).strip()
    customer = str(ticket.get("PrimaryCustomer") or "").strip()

    # Compute days since last modified
    days_since_modified = ""
    dm_dt = _parse_ts_datetime(date_modified)
    if dm_dt:
        now_dt = datetime.now(timezone.utc)
        days_since_modified = str((now_dt - dm_dt).days)

    return {
        "date_created": date_created,
        "date_modified": date_modified,
        "days_opened": days_opened,
        "days_since_modified": days_since_modified,
        "status": status,
        "severity": severity,
        "product_name": product_name,
        "assignee": assignee,
        "customer": customer,
    }


def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 1. Fetch tickets (filtered at the API when TARGET_TICKETS is set)
    _log("[run] Fetching tickets...")
    open_tickets = fetch_open_tickets(ticket_numbers=TARGET_TICKETS or None)
    if not open_tickets:
        if TARGET_TICKETS:
            _log(f"[run] Ticket(s) {', '.join(TARGET_TICKETS)} not found. Exiting.")
            sys.exit(1)
        _log("[run] No open tickets found. Exiting.")
        sys.exit(0)

    # 2. Apply ticket limit (only when not targeting specific tickets)
    if TARGET_TICKETS:
        _log(f"[run] Targeting ticket(s): {', '.join(TARGET_TICKETS)}.")
    elif MAX_TICKETS > 0:
        open_tickets = open_tickets[:MAX_TICKETS]
        _log(f"[run] Limited to {MAX_TICKETS} ticket(s) for this run.")

    # 3. Prepare output file
    ts = _run_timestamp()
    out_path = os.path.join(OUTPUT_DIR, f"activities_{ts}.json")

    all_tickets_data = []
    total_activities = 0

    for idx, ticket in enumerate(open_tickets, 1):
        tid = ticket_id(ticket)
        tnum = str(ticket.get("TicketNumber") or ticket.get("Number") or tid).strip()
        tname = str(ticket.get("Name") or "").strip()
        _log(f"[run] ({idx}/{len(open_tickets)}) Ticket {tnum} — {tname}")

        if not tid:
            _log(f"  [warn] Could not resolve ID for ticket; skipping.")
            continue

        # Extract ticket-level metadata once per ticket
        meta = _ticket_meta(ticket)

        # 4. Fetch all activities for this ticket
        _log(f"  [run] Fetching activities for ticket {tid}...")
        try:
            raw_actions = fetch_all_activities(tid)
        except Exception as e:
            _log(f"  [error] Failed to fetch activities: {e}")
            continue
        _log(f"  [run] {len(raw_actions)} raw activity/ies fetched.")

        # 5. Cleanse each activity
        activities = []
        for action in raw_actions:
            activities.append(clean_activity_dict(action))
            total_activities += 1

        ticket_record = {
            "ticket_id": tid,
            "ticket_number": tnum,
            "ticket_name": tname,
            **meta,
            "activities": activities,
        }
        all_tickets_data.append(ticket_record)

    # 6. Write nested JSON
    with open(out_path, "w", encoding="utf-8") as fout:
        json.dump(all_tickets_data, fout, ensure_ascii=False, indent=2)

    _log(f"[run] Done. {total_activities} activity/ies across {len(all_tickets_data)} ticket(s) written to {out_path}")


if __name__ == "__main__":
    main()
