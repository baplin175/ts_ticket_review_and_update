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
import csv
from datetime import datetime, timezone
from pathlib import Path

from config import MAX_TICKETS, OUTPUT_DIR, SKIP_OUTPUT_FILES, TARGET_TICKETS
from ts_client import fetch_open_tickets, fetch_all_activities, ticket_id
from activity_cleaner import clean_activity_dict, clean_activity


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


CSV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Activities.csv")


def _load_known_inh_names() -> set:
    """Scan prior activities JSON files for creator names with party=='inh'."""
    names: set = set()
    out = Path(OUTPUT_DIR)
    for f in sorted(out.glob("activities_*.json"), reverse=True):
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            for t in data:
                for a in t.get("activities", []):
                    if a.get("party") == "inh" and a.get("creator_name"):
                        names.add(a["creator_name"])
            if names:
                break  # one file is enough
        except Exception:
            continue
    return names


def _load_from_csv(ticket_numbers: list[str] | None) -> list[dict]:
    """Read Activities.csv and build the same ticket+activities JSON structure."""
    if not os.path.exists(CSV_PATH):
        return []

    _log(f"[csv-fallback] Reading {CSV_PATH}...")
    inh_names = _load_known_inh_names()
    if inh_names:
        _log(f"[csv-fallback] Loaded {len(inh_names)} known inHANCE name(s) from prior run.")
    else:
        _log("[csv-fallback] No prior inHANCE names found; party will be 'unknown'.")

    target_set = set(ticket_numbers) if ticket_numbers else None

    # Group rows by ticket number
    tickets_map: dict[str, list[dict]] = {}
    ticket_meta_map: dict[str, dict] = {}
    with open(CSV_PATH, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            tnum = (row.get("Ticket Number") or "").strip()
            if not tnum:
                continue
            if target_set and tnum not in target_set:
                continue

            if tnum not in ticket_meta_map:
                ticket_meta_map[tnum] = {
                    "ticket_name": (row.get("Ticket Name") or "").strip(),
                    "product_name": (row.get("Ticket Product Name") or "").strip(),
                    "customer": (row.get("Primary Customer") or "").strip(),
                }
                tickets_map[tnum] = []

            creator_name = (row.get("Action Creator Name") or "").strip()
            if inh_names:
                party = "inh" if creator_name in inh_names else "cust"
            else:
                party = "unknown"

            raw_desc = row.get("Action Description") or ""
            cleaned = clean_activity(raw_desc, is_html=bool(__import__("re").search(r"<[a-zA-Z][^>]*>", raw_desc)))

            activity = {
                "action_id": "",
                "created_at": (row.get("Date Action Created") or "").strip(),
                "action_type": (row.get("Action Type") or "").strip(),
                "creator_id": "",
                "creator_name": creator_name,
                "party": party,
                "is_visible": True,
                "description": cleaned,
            }
            tickets_map[tnum].append(activity)

    # Build ticket records
    all_tickets = []
    for tnum, activities in tickets_map.items():
        meta = ticket_meta_map[tnum]
        record = {
            "ticket_id": "",
            "ticket_number": tnum,
            "ticket_name": meta["ticket_name"],
            "date_created": "",
            "date_modified": "",
            "days_opened": "",
            "days_since_modified": "",
            "status": "",
            "severity": "",
            "product_name": meta["product_name"],
            "assignee": "",
            "customer": meta["customer"],
            "activities": activities,
        }
        all_tickets.append(record)

    _log(f"[csv-fallback] Loaded {sum(len(a) for a in tickets_map.values())} activities across {len(all_tickets)} ticket(s) from CSV.")
    return all_tickets


def main() -> str | None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 1. Try API first; fall back to CSV on failure
    api_ok = True
    try:
        _log("[run] Fetching tickets from API...")
        open_tickets = fetch_open_tickets(ticket_numbers=TARGET_TICKETS or None)
    except Exception as e:
        _log(f"[run] API fetch failed ({e}). Falling back to Activities.csv...")
        api_ok = False
        open_tickets = []

    if not open_tickets and not api_ok:
        # API failed — try CSV
        all_tickets_data = _load_from_csv(TARGET_TICKETS or None)
        if not all_tickets_data:
            _log("[run] No data from API or CSV. Exiting.")
            sys.exit(1)

        total = sum(len(t["activities"]) for t in all_tickets_data)
        if SKIP_OUTPUT_FILES:
            _log(f"[run] Done (CSV fallback). {total} activity/ies across {len(all_tickets_data)} ticket(s) (file write skipped).")
            return None
        ts = _run_timestamp()
        out_path = os.path.join(OUTPUT_DIR, f"activities_{ts}.json")
        with open(out_path, "w", encoding="utf-8") as fout:
            json.dump(all_tickets_data, fout, ensure_ascii=False, indent=2)
        _log(f"[run] Done (CSV fallback). {total} activity/ies across {len(all_tickets_data)} ticket(s) written to {out_path}")
        return out_path

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

        # 5. Cleanse each activity, keep only visible (public) ones
        activities = []
        skipped_private = 0
        for action in raw_actions:
            cleaned = clean_activity_dict(action)
            if not cleaned.get("is_visible", True):
                skipped_private += 1
                continue
            activities.append(cleaned)
            total_activities += 1
        if skipped_private:
            _log(f"  [run] Filtered out {skipped_private} non-visible (private) activity/ies.")

        # 6. Recalculate date_modified from last inh/cust action date
        last_action_date = ""
        for a in activities:
            if a.get("party") in ("inh", "cust") and a.get("created_at"):
                last_action_date = a["created_at"]
        if last_action_date:
            meta["date_modified"] = last_action_date
            dm_dt = _parse_ts_datetime(last_action_date)
            if dm_dt:
                meta["days_since_modified"] = str((datetime.now(timezone.utc) - dm_dt).days)

        ticket_record = {
            "ticket_id": tid,
            "ticket_number": tnum,
            "ticket_name": tname,
            **meta,
            "activities": activities,
        }
        all_tickets_data.append(ticket_record)

    # 6. Write nested JSON
    if SKIP_OUTPUT_FILES:
        _log(f"[run] Done. {total_activities} activity/ies across {len(all_tickets_data)} ticket(s) (file write skipped).")
        return None
    with open(out_path, "w", encoding="utf-8") as fout:
        json.dump(all_tickets_data, fout, ensure_ascii=False, indent=2)

    _log(f"[run] Done. {total_activities} activity/ies across {len(all_tickets_data)} ticket(s) written to {out_path}")
    return out_path


if __name__ == "__main__":
    main()
