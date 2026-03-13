"""
Part 1 — Pull open-ticket activities from TeamSupport, cleanse them,
and write everything to a single timestamped JSONL file.

Usage:
    python run_pull_activities.py              # pulls up to MAX_TICKETS (default 5)
    MAX_TICKETS=20 python run_pull_activities.py   # override via env
    MAX_TICKETS=0  python run_pull_activities.py   # unlimited
"""

import json
import os
import sys
from datetime import datetime, timezone

from config import MAX_TICKETS, OUTPUT_DIR, TARGET_TICKET
from ts_client import fetch_open_tickets, fetch_all_activities, ticket_id
from activity_cleaner import clean_activity_dict


def _log(msg: str) -> None:
    print(msg, flush=True)


def _run_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def main() -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 1. Fetch open tickets (filtered at the API when TARGET_TICKET is set)
    _log("[run] Fetching open tickets...")
    open_tickets = fetch_open_tickets(ticket_number=TARGET_TICKET or None)
    if not open_tickets:
        if TARGET_TICKET:
            _log(f"[run] Ticket {TARGET_TICKET} not found among open tickets. Exiting.")
            sys.exit(1)
        _log("[run] No open tickets found. Exiting.")
        sys.exit(0)

    # 2. Apply ticket limit (only when not targeting a specific ticket)
    if TARGET_TICKET:
        _log(f"[run] Targeting ticket {TARGET_TICKET}.")
    elif MAX_TICKETS > 0:
        open_tickets = open_tickets[:MAX_TICKETS]
        _log(f"[run] Limited to {MAX_TICKETS} ticket(s) for this run.")

    # 3. Prepare output file
    ts = _run_timestamp()
    out_path = os.path.join(OUTPUT_DIR, f"activities_{ts}.jsonl")

    total_activities = 0

    with open(out_path, "w", encoding="utf-8") as fout:
        for idx, ticket in enumerate(open_tickets, 1):
            tid = ticket_id(ticket)
            tnum = str(ticket.get("TicketNumber") or ticket.get("Number") or tid).strip()
            tname = str(ticket.get("Name") or "").strip()
            _log(f"[run] ({idx}/{len(open_tickets)}) Ticket {tnum} — {tname}")

            if not tid:
                _log(f"  [warn] Could not resolve ID for ticket; skipping.")
                continue

            # 4. Fetch all activities for this ticket
            _log(f"  [run] Fetching activities for ticket {tid}...")
            try:
                raw_actions = fetch_all_activities(tid)
            except Exception as e:
                _log(f"  [error] Failed to fetch activities: {e}")
                continue
            _log(f"  [run] {len(raw_actions)} raw activity/ies fetched.")

            # 5. Cleanse and write each activity as a JSONL line
            for action in raw_actions:
                cleaned = clean_activity_dict(action)
                record = {
                    "ticket_id": tid,
                    "ticket_number": tnum,
                    "ticket_name": tname,
                    **cleaned,
                }
                fout.write(json.dumps(record, ensure_ascii=False) + "\n")
                total_activities += 1

    _log(f"[run] Done. {total_activities} activity/ies written to {out_path}")


if __name__ == "__main__":
    main()
