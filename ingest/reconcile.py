"""Closed-ticket reconciliation helpers for ingestion."""

from __future__ import annotations

from datetime import datetime, timezone

import db
from activity_cleaner import clean_activity_dict
from ts_client import fetch_all_activities, fetch_ticket_by_id

from .extractors import extract_action_row, extract_ticket_row


def reconcile_closed(upserted_ids: list[int], *, verbose: bool = False) -> list[int]:
    """Refresh tickets that are still open in DB but absent from the latest full sync."""
    db_open_ids = set(db.get_open_ticket_ids())
    synced_ids = set(upserted_ids)
    missing_ids = sorted(db_open_ids - synced_ids)

    if not missing_ids:
        print("[reconcile] All DB-open tickets were seen in sync — nothing to reconcile.", flush=True)
        return []

    print(
        f"[reconcile] {len(missing_ids)} ticket(s) open in DB but not returned by TS — "
        f"re-fetching to check if closed…",
        flush=True,
    )

    now = datetime.now(timezone.utc)
    reconciled = []

    for tid in missing_ids:
        try:
            raw_tickets = fetch_ticket_by_id(str(tid))
            if not raw_tickets:
                if verbose:
                    print(f"  [reconcile] ticket_id={tid}: not found in TS — skipping.", flush=True)
                continue

            ticket_row = extract_ticket_row(raw_tickets[0], now)
            tnum = ticket_row.get("ticket_number") or "?"
            action_rows = []

            for action_raw in fetch_all_activities(str(tid)):
                cleaned = clean_activity_dict(action_raw)
                if not cleaned.get("action_id"):
                    continue
                action_row = extract_action_row(action_raw, tid, cleaned)
                action_row["ticket_number"] = tnum
                action_rows.append(action_row)

            db.upsert_ticket_with_actions(ticket_row, action_rows, now=now)
            reconciled.append(tid)

            status = ticket_row.get("status") or "unknown"
            closed = ticket_row.get("closed_at")
            label = f"CLOSED ({closed})" if closed else f"status={status}"
            if verbose:
                print(f"  [reconcile] #{tnum} (id={tid}): {label}", flush=True)

        except Exception as exc:
            print(f"  [reconcile] ERROR ticket_id={tid}: {exc}", flush=True)

    print(f"[reconcile] Reconciled {len(reconciled)} ticket(s).", flush=True)
    return reconciled
