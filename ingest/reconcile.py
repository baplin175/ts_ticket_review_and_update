"""Closed-ticket reconciliation helpers for ingestion."""

from __future__ import annotations

from datetime import datetime, timezone

import requests

import db
from activity_cleaner import clean_activity_dict
from ts_client import fetch_all_activities, fetch_ticket_by_id

from .extractors import extract_action_row, extract_ticket_row


def _mark_deleted(ticket_id: int, now: datetime) -> None:
    """Mark a ticket as deleted/closed in the DB when TS returns 400 (ticket no longer exists)."""
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """UPDATE tickets
                      SET status    = 'Deleted',
                          closed_at = %(now)s,
                          last_ingested_at = %(now)s,
                          last_seen_at     = %(now)s
                    WHERE ticket_id = %(tid)s;""",
                {"tid": ticket_id, "now": now},
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        db.put_conn(conn)


def reconcile_closed(synced_open_ids: list[int], *, verbose: bool = False) -> list[int]:
    """Refresh tickets that are still open in DB but absent from the latest TS open-ticket fetch.

    *synced_open_ids* should contain ALL ticket IDs that TeamSupport reported
    as open (before any date-based filtering), so the diff accurately
    identifies tickets that TS no longer considers open.
    """
    db_open_ids = set(db.get_open_ticket_ids())
    synced_ids = set(synced_open_ids)
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

        except requests.exceptions.HTTPError as exc:
            if exc.response is not None and exc.response.status_code == 400:
                # 400 = ticket deleted/merged in TS — mark closed in DB so it
                # doesn't reappear in reconcile every sync.
                try:
                    _mark_deleted(tid, now)
                    reconciled.append(tid)
                    print(
                        f"  [reconcile] ticket_id={tid}: TS returned 400 (deleted/merged) "
                        f"— marked as Deleted in DB.",
                        flush=True,
                    )
                except Exception as inner:
                    print(f"  [reconcile] ERROR marking ticket_id={tid} as deleted: {inner}", flush=True)
            else:
                print(f"  [reconcile] ERROR ticket_id={tid}: {exc}", flush=True)

        except Exception as exc:
            print(f"  [reconcile] ERROR ticket_id={tid}: {exc}", flush=True)

    print(f"[reconcile] Reconciled {len(reconciled)} ticket(s).", flush=True)
    return reconciled
