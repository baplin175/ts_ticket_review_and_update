"""
run_ingest.py — Incremental TeamSupport ticket + action ingestion into Postgres.

Usage:
    python run_ingest.py sync                       # incremental sync (open tickets)
    python run_ingest.py sync --ticket 29696        # resync one ticket by number
    python run_ingest.py sync --ticket 29696,110554 # resync multiple tickets
    python run_ingest.py sync --since 2026-03-01    # replay tickets modified since date
    python run_ingest.py sync --all                 # full fetch (ignore MAX_TICKETS)
    python run_ingest.py sync --dry-run             # fetch & log but don't write to DB
    python run_ingest.py status                     # show sync state + recent runs

Requires DATABASE_URL to be set.  Does NOT replace the existing JSON pipeline
(run_pull_activities.py); this is the new DB-backed ingestion path.
"""

import argparse
import json
import sys
import traceback
from datetime import datetime, timezone, timedelta

import config
import db
from ts_client import (
    fetch_open_tickets,
    fetch_all_activities,
    fetch_inhance_user_ids,
    ticket_id as extract_ticket_id,
)
from activity_cleaner import clean_activity_dict


# ── Datetime parsing (duplicated from run_pull_activities to avoid coupling) ─

def _parse_ts_datetime(value):
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


# ── Ticket metadata extraction ───────────────────────────────────────

def _extract_ticket_row(ticket_raw: dict, now: datetime) -> dict:
    """Build a dict suitable for db.upsert_ticket from a raw TS ticket dict."""
    tid = extract_ticket_id(ticket_raw)
    ticket_number = str(ticket_raw.get("TicketNumber") or "").strip()
    ticket_name = str(ticket_raw.get("Name") or ticket_raw.get("TicketName") or "").strip()

    date_created_str = str(ticket_raw.get("DateCreated") or "").strip()
    date_modified_str = str(ticket_raw.get("DateModified") or "").strip()
    date_created = _parse_ts_datetime(date_created_str)
    date_modified = _parse_ts_datetime(date_modified_str)

    closed_at_str = str(ticket_raw.get("DateClosed") or "").strip()
    closed_at = _parse_ts_datetime(closed_at_str)

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
    product_name = str(
        ticket_raw.get("ProductName") or ticket_raw.get("Product") or ""
    ).strip() or None
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


def _extract_action_row(action_raw: dict, tid: int, cleaned: dict) -> dict:
    """Build a dict suitable for db.upsert_action from raw + cleaned action dicts."""
    action_id_str = cleaned.get("action_id") or ""
    action_id = int(action_id_str) if action_id_str else None

    created_at = _parse_ts_datetime(cleaned.get("created_at"))

    raw_desc = action_raw.get("Description") or action_raw.get("Text") or ""
    cleaned_desc = cleaned.get("description") or ""
    is_empty = not cleaned_desc.strip()

    return {
        "action_id": action_id,
        "ticket_id": tid,
        "created_at": created_at,
        "action_type": cleaned.get("action_type") or None,
        "creator_id": cleaned.get("creator_id") or None,
        "creator_name": cleaned.get("creator_name") or None,
        "party": cleaned.get("party") or None,
        "is_visible": cleaned.get("is_visible"),
        "description": raw_desc or None,
        "cleaned_description": cleaned_desc or None,
        "action_class": None,  # reserved for future classification
        "is_empty": is_empty,
        "is_customer_visible": cleaned.get("is_visible"),
        "source_payload": action_raw,
    }


# ── Core ingestion ───────────────────────────────────────────────────

SOURCE_NAME = "teamsupport"


def _sync(
    ticket_numbers: list[str] | None = None,
    since: datetime | None = None,
    max_tickets: int | None = None,
    dry_run: bool = False,
    verbose: bool = False,
) -> dict:
    """Run one ingestion cycle.  Returns a summary dict."""
    now = datetime.now(timezone.utc)

    # ── Config snapshot
    cfg_snap = {
        "ticket_numbers": ticket_numbers,
        "since": since.isoformat() if since else None,
        "max_tickets": max_tickets,
        "dry_run": dry_run,
        "ts_base": config.TS_BASE,
    }

    # ── Create ingest run
    run_id = None
    if not dry_run:
        run_id = db.create_ingest_run(SOURCE_NAME, config_snapshot=cfg_snap)
        db.upsert_sync_state(SOURCE_NAME, status="running")

    tickets_seen = 0
    tickets_upserted = 0
    actions_seen = 0
    actions_upserted = 0
    upserted_ids: list[int] = []
    errors: list[str] = []

    try:
        # ── Ensure inHANCE user IDs are cached (needed by clean_activity_dict)
        fetch_inhance_user_ids()

        # ── Fetch tickets
        print("[ingest] Fetching tickets …", flush=True)
        if ticket_numbers:
            raw_tickets = fetch_open_tickets(ticket_numbers=ticket_numbers)
        else:
            raw_tickets = fetch_open_tickets()

        # ── Filter by --since (local post-filter; TS API doesn't support date filters)
        if since:
            before_count = len(raw_tickets)
            raw_tickets = [
                t for t in raw_tickets
                if _parse_ts_datetime(str(t.get("DateModified") or "")) is not None
                and _parse_ts_datetime(str(t.get("DateModified") or "")) >= since
            ]
            print(
                f"[ingest] Filtered {before_count} → {len(raw_tickets)} ticket(s) "
                f"modified since {since.date()}.",
                flush=True,
            )

        # ── Apply max_tickets limit
        effective_max = max_tickets if max_tickets is not None else config.MAX_TICKETS
        if effective_max and not ticket_numbers:
            raw_tickets = raw_tickets[:effective_max]

        tickets_seen = len(raw_tickets)
        print(f"[ingest] Processing {tickets_seen} ticket(s) …", flush=True)

        # ── Process each ticket
        for idx, ticket_raw in enumerate(raw_tickets, 1):
            tid_str = extract_ticket_id(ticket_raw)
            tnum = str(ticket_raw.get("TicketNumber") or "?")

            if not tid_str:
                msg = f"Ticket at index {idx} has no ID — skipping."
                print(f"[ingest] WARNING: {msg}", flush=True)
                errors.append(msg)
                continue

            tid = int(tid_str)

            try:
                # ── Upsert ticket
                ticket_row = _extract_ticket_row(ticket_raw, now)
                if verbose:
                    print(f"[ingest] [{idx}/{tickets_seen}] Ticket #{tnum} (id={tid})", flush=True)

                # ── Fetch + clean actions
                raw_actions = fetch_all_activities(tid_str)
                actions_seen += len(raw_actions)

                action_rows = []
                for action_raw in raw_actions:
                    cleaned = clean_activity_dict(action_raw)
                    aid_str = cleaned.get("action_id", "")
                    if not aid_str:
                        continue

                    action_row = _extract_action_row(action_raw, tid, cleaned)
                    if not dry_run:
                        action_rows.append(action_row)
                    elif verbose:
                        print(
                            f"  [dry-run] action {aid_str}: party={cleaned.get('party')}, "
                            f"visible={cleaned.get('is_visible')}",
                            flush=True,
                        )

                # ── Batch upsert ticket + actions in a single transaction
                if not dry_run:
                    db.upsert_ticket_with_actions(ticket_row, action_rows, now=now)
                    tickets_upserted += 1
                    actions_upserted += len(action_rows)
                    upserted_ids.append(tid)

            except Exception as exc:
                msg = f"Error processing ticket #{tnum} (id={tid_str}): {exc}"
                print(f"[ingest] ERROR: {msg}", flush=True)
                if verbose:
                    traceback.print_exc()
                errors.append(msg)

        # ── Success
        status = "completed"
        print(
            f"[ingest] Done — tickets: {tickets_upserted}/{tickets_seen} upserted, "
            f"actions: {actions_upserted}/{actions_seen} upserted.",
            flush=True,
        )

    except Exception as exc:
        status = "failed"
        msg = f"Ingest run failed: {exc}"
        print(f"[ingest] FATAL: {msg}", flush=True)
        traceback.print_exc()
        errors.append(msg)

    # ── Record run outcome
    error_text = ("\n".join(errors)) if errors else None

    if not dry_run and run_id:
        db.complete_ingest_run(
            run_id,
            status=status,
            tickets_seen=tickets_seen,
            tickets_upserted=tickets_upserted,
            actions_seen=actions_seen,
            actions_upserted=actions_upserted,
            error_text=error_text,
        )
        db.upsert_sync_state(
            SOURCE_NAME,
            status=status,
            error=error_text,
            is_success=(status == "completed"),
        )

    return {
        "status": status,
        "tickets_seen": tickets_seen,
        "tickets_upserted": tickets_upserted,
        "actions_seen": actions_seen,
        "actions_upserted": actions_upserted,
        "upserted_ids": upserted_ids,
        "errors": errors,
    }


# ── Status display ───────────────────────────────────────────────────

def _show_status():
    """Print sync_state and recent ingest_runs."""
    if not db._is_enabled():
        print("DATABASE_URL is not set.", file=sys.stderr)
        sys.exit(1)

    # Sync state
    row = db.fetch_one("SELECT * FROM sync_state WHERE source_name = %s;", (SOURCE_NAME,))
    print("=== Sync State ===")
    if row:
        cols = (
            "source_name", "last_successful_sync_at", "last_attempted_sync_at",
            "last_status", "last_error", "last_cursor", "updated_at",
        )
        for col, val in zip(cols, row):
            print(f"  {col}: {val}")
    else:
        print("  (no sync state recorded yet)")

    # Recent ingest runs
    runs = db.fetch_all(
        "SELECT ingest_run_id, started_at, completed_at, status, "
        "tickets_seen, tickets_upserted, actions_seen, actions_upserted, error_text "
        "FROM ingest_runs WHERE source_name = %s "
        "ORDER BY started_at DESC LIMIT 10;",
        (SOURCE_NAME,),
    )
    print(f"\n=== Recent Ingest Runs ({len(runs)}) ===")
    for r in runs:
        rid, started, completed, st, ts, tu, acs, au, err = r
        dur = ""
        if started and completed:
            dur = f" ({(completed - started).total_seconds():.1f}s)"
        print(
            f"  {started}  {st}{dur}  "
            f"tickets={tu}/{ts}  actions={au}/{acs}"
            f"{'  error=' + (err[:80] if err else '') if err else ''}"
        )
    if not runs:
        print("  (no runs recorded yet)")


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Incremental TeamSupport ingestion into Postgres.",
    )
    sub = parser.add_subparsers(dest="command")

    # sync
    p_sync = sub.add_parser("sync", help="Run an ingestion cycle.")
    p_sync.add_argument(
        "--ticket", "-t",
        help="Comma-delimited ticket number(s) to resync.",
    )
    p_sync.add_argument(
        "--since", "-s",
        help="Only process tickets modified since this date (YYYY-MM-DD).",
    )
    p_sync.add_argument(
        "--all", action="store_true",
        help="Fetch all open tickets (ignore MAX_TICKETS).",
    )
    p_sync.add_argument(
        "--dry-run", action="store_true",
        help="Fetch and log but do not write to the database.",
    )
    p_sync.add_argument(
        "--verbose", "-v", action="store_true",
        help="Print per-ticket/action detail.",
    )

    # status
    sub.add_parser("status", help="Show sync state and recent ingest runs.")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if args.command == "status":
        _show_status()
        return

    # ── sync command ──
    if not db._is_enabled():
        print("ERROR: DATABASE_URL is not set.", file=sys.stderr)
        sys.exit(1)

    # Ensure schema and tables exist before syncing
    db.migrate()

    # Parse --ticket
    ticket_numbers = None
    if args.ticket:
        ticket_numbers = [t.strip() for t in args.ticket.split(",") if t.strip()]

    # Parse --since
    since = None
    if args.since:
        try:
            since = datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            print(f"ERROR: Invalid --since date: {args.since!r}  (expected YYYY-MM-DD)", file=sys.stderr)
            sys.exit(1)

    # Max tickets
    max_tickets = 0 if args.all else None  # 0 = unlimited; None = use config default

    result = _sync(
        ticket_numbers=ticket_numbers,
        since=since,
        max_tickets=max_tickets,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )

    # ── Post-sync: rebuild rollups + analytics for touched tickets ──
    if not args.dry_run and result["status"] == "completed":
        upserted = result.get("upserted_ids", [])
        if upserted:
            from run_rollups import (
                classify_actions, rebuild_rollups, rebuild_metrics,
                run_analytics_for_tickets,
            )
            print(
                f"\n[ingest] Post-sync: rebuilding rollups + analytics "
                f"for {len(upserted)} ticket(s)\u2026",
                flush=True,
            )
            classify_actions(upserted)
            rebuild_rollups(upserted)
            rebuild_metrics(upserted)
            run_analytics_for_tickets(upserted)

    if result["status"] != "completed":
        sys.exit(1)


if __name__ == "__main__":
    main()
