"""
run_ingest.py — Incremental TeamSupport ticket + action ingestion into Postgres.

Usage:
    python run_ingest.py sync                       # incremental sync (watermark-based; includes created-since)
    python run_ingest.py sync --ticket 29696        # resync one ticket by number
    python run_ingest.py sync --ticket 29696,110554 # resync multiple tickets
    python run_ingest.py sync --ticket-id 123456    # resync one ticket by internal ID
    python run_ingest.py sync --since 2026-03-01    # replay tickets modified since date
    python run_ingest.py sync --days 7              # replay last N days
    python run_ingest.py sync --all                 # full fetch (ignore MAX_TICKETS)
    python run_ingest.py sync --dry-run             # fetch & log but don't write to DB
    python run_ingest.py status                     # show sync state + recent runs

Requires DATABASE_URL to be set.  Does NOT replace the existing JSON pipeline
(run_pull_activities.py); this is the new DB-backed ingestion path.

Incremental sync logic:
    1. Read last_successful_sync_at from sync_state (the "watermark").
    2. Subtract SAFETY_BUFFER_MINUTES to compute from_ts.  The overlap is
       safe because all writes use ON CONFLICT … DO UPDATE (idempotent).
    3. Fetch all open tickets from TeamSupport, then post-filter locally to
       tickets with DateModified >= from_ts.  (The TS API does not support
       server-side date filtering, so a local post-filter is the safest
       practical approach.)
    4. Upsert each ticket and its actions into Postgres.
    5. Only advance last_successful_sync_at if the *entire* run succeeds.
       This means a failed or partial run will be safely replayed on the
       next invocation — no data loss, no duplicates.
"""

import argparse
import json
import os
import sys
import traceback
from datetime import datetime, timezone, timedelta

import config
import db
from ts_client import (
    fetch_open_tickets,
    fetch_tickets_created_since,
    fetch_ticket_by_id,
    fetch_all_activities,
    fetch_inhance_user_ids,
    ticket_id as extract_ticket_id,
)
from activity_cleaner import clean_activity_dict


# ── Tee-style logger (stdout + file) ─────────────────────────────────

class _TeeWriter:
    """Write to both the original stream and a log file."""

    def __init__(self, stream, log_file):
        self._stream = stream
        self._log_file = log_file

    def write(self, data):
        self._stream.write(data)
        self._log_file.write(data)
        self._log_file.flush()

    def flush(self):
        self._stream.flush()
        self._log_file.flush()

    # Delegate attribute lookups (e.g. fileno, isatty) to the real stream
    def __getattr__(self, name):
        return getattr(self._stream, name)


def _start_log_file():
    """Open a timestamped log file in OUTPUT_DIR and tee stdout+stderr to it."""
    os.makedirs(config.OUTPUT_DIR, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(config.OUTPUT_DIR, f"ingest_{ts}.log")
    log_fh = open(log_path, "w", encoding="utf-8")
    sys.stdout = _TeeWriter(sys.__stdout__, log_fh)
    sys.stderr = _TeeWriter(sys.__stderr__, log_fh)
    print(f"[ingest] Logging to {log_path}", flush=True)
    return log_fh


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
    ticket_ids: list[str] | None = None,
    since: datetime | None = None,
    max_tickets: int | None = None,
    dry_run: bool = False,
    verbose: bool = False,
    new_only: bool = False,
) -> dict:
    """Run one ingestion cycle.  Returns a summary dict.

    Sync modes (mutually exclusive):
    - *ticket_numbers*: resync specific tickets by TicketNumber.
    - *ticket_ids*: resync specific tickets by internal TicketID.
    - *since*: replay all tickets modified since a given timestamp.
    - *new_only*: only tickets *created* (not just modified) since the
      watermark or the explicit *since* date.
    - default: true incremental sync using the stored watermark.
      Also fetches tickets *created* since the watermark (open or closed)
      so that tickets opened and closed between syncs are captured.

    Watermark semantics:
    - Only a normal incremental sync (no --ticket/--ticket-id/--since/--days/--new-only)
      advances last_successful_sync_at on success.
    - Single-ticket, replay, and new-only syncs do NOT advance the watermark
      because they cover a subset of data and must not imply "everything is
      caught up".
    """
    now = datetime.now(timezone.utc)

    # Determine whether this is a "targeted" (single-ticket / replay) sync.
    # Targeted syncs must NOT advance the global watermark.
    is_targeted = bool(ticket_numbers or ticket_ids or since or new_only)

    # ── Resolve the effective from_ts for incremental filtering ──
    # When no explicit filter is provided, read the watermark from sync_state
    # and apply the safety buffer.
    effective_since = since

    # new_only mode: read the watermark if no explicit --since was given
    if new_only and not since and not dry_run:
        state = db.get_sync_state(SOURCE_NAME)
        watermark = state["last_successful_sync_at"] if state else None
        if watermark:
            effective_since = watermark - timedelta(minutes=config.SAFETY_BUFFER_MINUTES)
            print(
                f"[ingest] New-only mode: watermark {watermark.isoformat()}, "
                f"safety buffer {config.SAFETY_BUFFER_MINUTES}min → "
                f"tickets created since {effective_since.isoformat()}",
                flush=True,
            )
        else:
            print("[ingest] New-only mode: no prior watermark — will fetch all open tickets.", flush=True)

    if not is_targeted and not dry_run:
        state = db.get_sync_state(SOURCE_NAME)
        watermark = state["last_successful_sync_at"] if state else None
        if watermark:
            # Subtract the safety buffer so we re-process a small overlap
            # window.  This guards against clock skew between TeamSupport
            # and our DB, as well as in-flight writes that completed after
            # the watermark was recorded.  It is safe because all upserts
            # use ON CONFLICT … DO UPDATE (idempotent).
            effective_since = watermark - timedelta(minutes=config.SAFETY_BUFFER_MINUTES)
            print(
                f"[ingest] Watermark: {watermark.isoformat()}, "
                f"safety buffer: {config.SAFETY_BUFFER_MINUTES}min → "
                f"fetching tickets modified since {effective_since.isoformat()}",
                flush=True,
            )
        elif config.INITIAL_BACKFILL_DAYS > 0:
            effective_since = now - timedelta(days=config.INITIAL_BACKFILL_DAYS)
            print(
                f"[ingest] No prior watermark — backfilling last "
                f"{config.INITIAL_BACKFILL_DAYS} day(s) "
                f"(since {effective_since.isoformat()}).",
                flush=True,
            )
        else:
            print("[ingest] No prior watermark — full backfill of open tickets.", flush=True)

    # ── Config snapshot
    cfg_snap = {
        "ticket_numbers": ticket_numbers,
        "ticket_ids": ticket_ids,
        "since": effective_since.isoformat() if effective_since else None,
        "max_tickets": max_tickets,
        "dry_run": dry_run,
        "new_only": new_only,
        "is_targeted": is_targeted,
        "safety_buffer_minutes": config.SAFETY_BUFFER_MINUTES,
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
        if ticket_ids:
            # Fetch each ticket individually by internal ID
            raw_tickets = []
            for tid_str in ticket_ids:
                raw_tickets.extend(fetch_ticket_by_id(tid_str))
        elif ticket_numbers:
            raw_tickets = fetch_open_tickets(ticket_numbers=ticket_numbers)
        elif new_only and effective_since:
            # Server-side DateCreated filter — returns open + closed tickets
            # created after the cutoff.  No local post-filter needed.
            raw_tickets = fetch_tickets_created_since(effective_since)
        elif new_only:
            # No cutoff date — fall back to all open tickets
            raw_tickets = fetch_open_tickets()
        else:
            raw_tickets = fetch_open_tickets()

            # Also fetch tickets *created* since the watermark so that
            # tickets opened and closed between syncs are captured.
            # Deduplicate by TicketID — upserts are idempotent, but this
            # avoids fetching actions twice for the same ticket.
            if effective_since:
                created_tickets = fetch_tickets_created_since(effective_since)
                if created_tickets:
                    seen_ids = {str(t.get("TicketID") or "") for t in raw_tickets}
                    new_count = 0
                    for ct in created_tickets:
                        ct_id = str(ct.get("TicketID") or "")
                        if ct_id and ct_id not in seen_ids:
                            raw_tickets.append(ct)
                            seen_ids.add(ct_id)
                            new_count += 1
                    if new_count:
                        print(
                            f"[ingest] Merged {new_count} created-since ticket(s) "
                            f"(opened+closed between syncs).",
                            flush=True,
                        )

        # ── Filter by effective_since (local post-filter).
        # The TeamSupport API supports server-side date filtering using the
        # format YYYYMMDDHHMMSS on any date field.  For new_only mode, the
        # server already filtered by DateCreated so we skip the local filter.
        # For normal sync we still post-filter by DateModified locally.
        if effective_since and not new_only:
            before_count = len(raw_tickets)
            filter_field = "DateCreated" if new_only else "DateModified"
            filter_label = "created" if new_only else "modified"
            raw_tickets = [
                t for t in raw_tickets
                if _parse_ts_datetime(str(t.get(filter_field) or "")) is not None
                and _parse_ts_datetime(str(t.get(filter_field) or "")) >= effective_since
            ]
            print(
                f"[ingest] Filtered {before_count} → {len(raw_tickets)} ticket(s) "
                f"{filter_label} since {effective_since.isoformat()}.",
                flush=True,
            )

        # ── Apply max_tickets limit
        effective_max = max_tickets if max_tickets is not None else config.MAX_TICKETS
        if effective_max and not ticket_numbers and not ticket_ids:
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
                    action_row["ticket_number"] = tnum
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
        # Only advance last_successful_sync_at when the run fully succeeds
        # AND this is a normal incremental sync (not a targeted resync or
        # replay).  Advancing the watermark on a targeted sync would falsely
        # signal that all tickets are up to date.
        advance_watermark = (status == "completed") and not is_targeted
        db.upsert_sync_state(
            SOURCE_NAME,
            status=status,
            error=error_text,
            is_success=advance_watermark,
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


# ── Closed-ticket reconciliation ─────────────────────────────────────

def _reconcile_closed(upserted_ids: list[int], verbose: bool = False) -> list[int]:
    """Re-fetch tickets the DB thinks are open but were NOT returned by TS.

    After a full sync that fetched all open tickets, any ticket still marked
    open in the DB but absent from the sync results has likely been closed in
    TeamSupport.  This function re-fetches each one individually and upserts
    the current state so that closed_at / status are updated.

    Returns the list of ticket_ids that were reconciled.
    """
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

            ticket_raw = raw_tickets[0]
            ticket_row = _extract_ticket_row(ticket_raw, now)
            tnum = ticket_row.get("ticket_number") or "?"

            # Fetch + clean actions
            raw_actions = fetch_all_activities(str(tid))
            action_rows = []
            for action_raw in raw_actions:
                cleaned = clean_activity_dict(action_raw)
                if not cleaned.get("action_id"):
                    continue
                action_row = _extract_action_row(action_raw, tid, cleaned)
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
        "--ticket-id",
        help="Comma-delimited internal ticket ID(s) to resync.",
    )
    p_sync.add_argument(
        "--since", "-s",
        help="Only process tickets modified since this date (YYYY-MM-DD).",
    )
    p_sync.add_argument(
        "--days", "-d",
        type=int,
        help="Replay the last N days (shorthand for --since).",
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
    p_sync.add_argument(
        "--no-reconcile", action="store_true",
        help="Skip closed-ticket reconciliation after sync.",
    )
    p_sync.add_argument(
        "--new-only", action="store_true",
        help="Only sync tickets created (not just modified) since the watermark or --since date.",
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
    log_fh = _start_log_file()

    if not db._is_enabled():
        print("ERROR: DATABASE_URL is not set.", file=sys.stderr)
        sys.exit(1)

    # Ensure schema and tables exist before syncing
    applied = db.migrate()
    if applied:
        from run_rollups import run_full_rollups
        run_full_rollups()

    # Parse --ticket (CLI takes precedence over config.TARGET_TICKETS)
    ticket_numbers = None
    if args.ticket:
        ticket_numbers = [t.strip() for t in args.ticket.split(",") if t.strip()]
    elif config.TARGET_TICKETS:
        ticket_numbers = config.TARGET_TICKETS
        print(f"[ingest] Using TARGET_TICKET from config: {', '.join(ticket_numbers)}", flush=True)

    # Parse --ticket-id
    ticket_ids = None
    if args.ticket_id:
        ticket_ids = [t.strip() for t in args.ticket_id.split(",") if t.strip()]

    # Parse --since / --days
    since = None
    if args.since and args.days:
        print("ERROR: --since and --days are mutually exclusive.", file=sys.stderr)
        sys.exit(1)
    if args.since:
        try:
            since = datetime.strptime(args.since, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            print(f"ERROR: Invalid --since date: {args.since!r}  (expected YYYY-MM-DD)", file=sys.stderr)
            sys.exit(1)
    elif args.days:
        since = datetime.now(timezone.utc) - timedelta(days=args.days)
        print(f"[ingest] Replay mode: last {args.days} day(s) (since {since.isoformat()}).", flush=True)

    # Max tickets
    max_tickets = 0 if args.all else None  # 0 = unlimited; None = use config default

    result = _sync(
        ticket_numbers=ticket_numbers,
        ticket_ids=ticket_ids,
        since=since,
        max_tickets=max_tickets,
        dry_run=args.dry_run,
        verbose=args.verbose,
        new_only=args.new_only,
    )

    # ── Post-sync: reconcile closed tickets ──
    # Only safe when we fetched ALL open tickets (not a targeted/partial sync).
    is_full_sync = not (args.ticket or args.ticket_id or args.since or args.days or args.new_only)
    effective_max = 0 if args.all else config.MAX_TICKETS
    can_reconcile = (
        is_full_sync
        and not effective_max          # 0 = unlimited
        and not args.dry_run
        and not args.no_reconcile
        and result["status"] == "completed"
    )

    reconciled_ids = []
    if can_reconcile:
        reconciled_ids = _reconcile_closed(
            result.get("upserted_ids", []),
            verbose=args.verbose,
        )

    # ── Post-sync: rebuild rollups + analytics for touched tickets ──
    if not args.dry_run and result["status"] == "completed":
        all_touched = result.get("upserted_ids", []) + reconciled_ids
        if all_touched:
            from run_rollups import (
                classify_actions, rebuild_rollups, rebuild_metrics,
                run_analytics_for_tickets,
            )
            print(
                f"\n[ingest] Post-sync: rebuilding rollups + analytics "
                f"for {len(all_touched)} ticket(s)\u2026",
                flush=True,
            )
            classify_actions(all_touched)
            rebuild_rollups(all_touched)
            rebuild_metrics(all_touched)
            run_analytics_for_tickets(all_touched)

    if result["status"] != "completed":
        log_fh.close()
        sys.exit(1)

    log_fh.close()


if __name__ == "__main__":
    main()
