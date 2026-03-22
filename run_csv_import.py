"""
Bulk-import Activities.csv into the Postgres database.

Reads a TeamSupport CSV export (one row per action), groups by ticket,
generates synthetic deterministic action IDs (the CSV export has no
Action ID column), cleans descriptions, and upserts into the tickets
and ticket_actions tables.

Usage:
    python run_csv_import.py                     # import all rows
    python run_csv_import.py --ticket 109683     # import specific tickets
    python run_csv_import.py --dry-run            # preview without writing
    python run_csv_import.py --verbose            # show per-ticket detail
"""

import argparse
import csv
import hashlib
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

from activity_cleaner import clean_activity
import db

CSV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "Activities.csv")
SOURCE_NAME = "csv_import"


def _log(msg: str) -> None:
    print(msg, flush=True)


# ── Synthetic action ID generation ───────────────────────────────────

def _synthetic_action_id(ticket_id: str, date_created: str, description_prefix: str) -> int:
    """Generate a deterministic 63-bit positive integer from key fields.

    Uses SHA-256 of (ticket_id | date_created | first 200 chars of description)
    and takes the low 63 bits to stay within BIGINT range.
    Re-importing the same CSV will produce identical IDs → idempotent.
    """
    payload = f"{ticket_id}|{date_created}|{description_prefix[:200]}"
    h = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return int(h[:15], 16)  # 60-bit positive integer, well within BIGINT


# ── Party detection ──────────────────────────────────────────────────

def _load_known_inh_names() -> set:
    """Build the set of inHANCE employee names.

    Sources (in priority order):
    1. TeamSupport API — /Users?Organization=inHANCE (most authoritative)
    2. DB — distinct creator_name where party='inh'
    3. Prior activities JSON files
    """
    import json
    names: set = set()

    # 1. Try TS API first (single call, gives the canonical list)
    try:
        from ts_client import ts_get, TS_BASE
        data = ts_get(f"{TS_BASE}/Users", params={"Organization": "inHANCE"})
        users = data if isinstance(data, list) else data.get("Users", data.get("users", []))
        if isinstance(users, dict):
            users = users.get("User", users.get("user", []))
        if not isinstance(users, list):
            users = [users]
        for u in users:
            fn = (u.get("FirstName") or "").strip()
            ln = (u.get("LastName") or "").strip()
            name = f"{fn} {ln}".strip() if fn or ln else ""
            if name:
                names.add(name)
            for k in ("Name", "DisplayName"):
                v = (u.get(k) or "").strip()
                if v:
                    names.add(v)
        if names:
            _log(f"[csv-import] Loaded {len(names)} inHANCE name(s) from TS API.")
            return names
    except Exception as exc:
        _log(f"[csv-import] TS API user fetch failed ({exc}), falling back to local sources.")

    # 2. Pull from DB if available
    if db._is_enabled():
        try:
            rows = db.fetch_all(
                "SELECT DISTINCT creator_name FROM ticket_actions WHERE party = 'inh' AND creator_name IS NOT NULL;"
            )
            for r in rows:
                if r[0]:
                    names.add(r[0])
        except Exception:
            pass

    # 3. Scan prior activities JSON files
    out = Path(os.path.join(os.path.dirname(os.path.abspath(__file__)), "output"))
    for f in sorted(out.glob("activities_*.json"), reverse=True):
        try:
            data = json.loads(f.read_text(encoding="utf-8"))
            for t in data:
                for a in t.get("activities", []):
                    if a.get("party") == "inh" and a.get("creator_name"):
                        names.add(a["creator_name"])
            if names:
                break
        except Exception:
            continue

    return names


# ── Date parsing ─────────────────────────────────────────────────────

def _parse_ts_date(raw: str) -> Optional[datetime]:
    """Parse common TeamSupport CSV date formats into a tz-aware datetime."""
    if not raw or not raw.strip():
        return None
    raw = raw.strip()
    for fmt in (
        "%m/%d/%Y %I:%M %p",   # 3/13/2026 2:30 PM
        "%m/%d/%Y %H:%M:%S",   # 3/13/2026 14:30:00
        "%m/%d/%Y %H:%M",      # 3/13/2026 14:30
        "%m/%d/%Y",             # 3/13/2026
        "%Y-%m-%dT%H:%M:%S",   # ISO-ish
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


# ── Core import logic ────────────────────────────────────────────────

def run_import(
    csv_path: str = CSV_PATH,
    *,
    ticket_filter: Optional[list[str]] = None,
    dry_run: bool = False,
    verbose: bool = False,
) -> dict:
    """Read the CSV and upsert into DB.  Returns summary stats."""

    if not os.path.exists(csv_path):
        _log(f"[csv-import] File not found: {csv_path}")
        sys.exit(1)

    if not db._is_enabled():
        _log("[csv-import] DATABASE_URL not set — cannot import to DB.")
        sys.exit(1)

    # Ensure schema/tables exist
    applied = db.migrate()
    if applied:
        from run_rollups import run_full_rollups
        run_full_rollups()

    inh_names = _load_known_inh_names()
    if inh_names:
        _log(f"[csv-import] Loaded {len(inh_names)} known inHANCE name(s) for party detection.")
    else:
        _log("[csv-import] No known inHANCE names found — party will be 'unknown'.")

    # Build name→user_id mapping from TS API
    name_to_id: dict[str, str] = {}
    try:
        from ts_client import fetch_all_users
        name_to_id = fetch_all_users()
    except Exception as exc:
        _log(f"[csv-import] Could not fetch user ID mapping: {exc}")

    ticket_set = set(ticket_filter) if ticket_filter else None

    # Track ingest run
    run_id = None
    if not dry_run:
        run_id = db.create_ingest_run(SOURCE_NAME, {
            "csv_path": csv_path,
            "ticket_filter": ticket_filter,
        })

    stats = {
        "rows_read": 0,
        "rows_skipped": 0,
        "tickets_seen": 0,
        "tickets_upserted": 0,
        "actions_seen": 0,
        "actions_upserted": 0,
    }
    tickets_upserted: set = set()
    seen_action_ids: set = set()

    try:
        # Pre-scan: determine CSV export date from the latest ticket creation date.
        # Days Closed values are relative to this date.
        csv_export_date = None
        with open(csv_path, "r", encoding="utf-8") as f:
            for pre_row in csv.DictReader(f):
                dtc = (pre_row.get("Date Ticket Created") or "").strip()
                if dtc:
                    dt = _parse_ts_date(dtc)
                    if dt and (csv_export_date is None or dt > csv_export_date):
                        csv_export_date = dt
        if csv_export_date is None:
            csv_export_date = datetime.now(timezone.utc)
        # Use just the date portion (closed_at is a date, not datetime)
        if hasattr(csv_export_date, 'date'):
            csv_export_date = csv_export_date.date()
        _log(f"[csv-import] Inferred CSV export date: {csv_export_date}")

        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            for row in reader:
                stats["rows_read"] += 1

                # ── Extract ticket-level fields ──────────────────────
                raw_ticket_id = (row.get("Ticket ID") or "").strip()
                ticket_number = (row.get("Ticket Number") or "").strip()

                if not raw_ticket_id:
                    stats["rows_skipped"] += 1
                    continue

                try:
                    ticket_id = int(raw_ticket_id)
                except ValueError:
                    stats["rows_skipped"] += 1
                    continue

                # Apply ticket filter (by ticket number OR ticket id)
                if ticket_set:
                    if ticket_number not in ticket_set and raw_ticket_id not in ticket_set:
                        stats["rows_skipped"] += 1
                        continue

                # ── Upsert ticket (once per ticket_id) ───────────────
                if ticket_id not in tickets_upserted:
                    ticket_name = (row.get("Ticket Name") or "").strip()
                    product_name = (row.get("Ticket Product Name") or "").strip()
                    assignee = (row.get("Assigned To") or "").strip()
                    customer = (row.get("Primary Customer") or "").strip()
                    severity = (row.get("Severity") or "").strip()
                    group_name = (row.get("Group Name") or "").strip()
                    ticket_source = (row.get("Ticket Source") or "").strip()
                    ticket_type = (row.get("Ticket Type") or "").strip()
                    is_closed_raw = (row.get("Is Closed") or "").strip().lower()
                    date_created_raw = (row.get("Date Ticket Created") or "").strip()
                    days_closed_raw = (row.get("Days Closed") or "").strip()
                    days_opened_raw = (row.get("Days Opened") or "").strip()
                    days_since_mod_raw = (row.get("Days Since Ticket was Last Modified") or "").strip()

                    date_created = _parse_ts_date(date_created_raw)

                    # Derive closed_at from Days Closed (relative to CSV export date)
                    closed_at = None
                    if is_closed_raw in ("true", "yes", "1") and days_closed_raw:
                        try:
                            days_closed_int = int(days_closed_raw)
                            closed_at = csv_export_date - timedelta(days=days_closed_int)
                        except (ValueError, TypeError):
                            pass

                    # Derive status from Is Closed flag
                    status = None
                    if is_closed_raw in ("true", "yes", "1"):
                        status = "Closed"
                    elif is_closed_raw in ("false", "no", "0"):
                        status = "Open"

                    days_opened = None
                    if days_opened_raw:
                        try:
                            days_opened = float(days_opened_raw)
                        except ValueError:
                            pass

                    days_since_mod = None
                    if days_since_mod_raw:
                        try:
                            days_since_mod = float(days_since_mod_raw)
                        except ValueError:
                            pass

                    ticket_dict = {
                        "ticket_id": ticket_id,
                        "ticket_number": ticket_number or None,
                        "ticket_name": ticket_name or None,
                        "status": status,
                        "severity": severity or None,
                        "product_name": product_name or None,
                        "assignee": assignee or None,
                        "group_name": group_name or None,
                        "customer": customer or None,
                        "date_created": date_created,
                        "closed_at": closed_at,
                        "days_opened": days_opened,
                        "days_since_modified": days_since_mod,
                        "source_payload": {
                            "csv_source": True,
                            "ticket_source": ticket_source or None,
                            "ticket_type": ticket_type or None,
                        },
                    }

                    if not dry_run:
                        db.upsert_ticket(ticket_dict)
                    tickets_upserted.add(ticket_id)
                    stats["tickets_upserted"] += 1

                    if verbose:
                        _log(f"  [ticket] #{ticket_number} (id={ticket_id}) — {ticket_name[:60]}")

                # ── Build and upsert action ──────────────────────────
                stats["actions_seen"] += 1

                creator_name = (row.get("Action Creator Name") or "").strip()
                raw_desc = row.get("Action Description") or ""
                action_type = (row.get("Action Type") or "").strip()
                date_action_raw = (row.get("Date Action Created") or "").strip()
                hours_spent_raw = (row.get("Action Hours Spent") or "").strip()
                action_source = (row.get("Action Source") or "").strip()

                action_created = _parse_ts_date(date_action_raw)

                # Party detection + creator_id lookup
                creator_id = name_to_id.get(creator_name)
                if inh_names:
                    party = "inh" if creator_name in inh_names else "cust"
                else:
                    party = "unknown"

                # Clean description
                is_html = bool(re.search(r"<[a-zA-Z][^>]*>", raw_desc))
                cleaned = clean_activity(raw_desc, is_html=is_html)
                is_empty = not cleaned.strip()

                # Synthetic action_id (deterministic)
                action_id = _synthetic_action_id(raw_ticket_id, date_action_raw, raw_desc)

                # Handle collisions: if we've already seen this synthetic ID
                # in this import, add a counter suffix
                base_id = action_id
                collision_n = 0
                while action_id in seen_action_ids:
                    collision_n += 1
                    payload = f"{raw_ticket_id}|{date_action_raw}|{raw_desc[:200]}|{collision_n}"
                    h = hashlib.sha256(payload.encode("utf-8")).hexdigest()
                    action_id = int(h[:15], 16)
                seen_action_ids.add(action_id)

                action_dict = {
                    "action_id": action_id,
                    "ticket_id": ticket_id,
                    "ticket_number": ticket_number,
                    "created_at": action_created,
                    "action_type": action_type or None,
                    "creator_id": creator_id,
                    "creator_name": creator_name or None,
                    "party": party,
                    "is_visible": True,
                    "description": raw_desc,
                    "cleaned_description": cleaned,
                    "is_empty": is_empty,
                    "source_payload": {
                        "csv_source": True,
                        "synthetic_action_id": True,
                        "hours_spent": hours_spent_raw or None,
                        "action_source": action_source or None,
                    },
                }

                if not dry_run:
                    db.upsert_action(action_dict)
                stats["actions_upserted"] += 1

                # Progress logging every 5000 rows
                if stats["rows_read"] % 5000 == 0:
                    _log(f"[csv-import] … {stats['rows_read']:,} rows processed "
                         f"({stats['tickets_upserted']:,} tickets, {stats['actions_upserted']:,} actions)")

        stats["tickets_seen"] = len(tickets_upserted)
        stats["upserted_ids"] = sorted(tickets_upserted)

        # Complete the ingest run
        if run_id and not dry_run:
            db.complete_ingest_run(
                run_id,
                status="completed",
                tickets_seen=stats["tickets_seen"],
                tickets_upserted=stats["tickets_upserted"],
                actions_seen=stats["actions_seen"],
                actions_upserted=stats["actions_upserted"],
            )

    except Exception as exc:
        if run_id and not dry_run:
            db.complete_ingest_run(
                run_id,
                status="failed",
                tickets_seen=stats.get("tickets_seen", 0),
                tickets_upserted=stats.get("tickets_upserted", 0),
                actions_seen=stats.get("actions_seen", 0),
                actions_upserted=stats.get("actions_upserted", 0),
                error_text=str(exc),
            )
        raise

    return stats


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Bulk-import Activities.csv into the Postgres database."
    )
    parser.add_argument(
        "--csv", default=CSV_PATH,
        help="Path to the CSV file (default: Activities.csv in project root)."
    )
    parser.add_argument(
        "--ticket", dest="tickets", action="append", default=[],
        help="Import only these ticket numbers (repeatable). Accepts comma-separated values."
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Parse and validate without writing to the database."
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Print per-ticket detail during import."
    )
    args = parser.parse_args()

    # Flatten comma-delimited --ticket args
    ticket_filter = []
    for t in args.tickets:
        ticket_filter.extend(s.strip() for s in t.split(",") if s.strip())

    mode = "DRY-RUN" if args.dry_run else "LIVE"
    _log(f"[csv-import] Starting {mode} import from {args.csv}")
    if ticket_filter:
        _log(f"[csv-import] Filtering to ticket(s): {', '.join(ticket_filter)}")

    stats = run_import(
        args.csv,
        ticket_filter=ticket_filter or None,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )

    _log(f"\n[csv-import] {'DRY-RUN ' if args.dry_run else ''}Complete.")
    _log(f"  Rows read:         {stats['rows_read']:,}")
    _log(f"  Rows skipped:      {stats['rows_skipped']:,}")
    _log(f"  Tickets upserted:  {stats['tickets_upserted']:,}")
    _log(f"  Actions upserted:  {stats['actions_upserted']:,}")

    # ── Post-import: rebuild rollups + analytics for touched tickets ──
    if not args.dry_run:
        upserted = stats.get("upserted_ids", [])
        if upserted:
            from run_rollups import (
                classify_actions, rebuild_rollups, rebuild_metrics,
                run_analytics_for_tickets,
            )
            _log(
                f"\n[csv-import] Post-import: rebuilding rollups + analytics "
                f"for {len(upserted)} ticket(s)\u2026"
            )
            classify_actions(upserted)
            rebuild_rollups(upserted)
            rebuild_metrics(upserted)
            run_analytics_for_tickets(upserted)

        _log("\n[csv-import] Next steps:")
        _log("  1. python run_sentiment.py            # sentiment scoring")
        _log("  2. python run_priority.py             # priority scoring")
        _log("  3. python run_complexity.py           # complexity scoring")


if __name__ == "__main__":
    main()
