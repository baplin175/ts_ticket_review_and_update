"""
run_rollups.py — Rebuild action classification, thread rollups, and metrics
from canonical DB state.  No TeamSupport API calls required.

Usage:
    python run_rollups.py classify                  # classify all actions
    python run_rollups.py classify --ticket 29696   # classify actions for one ticket
    python run_rollups.py rollups                   # rebuild rollups for all tickets
    python run_rollups.py rollups --ticket 29696    # rebuild rollups for one ticket
    python run_rollups.py metrics                   # rebuild metrics for all tickets
    python run_rollups.py metrics --ticket 29696    # rebuild metrics for one ticket
    python run_rollups.py all                       # classify + rollups + metrics (all)
    python run_rollups.py all --ticket 29696        # classify + rollups + metrics (one)

Requires DATABASE_URL to be set and data to be ingested via run_ingest.py.
"""

import argparse
import hashlib
import sys
from datetime import datetime, timezone
from typing import Optional

import db
from action_classifier import classify_action, is_noise, is_technical_substance


# ── Helpers ──────────────────────────────────────────────────────────

def _ticket_ids(ticket_number: str | None = None) -> list[int]:
    """Return ticket_id(s) to process.  If ticket_number is given, look it up."""
    if ticket_number:
        rows = db.fetch_all(
            "SELECT ticket_id FROM tickets WHERE ticket_number = %s;",
            (ticket_number,),
        )
        if not rows:
            print(f"[rollups] No ticket found with number {ticket_number}.", flush=True)
            return []
        return [r[0] for r in rows]
    else:
        rows = db.fetch_all("SELECT ticket_id FROM tickets ORDER BY ticket_id;")
        return [r[0] for r in rows]


def _sha256(text: str) -> str:
    """Return hex SHA-256 of the given text."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ── A. Classify actions ──────────────────────────────────────────────

def classify_actions(ticket_ids: list[int]) -> dict:
    """Apply deterministic classification to actions and UPDATE action_class in DB.

    Returns counts: {classified, skipped, total}.
    """
    classified = 0
    skipped = 0

    for tid in ticket_ids:
        rows = db.fetch_all(
            "SELECT action_id, cleaned_description, party, action_type, is_empty "
            "FROM ticket_actions WHERE ticket_id = %s ORDER BY created_at;",
            (tid,),
        )
        for action_id, desc, party, action_type, is_empty in rows:
            action_class = classify_action(desc, party=party, action_type=action_type, is_empty=is_empty)
            db.execute(
                "UPDATE ticket_actions SET action_class = %s WHERE action_id = %s;",
                (action_class, action_id),
            )
            classified += 1

    total = classified + skipped
    print(f"[rollups] Classified {classified} action(s) across {len(ticket_ids)} ticket(s).", flush=True)
    return {"classified": classified, "skipped": skipped, "total": total}


# ── B. Thread rollups ────────────────────────────────────────────────

def rebuild_rollups(ticket_ids: list[int]) -> int:
    """Rebuild ticket_thread_rollups from ticket_actions for each ticket.

    Returns count of tickets processed.
    """
    now = datetime.now(timezone.utc)
    count = 0

    for tid in ticket_ids:
        # Fetch all actions (sorted chronologically)
        rows = db.fetch_all(
            "SELECT action_id, cleaned_description, party, action_class, is_empty, "
            "       is_visible, creator_name, created_at "
            "FROM ticket_actions WHERE ticket_id = %s ORDER BY created_at;",
            (tid,),
        )

        # Build thread segments
        full_parts = []
        customer_visible_parts = []
        technical_core_parts = []
        latest_customer_text = None
        latest_inhance_text = None

        for aid, desc, party, ac, empty, vis, cname, cat in rows:
            text = (desc or "").strip()
            if not text or empty:
                continue

            # Prefix for thread context
            prefix = f"[{cname or party or '?'}]"
            line = f"{prefix} {text}"

            # full_thread_text: all non-empty actions, even noise
            full_parts.append(line)

            # customer_visible_text: non-noise actions
            if not is_noise(ac or "unknown"):
                customer_visible_parts.append(line)

            # technical_core_text: only technical substance
            if is_technical_substance(ac or "unknown"):
                technical_core_parts.append(line)

            # Track latest per party
            if party == "cust":
                latest_customer_text = text
            elif party == "inh":
                latest_inhance_text = text

        full_thread = "\n\n".join(full_parts) if full_parts else None
        customer_visible = "\n\n".join(customer_visible_parts) if customer_visible_parts else None
        technical_core = "\n\n".join(technical_core_parts) if technical_core_parts else None

        # summary_for_embedding: customer_visible (non-noise) capped at ~4000 chars
        summary_for_embedding = None
        if customer_visible:
            summary_for_embedding = customer_visible[:4000]

        thread_hash = _sha256(full_thread) if full_thread else None
        technical_core_hash = _sha256(technical_core) if technical_core else None

        # Upsert into ticket_thread_rollups
        db.execute("""
            INSERT INTO ticket_thread_rollups (
                ticket_id, full_thread_text, customer_visible_text,
                latest_customer_text, latest_inhance_text,
                technical_core_text, summary_for_embedding,
                thread_hash, technical_core_hash, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticket_id) DO UPDATE SET
                full_thread_text      = EXCLUDED.full_thread_text,
                customer_visible_text = EXCLUDED.customer_visible_text,
                latest_customer_text  = EXCLUDED.latest_customer_text,
                latest_inhance_text   = EXCLUDED.latest_inhance_text,
                technical_core_text   = EXCLUDED.technical_core_text,
                summary_for_embedding = EXCLUDED.summary_for_embedding,
                thread_hash           = EXCLUDED.thread_hash,
                technical_core_hash   = EXCLUDED.technical_core_hash,
                updated_at            = EXCLUDED.updated_at;
        """, (
            tid, full_thread, customer_visible,
            latest_customer_text, latest_inhance_text,
            technical_core, summary_for_embedding,
            thread_hash, technical_core_hash, now,
        ))
        count += 1

    print(f"[rollups] Rebuilt rollups for {count} ticket(s).", flush=True)
    return count


# ── C. Metrics ───────────────────────────────────────────────────────

def rebuild_metrics(ticket_ids: list[int]) -> int:
    """Rebuild ticket_metrics from ticket_actions for each ticket.

    Returns count of tickets processed.

    Heuristics / approximations:
    - first_response_at: earliest inh action after the first cust action
    - last_human_activity_at: latest inh or cust action (ignores system actions)
    - handoff_count: number of times the party switches between consecutive
      human actions (inh→cust or cust→inh). This is a rough proxy — it counts
      back-and-forth turns, not literal reassignments.
    """
    now = datetime.now(timezone.utc)
    count = 0

    for tid in ticket_ids:
        rows = db.fetch_all(
            "SELECT action_id, party, is_empty, created_at, creator_id "
            "FROM ticket_actions WHERE ticket_id = %s ORDER BY created_at;",
            (tid,),
        )

        action_count = len(rows)
        nonempty_count = 0
        cust_count = 0
        inh_count = 0
        participants = set()
        first_cust_at = None
        first_response_at = None
        last_human_at = None
        handoffs = 0
        prev_party = None

        for aid, party, is_empty, cat, cid in rows:
            if not is_empty:
                nonempty_count += 1

            if party == "cust":
                cust_count += 1
                if first_cust_at is None and cat:
                    first_cust_at = cat
            elif party == "inh":
                inh_count += 1

            # Track participants
            if cid:
                participants.add(cid)

            # Last human activity
            if party in ("inh", "cust") and cat:
                last_human_at = cat

            # First response: first inh action after first cust action
            if (first_response_at is None
                    and first_cust_at is not None
                    and party == "inh" and cat):
                first_response_at = cat

            # Handoff counting
            if party in ("inh", "cust"):
                if prev_party and prev_party != party:
                    handoffs += 1
                prev_party = party

        empty_ratio = None
        if action_count > 0:
            empty_ratio = round((action_count - nonempty_count) / action_count, 4)

        # Upsert into ticket_metrics
        db.execute("""
            INSERT INTO ticket_metrics (
                ticket_id, action_count, nonempty_action_count,
                customer_message_count, inhance_message_count,
                distinct_participant_count, first_response_at,
                last_human_activity_at, empty_action_ratio,
                handoff_count, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticket_id) DO UPDATE SET
                action_count               = EXCLUDED.action_count,
                nonempty_action_count      = EXCLUDED.nonempty_action_count,
                customer_message_count     = EXCLUDED.customer_message_count,
                inhance_message_count      = EXCLUDED.inhance_message_count,
                distinct_participant_count = EXCLUDED.distinct_participant_count,
                first_response_at          = EXCLUDED.first_response_at,
                last_human_activity_at     = EXCLUDED.last_human_activity_at,
                empty_action_ratio         = EXCLUDED.empty_action_ratio,
                handoff_count              = EXCLUDED.handoff_count,
                updated_at                 = EXCLUDED.updated_at;
        """, (
            tid, action_count, nonempty_count,
            cust_count, inh_count,
            len(participants), first_response_at,
            last_human_at, empty_ratio,
            handoffs, now,
        ))
        count += 1

    print(f"[rollups] Rebuilt metrics for {count} ticket(s).", flush=True)
    return count


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Rebuild classification, thread rollups, and metrics from DB state.",
    )
    sub = parser.add_subparsers(dest="command")

    for cmd in ("classify", "rollups", "metrics", "all"):
        p = sub.add_parser(cmd, help={
            "classify": "Classify all actions (deterministic rules).",
            "rollups": "Rebuild ticket_thread_rollups.",
            "metrics": "Rebuild ticket_metrics.",
            "all": "Classify + rollups + metrics.",
        }[cmd])
        p.add_argument("--ticket", "-t", help="Process only this ticket number.")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if not db._is_enabled():
        print("ERROR: DATABASE_URL is not set.", file=sys.stderr)
        sys.exit(1)

    # Ensure schema and tables exist
    db.migrate()

    ticket_number = getattr(args, "ticket", None)
    tids = _ticket_ids(ticket_number)

    if not tids:
        print("[rollups] No tickets to process.", flush=True)
        return

    print(f"[rollups] Processing {len(tids)} ticket(s) …", flush=True)

    if args.command in ("classify", "all"):
        classify_actions(tids)

    if args.command in ("rollups", "all"):
        rebuild_rollups(tids)

    if args.command in ("metrics", "all"):
        rebuild_metrics(tids)

    print("[rollups] Done.", flush=True)


if __name__ == "__main__":
    main()
