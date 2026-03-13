"""
run_export.py — Export canonical DB state to timestamped JSON artifacts.

Generates the same JSON files the original pipeline produced, but reads
from the Postgres DB rather than calling the TeamSupport API.

Usage:
    python run_export.py activities                 # export activities JSON
    python run_export.py activities --ticket 29696  # export for one ticket
    python run_export.py sentiment                  # export latest sentiment scores
    python run_export.py priority                   # export latest priority scores
    python run_export.py complexity                 # export latest complexity scores
    python run_export.py all                        # export all artifact types
    python run_export.py all --ticket 29696         # all artifacts for one ticket

Requires DATABASE_URL to be set and data to be ingested via run_ingest.py.
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone

import db
from config import OUTPUT_DIR


def _log(msg: str) -> None:
    print(msg, flush=True)


def _ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _ticket_ids(ticket_number: str | None = None) -> list[tuple[int, str]]:
    """Return list of (ticket_id, ticket_number) pairs."""
    if ticket_number:
        rows = db.fetch_all(
            "SELECT ticket_id, ticket_number FROM tickets WHERE ticket_number = %s;",
            (ticket_number,),
        )
    else:
        rows = db.fetch_all(
            "SELECT ticket_id, ticket_number FROM tickets ORDER BY ticket_id;"
        )
    return [(r[0], r[1] or str(r[0])) for r in rows]


# ── Activities export ────────────────────────────────────────────────

def export_activities(ticket_number: str | None = None) -> str | None:
    """Export ticket + action data to activities_*.json.

    Returns the output file path, or None if no tickets found.
    """
    tids = _ticket_ids(ticket_number)
    if not tids:
        _log("[export] No tickets found.")
        return None

    tickets_out = []
    for tid, tnum in tids:
        t = db.load_ticket_with_actions(tid)
        if not t:
            continue
        tickets_out.append(t)

    if not tickets_out:
        _log("[export] No ticket data to export.")
        return None

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, f"activities_{_ts()}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(tickets_out, f, ensure_ascii=False, indent=2, default=str)
    _log(f"[export] Wrote {len(tickets_out)} ticket(s) to {out_path}")
    return out_path


# ── Sentiment export ─────────────────────────────────────────────────

def export_sentiment(ticket_number: str | None = None) -> str | None:
    """Export latest sentiment scores to sentiment_*.json."""
    tids = _ticket_ids(ticket_number)
    if not tids:
        _log("[export] No tickets found.")
        return None

    results = []
    for tid, tnum in tids:
        row = db.fetch_one(
            "SELECT frustrated, activity_id, created_at, scored_at, "
            "       thread_hash, model_name, source_file "
            "FROM ticket_sentiment WHERE ticket_id = %s "
            "ORDER BY scored_at DESC LIMIT 1;",
            (tid,),
        )
        if not row:
            continue
        results.append({
            "ticket_number": tnum,
            "frustrated": row[0],
            "activity_id": row[1],
            "created_at": str(row[2]) if row[2] else None,
            "scored_at": str(row[3]) if row[3] else None,
            "thread_hash": row[4],
            "model_name": row[5],
            "source_file": row[6],
        })

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, f"sentiment_{_ts()}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2, default=str)
    _log(f"[export] Wrote {len(results)} sentiment result(s) to {out_path}")
    return out_path


# ── Priority export ──────────────────────────────────────────────────

def export_priority(ticket_number: str | None = None) -> str | None:
    """Export latest priority scores to priority_*.json."""
    tids = _ticket_ids(ticket_number)
    if not tids:
        _log("[export] No tickets found.")
        return None

    results = []
    for tid, tnum in tids:
        row = db.fetch_one(
            "SELECT priority, priority_explanation, scored_at, "
            "       thread_hash, model_name "
            "FROM ticket_priority_scores WHERE ticket_id = %s "
            "ORDER BY scored_at DESC LIMIT 1;",
            (tid,),
        )
        if not row:
            continue
        results.append({
            "ticket_number": tnum,
            "priority": row[0],
            "priority_explanation": row[1],
            "scored_at": str(row[2]) if row[2] else None,
            "thread_hash": row[3],
            "model_name": row[4],
        })

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, f"priority_{_ts()}.json")
    output = {
        "source_file": "db",
        "tickets_sent": len(results),
        "results": results,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2, default=str)
    _log(f"[export] Wrote {len(results)} priority result(s) to {out_path}")
    return out_path


# ── Complexity export ────────────────────────────────────────────────

def export_complexity(ticket_number: str | None = None) -> str | None:
    """Export latest complexity scores to complexity_*.json."""
    tids = _ticket_ids(ticket_number)
    if not tids:
        _log("[export] No tickets found.")
        return None

    results = []
    for tid, tnum in tids:
        row = db.fetch_one(
            "SELECT intrinsic_complexity, coordination_load, elapsed_drag, "
            "       overall_complexity, confidence, primary_complexity_drivers, "
            "       complexity_summary, evidence, noise_factors, "
            "       duration_vs_complexity_note, scored_at, "
            "       technical_core_hash, model_name "
            "FROM ticket_complexity_scores WHERE ticket_id = %s "
            "ORDER BY scored_at DESC LIMIT 1;",
            (tid,),
        )
        if not row:
            continue
        results.append({
            "ticket_number": tnum,
            "intrinsic_complexity": row[0],
            "coordination_load": row[1],
            "elapsed_drag": row[2],
            "overall_complexity": row[3],
            "confidence": float(row[4]) if row[4] is not None else None,
            "primary_complexity_drivers": row[5],
            "complexity_summary": row[6],
            "evidence": row[7],
            "noise_factors": row[8],
            "duration_vs_complexity_note": row[9],
            "scored_at": str(row[10]) if row[10] else None,
            "technical_core_hash": row[11],
            "model_name": row[12],
        })

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    out_path = os.path.join(OUTPUT_DIR, f"complexity_{_ts()}.json")
    output = {
        "source_file": "db",
        "tickets_scored": len(results),
        "results": results,
    }
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2, default=str)
    _log(f"[export] Wrote {len(results)} complexity result(s) to {out_path}")
    return out_path


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Export canonical DB state to timestamped JSON artifacts.",
    )
    sub = parser.add_subparsers(dest="command")

    for cmd in ("activities", "sentiment", "priority", "complexity", "all"):
        p = sub.add_parser(cmd, help={
            "activities": "Export activities JSON from DB.",
            "sentiment": "Export latest sentiment scores.",
            "priority": "Export latest priority scores.",
            "complexity": "Export latest complexity scores.",
            "all": "Export all artifact types.",
        }[cmd])
        p.add_argument("--ticket", "-t", help="Export only this ticket number.")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if not db._is_enabled():
        print("ERROR: DATABASE_URL is not set.", file=sys.stderr)
        sys.exit(1)

    # Ensure schema and tables exist
    db.migrate()

    ticket = getattr(args, "ticket", None)

    if args.command in ("activities", "all"):
        export_activities(ticket)
    if args.command in ("sentiment", "all"):
        export_sentiment(ticket)
    if args.command in ("priority", "all"):
        export_priority(ticket)
    if args.command in ("complexity", "all"):
        export_complexity(ticket)

    _log("[export] Done.")


if __name__ == "__main__":
    main()
