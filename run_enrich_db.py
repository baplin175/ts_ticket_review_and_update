"""
DB-only enrichment — score all closed tickets using data already in Postgres.

No TeamSupport API calls are made. Only Matcha LLM calls for scoring.
Hash-based skipping ensures only new/changed tickets are scored.

Usage:
    python run_enrich_db.py                        # priority + complexity for all closed tickets
    python run_enrich_db.py --priority-only        # priority only
    python run_enrich_db.py --complexity-only      # complexity only
    python run_enrich_db.py --sentiment            # include sentiment (off by default)
    python run_enrich_db.py --force                # rescore even if hash unchanged
    python run_enrich_db.py --limit 100            # process at most 100 tickets
    python run_enrich_db.py --batch-size 10        # priority batch size (default 20)
    python run_enrich_db.py --status Open          # target Open tickets instead of Closed
    python run_enrich_db.py --no-closed              # all tickets except Closed / Closed with Survey
"""

import argparse
import sys
import time

from enrichment.orchestrator import elapsed_minutes, run_sentiment_stage, run_stage_batches


def _log(msg: str) -> None:
    print(msg, flush=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="DB-only enrichment for closed tickets.")
    parser.add_argument("--force", action="store_true",
                        help="Rescore even if content hash unchanged.")
    parser.add_argument("--limit", type=int, default=0,
                        help="Max tickets to process (0 = unlimited).")
    parser.add_argument("--batch-size", type=int, default=20,
                        help="Priority scoring batch size (default 20).")
    parser.add_argument("--priority-only", action="store_true",
                        help="Run only priority scoring.")
    parser.add_argument("--complexity-only", action="store_true",
                        help="Run only complexity scoring.")
    parser.add_argument("--no-sentiment", action="store_true",
                        help="Exclude sentiment analysis (on by default).")
    parser.add_argument("--status", default="Closed",
                        help="Ticket status to target (default: Closed).")
    parser.add_argument("--no-closed", action="store_true",
                        help="Exclude closed tickets (all other statuses included).")
    args = parser.parse_args()

    from config import FORCE_ENRICHMENT
    force = args.force or FORCE_ENRICHMENT

    # Determine which stages to run
    run_priority = not args.complexity_only
    run_complexity = not args.priority_only
    run_sentiment = not args.no_sentiment and not args.priority_only and not args.complexity_only

    # ── Verify DB ──
    try:
        import db
        if not db._is_enabled():
            _log("[enrich] DATABASE_URL is not set. This script requires a Postgres DB.")
            sys.exit(1)
    except Exception as e:
        _log(f"[enrich] DB connection failed: {e}")
        sys.exit(1)

    # ── Fetch ticket numbers with rollups ──
    status_label = "non-Closed" if args.no_closed else args.status
    _log(f"[enrich] Querying {status_label} tickets with rollups...")
    all_tickets = db.fetch_ticket_numbers_by_status(args.status, exclude_closed=args.no_closed)
    if not all_tickets:
        _log(f"[enrich] No {status_label} tickets with rollups found.")
        return

    if args.limit > 0:
        all_tickets = all_tickets[:args.limit]

    _log(f"[enrich] Found {len(all_tickets)} {status_label} ticket(s) to process.")
    _log("=" * 60)

    start_time = time.time()

    # ── Priority scoring (batched) ──
    if run_priority:
        _log(f"\n[enrich] Stage 1: Priority scoring (batch size {args.batch_size})")
        from run_priority import main as priority_main
        total_scored, total_skipped, total_errors = run_stage_batches(
            tickets=all_tickets,
            batch_size=args.batch_size,
            label="priority",
            runner=priority_main,
            force=force,
            write_back=False,
        )

        _log(f"\n[enrich] Priority complete: {total_scored} scored, {total_skipped} skipped (hash match), {total_errors} errors.")

    # ── Complexity scoring (one-by-one, handled internally) ──
    if run_complexity:
        _log(f"\n[enrich] Stage 2: Complexity scoring ({len(all_tickets)} tickets)")
        from run_complexity import main as complexity_main
        total_scored, total_skipped, total_errors = run_stage_batches(
            tickets=all_tickets,
            batch_size=50,
            label="complexity",
            runner=complexity_main,
            force=force,
            write_back=False,
        )

        _log(f"\n[enrich] Complexity complete: {total_scored} scored, {total_skipped} skipped (hash match), {total_errors} errors.")

    # ── Sentiment (optional, one-by-one) ──
    if run_sentiment:
        _log(f"\n[enrich] Stage 3: Sentiment analysis ({len(all_tickets)} tickets)")
        from run_sentiment import main as sentiment_main
        run_sentiment_stage(tickets=all_tickets, force=force, runner=sentiment_main)

    elapsed_m = elapsed_minutes(start_time)
    _log(f"\n{'=' * 60}")
    _log(f"[enrich] All stages complete. {len(all_tickets)} ticket(s) processed in {elapsed_m:.1f} min.")
    _log("=" * 60)


if __name__ == "__main__":
    main()
