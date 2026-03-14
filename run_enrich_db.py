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
"""

import argparse
import sys
import time
from datetime import datetime, timezone


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
    parser.add_argument("--sentiment", action="store_true",
                        help="Include sentiment analysis (off by default).")
    parser.add_argument("--status", default="Closed",
                        help="Ticket status to target (default: Closed).")
    args = parser.parse_args()

    # Determine which stages to run
    run_priority = not args.complexity_only
    run_complexity = not args.priority_only
    run_sentiment = args.sentiment and not args.priority_only and not args.complexity_only

    # ── Verify DB ──
    try:
        import db
        if not db._is_enabled():
            _log("[enrich] DATABASE_URL is not set. This script requires a Postgres DB.")
            sys.exit(1)
    except Exception as e:
        _log(f"[enrich] DB connection failed: {e}")
        sys.exit(1)

    # ── Fetch closed ticket numbers with rollups ──
    _log(f"[enrich] Querying {args.status} tickets with rollups...")
    all_tickets = db.fetch_ticket_numbers_by_status(args.status)
    if not all_tickets:
        _log(f"[enrich] No {args.status} tickets with rollups found.")
        return

    if args.limit > 0:
        all_tickets = all_tickets[:args.limit]

    _log(f"[enrich] Found {len(all_tickets)} {args.status} ticket(s) to process.")
    _log("=" * 60)

    start_time = time.time()

    # ── Priority scoring (batched) ──
    if run_priority:
        _log(f"\n[enrich] Stage 1: Priority scoring (batch size {args.batch_size})")
        from run_priority import main as priority_main
        batch_size = args.batch_size
        total_scored = 0
        total_skipped = 0
        total_errors = 0

        for i in range(0, len(all_tickets), batch_size):
            batch = all_tickets[i:i + batch_size]
            batch_num = i // batch_size + 1
            total_batches = (len(all_tickets) + batch_size - 1) // batch_size
            _log(f"\n  [priority] Batch {batch_num}/{total_batches} ({len(batch)} tickets: {batch[0]}..{batch[-1]})")

            try:
                results = priority_main(
                    write_back=False,
                    force=args.force,
                    ticket_numbers=batch,
                )
                scored = len(results)
                skipped = len(batch) - scored
                total_scored += scored
                total_skipped += skipped
            except SystemExit:
                _log(f"  [priority] Batch {batch_num} failed (system exit). Continuing...")
                total_errors += len(batch)
            except Exception as e:
                _log(f"  [priority] Batch {batch_num} error: {e}. Continuing...")
                total_errors += len(batch)

        _log(f"\n[enrich] Priority complete: {total_scored} scored, {total_skipped} skipped (hash match), {total_errors} errors.")

    # ── Complexity scoring (one-by-one, handled internally) ──
    if run_complexity:
        _log(f"\n[enrich] Stage 2: Complexity scoring ({len(all_tickets)} tickets)")
        from run_complexity import main as complexity_main
        # Complexity processes tickets one-by-one internally, but we still
        # batch the ticket_numbers list to get periodic progress updates.
        batch_size_c = 50  # progress reporting interval
        total_scored = 0
        total_skipped = 0
        total_errors = 0

        for i in range(0, len(all_tickets), batch_size_c):
            batch = all_tickets[i:i + batch_size_c]
            _log(f"\n  [complexity] Progress: {i}/{len(all_tickets)} — processing {len(batch)} tickets...")

            try:
                results = complexity_main(
                    write_back=False,
                    force=args.force,
                    ticket_numbers=batch,
                )
                scored = len(results)
                skipped = len(batch) - scored
                total_scored += scored
                total_skipped += skipped
            except SystemExit:
                _log(f"  [complexity] Batch at offset {i} failed (system exit). Continuing...")
                total_errors += len(batch)
            except Exception as e:
                _log(f"  [complexity] Batch at offset {i} error: {e}. Continuing...")
                total_errors += len(batch)

        _log(f"\n[enrich] Complexity complete: {total_scored} scored, {total_skipped} skipped (hash match), {total_errors} errors.")

    # ── Sentiment (optional, one-by-one) ──
    if run_sentiment:
        _log(f"\n[enrich] Stage 3: Sentiment analysis ({len(all_tickets)} tickets)")
        from run_sentiment import main as sentiment_main
        try:
            sentiment_main(force=args.force, ticket_numbers=all_tickets)
        except SystemExit:
            _log("[enrich] Sentiment stage exited.")
        except Exception as e:
            _log(f"[enrich] Sentiment error: {e}")
        _log("[enrich] Sentiment complete.")

    elapsed = time.time() - start_time
    elapsed_m = elapsed / 60
    _log(f"\n{'=' * 60}")
    _log(f"[enrich] All stages complete. {len(all_tickets)} ticket(s) processed in {elapsed_m:.1f} min.")
    _log("=" * 60)


if __name__ == "__main__":
    main()
