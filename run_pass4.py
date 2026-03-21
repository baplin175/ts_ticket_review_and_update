"""Legacy compatibility wrapper for the old Pass 4 entrypoint.

The active three-step pipeline is now:
  Pass 1 — phenomenon + grammar
  Pass 2 — mechanism inference
  Pass 3 — intervention mapping

This wrapper delegates to `run_ticket_pass3.py` so older automation keeps working.
"""

import argparse

from run_ticket_pass3 import main


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Legacy wrapper: delegates old Pass 4 CLI calls to the current Pass 3 intervention pipeline."
    )
    parser.add_argument("--ticket-id", type=str, default=None, help="Comma-separated ticket_id(s) to process.")
    parser.add_argument("--limit", type=int, default=0, help="Maximum number of tickets to process (0 = unlimited).")
    parser.add_argument("--force", action="store_true", help="Force rerun even for tickets with existing successful results.")
    parser.add_argument("--failed-only", action="store_true", help="Only rerun tickets that previously failed.")
    parser.add_argument("--aggregate-only", action="store_true", help="Skip LLM processing; compute and export ROI metrics from existing DB results.")
    args = parser.parse_args()

    ticket_ids = None
    if args.ticket_id:
        ticket_ids = [int(t.strip()) for t in args.ticket_id.split(",") if t.strip()]

    main(
        ticket_ids=ticket_ids,
        limit=args.limit,
        force=args.force,
        failed_only=args.failed_only,
        aggregate_only=args.aggregate_only,
    )
