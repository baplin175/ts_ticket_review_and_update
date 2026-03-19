"""
Pass 4 — Intervention mapping from Pass 3 mechanism.

Reads the mechanism from a successful Pass 3 result, sends it to
Matcha with the Pass 4 prompt, parses the JSON response into
mechanism_class / intervention_type / intervention_action, and stores
both the raw response and parsed output in ticket_llm_pass_results.

After all tickets are classified, computes engineering ROI aggregation
metrics and writes output artifacts.

Requires DATABASE_URL to be set (Postgres mode).

Usage:
    python run_pass4.py --limit 100
    python run_pass4.py --ticket-id 99784
    python run_pass4.py --ticket-id 99784,98154,100289
    python run_pass4.py --failed-only
    python run_pass4.py --force
    python run_pass4.py --aggregate-only
"""

import argparse
import json
import os
import sys
import time

from config import OUTPUT_DIR
from pass4.intervention_mapper import (
    PASS_NAME,
    PROMPT_VERSION,
    MODEL_NAME,
    process_ticket,
)
from pass4.intervention_mapper import _load_prompt_template
from pass4.intervention_aggregator import (
    aggregate_from_db,
    aggregate_from_results,
    write_artifacts,
)

# Pass 3 dependency
PASS3_PASS_NAME = "pass3_mechanism"
PASS3_PROMPT_VERSION = "2"


def _log(msg: str) -> None:
    print(msg, flush=True)


def main(
    *,
    ticket_ids: list[int] | None = None,
    limit: int = 0,
    force: bool = False,
    failed_only: bool = False,
    aggregate_only: bool = False,
) -> list[dict]:
    """Run Pass 4 for eligible tickets.

    Returns a list of result dicts (one per ticket processed).
    """
    import db

    if not db._is_enabled():
        _log("[pass4] DATABASE_URL is not set. Pass 4 requires a Postgres DB.")
        sys.exit(1)

    # Run migrations to ensure table/columns exist
    applied = db.migrate()
    if applied:
        from run_rollups import run_full_rollups
        run_full_rollups()

    # Aggregate-only mode: skip LLM processing, just compute metrics
    if aggregate_only:
        _log("[pass4] Aggregate-only mode — computing ROI metrics from DB.")
        aggregation = aggregate_from_db()
        output_dir = os.path.join(OUTPUT_DIR, "pass4")
        written = write_artifacts(aggregation, output_dir)
        for path in written:
            _log(f"[pass4] Wrote: {path}")
        _log(f"[pass4] Mechanism classes: {len(aggregation['mechanism_class_counts'])}")
        _log(f"[pass4] Intervention types: {len(aggregation['intervention_type_counts'])}")
        _log(f"[pass4] Top fixes: {len(aggregation['top_engineering_fixes'])}")
        return []

    prompt_template = _load_prompt_template()
    _log(f"[pass4] Loaded prompt from {os.path.basename(prompt_template) if isinstance(prompt_template, str) and os.path.exists(prompt_template) else 'pass4_intervention.txt'}")
    _log(f"[pass4] Pass: {PASS_NAME}  Prompt version: {PROMPT_VERSION}  Model: {MODEL_NAME}")
    _log(f"[pass4] Requires Pass 3: {PASS3_PASS_NAME} v{PASS3_PROMPT_VERSION}")

    # Fetch eligible tickets (those with successful Pass 3 mechanism)
    rows = db.fetch_pending_pass4_tickets(
        PROMPT_VERSION,
        pass3_pass_name=PASS3_PASS_NAME,
        pass3_prompt_version=PASS3_PROMPT_VERSION,
        limit=limit,
        ticket_ids=ticket_ids,
        failed_only=failed_only,
        force=force,
    )

    total = len(rows)
    if total == 0:
        _log("[pass4] No eligible tickets found.")
        return []

    _log(f"[pass4] Found {total} ticket(s) to process.")
    _log("=" * 60)

    results = []
    succeeded = 0
    failed = 0
    skipped = 0
    total_start = time.monotonic()

    for idx, (ticket_id, mechanism) in enumerate(rows, 1):
        _log(f"\n[pass4] [{idx}/{total}] Ticket {ticket_id}")
        _log(f"[pass4]   mechanism: {mechanism[:80]}{'…' if len(mechanism) > 80 else ''}")

        r = process_ticket(
            ticket_id,
            mechanism,
            prompt_template,
            force=force,
        )
        results.append(r)

        if r["status"] == "success":
            succeeded += 1
            _log(f"[pass4]   ✓ {r['mechanism_class']} / {r['intervention_type']}")
            _log(f"[pass4]     {r['intervention_action']}")
        elif r["status"] == "failed":
            failed += 1
            _log(f"[pass4]   ✗ error: {r['error']}")
        else:
            skipped += 1

        _log(f"[pass4]   elapsed: {r['elapsed_s']}s")

    total_elapsed = time.monotonic() - total_start

    # Compute aggregation from all DB results (includes prior runs)
    _log("\n[pass4] Computing aggregation metrics...")
    aggregation = aggregate_from_db()

    # Build interventions list for the JSON artifact
    interventions = []
    for r in results:
        if r["status"] == "success":
            interventions.append({
                "ticket_id": str(r["ticket_id"]),
                "mechanism_class": r["mechanism_class"],
                "intervention_type": r["intervention_type"],
                "intervention_action": r["intervention_action"],
            })

    # Write output artifacts
    output_dir = os.path.join(OUTPUT_DIR, "pass4")
    written = write_artifacts(aggregation, output_dir, interventions=interventions)
    for path in written:
        _log(f"[pass4] Wrote: {path}")

    # Summary
    _log(f"\n{'=' * 60}")
    _log("[pass4] Run complete.")
    _log(f"[pass4]   Total:     {total}")
    _log(f"[pass4]   Succeeded: {succeeded}")
    _log(f"[pass4]   Failed:    {failed}")
    _log(f"[pass4]   Skipped:   {skipped}")
    _log(f"[pass4]   Elapsed:   {total_elapsed:.1f}s")
    _log(f"[pass4]   Mechanism classes found: {len(aggregation['mechanism_class_counts'])}")
    _log(f"[pass4]   Top engineering fixes:   {len(aggregation['top_engineering_fixes'])}")
    _log("=" * 60)

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pass 4 — Intervention mapping from Pass 3 mechanism."
    )
    parser.add_argument(
        "--ticket-id",
        type=str,
        default=None,
        help="Comma-separated ticket_id(s) to process.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Maximum number of tickets to process (0 = unlimited).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force rerun even for tickets with existing successful results.",
    )
    parser.add_argument(
        "--failed-only",
        action="store_true",
        help="Only rerun tickets that previously failed.",
    )
    parser.add_argument(
        "--aggregate-only",
        action="store_true",
        help="Skip LLM processing; compute and export ROI metrics from existing DB results.",
    )

    args = parser.parse_args()

    tid_list = None
    if args.ticket_id:
        tid_list = [int(t.strip()) for t in args.ticket_id.split(",") if t.strip()]

    main(
        ticket_ids=tid_list,
        limit=args.limit,
        force=args.force,
        failed_only=args.failed_only,
        aggregate_only=args.aggregate_only,
    )
