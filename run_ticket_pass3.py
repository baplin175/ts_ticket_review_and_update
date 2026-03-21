"""
Pass 3 — Intervention mapping from Pass 2 mechanism.

Reads the mechanism from a successful Pass 2 result, sends it to
Matcha with the intervention prompt, parses the JSON response into
mechanism_class / intervention_type / intervention_action, and stores
both the raw response and parsed output in ticket_llm_pass_results.

After all tickets are classified, computes engineering ROI aggregation
metrics and writes output artifacts.

Requires DATABASE_URL to be set (Postgres mode).

Usage:
    python run_ticket_pass3.py --limit 100
    python run_ticket_pass3.py --ticket-id 99784
    python run_ticket_pass3.py --ticket-id 99784,98154,100289
    python run_ticket_pass3.py --failed-only
    python run_ticket_pass3.py --force
    python run_ticket_pass3.py --aggregate-only
"""

import argparse
import os
import sys
import time

from config import OUTPUT_DIR
from pass4.intervention_aggregator import aggregate_from_db, write_artifacts
from pass4.intervention_mapper import MODEL_NAME, PASS_NAME, PROMPT_VERSION, _load_prompt_template, process_ticket

PASS2_PASS_NAME = "pass2_mechanism"
PASS2_PROMPT_VERSION = "3"


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
    """Run Pass 3 for eligible tickets."""
    import db

    if not db._is_enabled():
        _log("[pass3] DATABASE_URL is not set. Pass 3 requires a Postgres DB.")
        sys.exit(1)

    applied = db.migrate()
    if applied:
        from run_rollups import run_full_rollups
        run_full_rollups()

    if aggregate_only:
        _log("[pass3] Aggregate-only mode — computing ROI metrics from DB.")
        aggregation = aggregate_from_db()
        output_dir = os.path.join(OUTPUT_DIR, "pass3")
        written = write_artifacts(aggregation, output_dir)
        for path in written:
            _log(f"[pass3] Wrote: {path}")
        _log(f"[pass3] Mechanism classes: {len(aggregation['mechanism_class_counts'])}")
        _log(f"[pass3] Intervention types: {len(aggregation['intervention_type_counts'])}")
        _log(f"[pass3] Top fixes: {len(aggregation['top_engineering_fixes'])}")
        return []

    prompt_template = _load_prompt_template()
    _log(f"[pass3] Loaded prompt from pass4_intervention.txt")
    _log(f"[pass3] Pass: {PASS_NAME}  Prompt version: {PROMPT_VERSION}  Model: {MODEL_NAME}")
    _log(f"[pass3] Requires Pass 2: {PASS2_PASS_NAME} v{PASS2_PROMPT_VERSION}")

    rows = db.fetch_pending_pass3_tickets(
        PROMPT_VERSION,
        pass2_pass_name=PASS2_PASS_NAME,
        pass2_prompt_version=PASS2_PROMPT_VERSION,
        limit=limit,
        ticket_ids=ticket_ids,
        failed_only=failed_only,
        force=force,
    )

    if ticket_ids:
        eligible_ids = {row[0] for row in rows}
        missing_p2 = [tid for tid in ticket_ids if tid not in eligible_ids]
        if missing_p2:
            invalidated = db.invalidate_stale_pass3(
                missing_p2,
                pass2_pass_name=PASS2_PASS_NAME,
                pass2_prompt_version=PASS2_PROMPT_VERSION,
            )
            if invalidated:
                _log(f"[pass3] Invalidated {invalidated} stale P3 result(s) for {len(missing_p2)} ticket(s) missing P2 v{PASS2_PROMPT_VERSION}.")
            else:
                _log(f"[pass3] {len(missing_p2)} ticket(s) skipped (no P2 v{PASS2_PROMPT_VERSION} mechanism).")

    total = len(rows)
    if total == 0:
        _log("[pass3] No eligible tickets found.")
        return []

    _log(f"[pass3] Found {total} ticket(s) to process.")
    _log("=" * 60)

    results = []
    succeeded = 0
    failed = 0
    skipped = 0
    total_start = time.monotonic()

    for idx, (ticket_id, mechanism) in enumerate(rows, 1):
        _log(f"\n[pass3] [{idx}/{total}] Ticket {ticket_id}")
        _log(f"[pass3]   mechanism: {mechanism[:80]}{'…' if len(mechanism) > 80 else ''}")

        r = process_ticket(ticket_id, mechanism, prompt_template, force=force)
        results.append(r)

        if r["status"] == "success":
            succeeded += 1
            _log(f"[pass3]   ✓ {r['mechanism_class']} / {r['intervention_type']}")
            _log(f"[pass3]     {r['intervention_action']}")
        elif r["status"] == "failed":
            failed += 1
            _log(f"[pass3]   ✗ error: {r['error']}")
        else:
            skipped += 1

        _log(f"[pass3]   elapsed: {r['elapsed_s']}s")

    total_elapsed = time.monotonic() - total_start
    _log("\n[pass3] Computing aggregation metrics...")
    aggregation = aggregate_from_db()

    interventions = []
    for r in results:
        if r["status"] == "success":
            interventions.append({
                "ticket_id": str(r["ticket_id"]),
                "mechanism_class": r["mechanism_class"],
                "intervention_type": r["intervention_type"],
                "intervention_action": r["intervention_action"],
            })

    output_dir = os.path.join(OUTPUT_DIR, "pass3")
    written = write_artifacts(aggregation, output_dir, interventions=interventions)
    for path in written:
        _log(f"[pass3] Wrote: {path}")

    _log(f"\n{'=' * 60}")
    _log("[pass3] Run complete.")
    _log(f"[pass3]   Total:     {total}")
    _log(f"[pass3]   Succeeded: {succeeded}")
    _log(f"[pass3]   Failed:    {failed}")
    _log(f"[pass3]   Skipped:   {skipped}")
    _log(f"[pass3]   Elapsed:   {total_elapsed:.1f}s")
    _log(f"[pass3]   Mechanism classes found: {len(aggregation['mechanism_class_counts'])}")
    _log(f"[pass3]   Top engineering fixes:   {len(aggregation['top_engineering_fixes'])}")
    _log("=" * 60)
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pass 3 — Intervention mapping from Pass 2 mechanism.")
    parser.add_argument("--ticket-id", type=str, default=None, help="Comma-separated ticket_id(s) to process.")
    parser.add_argument("--limit", type=int, default=0, help="Maximum number of tickets to process (0 = unlimited).")
    parser.add_argument("--force", action="store_true", help="Force rerun even for tickets with existing successful results.")
    parser.add_argument("--failed-only", action="store_true", help="Only rerun tickets that previously failed.")
    parser.add_argument("--aggregate-only", action="store_true", help="Skip LLM processing; compute and export ROI metrics from existing DB results.")

    args = parser.parse_args()
    ticket_ids = None
    if args.ticket_id:
        ticket_ids = [int(t.strip()) for t in args.ticket_id.split(",") if t.strip()]

    main(ticket_ids=ticket_ids, limit=args.limit, force=args.force, failed_only=args.failed_only, aggregate_only=args.aggregate_only)
