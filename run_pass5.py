"""
Pass 4 — Cluster key normalization from Pass 3 mechanism.

Reads the mechanism from a successful Pass 3 (internal pass4_intervention)
result, sends it to Matcha with the cluster-key normalization prompt,
parses the plain-text snake_case response, and stores both the raw
response and parsed cluster_key in ticket_llm_pass_results.

Requires DATABASE_URL to be set (Postgres mode).

Usage:
    python run_pass5.py --limit 100
    python run_pass5.py --ticket-id 99784
    python run_pass5.py --ticket-id 99784,98154,100289
    python run_pass5.py --failed-only
    python run_pass5.py --force
"""

import argparse
import json
import os
import sys
import time

from config import OUTPUT_DIR
from pipeline_stages import stage_title
from pass5.cluster_key_mapper import (
    PASS_NAME,
    MODEL_NAME,
    load_prompt_record,
    _load_prompt_template,
    process_ticket,
)
from prompt_store import get_prompt

# Upstream dependency: user-facing Pass 3, internal pass3_mechanism
UPSTREAM_PASS_NAME = "pass3_mechanism"
DEFAULT_PROMPT_VERSION = "2"
DEFAULT_UPSTREAM_PROMPT_VERSION = "3"


def _coerce_prompt_version(value, default: str) -> str:
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str) and value.strip().isdigit():
        return value
    return default


def _log(msg: str) -> None:
    print(msg, flush=True)


def main(
    *,
    ticket_ids: list[int] | None = None,
    limit: int = 0,
    force: bool = False,
    failed_only: bool = False,
) -> list[dict]:
    """Run the user-facing Pass 4 stage (cluster key normalization) for eligible tickets.

    Returns a list of result dicts (one per ticket processed).
    """
    import db

    if not db._is_enabled():
        _log("[pass5] DATABASE_URL is not set. Pass 4 requires a Postgres DB.")
        sys.exit(1)

    # Run migrations to ensure table/columns exist
    applied = db.migrate()
    if applied:
        from run_rollups import run_full_rollups
        run_full_rollups()

    prompt_record = load_prompt_record()
    upstream_prompt = get_prompt(UPSTREAM_PASS_NAME, allow_fallback=False)
    prompt_template = _load_prompt_template()
    prompt_version = _coerce_prompt_version(prompt_record.get("version"), DEFAULT_PROMPT_VERSION)
    upstream_prompt_version = _coerce_prompt_version(upstream_prompt.get("version"), DEFAULT_UPSTREAM_PROMPT_VERSION)
    _log(f"[pass5] Stage: {stage_title('cluster_key')}  Internal pass: {PASS_NAME}  Prompt version: {prompt_version}  Model: {MODEL_NAME}")
    _log(f"[pass5] Requires Pass 2: {UPSTREAM_PASS_NAME} v{upstream_prompt_version}")

    # Fetch eligible tickets (those with successful Pass 3 mechanism)
    rows = db.fetch_pending_pass5_tickets(
        prompt_version,
        upstream_pass_name=UPSTREAM_PASS_NAME,
        upstream_prompt_version=upstream_prompt_version,
        limit=limit,
        ticket_ids=ticket_ids,
        failed_only=failed_only,
        force=force,
    )

    # Invalidate stale P5 results for tickets missing a valid upstream mechanism
    if ticket_ids:
        eligible_ids = {row[0] for row in rows}
        missing_upstream = [tid for tid in ticket_ids if tid not in eligible_ids]
        if missing_upstream:
            invalidated = db.invalidate_stale_pass5(
                missing_upstream,
                upstream_pass_name=UPSTREAM_PASS_NAME,
                upstream_prompt_version=upstream_prompt_version,
            )
            if invalidated:
                _log(f"[pass5] Invalidated {invalidated} stale P5 result(s) for {len(missing_upstream)} ticket(s) missing upstream mechanism v{upstream_prompt_version}.")
            else:
                _log(f"[pass5] {len(missing_upstream)} ticket(s) skipped (no upstream mechanism v{upstream_prompt_version}).")

    total = len(rows)
    if total == 0:
        _log("[pass5] No eligible tickets found.")
        return []

    _log(f"[pass5] Found {total} ticket(s) to process.")
    _log("=" * 60)

    results = []
    succeeded = 0
    failed = 0
    skipped = 0
    total_start = time.monotonic()

    for idx, (ticket_id, mechanism) in enumerate(rows, 1):
        _log(f"\n[pass5] [{idx}/{total}] Ticket {ticket_id}")
        _log(f"[pass5]   mechanism: {mechanism[:80]}{'…' if len(mechanism) > 80 else ''}")

        r = process_ticket(
            ticket_id,
            mechanism,
            prompt_template,
            prompt_version,
            force=force,
        )
        results.append(r)

        if r["status"] == "success":
            succeeded += 1
            _log(f"[pass5]   ✓ {r['cluster_key']}")
        elif r["status"] == "failed":
            failed += 1
            _log(f"[pass5]   ✗ error: {r['error']}")
        else:
            skipped += 1

        _log(f"[pass5]   elapsed: {r['elapsed_s']}s")

    total_elapsed = time.monotonic() - total_start

    # Summary
    _log(f"\n{'=' * 60}")
    _log("[pass5] Run complete.")
    _log(f"[pass5]   Total:     {total}")
    _log(f"[pass5]   Succeeded: {succeeded}")
    _log(f"[pass5]   Failed:    {failed}")
    _log(f"[pass5]   Skipped:   {skipped}")
    _log(f"[pass5]   Elapsed:   {total_elapsed:.1f}s")
    _log("=" * 60)

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=f"{stage_title('cluster_key')} from Pass 2 mechanism."
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

    args = parser.parse_args()

    tid_list = None
    if args.ticket_id:
        tid_list = [int(t.strip()) for t in args.ticket_id.split(",") if t.strip()]

    main(
        ticket_ids=tid_list,
        limit=args.limit,
        force=args.force,
        failed_only=args.failed_only,
    )
