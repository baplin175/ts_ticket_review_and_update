"""
Pass 2 — Failure mechanism inference from Pass 1 canonical failure.

Reads the canonical_failure from a successful Pass 1 result, sends it to
Matcha with the mechanism prompt, parses the JSON response into a structured
mechanism field, and stores both the raw response and parsed output in
ticket_llm_pass_results.

Requires DATABASE_URL to be set (Postgres mode).

Usage:
    python run_ticket_pass3.py --limit 100
    python run_ticket_pass3.py --ticket-id 99784
    python run_ticket_pass3.py --ticket-id 99784,98154,100289
    python run_ticket_pass3.py --failed-only
    python run_ticket_pass3.py --force
"""

import argparse
import os
import sys
import time

from config import MATCHA_MISSION_ID
from passes.runtime import load_prompt_template, process_ticket_pass
from pass3_parser import parse_pass3_response, validate_mechanism
from pipeline_stages import stage_title

PROMPT_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "prompts", "pass3_mechanism.txt"
)

PASS_NAME = "pass3_mechanism"
PROMPT_VERSION = "3"
MODEL_NAME = f"matcha-{MATCHA_MISSION_ID}"

# Upstream dependency — source canonical_failure from Pass 1 v2
PASS2_PASS_NAME = "pass1_phenomenon"
PASS2_PROMPT_VERSION = "2"


def _log(msg: str) -> None:
    print(msg, flush=True)


def _load_prompt_template() -> str:
    return load_prompt_template(PROMPT_PATH)


def _build_prompt(template: str, canonical_failure: str, thread_context: str = "") -> str:
    """Replace {{input_text}} and {{thread_context}} placeholders in the prompt template."""
    prompt = template.replace("{{input_text}}", canonical_failure)
    # Cap thread context at 3000 chars to stay within token limits
    trimmed_context = thread_context[:3000] if thread_context else "(no thread context available)"
    prompt = prompt.replace("{{thread_context}}", trimmed_context)
    return prompt


def process_ticket(
    ticket_id: int,
    canonical_failure: str,
    prompt_template: str,
    *,
    thread_context: str = "",
    force: bool = False,
) -> dict:
    """Process a single ticket through the user-facing Pass 2 stage.

    Returns a result dict with status, parsed fields, timing, etc.
    """
    full_prompt = _build_prompt(prompt_template, canonical_failure, thread_context)

    def _success_update(parsed_output):
        parsed_json, mechanism = parsed_output
        return {
            "parsed_json": parsed_json,
            "mechanism": mechanism,
        }

    def _result_update(parsed_output):
        _, mechanism = parsed_output
        return {"mechanism": mechanism}

    def _validate_parsed(parsed_output):
        _, mechanism = parsed_output
        validate_mechanism(mechanism, canonical_failure)

    return process_ticket_pass(
        ticket_id,
        pass_name=PASS_NAME,
        prompt_version=PROMPT_VERSION,
        model_name=MODEL_NAME,
        input_text=canonical_failure,
        prompt_text=full_prompt,
        force=force,
        initial_result={"mechanism": None},
        parse_response=parse_pass3_response,
        build_success_update=_success_update,
        build_result_update=_result_update,
        validate_parsed=_validate_parsed,
    )


def main(
    *,
    ticket_ids: list[int] | None = None,
    limit: int = 0,
    force: bool = False,
    failed_only: bool = False,
) -> list[dict]:
    """Run the user-facing Pass 2 stage for eligible tickets.

    Returns a list of result dicts (one per ticket processed).
    """
    import db

    if not db._is_enabled():
        _log("[pass3] DATABASE_URL is not set. Pass 2 requires a Postgres DB.")
        sys.exit(1)

    # Run migrations to ensure table/columns exist
    applied = db.migrate()
    if applied:
        from run_rollups import run_full_rollups
        run_full_rollups()

    prompt_template = _load_prompt_template()
    _log(f"[pass3] Loaded prompt from {PROMPT_PATH}")
    _log(f"[pass3] Stage: {stage_title('mechanism')}  Internal pass: {PASS_NAME}  Prompt version: {PROMPT_VERSION}  Model: {MODEL_NAME}")
    _log(f"[pass3] Requires Pass 1: {PASS2_PASS_NAME} v{PASS2_PROMPT_VERSION}")

    # Fetch eligible tickets (those with successful Pass 1 canonical_failure)
    rows = db.fetch_pending_pass3_tickets(
        PROMPT_VERSION,
        pass2_pass_name=PASS2_PASS_NAME,
        pass2_prompt_version=PASS2_PROMPT_VERSION,
        limit=limit,
        ticket_ids=ticket_ids,
        failed_only=failed_only,
        force=force,
    )

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

    for idx, (ticket_id, canonical_failure, thread_context) in enumerate(rows, 1):
        _log(f"\n[pass3] [{idx}/{total}] Ticket {ticket_id}")
        _log(f"[pass3]   canonical_failure: {canonical_failure[:80]}{'…' if len(canonical_failure) > 80 else ''}")

        r = process_ticket(
            ticket_id,
            canonical_failure,
            prompt_template,
            thread_context=thread_context,
            force=force,
        )
        results.append(r)

        if r["status"] == "success":
            succeeded += 1
            _log(f"[pass3]   ✓ {r['mechanism']}")
        elif r["status"] == "failed":
            failed += 1
            _log(f"[pass3]   ✗ error: {r['error']}")
        else:
            skipped += 1

        _log(f"[pass3]   elapsed: {r['elapsed_s']}s")

    total_elapsed = time.monotonic() - total_start

    # Summary
    _log(f"\n{'=' * 60}")
    _log("[pass3] Run complete.")
    _log(f"[pass3]   Total:     {total}")
    _log(f"[pass3]   Succeeded: {succeeded}")
    _log(f"[pass3]   Failed:    {failed}")
    _log(f"[pass3]   Skipped:   {skipped}")
    _log(f"[pass3]   Elapsed:   {total_elapsed:.1f}s")
    _log("=" * 60)

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=f"{stage_title('mechanism')} from Pass 1 canonical failure."
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

    # Parse ticket IDs
    ticket_ids = None
    if args.ticket_id:
        ticket_ids = [int(tid.strip()) for tid in args.ticket_id.split(",") if tid.strip()]

    main(
        ticket_ids=ticket_ids,
        limit=args.limit,
        force=args.force,
        failed_only=args.failed_only,
    )
