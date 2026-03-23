"""
Legacy grammar pass — canonical failure grammar extraction from Pass 1 phenomenon.

Reads the phenomenon from a successful Pass 1 result, sends it to Matcha
with the Pass 2 prompt, parses the JSON response into structured fields
(component, operation, unexpected_state, canonical_failure), and stores
both the raw response and parsed output in ticket_llm_pass_results.

Deprecated. Pass 1 now performs the grammar extraction used by the active
pipeline. This script is retained only for backward compatibility.

Requires DATABASE_URL to be set (Postgres mode).

Usage:
    python run_ticket_pass2.py --limit 100
    python run_ticket_pass2.py --ticket-id 99784
    python run_ticket_pass2.py --ticket-id 99784,98154,100289
    python run_ticket_pass2.py --failed-only
    python run_ticket_pass2.py --force
"""

import argparse
import os
import sys
import time

from config import MATCHA_MISSION_ID
from matcha_client import call_matcha
from passes.runtime import process_ticket_pass
from pass2_parser import parse_pass2_response
from prompt_store import get_prompt

PASS_NAME = "pass2_grammar"
DEFAULT_PROMPT_VERSION = "1"
MODEL_NAME = f"matcha-{MATCHA_MISSION_ID}"

# Pass 1 dependency — which Pass 1 result to source phenomenon from
PASS1_PASS_NAME = "pass1_phenomenon"
DEFAULT_PASS1_PROMPT_VERSION = "1"


def _log(msg: str) -> None:
    print(msg, flush=True)


def _load_prompt_record() -> dict:
    return get_prompt(PASS_NAME, allow_fallback=False)


def _load_prompt_template() -> str:
    return _load_prompt_record()["content"]


def _coerce_prompt_version(value, default: str) -> str:
    if isinstance(value, int):
        return str(value)
    if isinstance(value, str) and value.strip().isdigit():
        return value
    return default


def _build_prompt(template: str, phenomenon: str) -> str:
    """Replace {{input_text}} placeholder in the prompt template."""
    return template.replace("{{input_text}}", phenomenon)


def process_ticket(
    ticket_id: int,
    phenomenon: str,
    prompt_template: str,
    prompt_version: str = DEFAULT_PROMPT_VERSION,
    *,
    force: bool = False,
) -> dict:
    """Process a single ticket through the legacy grammar pass.

    Returns a result dict with status, parsed fields, timing, etc.
    """
    full_prompt = _build_prompt(prompt_template, phenomenon)

    def _success_update(parsed_output):
        parsed_json, component, operation, unexpected_state, canonical_failure = parsed_output
        return {
            "parsed_json": parsed_json,
            "component": component,
            "operation": operation,
            "unexpected_state": unexpected_state,
            "canonical_failure": canonical_failure,
        }

    def _result_update(parsed_output):
        _, component, operation, unexpected_state, canonical_failure = parsed_output
        return {
            "component": component,
            "operation": operation,
            "unexpected_state": unexpected_state,
            "canonical_failure": canonical_failure,
        }

    return process_ticket_pass(
        ticket_id,
        pass_name=PASS_NAME,
        prompt_version=prompt_version,
        model_name=MODEL_NAME,
        input_text=phenomenon,
        prompt_text=full_prompt,
        force=force,
        initial_result={
            "component": None,
            "operation": None,
            "unexpected_state": None,
            "canonical_failure": None,
        },
        parse_response=parse_pass2_response,
        build_success_update=_success_update,
        build_result_update=_result_update,
        call_matcha_fn=call_matcha,
    )


def main(
    *,
    ticket_ids: list[int] | None = None,
    limit: int = 0,
    force: bool = False,
    failed_only: bool = False,
) -> list[dict]:
    """Run the legacy grammar pass for eligible tickets.

    Returns a list of result dicts (one per ticket processed).
    """
    import db

    if not db._is_enabled():
        _log("[pass2] DATABASE_URL is not set. Pass 2 requires a Postgres DB.")
        sys.exit(1)

    # Run migrations to ensure table/columns exist
    applied = db.migrate()
    if applied:
        from run_rollups import run_full_rollups
        run_full_rollups()

    prompt_record = _load_prompt_record()
    upstream_prompt = get_prompt(PASS1_PASS_NAME, allow_fallback=False)
    prompt_template = _load_prompt_template()
    prompt_version = _coerce_prompt_version(prompt_record.get("version"), DEFAULT_PROMPT_VERSION)
    pass1_prompt_version = _coerce_prompt_version(upstream_prompt.get("version"), DEFAULT_PASS1_PROMPT_VERSION)
    _log(f"[pass2] Pass: {PASS_NAME}  Prompt version: {prompt_version}  Model: {MODEL_NAME}")
    _log(f"[pass2] Requires Pass 1: {PASS1_PASS_NAME} v{pass1_prompt_version}")

    # Fetch eligible tickets (those with successful Pass 1 phenomenon)
    rows = db.fetch_pending_pass2_tickets(
        prompt_version,
        pass1_pass_name=PASS1_PASS_NAME,
        pass1_prompt_version=pass1_prompt_version,
        limit=limit,
        ticket_ids=ticket_ids,
        failed_only=failed_only,
        force=force,
    )

    total = len(rows)
    if total == 0:
        _log("[pass2] No eligible tickets found.")
        return []

    _log(f"[pass2] Found {total} ticket(s) to process.")
    _log("=" * 60)

    results = []
    succeeded = 0
    failed = 0
    skipped = 0
    total_start = time.monotonic()

    for idx, (ticket_id, phenomenon) in enumerate(rows, 1):
        _log(f"\n[pass2] [{idx}/{total}] Ticket {ticket_id}")
        _log(f"[pass2]   phenomenon: {phenomenon[:80]}{'…' if len(phenomenon) > 80 else ''}")

        r = process_ticket(
            ticket_id,
            phenomenon,
            prompt_template,
            prompt_version,
            force=force,
        )
        results.append(r)

        if r["status"] == "success":
            succeeded += 1
            _log(f"[pass2]   ✓ {r['canonical_failure']}")
        elif r["status"] == "failed":
            failed += 1
            _log(f"[pass2]   ✗ error: {r['error']}")
        else:
            skipped += 1

        _log(f"[pass2]   elapsed: {r['elapsed_s']}s")

    total_elapsed = time.monotonic() - total_start

    # Summary
    _log(f"\n{'=' * 60}")
    _log("[pass2] Run complete.")
    _log(f"[pass2]   Total:     {total}")
    _log(f"[pass2]   Succeeded: {succeeded}")
    _log(f"[pass2]   Failed:    {failed}")
    _log(f"[pass2]   Skipped:   {skipped}")
    _log(f"[pass2]   Elapsed:   {total_elapsed:.1f}s")
    _log("=" * 60)

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Legacy grammar pass retained for backward compatibility."
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
