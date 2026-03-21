"""
Pass 1 — Phenomenon extraction + grammar decomposition from support ticket threads.

Reads full_thread_text and ticket_name from tickets/rollups, sends to Matcha
with the Pass 1 v2 prompt, parses the JSON response (phenomenon + component +
operation + unexpected_state + confidence), and stores both the raw response
and parsed output in ticket_llm_pass_results.

This pass now absorbs the work previously done by Pass 2 (grammar extraction).

Requires DATABASE_URL to be set (Postgres mode).

Usage:
    python run_ticket_pass1.py --limit 100
    python run_ticket_pass1.py --ticket-id 99784
    python run_ticket_pass1.py --ticket-id 99784,98154,100289
    python run_ticket_pass1.py --failed-only
    python run_ticket_pass1.py --force
    python run_ticket_pass1.py --since 2026-03-01
"""

import argparse
import os
import re
import sys
import time

from config import MATCHA_MISSION_ID
from passes.runtime import load_prompt_template, process_ticket_pass
from pass1_parser import parse_pass1_response

PROMPT_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "prompts", "pass1_phenomenon.txt"
)

PASS_NAME = "pass1_phenomenon"
PROMPT_VERSION = "2"
MODEL_NAME = f"matcha-{MATCHA_MISSION_ID}"

# Pattern to strip automated violation/SLA warning lines
_VIOLATION_RE = re.compile(
    r"^.*(?:Ticket\s+\d+\s+is\s+in\s+violation|"
    r"Warning:\s*Ticket\s+\d+|"
    r"SLA\s+violation\s+notice).*$",
    re.MULTILINE | re.IGNORECASE,
)


def _log(msg: str) -> None:
    print(msg, flush=True)


def _load_prompt_template() -> str:
    return load_prompt_template(PROMPT_PATH)


def _strip_violation_warnings(text: str) -> str:
    """Remove automated violation/SLA warning lines from thread text."""
    return _VIOLATION_RE.sub("", text).strip()


def _build_prompt(template: str, full_thread_text: str, ticket_name: str = "") -> str:
    """Replace {{input_text}} and {{ticket_name}} placeholders in the prompt template."""
    prompt = template.replace("{{ticket_name}}", ticket_name or "(no title)")
    prompt = prompt.replace("{{input_text}}", full_thread_text)
    return prompt


def process_ticket(
    ticket_id: int,
    full_thread_text: str,
    prompt_template: str,
    *,
    ticket_name: str = "",
    force: bool = False,
) -> dict:
    """Process a single ticket through Pass 1.

    Returns a result dict with status, phenomenon, grammar fields, timing, etc.
    """
    # Strip violation warnings before building prompt
    cleaned_thread = _strip_violation_warnings(full_thread_text)
    full_prompt = _build_prompt(prompt_template, cleaned_thread, ticket_name)

    def _success_update(parsed_output):
        parsed_json, phenomenon = parsed_output
        return {
            "parsed_json": parsed_json,
            "phenomenon": phenomenon,
            "component": parsed_json.get("component"),
            "operation": parsed_json.get("operation"),
            "unexpected_state": parsed_json.get("unexpected_state"),
            "canonical_failure": parsed_json.get("canonical_failure"),
        }

    def _result_update(parsed_output):
        parsed_json, phenomenon = parsed_output
        return {
            "phenomenon": phenomenon,
            "component": parsed_json.get("component"),
            "operation": parsed_json.get("operation"),
            "unexpected_state": parsed_json.get("unexpected_state"),
            "canonical_failure": parsed_json.get("canonical_failure"),
            "confidence": parsed_json.get("confidence"),
        }

    result = process_ticket_pass(
        ticket_id,
        pass_name=PASS_NAME,
        prompt_version=PROMPT_VERSION,
        model_name=MODEL_NAME,
        input_text=full_thread_text,
        prompt_text=full_prompt,
        force=force,
        initial_result={
            "phenomenon": None,
            "component": None,
            "operation": None,
            "unexpected_state": None,
            "canonical_failure": None,
            "confidence": None,
        },
        parse_response=parse_pass1_response,
        build_success_update=_success_update,
        build_result_update=_result_update,
    )
    if result["status"] == "success" and result["phenomenon"] is None:
        _log(f"[pass1]   (no observable phenomenon — confidence: {result.get('confidence', 'N/A')})")
    return result


def main(
    *,
    ticket_ids: list[int] | None = None,
    limit: int = 0,
    force: bool = False,
    failed_only: bool = False,
    since: str | None = None,
) -> list[dict]:
    """Run Pass 1 for eligible tickets.

    Returns a list of result dicts (one per ticket processed).
    """
    import db

    if not db._is_enabled():
        _log("[pass1] DATABASE_URL is not set. Pass 1 requires a Postgres DB.")
        sys.exit(1)

    # Run migrations to ensure table exists
    applied = db.migrate()
    if applied:
        from run_rollups import run_full_rollups
        run_full_rollups()

    prompt_template = _load_prompt_template()
    _log(f"[pass1] Loaded prompt from {PROMPT_PATH}")
    _log(f"[pass1] Pass: {PASS_NAME}  Prompt version: {PROMPT_VERSION}  Model: {MODEL_NAME}")

    # Fetch eligible tickets
    rows = db.fetch_pending_pass1_tickets(
        PROMPT_VERSION,
        limit=limit,
        ticket_ids=ticket_ids,
        failed_only=failed_only,
        force=force,
        since=since,
    )

    total = len(rows)
    if total == 0:
        _log("[pass1] No eligible tickets found.")
        return []

    _log(f"[pass1] Found {total} ticket(s) to process.")
    _log("=" * 60)

    results = []
    succeeded = 0
    failed = 0
    skipped = 0
    total_start = time.monotonic()

    for idx, (ticket_id, ticket_name, full_thread_text) in enumerate(rows, 1):
        _log(f"\n[pass1] [{idx}/{total}] Ticket {ticket_id}")

        r = process_ticket(
            ticket_id,
            full_thread_text,
            prompt_template,
            ticket_name=ticket_name,
            force=force,
        )
        results.append(r)

        if r["status"] == "success":
            succeeded += 1
            if r["phenomenon"]:
                _log(f"[pass1]   ✓ phenomenon: {r['phenomenon'][:80]}{'…' if len(r['phenomenon']) > 80 else ''}")
                if r.get("canonical_failure"):
                    _log(f"[pass1]   ✓ grammar: {r['canonical_failure'][:80]}")
            else:
                _log(f"[pass1]   ✓ no observable phenomenon")
        elif r["status"] == "failed":
            failed += 1
            _log(f"[pass1]   ✗ error: {r['error']}")
        else:
            skipped += 1

        _log(f"[pass1]   elapsed: {r['elapsed_s']}s")

    total_elapsed = time.monotonic() - total_start

    # Summary
    _log(f"\n{'=' * 60}")
    _log(f"[pass1] Run complete.")
    _log(f"[pass1]   Total:     {total}")
    _log(f"[pass1]   Succeeded: {succeeded}")
    _log(f"[pass1]   Failed:    {failed}")
    _log(f"[pass1]   Skipped:   {skipped}")
    _log(f"[pass1]   Elapsed:   {total_elapsed:.1f}s")
    _log("=" * 60)

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Pass 1 — Phenomenon extraction from support ticket threads."
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
        "--since",
        type=str,
        default=None,
        help="Only process tickets created after this date (ISO 8601, e.g. 2026-03-01).",
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
        since=args.since,
    )
