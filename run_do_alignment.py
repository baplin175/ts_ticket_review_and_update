"""
DO alignment enrichment — compares each open ticket's status against its linked
Azure DevOps Delivery Order (DO) state and recent internal comments to detect
misalignment (e.g. ticket stalled, DO closed without resolution, or DO active
but Engineering comments reveal the issue was never reproduced).

Reads ticket + DO data from DB, calls Matcha LLM, and persists results to
ticket_do_alignment.

Requires DATABASE_URL to be set (Postgres mode).

Usage:
    python run_do_alignment.py                     # all tickets with a DO
    python run_do_alignment.py --ticket-id 105112
    python run_do_alignment.py --ticket-id 105112,110720
    python run_do_alignment.py --ticket-number 105112
    python run_do_alignment.py --force
    python run_do_alignment.py --limit 50
"""

import argparse
import hashlib
import json
import sys
import time
from datetime import datetime, timezone
from typing import Any, Optional

from config import MATCHA_MISSION_ID
from matcha_client import call_matcha
from prompt_store import get_prompt

PROMPT_NAME = "do_alignment"
MODEL_NAME = f"matcha-{MATCHA_MISSION_ID}"
DO_COMMENT_LIMIT = 8
DO_STATE_HISTORY_LIMIT = 10

_VALID_ALIGNED = {"Yes", "No", "Partial"}
_VALID_LABELS = {
    "aligned",
    "ticket_open_do_closed",
    "ticket_closed_do_active",
    "do_stalled_or_abandoned",
    "do_scope_mismatch",
    "unclear",
}


def _log(msg: str) -> None:
    print(msg, flush=True)


def _load_prompt_record() -> dict:
    return get_prompt(PROMPT_NAME, allow_fallback=True)


def _compute_input_hash(
    thread_hash: Optional[str],
    do_state: Optional[str],
    comments: list[dict],
) -> str:
    """Stable 16-char hash of the three key alignment inputs."""
    latest_comment_date = comments[0]["created_date"] if comments else ""
    raw = f"{thread_hash or ''}|{do_state or ''}|{latest_comment_date}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _build_input_block(ticket: dict, state_history: list[dict], comments: list[dict]) -> str:
    """Serialize ticket + DO context into the JSON block substituted into the prompt."""
    do_state_change = ticket.get("do_state_change_date")
    if hasattr(do_state_change, "isoformat"):
        do_state_change = do_state_change.isoformat()

    obj = {
        "ticket": {
            "ticket_number": ticket["ticket_number"],
            "status": ticket["status"],
            "customer": ticket["customer"],
            "days_open": ticket.get("days_opened"),
            "is_closed": ticket.get("closed_at") is not None,
            "latest_customer_message": (ticket.get("latest_customer_text") or "")[:600],
            "latest_internal_message": (ticket.get("latest_inhance_text") or "")[:600],
        },
        "delivery_order": {
            "do_number": ticket["do_number"],
            "title": ticket.get("do_title"),
            "state": ticket.get("do_state"),
            "assigned_to": ticket.get("do_assigned_to"),
            "state_changed_on": do_state_change,
        },
        "do_state_history": state_history,
        "do_recent_comments": comments,
    }
    return json.dumps(obj, default=str, indent=2)


def _build_prompt(template: str, input_block: str) -> str:
    return template.replace("{{input}}", input_block)


def _parse_response(raw: str) -> dict:
    """Parse the LLM JSON response, stripping markdown fences if present."""
    text = raw.strip()
    if text.startswith("```"):
        lines = text.splitlines()
        end = -1 if lines[-1].strip() == "```" else len(lines)
        text = "\n".join(lines[1:end])
    return json.loads(text)


def _validate_parsed(parsed: dict) -> None:
    if parsed.get("aligned") not in _VALID_ALIGNED:
        raise ValueError(f"Invalid aligned value: {parsed.get('aligned')!r}")
    if parsed.get("mismatch_label") not in _VALID_LABELS:
        raise ValueError(f"Invalid mismatch_label: {parsed.get('mismatch_label')!r}")


def process_ticket(
    ticket: dict,
    prompt_template: str,
    prompt_version: str,
    *,
    force: bool = False,
) -> dict:
    import db

    ticket_id = ticket["ticket_id"]
    ticket_number = ticket["ticket_number"]
    do_number = ticket["do_number"]
    do_state = ticket.get("do_state")

    # Fetch DO supplementary data from DB
    try:
        wid = int(do_number)
    except (TypeError, ValueError):
        return {
            "ticket_id": ticket_id,
            "ticket_number": ticket_number,
            "status": "skipped",
            "reason": f"do_number not a valid integer: {do_number!r}",
            "elapsed_s": 0.0,
        }

    state_history = db.fetch_do_state_transitions(wid, limit=DO_STATE_HISTORY_LIMIT)
    comments = db.fetch_do_comments(wid, limit=DO_COMMENT_LIMIT)

    # Skip if nothing meaningful has changed since the last run
    input_hash = _compute_input_hash(ticket.get("thread_hash"), do_state, comments)
    if not force and ticket.get("last_input_hash") == input_hash:
        return {
            "ticket_id": ticket_id,
            "ticket_number": ticket_number,
            "status": "skipped",
            "reason": "unchanged",
            "elapsed_s": 0.0,
        }

    input_block = _build_input_block(ticket, state_history, comments)
    prompt_text = _build_prompt(prompt_template, input_block)

    start = time.monotonic()
    raw_response = None
    try:
        raw_response = call_matcha(prompt_text)
        parsed = _parse_response(raw_response)
        _validate_parsed(parsed)
        elapsed = round(time.monotonic() - start, 2)

        db.insert_do_alignment(
            ticket_id,
            ticket_number=ticket_number,
            do_number=do_number,
            do_state=do_state,
            aligned=parsed.get("aligned"),
            mismatch_label=parsed.get("mismatch_label"),
            explanation=parsed.get("explanation"),
            model_name=MODEL_NAME,
            prompt_name=PROMPT_NAME,
            prompt_version=prompt_version,
            input_hash=input_hash,
            raw_response=parsed,
        )

        return {
            "ticket_id": ticket_id,
            "ticket_number": ticket_number,
            "status": "success",
            "aligned": parsed.get("aligned"),
            "mismatch_label": parsed.get("mismatch_label"),
            "explanation": parsed.get("explanation"),
            "elapsed_s": elapsed,
        }

    except Exception as exc:
        elapsed = round(time.monotonic() - start, 2)
        return {
            "ticket_id": ticket_id,
            "ticket_number": ticket_number,
            "status": "failed",
            "error": str(exc),
            "raw_response": raw_response,
            "elapsed_s": elapsed,
        }


def main(
    *,
    ticket_ids: Optional[list[int]] = None,
    ticket_numbers: Optional[list[str]] = None,
    limit: int = 0,
    force: bool = False,
) -> list[dict]:
    """Run DO alignment enrichment.  Returns a list of result dicts."""
    import db

    if not db._is_enabled():
        _log("[do-alignment] DATABASE_URL is not set. Requires a Postgres DB.")
        sys.exit(1)

    db.migrate()

    prompt_record = _load_prompt_record()
    prompt_template = prompt_record["content"]
    prompt_version = str(prompt_record.get("version", "1"))

    _log(f"[do-alignment] Prompt: {PROMPT_NAME} v{prompt_version}  Model: {MODEL_NAME}")

    tickets = db.fetch_tickets_for_do_alignment(
        ticket_ids=ticket_ids,
        ticket_numbers=ticket_numbers,
        limit=limit,
    )

    total = len(tickets)
    if total == 0:
        _log("[do-alignment] No eligible tickets with linked DOs found.")
        return []

    _log(f"[do-alignment] Found {total} ticket(s) with linked DOs.")
    _log("=" * 60)

    results = []
    succeeded = skipped = failed = 0
    total_start = time.monotonic()

    for idx, ticket in enumerate(tickets, 1):
        _log(
            f"\n[do-alignment] [{idx}/{total}] Ticket {ticket['ticket_number']} "
            f"— DO #{ticket['do_number']} ({ticket.get('do_state')})"
        )

        r = process_ticket(ticket, prompt_template, prompt_version, force=force)
        results.append(r)

        if r["status"] == "success":
            succeeded += 1
            _log(f"[do-alignment]   ✓ aligned={r['aligned']}  label={r['mismatch_label']}")
            if r.get("explanation"):
                _log(f"[do-alignment]   {r['explanation'][:140]}…")
        elif r["status"] == "skipped":
            skipped += 1
            _log(f"[do-alignment]   — skipped ({r.get('reason', 'unchanged')})")
        else:
            failed += 1
            _log(f"[do-alignment]   ✗ {r.get('error')}")

        _log(f"[do-alignment]   elapsed: {r['elapsed_s']}s")

    total_elapsed = round(time.monotonic() - total_start, 2)
    _log(
        f"\n[do-alignment] Done in {total_elapsed}s — "
        f"{succeeded} succeeded, {skipped} skipped, {failed} failed."
    )
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DO / ticket alignment enrichment.")
    parser.add_argument(
        "--ticket-id", dest="ticket_id",
        help="Comma-separated internal ticket IDs to process.",
    )
    parser.add_argument(
        "--ticket-number", dest="ticket_number",
        help="Comma-separated ticket numbers to process.",
    )
    parser.add_argument(
        "--limit", type=int, default=0,
        help="Maximum number of tickets to process (0 = no limit).",
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Re-run even if inputs have not changed since the last run.",
    )
    args = parser.parse_args()

    _ticket_ids = (
        [int(x.strip()) for x in args.ticket_id.split(",") if x.strip()]
        if args.ticket_id else None
    )
    _ticket_numbers = (
        [x.strip() for x in args.ticket_number.split(",") if x.strip()]
        if args.ticket_number else None
    )

    main(
        ticket_ids=_ticket_ids,
        ticket_numbers=_ticket_numbers,
        limit=args.limit,
        force=args.force,
    )
