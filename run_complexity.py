"""
Part 4 — Read the activities JSON, build a prompt from complexity.md
for each ticket, call Matcha for complexity scoring, and save results.

Usage:
    python run_complexity.py
    TARGET_TICKET=29696,110554 python run_complexity.py
"""

import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from config import OUTPUT_DIR, RUN_COMPLEXITY, TARGET_TICKETS
from matcha_client import call_matcha

PROMPT_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "prompts", "complexity.md"
)


def _log(msg: str) -> None:
    print(msg, flush=True)


def _run_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _load_prompt() -> str:
    with open(PROMPT_PATH, "r", encoding="utf-8") as f:
        return f.read()


def _latest_activities_file() -> str | None:
    out = Path(OUTPUT_DIR)
    files = sorted(out.glob("activities_*.json"), reverse=True)
    return str(files[0]) if files else None


def _load_tickets(json_path: str, ticket_numbers: list[str] | None) -> list[dict]:
    with open(json_path, "r", encoding="utf-8") as f:
        tickets = json.load(f)
    if ticket_numbers:
        nums = set(ticket_numbers)
        tickets = [t for t in tickets if t.get("ticket_number") in nums]
    return tickets


def _build_ticket_history(ticket: dict) -> str:
    """Build a text representation of the ticket for the prompt."""
    lines = [
        f"Ticket ID: {ticket.get('ticket_id', '')}",
        f"Ticket Number: {ticket.get('ticket_number', '')}",
        f"Ticket Name: {ticket.get('ticket_name', '')}",
        f"Date Created: {ticket.get('date_created', '')}",
        f"Date Modified: {ticket.get('date_modified', '')}",
        f"Days Opened: {ticket.get('days_opened', '')}",
        f"Days Since Modified: {ticket.get('days_since_modified', '')}",
        f"Status: {ticket.get('status', '')}",
        f"Severity: {ticket.get('severity', '')}",
        f"Product: {ticket.get('product_name', '')}",
        f"Assignee: {ticket.get('assignee', '')}",
        f"Customer: {ticket.get('customer', '')}",
        "",
        "--- Activity History (chronological) ---",
        "",
    ]

    for a in ticket.get("activities", []):
        lines.append(
            f"[{a.get('created_at', '')}] "
            f"({a.get('action_type', 'Unknown')}) "
            f"[{a.get('party', '').upper()}] "
            f"{a.get('creator_name', '')}: "
            f"{a.get('description', '')}"
        )
        lines.append("")

    return "\n".join(lines)


def _parse_json_response(raw: str) -> dict | None:
    """Extract the JSON object from Matcha's response text."""
    try:
        result = json.loads(raw)
        if isinstance(result, dict):
            return result
        if isinstance(result, list) and result:
            return result[0]
    except json.JSONDecodeError:
        pass

    match = re.search(r"\{.*\}", raw, re.DOTALL)
    if match:
        try:
            obj = json.loads(match.group())
            if isinstance(obj, dict):
                return obj
        except json.JSONDecodeError:
            pass

    return None


def main() -> None:
    if not TARGET_TICKETS:
        _log("[complexity] TARGET_TICKET is required. Set it as an env var.")
        sys.exit(1)

    # 1. Locate most recent activities file
    activities_file = _latest_activities_file()
    if not activities_file:
        _log(f"[complexity] No activities JSON found in {OUTPUT_DIR}. Run run_pull_activities.py first.")
        sys.exit(1)
    _log(f"[complexity] Using activities file: {activities_file}")

    # 2. Load ticket data
    tickets = _load_tickets(activities_file, TARGET_TICKETS)
    if not tickets:
        _log(f"[complexity] Ticket(s) {', '.join(TARGET_TICKETS)} not found in {activities_file}.")
        sys.exit(1)
    _log(f"[complexity] Loaded {len(tickets)} ticket(s).")

    prompt_template = _load_prompt()
    all_results = []

    for ticket in tickets:
        tnum = ticket.get("ticket_number", "?")
        _log(f"[complexity] Scoring ticket {tnum}...")

        # 3. Build prompt with ticket history
        ticket_history = _build_ticket_history(ticket)
        full_prompt = prompt_template.replace("{{TICKET_HISTORY}}", ticket_history)

        # 4. Call Matcha
        try:
            raw_reply = call_matcha(full_prompt, timeout=600)
        except Exception as e:
            _log(f"[complexity] Matcha call failed for ticket {tnum}: {e}")
            continue

        _log(f"[complexity] Matcha response for {tnum}:\n{raw_reply}")

        # 5. Parse response
        result = _parse_json_response(raw_reply)
        if not result:
            _log(f"[complexity] Could not parse response for ticket {tnum}.")
            continue

        all_results.append(result)

    _log(f"[complexity] Scored {len(all_results)}/{len(tickets)} ticket(s).")

    # 6. Save results
    ts = _run_timestamp()
    out_path = os.path.join(OUTPUT_DIR, f"complexity_{ts}.json")
    output = {
        "source_file": os.path.basename(activities_file),
        "tickets_scored": len(all_results),
        "results": all_results,
    }
    with open(out_path, "w", encoding="utf-8") as fout:
        json.dump(output, fout, ensure_ascii=False, indent=2)

    _log(f"[complexity] Results saved to {out_path}")


if __name__ == "__main__":
    if not RUN_COMPLEXITY:
        print("[complexity] Skipped (RUN_COMPLEXITY=0).")
    else:
        main()
