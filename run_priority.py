"""
Part 3 — Read the activities JSON, build a prompt from ai_priority.md
with the ticket data as input, call Matcha for AI priority scoring,
parse the structured JSON response, write it back to TeamSupport, and
save the results locally.

Usage:
    TARGET_TICKET=29696 python run_priority.py
"""

import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from config import OUTPUT_DIR, RUN_PRIORITY, TARGET_TICKETS, TS_BASE
from matcha_client import call_matcha
from ts_client import ts_put

PROMPT_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "prompts", "ai_priority.md"
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


def _build_input_block(ticket: dict) -> dict:
    """Build the input data block that the prompt expects for one ticket."""
    activities = []
    for a in ticket.get("activities", []):
        activities.append({
            "date": a.get("created_at", ""),
            "party": a.get("party", ""),
            "description": a.get("description", ""),
        })

    return {
        "ticket_number": ticket.get("ticket_number", ""),
        "ticket_name": ticket.get("ticket_name", ""),
        "date_created": ticket.get("date_created", ""),
        "date_modified": ticket.get("date_modified", ""),
        "days_opened": ticket.get("days_opened", ""),
        "days_since_modified": ticket.get("days_since_modified", ""),
        "status": ticket.get("status", ""),
        "severity": ticket.get("severity", ""),
        "product_name": ticket.get("product_name", ""),
        "assignee": ticket.get("assignee", ""),
        "customer": ticket.get("customer", ""),
        "activities": activities,
    }


def _parse_json_response(raw: str) -> list[dict]:
    """Extract the JSON array from Matcha's response text."""
    # Try direct parse first
    try:
        result = json.loads(raw)
        if isinstance(result, list):
            return result
        if isinstance(result, dict):
            return [result]
    except json.JSONDecodeError:
        pass

    # Try to find JSON array in the text
    match = re.search(r"\[.*\]", raw, re.DOTALL)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass

    # Try to find JSON object
    match = re.search(r"\{.*\}", raw, re.DOTALL)
    if match:
        try:
            obj = json.loads(match.group())
            return [obj] if isinstance(obj, dict) else []
        except json.JSONDecodeError:
            pass

    return []


def _last_comment_timestamps(ticket: dict) -> tuple[str, str]:
    """Return (last_inh_comment, last_cust_comment) timestamps from activities."""
    last_inh = ""
    last_cust = ""
    for a in ticket.get("activities", []):
        ts = a.get("created_at", "")
        if not ts:
            continue
        if a.get("party") == "inh":
            last_inh = ts
        elif a.get("party") == "cust":
            last_cust = ts
    return last_inh, last_cust


def _write_back_to_ts(ticket_id: str, ticket_number: str, priority_result: dict,
                       ticket_data: dict) -> bool:
    """PUT AIPriority, AIPriExpln, AILastUpdate, LastInhComment, LastCustComment back to TeamSupport."""
    priority = str(priority_result.get("priority", "")).strip()
    explanation = str(priority_result.get("priority_explanation", "")).strip()

    if not priority or not explanation:
        _log(f"  [ts] Missing priority or explanation for {ticket_number}; skipping write-back.")
        return False

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    last_inh, last_cust = _last_comment_timestamps(ticket_data)

    ticket_payload = {
        "AIPriority": priority,
        "AIPriExpln": explanation,
        "AILastUpdate": now_str,
    }
    if last_inh:
        ticket_payload["LastInhComment"] = last_inh
    if last_cust:
        ticket_payload["LastCustComment"] = last_cust

    payload = {"Ticket": ticket_payload}

    try:
        ts_put(f"{TS_BASE}/Tickets/{ticket_id}", payload)
        _log(f"  [ts] Wrote back AI fields for ticket {ticket_number}"
             f" (LastInh={last_inh or 'n/a'}, LastCust={last_cust or 'n/a'}).")
        return True
    except Exception as e:
        _log(f"  [ts] Failed to write back AI fields for {ticket_number}: {e}")
        return False


def main(activities_file: str | None = None) -> None:
    if not TARGET_TICKETS:
        _log("[priority] TARGET_TICKET is required. Set it as an env var (comma-delimited for multiple).")
        sys.exit(1)

    # 1. Locate most recent activities file
    if not activities_file:
        activities_file = _latest_activities_file()
    if not activities_file:
        _log(f"[priority] No activities JSON found in {OUTPUT_DIR}. Run run_pull_activities.py first.")
        sys.exit(1)
    _log(f"[priority] Using activities file: {activities_file}")

    # 2. Load ticket data
    tickets = _load_tickets(activities_file, TARGET_TICKETS)
    if not tickets:
        _log(f"[priority] Ticket(s) {', '.join(TARGET_TICKETS)} not found in {activities_file}.")
        sys.exit(1)
    _log(f"[priority] Loaded {len(tickets)} ticket(s).")

    # 3. Build prompt
    prompt_instructions = _load_prompt()
    input_data = [_build_input_block(t) for t in tickets]
    input_json = json.dumps(input_data, ensure_ascii=False, indent=2)

    full_prompt = f"""{prompt_instructions}

--- INPUT DATA ---
{input_json}"""

    _log(f"[priority] Calling Matcha with {len(tickets)} ticket(s)...")

    # 4. Call Matcha
    try:
        raw_reply = call_matcha(full_prompt, timeout=600)
    except Exception as e:
        _log(f"[priority] Matcha call failed: {e}")
        sys.exit(1)

    _log(f"[priority] Matcha raw response:\n{raw_reply}")

    # 5. Parse response
    results = _parse_json_response(raw_reply)
    if not results:
        _log("[priority] Could not parse Matcha response as JSON.")
        sys.exit(1)

    _log(f"[priority] Parsed {len(results)} priority result(s).")

    # 6. Write back to TeamSupport
    updated = 0
    # Build lookups from ticket_number -> ticket_id and ticket_number -> ticket_data
    tid_map = {t["ticket_number"]: t["ticket_id"] for t in tickets}
    ticket_data_map = {t["ticket_number"]: t for t in tickets}

    for result in results:
        tnum = str(result.get("ticket_number", "")).strip()
        tid = tid_map.get(tnum)
        if not tid:
            _log(f"  [priority] No ticket_id for {tnum}; skipping write-back.")
            continue
        tdata = ticket_data_map.get(tnum, {})
        if _write_back_to_ts(tid, tnum, result, tdata):
            updated += 1

    _log(f"[priority] Updated {updated}/{len(results)} ticket(s) in TeamSupport.")

    # 7. Save results locally
    ts = _run_timestamp()
    out_path = os.path.join(OUTPUT_DIR, f"priority_{ts}.json")
    output = {
        "source_file": os.path.basename(activities_file),
        "tickets_sent": len(tickets),
        "results": results,
        "writeback_count": updated,
    }
    with open(out_path, "w", encoding="utf-8") as fout:
        json.dump(output, fout, ensure_ascii=False, indent=2)

    _log(f"[priority] Results saved to {out_path}")


if __name__ == "__main__":
    if not RUN_PRIORITY:
        print("[priority] Skipped (RUN_PRIORITY=0).")
    else:
        main()
