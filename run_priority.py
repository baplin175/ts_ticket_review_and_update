"""
Part 3 — Read the activities JSON, build a prompt from ai_priority.md
with the ticket data as input, call Matcha for AI priority scoring,
parse the structured JSON response, write it back to TeamSupport, and
save the results locally.

When DATABASE_URL is set:
  - Can read ticket data from DB (avoiding need for activities JSON)
  - Checks thread_hash to skip unchanged tickets (unless --force)
  - Persists results to ticket_priority_scores table
  - Still emits the JSON artifact and supports TS write-back

When DATABASE_URL is not set:
  - Falls back to JSON-only mode (existing behaviour)

Usage:
    TARGET_TICKET=29696 python run_priority.py
    TARGET_TICKET=29696 python run_priority.py --force
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from config import FORCE_ENRICHMENT, OUTPUT_DIR, RUN_PRIORITY, TARGET_TICKETS, MATCHA_MISSION_ID, TS_WRITEBACK, SKIP_OUTPUT_FILES
from matcha_client import call_matcha
from prompt_store import get_prompt
from ts_client import update_ticket

PROMPT_NAME = "ai_priority"
MODEL_NAME = f"matcha-{MATCHA_MISSION_ID}"


def _log(msg: str) -> None:
    print(msg, flush=True)


def _json_default(obj):
    """Handle non-standard types (e.g. Decimal from Postgres) in JSON serialization."""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")


def _run_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _load_prompt() -> str:
    return get_prompt(PROMPT_NAME, allow_fallback=True)["content"]


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


def _load_tickets_from_db(ticket_numbers: list[str]) -> list[dict]:
    """Load ticket data from DB in the same shape as JSON pipeline."""
    import db
    tid_map = db.ticket_ids_for_numbers(ticket_numbers)
    tickets = []
    for tnum in ticket_numbers:
        tid = tid_map.get(tnum)
        if not tid:
            continue
        t = db.load_ticket_with_actions(tid)
        if t:
            tickets.append(t)
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


def _should_skip(ticket_id: int, force: bool) -> bool:
    """Return True if the ticket's priority is up-to-date (hash unchanged)."""
    if force:
        return False
    import db
    if not db._is_enabled():
        return False
    hashes = db.get_current_hashes(ticket_id)
    current_hash = hashes.get("thread_hash")
    if not current_hash:
        return False
    last_hash = db.get_latest_enrichment_hash(ticket_id, "priority")
    return last_hash == current_hash


def _persist_to_db(ticket_id: int, ticket_number: str | None, thread_hash: str | None,
                   result: dict, raw_reply: str, prompt_version: str) -> None:
    """Insert priority result into DB."""
    import db
    if not db._is_enabled():
        return
    priority_val = result.get("priority")
    try:
        priority_val = int(priority_val)
    except (TypeError, ValueError):
        priority_val = None
    db.insert_priority(
        ticket_id,
        ticket_number=ticket_number,
        thread_hash=thread_hash,
        model_name=MODEL_NAME,
        prompt_name=PROMPT_NAME,
        prompt_version=prompt_version,
        priority=priority_val,
        priority_explanation=result.get("priority_explanation"),
        raw_response={"parsed": result, "raw_text": raw_reply},
    )


def main(activities_file: str | None = None, write_back: bool | None = None,
         *, force: bool = False, ticket_numbers: list[str] | None = None) -> dict:
    """Run priority scoring.  Returns a dict mapping ticket_number to
    ``{"ticket_id": ..., "fields": {...}, "activities": [...]}`` so the
    caller can merge fields with other stages before a single API call.
    When *write_back* is True (default / standalone), the function also
    writes each ticket back to TeamSupport individually.
    """
    if write_back is None:
        write_back = TS_WRITEBACK

    target_tickets = ticket_numbers or TARGET_TICKETS
    if not target_tickets:
        _log("[priority] TARGET_TICKET is required. Set it as an env var (comma-delimited for multiple).")
        sys.exit(1)

    prompt_record = get_prompt(PROMPT_NAME, allow_fallback=True)
    prompt_template = prompt_record["content"]
    prompt_version = prompt_record["version"]
    _log(f"[priority] Prompt: {PROMPT_NAME} v{prompt_version}")

    # Check if DB mode is available
    try:
        import db
        db_enabled = db._is_enabled()
    except Exception:
        db_enabled = False

    # Resolve ticket_number → ticket_id map (for DB mode)
    tid_map: dict[str, int] = {}
    hash_map: dict[str, str | None] = {}  # ticket_number → thread_hash
    if db_enabled:
        tid_map = db.ticket_ids_for_numbers(target_tickets)
        for tnum, tid in tid_map.items():
            hashes = db.get_current_hashes(tid)
            hash_map[tnum] = hashes.get("thread_hash")

    # Filter tickets by hash-based skip logic
    tickets_to_score = []
    skipped = []
    for tnum in target_tickets:
        tid = tid_map.get(tnum)
        if tid and _should_skip(tid, force):
            _log(f"[priority] Ticket {tnum} thread unchanged (hash match). Skipping. Use --force to override.")
            skipped.append(tnum)
        else:
            tickets_to_score.append(tnum)

    if not tickets_to_score:
        _log("[priority] All tickets skipped (unchanged). Nothing to score.")
        return {}

    # 1. Load ticket data
    if db_enabled and tid_map:
        tickets = _load_tickets_from_db(tickets_to_score)
        _log(f"[priority] Loaded {len(tickets)} ticket(s) from DB.")
    else:
        if not activities_file:
            activities_file = _latest_activities_file()
        if not activities_file:
            _log(f"[priority] No activities JSON found in {OUTPUT_DIR}. Run run_pull_activities.py first.")
            sys.exit(1)
        _log(f"[priority] Using activities file: {activities_file}")
        tickets = _load_tickets(activities_file, tickets_to_score)

    if not tickets:
        _log(f"[priority] Ticket(s) {', '.join(tickets_to_score)} not found.")
        sys.exit(1)
    _log(f"[priority] Scoring {len(tickets)} ticket(s).")

    # 2. Build prompt
    prompt_instructions = prompt_template
    input_data = [_build_input_block(t) for t in tickets]
    input_json = json.dumps(input_data, ensure_ascii=False, indent=2, default=_json_default)

    full_prompt = f"""{prompt_instructions}

--- INPUT DATA ---
{input_json}"""

    _log(f"[priority] Calling Matcha with {len(tickets)} ticket(s)...")

    # 3. Call Matcha
    try:
        raw_reply = call_matcha(full_prompt, timeout=600)
    except Exception as e:
        _log(f"[priority] Matcha call failed: {e}")
        sys.exit(1)

    _log(f"[priority] Matcha raw response:\n{raw_reply}")

    # 4. Parse response
    results = _parse_json_response(raw_reply)
    if not results:
        _log("[priority] Could not parse Matcha response as JSON.")
        sys.exit(1)

    _log(f"[priority] Parsed {len(results)} priority result(s).")

    # 5. Build per-ticket field maps and persist
    ticket_id_map = {t["ticket_number"]: t.get("ticket_id", "") for t in tickets}
    ticket_data_map = {t["ticket_number"]: t for t in tickets}
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    ticket_fields: dict[str, dict] = {}  # return value

    total_results = len(results)
    for idx, result in enumerate(results, 1):
        tnum = str(result.get("ticket_number", "")).strip()
        priority = str(result.get("priority", "")).strip()
        explanation = str(result.get("priority_explanation", "")).strip()
        if not priority or not explanation:
            _log(f"  [priority] ticket count {idx}/{total_results} — Missing priority or explanation for {tnum}; skipping.")
            continue

        fields = {
            "AIPriority": priority,
            "AIPriExpln": explanation,
            "AILastUpdate": now_str,
        }
        tid_str = ticket_id_map.get(tnum, "")
        tdata = ticket_data_map.get(tnum, {})
        ticket_fields[tnum] = {
            "ticket_id": tid_str,
            "fields": fields,
            "activities": tdata.get("activities", []),
        }

        # Persist to DB
        tid_int = tid_map.get(tnum)
        if tid_int and db_enabled:
            _persist_to_db(tid_int, tnum, hash_map.get(tnum), result, raw_reply, prompt_version)
            _log(f"  [priority] ticket count {idx}/{total_results} — Persisted to DB for ticket {tnum}.")

    # 6. Write back to TeamSupport (only when running standalone)
    updated = 0
    deferred = 0
    if write_back:
        for tnum, data in ticket_fields.items():
            try:
                update_ticket(data["ticket_id"], dict(data["fields"]), data["activities"])
                _log(f"  [ts] Wrote back AI fields for ticket {tnum}.")
                updated += 1
            except Exception as e:
                if hasattr(e, 'response') and getattr(e.response, 'status_code', None) == 403:
                    _log(f"  [ts] API rate-limited for {tnum}; payload saved to dry-run file.")
                    deferred += 1
                else:
                    _log(f"  [ts] Failed to write back AI fields for {tnum}: {e}")
        _log(f"[priority] Updated {updated}/{len(ticket_fields)} ticket(s) in TeamSupport, {deferred} deferred (rate-limited).")
    else:
        _log(f"[priority] Collected fields for {len(ticket_fields)} ticket(s) (write-back deferred).")

    # 7. Save results locally (JSON artifact)
    if not SKIP_OUTPUT_FILES:
        ts = _run_timestamp()
        out_path = os.path.join(OUTPUT_DIR, f"priority_{ts}.json")
        source = os.path.basename(activities_file) if activities_file else "db"
        output = {
            "source_file": source,
            "tickets_sent": len(tickets),
            "tickets_skipped": len(skipped),
            "results": results,
            "writeback_count": updated,
        }
        with open(out_path, "w", encoding="utf-8") as fout:
            json.dump(output, fout, ensure_ascii=False, indent=2)

        _log(f"[priority] Results saved to {out_path}")
    else:
        _log("[priority] JSON artifact skipped (SKIP_OUTPUT_FILES=1).")
    return ticket_fields


if __name__ == "__main__":
    if not RUN_PRIORITY:
        print("[priority] Skipped (RUN_PRIORITY=0).")
    else:
        parser = argparse.ArgumentParser(description="AI priority scoring with optional DB persistence.")
        parser.add_argument("--force", action="store_true",
                            help="Force rerun even if thread_hash is unchanged.")
        args = parser.parse_args()
        main(force=args.force or FORCE_ENRICHMENT)
