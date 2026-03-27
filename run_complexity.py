"""
Part 4 — Read the activities JSON, build a prompt from complexity.md
for each ticket, call Matcha for complexity scoring, and save results.

When DATABASE_URL is set:
  - Can read ticket data from DB (avoiding need for activities JSON)
  - Checks technical_core_hash to skip unchanged tickets (unless --force)
  - Persists full complexity results to ticket_complexity_scores table
  - Still emits the JSON artifact and supports TS write-back

When DATABASE_URL is not set:
  - Falls back to JSON-only mode (existing behaviour)

Usage:
    python run_complexity.py
    TARGET_TICKET=29696,110554 python run_complexity.py
    TARGET_TICKET=29696 python run_complexity.py --force
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

from config import FORCE_ENRICHMENT, OUTPUT_DIR, RUN_COMPLEXITY, TARGET_TICKETS, MATCHA_MISSION_ID, TS_WRITEBACK, SKIP_OUTPUT_FILES
from matcha_client import call_matcha
from prompt_store import get_prompt
from ts_client import update_ticket

PROMPT_NAME = "complexity"
MODEL_NAME = f"matcha-{MATCHA_MISSION_ID}"


def _log(msg: str) -> None:
    print(msg, flush=True)


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


def _should_skip(ticket_id: int, force: bool) -> bool:
    """Return True if the ticket's complexity is up-to-date (hash unchanged)."""
    if force:
        return False
    import db
    if not db._is_enabled():
        return False
    hashes = db.get_current_hashes(ticket_id)
    current_hash = hashes.get("technical_core_hash")
    if not current_hash:
        return False
    last_hash = db.get_latest_enrichment_hash(ticket_id, "complexity")
    return last_hash == current_hash


def _persist_to_db(ticket_id: int, ticket_number: str | None, technical_core_hash: str | None,
                   result: dict, raw_reply: str, prompt_version: str) -> None:
    """Insert complexity result into DB."""
    import db
    if not db._is_enabled():
        return

    def _safe_int(val):
        try:
            return int(val)
        except (TypeError, ValueError):
            return None

    def _safe_float(val):
        try:
            return float(val)
        except (TypeError, ValueError):
            return None

    db.insert_complexity(
        ticket_id,
        ticket_number=ticket_number,
        technical_core_hash=technical_core_hash,
        model_name=MODEL_NAME,
        prompt_name=PROMPT_NAME,
        prompt_version=prompt_version,
        intrinsic_complexity=_safe_int(result.get("intrinsic_complexity")),
        coordination_load=_safe_int(result.get("coordination_load")),
        elapsed_drag=_safe_int(result.get("elapsed_drag")),
        overall_complexity=_safe_int(result.get("overall_complexity")),
        confidence=_safe_float(result.get("confidence")),
        primary_complexity_drivers=result.get("primary_complexity_drivers"),
        complexity_summary=result.get("complexity_summary"),
        evidence=result.get("evidence"),
        noise_factors=result.get("noise_factors"),
        duration_vs_complexity_note=result.get("duration_vs_complexity_note"),
        raw_response={"parsed": result, "raw_text": raw_reply},
    )


def main(activities_file: str | None = None, write_back: bool | None = None,
         *, force: bool = False, ticket_numbers: list[str] | None = None) -> dict:
    """Run complexity scoring.  Returns a dict mapping ticket_number to
    ``{"ticket_id": ..., "fields": {...}, "activities": [...]}`` so the
    caller can merge fields with other stages before a single API call.
    When *write_back* is True (default / standalone), the function also
    writes each ticket back to TeamSupport individually.
    """
    if write_back is None:
        write_back = TS_WRITEBACK

    target_tickets = ticket_numbers or TARGET_TICKETS
    if not target_tickets:
        _log("[complexity] TARGET_TICKET is required. Set it as an env var.")
        sys.exit(1)

    prompt_record = get_prompt(PROMPT_NAME, allow_fallback=True)
    prompt_template = prompt_record["content"]
    prompt_version = prompt_record["version"]
    _log(f"[complexity] Prompt: {PROMPT_NAME} v{prompt_version}")

    # Check if DB mode is available
    try:
        import db
        db_enabled = db._is_enabled()
    except Exception:
        db_enabled = False

    # Resolve ticket_number → ticket_id map (for DB mode)
    tid_map: dict[str, int] = {}
    hash_map: dict[str, str | None] = {}  # ticket_number → technical_core_hash
    if db_enabled:
        tid_map = db.ticket_ids_for_numbers(target_tickets)
        for tnum, tid in tid_map.items():
            hashes = db.get_current_hashes(tid)
            hash_map[tnum] = hashes.get("technical_core_hash")

    # Filter tickets by hash-based skip logic
    tickets_to_score = []
    skipped = []
    for tnum in target_tickets:
        tid = tid_map.get(tnum)
        if tid and _should_skip(tid, force):
            _log(f"[complexity] Ticket {tnum} technical core unchanged (hash match). Skipping. Use --force to override.")
            skipped.append(tnum)
        else:
            tickets_to_score.append(tnum)

    if not tickets_to_score:
        _log("[complexity] All tickets skipped (unchanged). Nothing to score.")
        return {}

    # Filter out tickets explicitly excluded from complexity scoring
    if db_enabled:
        excluded = db.get_excluded_ticket_numbers("complexity")
        if excluded:
            before = len(tickets_to_score)
            tickets_to_score = [t for t in tickets_to_score if t not in excluded]
            n = before - len(tickets_to_score)
            if n:
                _log(f"[complexity] {n} ticket(s) excluded from complexity scoring (ticket_exclusions table).")

    if not tickets_to_score:
        _log("[complexity] All tickets excluded or skipped. Nothing to score.")
        return {}

    # 1. Load ticket data
    if db_enabled and tid_map:
        tickets = _load_tickets_from_db(tickets_to_score)
        _log(f"[complexity] Loaded {len(tickets)} ticket(s) from DB.")
    else:
        if not activities_file:
            activities_file = _latest_activities_file()
        if not activities_file:
            _log(f"[complexity] No activities JSON found in {OUTPUT_DIR}. Run run_pull_activities.py first.")
            sys.exit(1)
        _log(f"[complexity] Using activities file: {activities_file}")
        tickets = _load_tickets(activities_file, tickets_to_score)

    if not tickets:
        _log(f"[complexity] Ticket(s) {', '.join(tickets_to_score)} not found.")
        sys.exit(1)
    _log(f"[complexity] Scoring {len(tickets)} ticket(s).")

    all_results = []
    total_tickets = len(tickets)

    for idx, ticket in enumerate(tickets, 1):
        tnum = ticket.get("ticket_number", "?")
        _log(f"[complexity] ticket count {idx}/{total_tickets} — Scoring ticket {tnum}...")

        # 2. Build prompt with ticket history
        ticket_history = _build_ticket_history(ticket)
        full_prompt = prompt_template.replace("{{TICKET_HISTORY}}", ticket_history)

        # 3. Call Matcha
        try:
            raw_reply = call_matcha(full_prompt, timeout=600)
        except Exception as e:
            _log(f"[complexity] Matcha call failed for ticket {tnum}: {e}")
            continue

        _log(f"[complexity] Matcha response for {tnum}:\n{raw_reply}")

        # 4. Parse response
        result = _parse_json_response(raw_reply)
        if not result:
            _log(f"[complexity] Could not parse response for ticket {tnum}.")
            continue

        result["ticket_id"] = ticket.get("ticket_id", "")
        result["ticket_number"] = tnum
        all_results.append(result)

        # 5. Persist to DB
        tid_int = tid_map.get(tnum)
        if tid_int and db_enabled:
            _persist_to_db(tid_int, tnum, hash_map.get(tnum), result, raw_reply, prompt_version)
            _log(f"  [complexity] Persisted to DB for ticket {tnum}.")

    _log(f"[complexity] Scored {len(all_results)}/{len(tickets)} ticket(s). Skipped {len(skipped)}.")

    # 6. Build per-ticket field maps
    ticket_map = {t["ticket_number"]: t for t in tickets}
    ticket_fields: dict[str, dict] = {}  # return value

    for result in all_results:
        tnum = str(result.get("ticket_number", "")).strip()
        tid = str(result.get("ticket_id", "")).strip()
        fields = {
            "Complexity": str(result.get("overall_complexity", "")),
            "COORDINATIONLOAD": str(result.get("coordination_load", "")),
            "ELAPSEDDRAG": str(result.get("elapsed_drag", "")),
            "INTRINSICCOMPLEXITY": str(result.get("intrinsic_complexity", "")),
        }
        tdata = ticket_map.get(tnum, {})
        ticket_fields[tnum] = {
            "ticket_id": tid,
            "fields": fields,
            "activities": tdata.get("activities", []),
        }

    # 7. Write back to TeamSupport (only when running standalone)
    updated = 0
    deferred = 0
    if write_back:
        for tnum, data in ticket_fields.items():
            try:
                update_ticket(data["ticket_id"], dict(data["fields"]), data["activities"])
                _log(f"  [ts] Wrote back complexity fields for ticket {tnum}.")
                updated += 1
            except Exception as e:
                if hasattr(e, 'response') and getattr(e.response, 'status_code', None) == 403:
                    _log(f"  [ts] API rate-limited for {tnum}; payload saved to dry-run file.")
                    deferred += 1
                else:
                    _log(f"  [ts] Failed to write back complexity for {tnum}: {e}")
        _log(f"[complexity] Updated {updated}/{len(ticket_fields)} ticket(s) in TeamSupport, {deferred} deferred (rate-limited).")
    else:
        _log(f"[complexity] Collected fields for {len(ticket_fields)} ticket(s) (write-back deferred).")

    # 8. Save results locally (JSON artifact)
    if not SKIP_OUTPUT_FILES:
        ts = _run_timestamp()
        out_path = os.path.join(OUTPUT_DIR, f"complexity_{ts}.json")
        source = os.path.basename(activities_file) if activities_file else "db"
        output = {
            "source_file": source,
            "tickets_scored": len(all_results),
            "tickets_skipped": len(skipped),
            "writeback_count": updated,
            "results": all_results,
        }
        with open(out_path, "w", encoding="utf-8") as fout:
            json.dump(output, fout, ensure_ascii=False, indent=2)

        _log(f"[complexity] Results saved to {out_path}")
    else:
        _log("[complexity] JSON artifact skipped (SKIP_OUTPUT_FILES=1).")
    return ticket_fields


if __name__ == "__main__":
    if not RUN_COMPLEXITY:
        print("[complexity] Skipped (RUN_COMPLEXITY=0).")
    else:
        parser = argparse.ArgumentParser(description="Complexity scoring with optional DB persistence.")
        parser.add_argument("--force", action="store_true",
                            help="Force rerun even if technical_core_hash is unchanged.")
        args = parser.parse_args()
        main(force=args.force or FORCE_ENRICHMENT)
