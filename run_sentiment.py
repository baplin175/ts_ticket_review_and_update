"""
Part 2 — Read a ticket's activities JSON, extract the last N customer
comments, send them to Matcha with the sentiment prompt, and write the
response to a JSON file.

When DATABASE_URL is set:
  - Reads ticket data from DB (ticket_thread_rollups + ticket_actions)
  - Checks thread_hash to skip unchanged tickets (unless --force)
  - Persists results to ticket_sentiment table
  - Still emits the JSON artifact

When DATABASE_URL is not set:
  - Falls back to JSON-only mode (existing behaviour)

Usage:
    TARGET_TICKET=29696 python run_sentiment.py
    TARGET_TICKET=29696,110554 python run_sentiment.py
    TARGET_TICKET=29696 python run_sentiment.py --force
    TARGET_TICKET=29696 CUST_COMMENT_COUNT=4 python run_sentiment.py
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from config import OUTPUT_DIR, RUN_SENTIMENT, TARGET_TICKETS, MATCHA_MISSION_ID, SKIP_OUTPUT_FILES
from matcha_client import call_matcha

CUST_COMMENT_COUNT = int(os.getenv("CUST_COMMENT_COUNT", "4"))
PROMPT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "prompts", "sentiment.md")

PROMPT_NAME = "sentiment"
PROMPT_VERSION = "1"
MODEL_NAME = f"matcha-{MATCHA_MISSION_ID}"


def _log(msg: str) -> None:
    print(msg, flush=True)


def _run_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _load_prompt_template() -> str:
    with open(PROMPT_PATH, "r", encoding="utf-8") as f:
        return f.read()


def _latest_activities_file() -> str | None:
    """Find the most recent activities JSON file in OUTPUT_DIR."""
    out = Path(OUTPUT_DIR)
    files = sorted(out.glob("activities_*.json"), reverse=True)
    return str(files[0]) if files else None


def _load_customer_comments(json_path: str, ticket_number: str) -> list[dict]:
    """Load customer (party=cust) comments for a ticket, newest first."""
    with open(json_path, "r", encoding="utf-8") as f:
        tickets = json.load(f)

    records = []
    for ticket in tickets:
        if ticket.get("ticket_number") != ticket_number:
            continue
        for act in ticket.get("activities", []):
            if act.get("party") != "cust":
                continue
            if not act.get("description", "").strip():
                continue
            records.append(act)
    return records


def _load_customer_comments_from_db(ticket_id: int) -> list[dict]:
    """Load customer comments from DB for a ticket, newest first."""
    import db
    rows = db.fetch_all(
        "SELECT action_id, created_at, cleaned_description "
        "FROM ticket_actions WHERE ticket_id = %s AND party = 'cust' "
        "AND is_empty = FALSE AND cleaned_description IS NOT NULL "
        "ORDER BY created_at DESC;",
        (ticket_id,),
    )
    return [
        {
            "action_id": str(r[0]),
            "created_at": r[1].isoformat() if r[1] else "",
            "description": r[2] or "",
        }
        for r in rows
    ]


def _build_sentiment_input(ticket_number: str, activities: list[dict]) -> str:
    """Build the JSON input block that gets appended to the prompt."""
    activity_list = []
    for a in activities:
        activity_list.append({
            "activity_id": a["action_id"],
            "created_at": a["created_at"],
            "description": a["description"],
        })

    input_obj = {
        "ticket_number": ticket_number,
        "activities": activity_list,
    }
    return json.dumps(input_obj, ensure_ascii=False, indent=2)


def _should_skip(ticket_id: int, force: bool) -> bool:
    """Return True if the ticket's sentiment is up-to-date (hash unchanged)."""
    if force:
        return False
    import db
    if not db._is_enabled():
        return False
    hashes = db.get_current_hashes(ticket_id)
    current_hash = hashes.get("thread_hash")
    if not current_hash:
        return False  # no rollup data yet, score it
    last_hash = db.get_latest_enrichment_hash(ticket_id, "sentiment")
    if last_hash == current_hash:
        return True
    return False


def _persist_to_db(ticket_id: int, thread_hash: str | None,
                   response_obj: dict, source_file: str | None) -> None:
    """Insert sentiment result into DB."""
    import db
    if not db._is_enabled():
        return
    db.insert_sentiment(
        ticket_id,
        thread_hash=thread_hash,
        model_name=MODEL_NAME,
        prompt_name=PROMPT_NAME,
        prompt_version=PROMPT_VERSION,
        frustrated=response_obj.get("frustrated"),
        activity_id=str(response_obj.get("activity_id", "")) or None,
        created_at=response_obj.get("created_at"),
        source_file=source_file,
        raw_response=response_obj,
    )


def main(activities_file: str | None = None, *, force: bool = False,
         ticket_numbers: list[str] | None = None) -> None:
    target_tickets = ticket_numbers or TARGET_TICKETS
    if not target_tickets:
        _log("[sentiment] TARGET_TICKET is required. Set it as an env var.")
        sys.exit(1)

    # Check if DB mode is available
    try:
        import db
        db_enabled = db._is_enabled()
    except Exception:
        db_enabled = False

    # Resolve ticket_number → ticket_id map (for DB mode)
    tid_map: dict[str, int] = {}
    if db_enabled:
        tid_map = db.ticket_ids_for_numbers(target_tickets)

    # 1. Locate most recent activities file (still needed for JSON fallback)
    if not activities_file:
        activities_file = _latest_activities_file()
    if not activities_file and not db_enabled:
        _log(f"[sentiment] No activities JSON found in {OUTPUT_DIR}. Run run_pull_activities.py first.")
        sys.exit(1)
    if activities_file:
        _log(f"[sentiment] Using activities file: {activities_file}")

    all_results = []

    for tkt_num in target_tickets:
        _log(f"[sentiment] Processing ticket {tkt_num}...")

        ticket_id = tid_map.get(tkt_num)
        thread_hash = None

        # Hash-based skip check
        if ticket_id and db_enabled:
            hashes = db.get_current_hashes(ticket_id)
            thread_hash = hashes.get("thread_hash")
            if _should_skip(ticket_id, force):
                _log(f"[sentiment] Ticket {tkt_num} thread unchanged (hash match). Skipping. Use --force to override.")
                continue

        # 2. Load last N customer comments
        if ticket_id and db_enabled:
            cust_comments = _load_customer_comments_from_db(ticket_id)
        elif activities_file:
            cust_comments = _load_customer_comments(activities_file, tkt_num)
        else:
            _log(f"[sentiment] No data source for ticket {tkt_num}. Skipping.")
            continue

        _log(f"[sentiment] Found {len(cust_comments)} customer comment(s) for ticket {tkt_num}.")
        last_n = cust_comments[:CUST_COMMENT_COUNT]
        if not last_n:
            _log(f"[sentiment] No customer comments for ticket {tkt_num}. Skipping.")
            continue
        _log(f"[sentiment] Sending last {len(last_n)} customer comment(s) to Matcha.")

        # 3. Build prompt
        prompt_template = _load_prompt_template()
        parts = prompt_template.split("Input:", 1)
        instructions = parts[0].strip()

        sentiment_input = _build_sentiment_input(tkt_num, last_n)
        full_prompt = f"""{instructions}

Input:
{sentiment_input}

Output format (strict JSON):
{{
  "frustrated": "Yes" or "No",
  "ticket_number": "{tkt_num}",
  "activity_id": "<id>" or null,
  "created_at": "<timestamp>" or null
}}"""

        _log(f"[sentiment] Calling Matcha...")

        # 4. Call Matcha
        try:
            reply = call_matcha(full_prompt)
        except Exception as e:
            _log(f"[sentiment] Matcha call failed for ticket {tkt_num}: {e}")
            continue

        _log(f"[sentiment] Matcha response: {reply}")

        # Parse response
        try:
            response_obj = json.loads(reply)
        except json.JSONDecodeError:
            response_obj = {"raw_response": reply}

        # 5. Persist to DB
        if ticket_id and db_enabled:
            _persist_to_db(ticket_id, thread_hash, response_obj,
                           os.path.basename(activities_file) if activities_file else None)
            _log(f"[sentiment] Persisted to DB for ticket {tkt_num}.")

        record = {
            "ticket_number": tkt_num,
            "comments_sent": len(last_n),
            "source_file": os.path.basename(activities_file) if activities_file else "db",
            **response_obj,
        }
        all_results.append(record)

    # 6. Write response JSON artifact
    if not SKIP_OUTPUT_FILES:
        ts = _run_timestamp()
        out_path = os.path.join(OUTPUT_DIR, f"sentiment_{ts}.json")

        with open(out_path, "w", encoding="utf-8") as fout:
            json.dump(all_results, fout, ensure_ascii=False, indent=2)

        _log(f"[sentiment] {len(all_results)} result(s) written to {out_path}")
    else:
        _log("[sentiment] JSON artifact skipped (SKIP_OUTPUT_FILES=1).")


if __name__ == "__main__":
    if not RUN_SENTIMENT:
        print("[sentiment] Skipped (RUN_SENTIMENT=0).")
    else:
        parser = argparse.ArgumentParser(description="Sentiment analysis with optional DB persistence.")
        parser.add_argument("--force", action="store_true",
                            help="Force rerun even if thread_hash is unchanged.")
        args = parser.parse_args()
        main(force=args.force)
