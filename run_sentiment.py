"""
Part 2 — Read a ticket's activities JSONL, extract the last N customer
comments, send them to Matcha with the sentiment prompt, and write the
response to a JSONL file.

Usage:
    TARGET_TICKET=29696 python run_sentiment.py
    TARGET_TICKET=29696 CUST_COMMENT_COUNT=4 python run_sentiment.py
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from config import OUTPUT_DIR, TARGET_TICKET
from matcha_client import call_matcha

CUST_COMMENT_COUNT = int(os.getenv("CUST_COMMENT_COUNT", "4"))
PROMPT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "prompts", "sentiment.md")


def _log(msg: str) -> None:
    print(msg, flush=True)


def _run_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")


def _load_prompt_template() -> str:
    with open(PROMPT_PATH, "r", encoding="utf-8") as f:
        return f.read()


def _latest_activities_file() -> str | None:
    """Find the most recent activities JSONL file in OUTPUT_DIR."""
    out = Path(OUTPUT_DIR)
    files = sorted(out.glob("activities_*.jsonl"), reverse=True)
    return str(files[0]) if files else None


def _load_customer_comments(jsonl_path: str, ticket_number: str) -> list[dict]:
    """Load customer (party=cust) comments for a ticket, newest first."""
    records = []
    with open(jsonl_path, "r", encoding="utf-8") as f:
        for line in f:
            rec = json.loads(line)
            if rec.get("ticket_number") != ticket_number:
                continue
            if rec.get("party") != "cust":
                continue
            if not rec.get("description", "").strip():
                continue
            records.append(rec)
    # Sort newest first (by created_at or action_id descending)
    return records


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


def main() -> None:
    if not TARGET_TICKET:
        _log("[sentiment] TARGET_TICKET is required. Set it as an env var.")
        sys.exit(1)

    # 1. Locate most recent activities file
    activities_file = _latest_activities_file()
    if not activities_file:
        _log(f"[sentiment] No activities JSONL found in {OUTPUT_DIR}. Run run_pull_activities.py first.")
        sys.exit(1)
    _log(f"[sentiment] Using activities file: {activities_file}")

    # 2. Load last N customer comments
    cust_comments = _load_customer_comments(activities_file, TARGET_TICKET)
    _log(f"[sentiment] Found {len(cust_comments)} customer comment(s) for ticket {TARGET_TICKET}.")
    last_n = cust_comments[:CUST_COMMENT_COUNT]
    if not last_n:
        _log("[sentiment] No customer comments found. Nothing to analyse.")
        sys.exit(0)
    _log(f"[sentiment] Sending last {len(last_n)} customer comment(s) to Matcha.")

    # 3. Build prompt
    prompt_template = _load_prompt_template()
    # Replace the example input block with our real data
    # The prompt ends with the Input: {...} and Output format sections
    # We'll use the instructions portion and append our input
    # Split at "Input:" to get just the instructions
    parts = prompt_template.split("Input:", 1)
    instructions = parts[0].strip()

    sentiment_input = _build_sentiment_input(TARGET_TICKET, last_n)
    full_prompt = f"""{instructions}

Input:
{sentiment_input}

Output format (strict JSON):
{{
  "frustrated": "Yes" or "No",
  "ticket_number": "{TARGET_TICKET}",
  "activity_id": "<id>" or null,
  "created_at": "<timestamp>" or null
}}"""

    _log(f"[sentiment] Calling Matcha...")

    # 4. Call Matcha
    try:
        reply = call_matcha(full_prompt)
    except Exception as e:
        _log(f"[sentiment] Matcha call failed: {e}")
        sys.exit(1)

    _log(f"[sentiment] Matcha response: {reply}")

    # 5. Write response JSONL
    ts = _run_timestamp()
    out_path = os.path.join(OUTPUT_DIR, f"sentiment_{ts}.jsonl")

    # Try to parse Matcha's JSON response
    try:
        response_obj = json.loads(reply)
    except json.JSONDecodeError:
        response_obj = {"raw_response": reply}

    record = {
        "ticket_number": TARGET_TICKET,
        "comments_sent": len(last_n),
        "source_file": os.path.basename(activities_file),
        **response_obj,
    }

    with open(out_path, "w", encoding="utf-8") as fout:
        fout.write(json.dumps(record, ensure_ascii=False) + "\n")

    _log(f"[sentiment] Result written to {out_path}")


if __name__ == "__main__":
    main()
