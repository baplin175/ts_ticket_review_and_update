"""
Part 2 — Read a ticket's activities JSON, extract the last N customer
comments, send them to Matcha with the sentiment prompt, and write the
response to a JSON file.

Usage:
    TARGET_TICKET=29696 python run_sentiment.py
    TARGET_TICKET=29696,110554 python run_sentiment.py
    TARGET_TICKET=29696 CUST_COMMENT_COUNT=4 python run_sentiment.py
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from config import OUTPUT_DIR, RUN_SENTIMENT, TARGET_TICKETS
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


def main(activities_file: str | None = None) -> None:
    if not TARGET_TICKETS:
        _log("[sentiment] TARGET_TICKET is required. Set it as an env var.")
        sys.exit(1)

    # 1. Locate most recent activities file
    if not activities_file:
        activities_file = _latest_activities_file()
    if not activities_file:
        _log(f"[sentiment] No activities JSON found in {OUTPUT_DIR}. Run run_pull_activities.py first.")
        sys.exit(1)
    _log(f"[sentiment] Using activities file: {activities_file}")

    all_results = []

    for tkt_num in TARGET_TICKETS:
        _log(f"[sentiment] Processing ticket {tkt_num}...")

        # 2. Load last N customer comments
        cust_comments = _load_customer_comments(activities_file, tkt_num)
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

        record = {
            "ticket_number": tkt_num,
            "comments_sent": len(last_n),
            "source_file": os.path.basename(activities_file),
            **response_obj,
        }
        all_results.append(record)

    # 5. Write response JSON
    ts = _run_timestamp()
    out_path = os.path.join(OUTPUT_DIR, f"sentiment_{ts}.json")

    with open(out_path, "w", encoding="utf-8") as fout:
        json.dump(all_results, fout, ensure_ascii=False, indent=2)

    _log(f"[sentiment] {len(all_results)} result(s) written to {out_path}")


if __name__ == "__main__":
    if not RUN_SENTIMENT:
        print("[sentiment] Skipped (RUN_SENTIMENT=0).")
    else:
        main()
