import time
import os

import requests

MATCHA_URL = "https://matcha.harriscomputer.com/rest/api/v1/completions"
MATCHA_API_KEY = os.getenv("MATCHA_API_KEY", "")
MATCHA_MISSION_ID = os.getenv("MATCHA_MISSION_ID", "")

MAX_RETRIES = 3
RETRY_BACKOFF = 10  # seconds; doubles each retry


def _extract_reply_text(data: object) -> str:
    if not isinstance(data, dict):
        return ""

    output = data.get("output", "")
    if isinstance(output, list) and output:
        first = output[0]
        if isinstance(first, dict):
            content = first.get("content", [])
            if content and isinstance(content[0], dict):
                return str(content[0].get("text", ""))

    if isinstance(output, list):
        return "\n".join(str(item) for item in output)

    return str(output) if output is not None else ""


def call_matcha(
    prompt: str,
    api_key_header: str,
    url: str = MATCHA_URL,
    api_key: str = MATCHA_API_KEY,
    mission_id: str = MATCHA_MISSION_ID,
    timeout: int = 300,
    max_retries: int = MAX_RETRIES,
    retry_backoff: int = RETRY_BACKOFF,
) -> str:
    headers = {
        "Content-Type": "application/json",
        api_key_header: api_key,
    }
    payload = {
        "mission_id": mission_id,
        "input": prompt,
    }

    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=timeout)
            response.raise_for_status()
            return _extract_reply_text(response.json())
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            last_error = exc
            if attempt < max_retries:
                wait = retry_backoff * (2 ** (attempt - 1))
                print(f"    Matcha call failed (attempt {attempt}/{max_retries}): {exc}")
                print(f"    Retrying in {wait}s ...")
                time.sleep(wait)

    raise last_error  # type: ignore[misc]


# Usage:
# reply = call_matcha(
#     "Summarize this ticket...",
#     api_key_header="X-API-Key",
# )
