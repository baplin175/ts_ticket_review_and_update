"""
Matcha LLM API client — send prompts and extract reply text.
"""

import time

import requests

from config import LOG_API_CALLS, MATCHA_URL, MATCHA_API_KEY, MATCHA_MISSION_ID
from ts_client import _log_api_call

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
    timeout: int = 300,
    max_retries: int = MAX_RETRIES,
    retry_backoff: int = RETRY_BACKOFF,
) -> str:
    headers = {
        "Content-Type": "application/json",
        "MATCHA-API-KEY": MATCHA_API_KEY,
    }
    payload = {
        "mission_id": MATCHA_MISSION_ID,
        "input": prompt,
    }

    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.post(
                MATCHA_URL, json=payload, headers=headers, timeout=timeout
            )
            reply_text = _extract_reply_text(response.json())
            _log_api_call(
                "POST", MATCHA_URL,
                payload=payload,
                status=response.status_code,
                response_body=reply_text,
            )
            if response.status_code >= 500 and attempt < max_retries:
                wait = retry_backoff * (2 ** (attempt - 1))
                print(f"    [matcha] Attempt {attempt}/{max_retries} server error ({response.status_code})", flush=True)
                print(f"    [matcha] Retrying in {wait}s ...", flush=True)
                last_error = requests.exceptions.HTTPError(
                    f"Server error {response.status_code}", response=response
                )
                time.sleep(wait)
                continue
            response.raise_for_status()
            return reply_text
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
        ) as exc:
            _log_api_call("POST", MATCHA_URL,
                          payload=payload,
                          error=str(exc))
            last_error = exc
            if attempt < max_retries:
                wait = retry_backoff * (2 ** (attempt - 1))
                print(f"    [matcha] Attempt {attempt}/{max_retries} failed: {exc}", flush=True)
                print(f"    [matcha] Retrying in {wait}s ...", flush=True)
                time.sleep(wait)

    raise last_error  # type: ignore[misc]
