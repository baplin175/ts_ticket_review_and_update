"""
matcha.py — self-contained Matcha LLM client

Everything a program needs to talk to Matcha:
  • fetches the live model list from the API
  • lets you choose an LLM interactively (or pass one via CLI / env var)
  • exposes call_matcha() for use as a library
  • runs an interactive prompt loop when executed directly

Credentials are read from environment variables:
    MATCHA_URL        (default: https://matcha.harriscomputer.com/rest/api/v1/completions)
    MATCHA_API_KEY    (required)
    MATCHA_MISSION_ID (required)

Usage:
    python matcha.py                         # interactive model picker + REPL
    python matcha.py --model 87              # use model id 87 directly
    python matcha.py --list-models           # print available models and exit
    python matcha.py --prompt "hello"        # single-shot, print reply and exit
"""

import argparse
import json
import os
import sys
import time

import requests

# ── Config ───────────────────────────────────────────────────────────

BASE_URL    = os.getenv("MATCHA_URL",        "https://matcha.harriscomputer.com/rest/api/v1/completions")
LLMS_URL    = BASE_URL.rsplit("/completions", 1)[0] + "/llms"
API_KEY     = os.getenv("MATCHA_API_KEY",    "15c0db915567455e98b90f1ecc22e088")
MISSION_ID  = os.getenv("MATCHA_MISSION_ID", "27301")

MAX_RETRIES    = 3
RETRY_BACKOFF  = 10  # seconds; doubles each retry


def _headers() -> dict:
    return {
        "Content-Type": "application/json",
        "MATCHA-API-KEY": API_KEY,
    }


# ── Model list ───────────────────────────────────────────────────────

def fetch_models() -> list[dict]:
    """Return [{id, name}, ...] from the live Matcha /llms endpoint."""
    resp = requests.get(
        LLMS_URL,
        headers=_headers(),
        params={"select": "id,name"},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, list):
        return sorted(data, key=lambda m: m.get("name", "").lower())
    return []


def print_models(models: list[dict]) -> None:
    print(f"\n{'ID':>5}  Name")
    print("-" * 50)
    for m in models:
        print(f"{m['id']:>5}  {m['name']}")
    print()


def pick_model(models: list[dict]) -> int:
    """Interactive model picker. Returns the selected model id."""
    print_models(models)
    id_set = {m["id"] for m in models}
    while True:
        raw = input("Enter model ID (or press Enter to use mission default): ").strip()
        if raw == "":
            return None
        try:
            mid = int(raw)
            if mid in id_set:
                name = next(m["name"] for m in models if m["id"] == mid)
                print(f"  → Using: {name} (id={mid})\n")
                return mid
            print("  Not a valid model ID, try again.")
        except ValueError:
            print("  Please enter a number.")


# ── Core call ────────────────────────────────────────────────────────

def _extract_reply(data: object) -> str:
    if not isinstance(data, dict):
        return str(data)
    output = data.get("output", "")
    if isinstance(output, list) and output:
        first = output[0]
        if isinstance(first, dict):
            content = first.get("content", [])
            if content and isinstance(content[0], dict):
                return str(content[0].get("text", ""))
        return "\n".join(str(item) for item in output)
    return str(output) if output is not None else ""


def call_matcha(
    prompt: str,
    model_id: int | None = None,
    mission_id: str | None = None,
    timeout: int = 300,
) -> str:
    """
    Send a prompt to Matcha and return the reply text.

    Args:
        prompt:     The prompt string.
        model_id:   Matcha LLM id to override the mission default (optional).
        mission_id: Override the MATCHA_MISSION_ID env var (optional).
        timeout:    HTTP timeout in seconds.

    Returns:
        The model's reply as a plain string.

    Raises:
        requests.HTTPError / requests.ConnectionError on unrecoverable failure.
    """
    mid = mission_id or MISSION_ID
    if not mid:
        raise ValueError("MATCHA_MISSION_ID is not set. Pass mission_id= or set the env var.")
    if not API_KEY:
        raise ValueError("MATCHA_API_KEY is not set.")

    payload: dict = {"mission_id": mid, "input": prompt}
    if model_id is not None:
        payload["options"] = {"responseLLM": model_id}

    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.post(BASE_URL, json=payload, headers=_headers(), timeout=timeout)
            if resp.status_code >= 500 and attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF * (2 ** (attempt - 1))
                print(f"[matcha] attempt {attempt}/{MAX_RETRIES} — server error {resp.status_code}, retry in {wait}s", file=sys.stderr)
                time.sleep(wait)
                last_error = requests.exceptions.HTTPError(response=resp)
                continue
            resp.raise_for_status()
            return _extract_reply(resp.json())
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            last_error = exc
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF * (2 ** (attempt - 1))
                print(f"[matcha] attempt {attempt}/{MAX_RETRIES} — {exc}, retry in {wait}s", file=sys.stderr)
                time.sleep(wait)

    raise last_error  # type: ignore[misc]


# ── CLI ──────────────────────────────────────────────────────────────

def _check_credentials() -> None:
    missing = []
    if not API_KEY:
        missing.append("MATCHA_API_KEY")
    if not MISSION_ID:
        missing.append("MATCHA_MISSION_ID")
    if missing:
        print(f"Error: missing environment variable(s): {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Matcha LLM client")
    parser.add_argument("--list-models", action="store_true", help="Print available models and exit")
    parser.add_argument("--model", type=int, default=None, metavar="ID", help="Model ID to use (skips interactive picker)")
    parser.add_argument("--prompt", type=str, default=None, help="Single prompt; print reply and exit")
    parser.add_argument("--timeout", type=int, default=300, help="HTTP timeout in seconds (default: 300)")
    args = parser.parse_args()

    _check_credentials()

    # -- list models and exit
    if args.list_models:
        models = fetch_models()
        print_models(models)
        return

    # -- resolve model id
    model_id = args.model
    if model_id is None and args.prompt is None:
        # interactive picker
        print("Fetching available models from Matcha …")
        try:
            models = fetch_models()
        except Exception as exc:
            print(f"Warning: could not fetch models ({exc}). Will use mission default.", file=sys.stderr)
            models = []
        if models:
            model_id = pick_model(models)

    # -- single-shot mode
    if args.prompt is not None:
        reply = call_matcha(args.prompt, model_id=model_id, timeout=args.timeout)
        print(reply)
        return

    # -- interactive REPL
    model_label = f"model {model_id}" if model_id is not None else "mission default"
    print(f"Matcha REPL [{model_label}] — type 'exit' or Ctrl-C to quit.\n")
    while True:
        try:
            prompt = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nBye.")
            break
        if not prompt:
            continue
        if prompt.lower() in {"exit", "quit", "q"}:
            print("Bye.")
            break
        try:
            reply = call_matcha(prompt, model_id=model_id, timeout=args.timeout)
            print(f"\nMatcha: {reply}\n")
        except Exception as exc:
            print(f"Error: {exc}", file=sys.stderr)


if __name__ == "__main__":
    main()
