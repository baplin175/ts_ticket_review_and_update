"""
Pass 3 response parser — strict validation of Matcha JSON output.

Expected shape:
    {
        "mechanism": "<non-empty string>"
    }

Rejects empty, missing, or malformed values.  Provides lightweight
validation to catch obvious symptom restatements and administrative text.
"""

import json
import re
from typing import Tuple


class Pass3ParseError(Exception):
    """Raised when the Matcha response cannot be parsed into a valid Pass 3 result."""


# ── Words that indicate administrative / support-workflow text ────────

_ADMIN_WORDS = frozenset({
    "ticket",
    "customer",
    "agent",
    "troubleshoot",
    "troubleshooting",
    "support team",
    "support staff",
    "escalat",          # catches escalate, escalated, escalation
})


def validate_mechanism(mechanism: str, canonical_failure: str) -> str:
    """Run lightweight heuristics on a parsed mechanism string.

    Raises Pass3ParseError if the mechanism appears to be a restatement
    of the canonical failure or contains administrative/support-workflow
    language.  Returns the mechanism unchanged on success.
    """
    mech_lower = mechanism.lower().strip()
    cf_lower = canonical_failure.lower().strip()

    # Reject exact restatement (case-insensitive)
    if mech_lower == cf_lower:
        raise Pass3ParseError(
            "Mechanism is an exact restatement of the canonical failure"
        )

    # Reject administrative / support-workflow language
    for word in _ADMIN_WORDS:
        if word in mech_lower:
            raise Pass3ParseError(
                f"Mechanism contains administrative language: '{word}'"
            )

    return mechanism


# ── Response parser ──────────────────────────────────────────────────

def parse_pass3_response(raw_text: str) -> Tuple[dict, str]:
    """Parse and validate a Pass 3 Matcha response.

    Returns:
        (parsed_json_dict, mechanism)

    Raises:
        Pass3ParseError  if the response is not valid JSON, is missing
                         the required key, or contains invalid values.
    """
    if not raw_text or not raw_text.strip():
        raise Pass3ParseError("Empty response from model")

    # Strip markdown code fences if present (```json ... ```)
    cleaned = raw_text.strip()
    cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    cleaned = cleaned.strip()

    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError as exc:
        raise Pass3ParseError(f"Invalid JSON: {exc}") from exc

    if not isinstance(parsed, dict):
        raise Pass3ParseError(f"Expected JSON object, got {type(parsed).__name__}")

    # Validate required key
    if "mechanism" not in parsed:
        raise Pass3ParseError("Missing 'mechanism' key in response")

    mechanism = parsed["mechanism"]

    if mechanism is None:
        raise Pass3ParseError("'mechanism' must not be null")

    if not isinstance(mechanism, str):
        raise Pass3ParseError(
            f"'mechanism' must be a string, got {type(mechanism).__name__}"
        )

    mechanism = mechanism.strip()
    if not mechanism:
        raise Pass3ParseError("'mechanism' is empty after trimming")

    parsed["mechanism"] = mechanism

    return parsed, mechanism
