"""
Pass 3 response parser — strict validation of Matcha JSON output.

Expected shape:
    {
        "mechanism": "<non-empty string>",
        "category": "software_defect" | "configuration" | "user_training" | "data_issue",
        "evidence": "from_thread" | "inferred"
    }

Rejects empty, missing, or malformed values.  Provides lightweight
validation to catch obvious symptom restatements and administrative text.
"""

import json
import re
from typing import Tuple


class Pass3ParseError(Exception):
    """Raised when the Matcha response cannot be parsed into a valid Pass 3 result."""


# ── Phrases that indicate administrative / support-workflow text ──────

_ADMIN_PHRASES = frozenset({
    "ticket",
    "support agent",
    "support team",
    "support staff",
    "troubleshoot",
    "troubleshooting",
    "escalat",          # catches escalate, escalated, escalation
    "customer reported",
    "customer contacted",
    "customer called",
    "customer requested",
    "customer asked",
    "customer says",
    "customer said",
    "customer needs",
    "customer complaint",
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
    for phrase in _ADMIN_PHRASES:
        if phrase in mech_lower:
            raise Pass3ParseError(
                f"Mechanism contains administrative language: '{phrase}'"
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

    # Validate required key: mechanism
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

    # Validate optional-but-expected fields: category, evidence
    _VALID_CATEGORIES = {"software_defect", "configuration", "user_training", "data_issue"}
    _VALID_EVIDENCE = {"from_thread", "inferred"}

    category = parsed.get("category", "software_defect")
    if isinstance(category, str):
        category = category.strip().lower()
    if category not in _VALID_CATEGORIES:
        category = "software_defect"
    parsed["category"] = category

    evidence = parsed.get("evidence", "inferred")
    if isinstance(evidence, str):
        evidence = evidence.strip().lower()
    if evidence not in _VALID_EVIDENCE:
        evidence = "inferred"
    parsed["evidence"] = evidence

    return parsed, mechanism
