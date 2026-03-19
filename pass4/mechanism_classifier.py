"""
Pass 4 response parser — strict validation of Matcha JSON output.

Expected shape:
    {
        "mechanism_class": "<one of MECHANISM_CLASSES>",
        "intervention_type": "<one of INTERVENTION_TYPES>",
        "intervention_action": "<non-empty string>"
    }

Rejects unknown classes/types, empty values, and malformed JSON.
"""

import json
import re
from typing import Dict, Tuple

from pass4.mechanism_classes import MECHANISM_CLASSES
from pass4.intervention_types import INTERVENTION_TYPES


class Pass4ParseError(Exception):
    """Raised when the Matcha response cannot be parsed into a valid Pass 4 result."""


_REQUIRED_KEYS = ("mechanism_class", "intervention_type", "intervention_action")


def parse_pass4_response(raw_text: str) -> Tuple[dict, str, str, str]:
    """Parse and validate a Pass 4 Matcha response.

    Returns:
        (parsed_json_dict, mechanism_class, intervention_type, intervention_action)

    Raises:
        Pass4ParseError  if the response is not valid JSON, is missing
                         required keys, or contains invalid values.
    """
    if not raw_text or not raw_text.strip():
        raise Pass4ParseError("Empty response from model")

    # Strip markdown code fences if present (```json ... ```)
    cleaned = raw_text.strip()
    cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    cleaned = cleaned.strip()

    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError as exc:
        raise Pass4ParseError(f"Invalid JSON: {exc}") from exc

    if not isinstance(parsed, dict):
        raise Pass4ParseError(f"Expected JSON object, got {type(parsed).__name__}")

    # Validate required keys
    for key in _REQUIRED_KEYS:
        if key not in parsed:
            raise Pass4ParseError(f"Missing '{key}' key in response")

    # Extract and validate mechanism_class
    mechanism_class = parsed["mechanism_class"]
    if mechanism_class is None:
        raise Pass4ParseError("'mechanism_class' must not be null")
    if not isinstance(mechanism_class, str):
        raise Pass4ParseError(
            f"'mechanism_class' must be a string, got {type(mechanism_class).__name__}"
        )
    mechanism_class = mechanism_class.strip().lower()
    if not mechanism_class:
        raise Pass4ParseError("'mechanism_class' is empty after trimming")
    if mechanism_class not in MECHANISM_CLASSES:
        raise Pass4ParseError(
            f"Unknown mechanism_class '{mechanism_class}'. "
            f"Must be one of: {sorted(MECHANISM_CLASSES)}"
        )
    parsed["mechanism_class"] = mechanism_class

    # When mechanism_class is 'other', require proposed_class
    if mechanism_class == "other":
        proposed = parsed.get("proposed_class")
        if not proposed or not isinstance(proposed, str) or not proposed.strip():
            raise Pass4ParseError(
                "'proposed_class' is required when mechanism_class is 'other'"
            )
        parsed["proposed_class"] = proposed.strip().lower()

    # Extract and validate intervention_type
    intervention_type = parsed["intervention_type"]
    if intervention_type is None:
        raise Pass4ParseError("'intervention_type' must not be null")
    if not isinstance(intervention_type, str):
        raise Pass4ParseError(
            f"'intervention_type' must be a string, got {type(intervention_type).__name__}"
        )
    intervention_type = intervention_type.strip().lower()
    if not intervention_type:
        raise Pass4ParseError("'intervention_type' is empty after trimming")
    if intervention_type not in INTERVENTION_TYPES:
        raise Pass4ParseError(
            f"Unknown intervention_type '{intervention_type}'. "
            f"Must be one of: {sorted(INTERVENTION_TYPES)}"
        )
    parsed["intervention_type"] = intervention_type

    # When intervention_type is 'other', require proposed_type
    if intervention_type == "other":
        proposed = parsed.get("proposed_type")
        if not proposed or not isinstance(proposed, str) or not proposed.strip():
            raise Pass4ParseError(
                "'proposed_type' is required when intervention_type is 'other'"
            )
        parsed["proposed_type"] = proposed.strip().lower()

    # Extract and validate intervention_action
    intervention_action = parsed["intervention_action"]
    if intervention_action is None:
        raise Pass4ParseError("'intervention_action' must not be null")
    if not isinstance(intervention_action, str):
        raise Pass4ParseError(
            f"'intervention_action' must be a string, got {type(intervention_action).__name__}"
        )
    intervention_action = intervention_action.strip()
    if not intervention_action:
        raise Pass4ParseError("'intervention_action' is empty after trimming")
    parsed["intervention_action"] = intervention_action

    return parsed, mechanism_class, intervention_type, intervention_action


def validate_intervention_action(intervention_action: str, mechanism: str) -> str:
    """Run lightweight heuristics on a parsed intervention_action string.

    Raises Pass4ParseError if the action contains support/ticket language
    or is a restatement of the mechanism.  Returns the action unchanged
    on success.
    """
    action_lower = intervention_action.lower().strip()
    mech_lower = mechanism.lower().strip()

    # Reject exact restatement of the mechanism
    if action_lower == mech_lower:
        raise Pass4ParseError(
            "intervention_action is an exact restatement of the mechanism"
        )

    # Reject administrative / support-workflow language
    _FORBIDDEN_PHRASES = (
        "ticket",
        "support agent",
        "support team",
        "support staff",
        "troubleshoot",
        "troubleshooting",
        "escalat",
    )
    for phrase in _FORBIDDEN_PHRASES:
        if phrase in action_lower:
            raise Pass4ParseError(
                f"intervention_action contains administrative language: '{phrase}'"
            )

    return intervention_action
