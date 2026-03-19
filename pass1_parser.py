"""
Pass 1 response parser — strict validation of Matcha JSON output.

Expected shapes (v2 — merged with grammar decomposition):

    {
        "phenomenon": "<non-empty string>",
        "confidence": "HIGH" | "MEDIUM",
        "component": "<non-empty string>",
        "operation": "<normalized verb>",
        "unexpected_state": "<non-empty string>"
    }

    {
        "phenomenon": null,
        "confidence": "LOW",
        "component": null,
        "operation": null,
        "unexpected_state": null
    }

Also accepts legacy v1 format (phenomenon-only) for backward compatibility.

Rejects empty, missing, or malformed values.  Always stores the raw
response for later inspection when parsing fails.
"""

import json
import re
from typing import Optional, Tuple

from pass2_parser import normalize_operation, VALID_OPERATIONS, OPERATION_SYNONYMS, Pass2ParseError


class Pass1ParseError(Exception):
    """Raised when the Matcha response cannot be parsed into a valid Pass 1 result."""


VALID_CONFIDENCES = frozenset({"HIGH", "MEDIUM", "LOW"})


def parse_pass1_response(raw_text: str) -> Tuple[dict, Optional[str]]:
    """Parse and validate a Pass 1 Matcha response.

    Returns:
        (parsed_json_dict, phenomenon_string_or_None)

    The parsed_json_dict will contain component, operation, unexpected_state,
    and canonical_failure fields when phenomenon is not None (v2 format).

    Raises:
        Pass1ParseError  if the response is not valid JSON, is missing the
                         ``phenomenon`` key, or contains an empty/whitespace value.
    """
    if not raw_text or not raw_text.strip():
        raise Pass1ParseError("Empty response from model")

    # Strip markdown code fences if present (```json ... ```)
    cleaned = raw_text.strip()
    cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    cleaned = cleaned.strip()

    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError as exc:
        raise Pass1ParseError(f"Invalid JSON: {exc}") from exc

    if not isinstance(parsed, dict):
        raise Pass1ParseError(f"Expected JSON object, got {type(parsed).__name__}")

    if "phenomenon" not in parsed:
        raise Pass1ParseError("Missing 'phenomenon' key in response")

    phenomenon = parsed["phenomenon"]

    # Normalize confidence — default to HIGH for legacy responses without it
    confidence = parsed.get("confidence", "HIGH" if phenomenon is not None else "LOW")
    if isinstance(confidence, str):
        confidence = confidence.upper().strip()
    if confidence not in VALID_CONFIDENCES:
        confidence = "HIGH" if phenomenon is not None else "LOW"
    parsed["confidence"] = confidence

    # LOW confidence → treat as null phenomenon regardless of what model returned
    if confidence == "LOW":
        parsed["phenomenon"] = None
        parsed["component"] = None
        parsed["operation"] = None
        parsed["unexpected_state"] = None
        parsed["canonical_failure"] = None
        return parsed, None

    # null is a valid response — means no observable system behavior
    if phenomenon is None:
        parsed["component"] = None
        parsed["operation"] = None
        parsed["unexpected_state"] = None
        parsed["canonical_failure"] = None
        return parsed, None

    if not isinstance(phenomenon, str):
        raise Pass1ParseError(
            f"'phenomenon' must be a string, got {type(phenomenon).__name__}"
        )

    phenomenon = phenomenon.strip()
    if not phenomenon:
        raise Pass1ParseError("'phenomenon' value is empty after trimming")

    # Parse grammar fields if present (v2 format)
    component = parsed.get("component")
    operation_raw = parsed.get("operation")
    unexpected_state = parsed.get("unexpected_state")

    if component is not None and operation_raw is not None and unexpected_state is not None:
        # Validate grammar fields
        if not isinstance(component, str) or not component.strip():
            raise Pass1ParseError("'component' must be a non-empty string when phenomenon is present")
        if not isinstance(operation_raw, str) or not operation_raw.strip():
            raise Pass1ParseError("'operation' must be a non-empty string when phenomenon is present")
        if not isinstance(unexpected_state, str) or not unexpected_state.strip():
            raise Pass1ParseError("'unexpected_state' must be a non-empty string when phenomenon is present")

        component = component.strip()
        unexpected_state = unexpected_state.strip()

        # Normalize operation verb
        try:
            operation = normalize_operation(operation_raw.strip())
        except Pass2ParseError as exc:
            raise Pass1ParseError(str(exc)) from exc

        # Reconstruct canonical_failure
        canonical_failure = f"{component} + {operation} + {unexpected_state}"

        parsed["component"] = component
        parsed["operation"] = operation
        parsed["unexpected_state"] = unexpected_state
        parsed["canonical_failure"] = canonical_failure
    else:
        # Legacy v1 format or model omitted grammar fields — leave as-is
        parsed.setdefault("component", None)
        parsed.setdefault("operation", None)
        parsed.setdefault("unexpected_state", None)
        parsed.setdefault("canonical_failure", None)

    return parsed, phenomenon
