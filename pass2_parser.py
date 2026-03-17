"""
Pass 2 response parser — strict validation of Matcha JSON output.

Expected shape:
    {
        "component": "<non-empty string>",
        "operation": "<normalized verb>",
        "unexpected_state": "<non-empty string>",
        "canonical_failure": "<Component> + <Operation> + <Unexpected State>"
    }

Rejects empty, missing, or malformed values.  Normalizes operation verbs
through an explicit synonym map.  Reconstructs canonical_failure from
parsed fields to ensure structural consistency.
"""

import json
import re
from typing import Tuple


class Pass2ParseError(Exception):
    """Raised when the Matcha response cannot be parsed into a valid Pass 2 result."""


# ── Operation normalization ──────────────────────────────────────────

VALID_OPERATIONS = frozenset({
    "post",
    "import",
    "export",
    "print",
    "load",
    "transfer",
    "calculate",
    "attach",
    "generate",
    "recover",
    "create",
    "update",
})

OPERATION_SYNONYMS = {
    # import family
    "importing": "import",
    "upload": "import",
    "ingest": "import",
    "read": "import",
    # export family
    "exporting": "export",
    "download": "export",
    "extract": "export",
    # transfer family
    "send": "transfer",
    "transmit": "transfer",
    "move": "transfer",
    "sync": "transfer",
    "synchronize": "transfer",
    # generate family
    "build": "generate",
    "produce": "generate",
    "compile": "generate",
    # calculate family
    "compute": "calculate",
    "sum": "calculate",
    "tally": "calculate",
    # create family
    "insert": "create",
    "add": "create",
    # update family
    "modify": "update",
    "edit": "update",
    "change": "update",
    "save": "update",
    "write": "update",
    "delete": "update",
    "remove": "update",
    # print family
    "printing": "print",
    # post family
    "posting": "post",
    # load family
    "loading": "load",
    "open": "load",
    "launch": "load",
    "display": "load",
    "show": "load",
    "view": "load",
    "render": "load",
    "validate": "load",
    "test": "load",
    # recover family
    "restore": "recover",
    "rollback": "recover",
    # attach family
    "link": "attach",
    "connect": "attach",
}


def normalize_operation(operation: str) -> str:
    """Normalize an operation verb to the canonical vocabulary.

    Returns the canonical verb if the input is a known operation or synonym.
    Raises Pass2ParseError if the operation cannot be mapped.
    """
    op = operation.lower().strip()
    if op in VALID_OPERATIONS:
        return op
    if op in OPERATION_SYNONYMS:
        return OPERATION_SYNONYMS[op]
    raise Pass2ParseError(
        f"Unknown operation '{operation}'. "
        f"Valid operations: {sorted(VALID_OPERATIONS)}"
    )


# ── Response parser ──────────────────────────────────────────────────

REQUIRED_KEYS = ("component", "operation", "unexpected_state", "canonical_failure")


def parse_pass2_response(raw_text: str) -> Tuple[dict, str, str, str, str]:
    """Parse and validate a Pass 2 Matcha response.

    Returns:
        (parsed_json_dict, component, operation, unexpected_state, canonical_failure)

    Raises:
        Pass2ParseError  if the response is not valid JSON, is missing
                         required keys, or contains invalid values.
    """
    if not raw_text or not raw_text.strip():
        raise Pass2ParseError("Empty response from model")

    # Strip markdown code fences if present (```json ... ```)
    cleaned = raw_text.strip()
    cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    cleaned = cleaned.strip()

    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError as exc:
        raise Pass2ParseError(f"Invalid JSON: {exc}") from exc

    if not isinstance(parsed, dict):
        raise Pass2ParseError(f"Expected JSON object, got {type(parsed).__name__}")

    # Validate all required keys exist and are non-empty strings
    for key in REQUIRED_KEYS:
        if key not in parsed:
            raise Pass2ParseError(f"Missing '{key}' key in response")
        value = parsed[key]
        if not isinstance(value, str):
            raise Pass2ParseError(
                f"'{key}' must be a string, got {type(value).__name__}"
            )
        if not value.strip():
            raise Pass2ParseError(f"'{key}' value is empty after trimming")

    component = parsed["component"].strip()
    operation_raw = parsed["operation"].strip()
    unexpected_state = parsed["unexpected_state"].strip()

    # Normalize operation verb
    operation = normalize_operation(operation_raw)

    # Reconstruct canonical_failure from parsed fields
    canonical_failure = f"{component} + {operation} + {unexpected_state}"

    # Update parsed dict with normalized values
    parsed["component"] = component
    parsed["operation"] = operation
    parsed["unexpected_state"] = unexpected_state
    parsed["canonical_failure"] = canonical_failure

    return parsed, component, operation, unexpected_state, canonical_failure
