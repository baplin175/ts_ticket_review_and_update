"""
Pass 1 response parser — strict validation of Matcha JSON output.

Expected shape:
    {"phenomenon": "<non-empty string>"}

Rejects empty, missing, or malformed values.  Always stores the raw
response for later inspection when parsing fails.
"""

import json
import re
from typing import Tuple, Optional


class Pass1ParseError(Exception):
    """Raised when the Matcha response cannot be parsed into a valid Pass 1 result."""


def parse_pass1_response(raw_text: str) -> Tuple[dict, str]:
    """Parse and validate a Pass 1 Matcha response.

    Returns:
        (parsed_json_dict, phenomenon_string)

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
    if not isinstance(phenomenon, str):
        raise Pass1ParseError(
            f"'phenomenon' must be a string, got {type(phenomenon).__name__}"
        )

    phenomenon = phenomenon.strip()
    if not phenomenon:
        raise Pass1ParseError("'phenomenon' value is empty after trimming")

    return parsed, phenomenon
