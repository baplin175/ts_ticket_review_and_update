"""
Pass 5 response parser — strict validation of Matcha plain-text output.

Expected shape:
    A single snake_case phrase (max 6 words, no surrounding whitespace or tags).

Rejects empty, multi-line, non-snake_case, or overly long values.
"""

import re
from typing import Tuple


class Pass5ParseError(Exception):
    """Raised when the Matcha response cannot be parsed into a valid Pass 5 cluster key."""


# ── Validation constants ─────────────────────────────────────────────

_MAX_WORDS = 6
_SNAKE_CASE_RE = re.compile(r"^[a-z][a-z0-9]*(?:_[a-z0-9]+)*$")

# Strip XML-style tags the model sometimes wraps around output
_TAG_RE = re.compile(r"<[^>]+>")


def _clean_response(raw_text: str) -> str:
    """Strip markdown fences, XML tags, and surrounding whitespace."""
    cleaned = raw_text.strip()
    # Strip markdown code fences
    cleaned = re.sub(r"^```(?:\w+)?\s*", "", cleaned)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    # Strip XML-style tags (e.g. <snake_case_key_only>)
    cleaned = _TAG_RE.sub("", cleaned)
    # Take only the first non-empty line (model may add explanation)
    for line in cleaned.strip().splitlines():
        line = line.strip()
        if line:
            cleaned = line
            break
    return cleaned.strip()


def parse_pass5_response(raw_text: str) -> Tuple[dict, str]:
    """Parse and validate a Pass 5 Matcha response.

    Returns:
        (parsed_dict, cluster_key)

    Raises:
        Pass5ParseError  if the response is empty, not valid snake_case,
                         or exceeds the max word count.
    """
    if not raw_text or not raw_text.strip():
        raise Pass5ParseError("Empty response from model")

    cluster_key = _clean_response(raw_text)

    if not cluster_key:
        raise Pass5ParseError("Cluster key is empty after cleaning")

    # Normalise: lowercase, strip surrounding quotes
    cluster_key = cluster_key.strip("'\"` ").lower()

    # Replace any remaining spaces/hyphens with underscores
    cluster_key = re.sub(r"[\s\-]+", "_", cluster_key)

    # Remove any trailing/leading underscores
    cluster_key = cluster_key.strip("_")

    if not cluster_key:
        raise Pass5ParseError("Cluster key is empty after normalisation")

    # Validate snake_case format
    if not _SNAKE_CASE_RE.match(cluster_key):
        raise Pass5ParseError(
            f"Cluster key is not valid snake_case: '{cluster_key}'"
        )

    # Validate word count (underscores separate words)
    word_count = len(cluster_key.split("_"))
    if word_count > _MAX_WORDS:
        raise Pass5ParseError(
            f"Cluster key has {word_count} words (max {_MAX_WORDS}): '{cluster_key}'"
        )

    parsed = {"cluster_key": cluster_key}
    return parsed, cluster_key
