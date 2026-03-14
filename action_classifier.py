"""
action_classifier.py — Deterministic first-pass action classification.

Classifies ticket actions into one of these categories based on
cleaned_description text, party, action_type, and is_empty:

    technical_work            — references to code, SQL, configs, bugs, testing
    customer_problem_statement — customer describing their issue or request
    status_update             — brief progress notes, "checking in", "FYI"
    scheduling                — meeting coordination, availability, timeslots
    waiting_on_customer       — awaiting file, review, approval, readiness
    delivery_confirmation     — confirming delivery, deployment, completion
    administrative_noise      — signatures only, greetings, "thanks", one-word ACKs
    system_noise              — auto-generated, system events, empty actions
    unknown                   — none of the above matched

Design notes:
- Rule-based only; no LLM calls.
- Patterns are checked in priority order; first match wins.
- Easy to extend: add patterns to the relevant list or add new categories.
"""

import re
from typing import Optional

# ── Pattern lists ────────────────────────────────────────────────────
# Each list contains (compiled_regex, description) tuples.
# Patterns are matched against lowercased cleaned_description.

_TECHNICAL_PATTERNS = [
    re.compile(p, re.IGNORECASE) for p in [
        r"\b(?:stored\s+proc|sproc|SP\b)",
        r"\bSQL\b",
        r"\b(?:SELECT|INSERT|UPDATE|DELETE|ALTER|CREATE)\s+",
        r"\b(?:database|db)\s+(?:table|column|index|schema|query|migration)",
        r"\b(?:script|cron|batch\s+job|etl)\b",
        r"\b(?:stack\s*trace|exception|error\s+log|traceback|null\s*ref)",
        r"\b(?:API|endpoint|REST|webhook|HTTP\s+\d{3})\b",
        r"\b(?:validation|regex|mapping|transform|parsing)\b",
        r"\b(?:file\s+format|CSV|XML|JSON|EDI|flat\s+file)\b",
        r"\b(?:duplicate\s+match|dedup|merge\s+logic)\b",
        r"\b(?:sequence|book\s+routing|routing\s+rule)\b",
        r"\b(?:test|QA|UAT|regression|unit\s+test|staging)\b",
        r"\b(?:deploy|release|promote|go[\s-]*live|production)\b",
        r"\b(?:config|setting|parameter|flag|toggle|env\s+var)\b",
        r"\b(?:bug|defect|fix|patch|hotfix|workaround)\b",
        r"\b(?:code|function|method|class|module|library|package)\b",
        r"\b(?:server|instance|cluster|container|docker|k8s)\b",
        r"\b(?:log|logging|debug|debugg?ing|profil(?:e|ing))\b",
        r"\b(?:import|export|load|extract|ingest)\b",
        r"\b(?:field|column|row|record|table)\s+(?:is|was|should|needs?|has)",
    ]
]

_SCHEDULING_PATTERNS = [
    re.compile(p, re.IGNORECASE) for p in [
        r"\b(?:schedule|reschedule|calendar|meeting)\b",
        r"\b(?:available|availability|free\s+at|slot)\b",
        r"\b(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)\b.*\b(?:at|from|between)\b",
        r"\b(?:\d{1,2}:\d{2}\s*(?:AM|PM|a\.m\.|p\.m\.))",
        r"\b(?:call|zoom|teams\s+meeting|screen\s*share)\b.*\b(?:at|on|tomorrow|today)\b",
        r"\b(?:reschedul|postpone|push\s+back|move\s+(?:the|our)\s+(?:call|meeting))\b",
    ]
]

_WAITING_ON_CUSTOMER_PATTERNS = [
    re.compile(p, re.IGNORECASE) for p in [
        r"\b(?:waiting|await)\w*\s+(?:on|for)\s+(?:your|the\s+(?:customer|client|file|data|approval|review|response))",
        r"\b(?:please\s+(?:send|provide|share|upload|confirm|review|approve))\b",
        r"\b(?:need(?:s|ed)?)\s+(?:your|the\s+(?:customer|client))\b",
        r"\b(?:pending)\s+(?:customer|client|approval|review|file)\b",
        r"\b(?:ball\s+is\s+in\s+(?:your|their)\s+court)\b",
        r"\bonce\s+(?:you|they|the\s+customer)\s+(?:provide|send|confirm|approve)\b",
    ]
]

_DELIVERY_PATTERNS = [
    re.compile(p, re.IGNORECASE) for p in [
        r"\b(?:deliver(?:ed|y)|ship(?:ped)?|push(?:ed)?\s+(?:to|live))\b",
        r"\b(?:promoted?\s+to\s+(?:live|production|prod))\b",
        r"\b(?:complet(?:ed?|ion)|done|finished|wrapped\s+up)\b.*\b(?:deploy|release|change|update)",
        r"\b(?:installed?|applied|rolled?\s+out)\b.*\b(?:fix|patch|update|change)\b",
        r"\bconfirm(?:ed|ing)?\s+(?:that\s+)?(?:the\s+)?(?:change|fix|update|deploy)",
    ]
]

_STATUS_UPDATE_PATTERNS = [
    re.compile(p, re.IGNORECASE) for p in [
        r"\b(?:just\s+)?(?:checking\s+in|following\s+up|circling\s+back)\b",
        r"\b(?:quick\s+update|FYI|heads[\s-]*up|for\s+your\s+(?:info|information|reference))\b",
        r"\b(?:still\s+(?:working|looking|investigating|reviewing|testing))\b",
        r"\b(?:update|progress)[\s:]+\b",
        r"\b(?:bumping|nudging|pinging)\s+this\b",
        r"\b(?:no\s+(?:update|change|news)\s+(?:yet|so\s+far))\b",
        r"\b(?:will\s+(?:follow\s+up|update\s+(?:you|the\s+ticket)|circle\s+back))\b",
        r"\b(?:let\s+me\s+know)\b",
    ]
]

_ADMIN_NOISE_PATTERNS = [
    re.compile(p, re.IGNORECASE) for p in [
        r"^(?:thanks?|thank\s+you|ty|thx|cheers|regards|best)\W*$",
        r"^(?:ok|okay|sounds\s+good|got\s+it|noted|acknowledged|ack|roger)\W*$",
        r"^(?:hi|hello|hey|good\s+(?:morning|afternoon|evening))\W*$",
        r"^(?:please\s+see\s+(?:above|below|attached))\W*$",
    ]
]


# ── Classifier function ─────────────────────────────────────────────

def classify_action(
    cleaned_description: str | None,
    party: str | None = None,
    action_type: str | None = None,
    is_empty: bool = False,
) -> str:
    """Return an action_class string for one ticket action.

    Parameters
    ----------
    cleaned_description : The cleaned/stripped action text.
    party : "inh" or "cust" (or None).
    action_type : e.g. "Comment", "Email", etc.
    is_empty : True if the action has no meaningful text.

    Returns
    -------
    One of the classification strings listed in the module docstring.
    """

    # ── Empty / system actions
    if is_empty or not cleaned_description or not cleaned_description.strip():
        return "system_noise"

    # ── "Description" action type is always the initial customer problem
    if action_type and action_type.strip().lower() == "description":
        return "customer_problem_statement"

    text = cleaned_description.strip()

    # Very short text (≤ 15 chars) that's likely just an ACK
    if len(text) <= 15:
        for pat in _ADMIN_NOISE_PATTERNS:
            if pat.search(text):
                return "administrative_noise"
        # Short text from system action types
        if action_type and action_type.lower() in ("status change", "assignment", "system"):
            return "system_noise"

    # ── Administrative noise (full patterns, checked before substance)
    for pat in _ADMIN_NOISE_PATTERNS:
        if pat.search(text):
            return "administrative_noise"

    # ── Scheduling
    for pat in _SCHEDULING_PATTERNS:
        if pat.search(text):
            return "scheduling"

    # ── Waiting on customer (inHANCE asking customer for something)
    if party == "inh":
        for pat in _WAITING_ON_CUSTOMER_PATTERNS:
            if pat.search(text):
                return "waiting_on_customer"

    # ── Delivery confirmation
    for pat in _DELIVERY_PATTERNS:
        if pat.search(text):
            return "delivery_confirmation"

    # ── Technical work (the richest pattern set)
    for pat in _TECHNICAL_PATTERNS:
        if pat.search(text):
            return "technical_work"

    # ── Status update (generic progress / FYI)
    for pat in _STATUS_UPDATE_PATTERNS:
        if pat.search(text):
            return "status_update"

    # ── Customer problem statement: customer text that wasn't caught above
    if party == "cust" and len(text) > 30:
        return "customer_problem_statement"

    # ── Fallback
    return "unknown"


# ── Noise filter helper ──────────────────────────────────────────────

# Classes considered "noise" for thread-rollup purposes
NOISE_CLASSES = frozenset({
    "administrative_noise",
    "system_noise",
    "scheduling",
})

# Classes considered substantive for technical-core rollups
TECHNICAL_SUBSTANCE_CLASSES = frozenset({
    "technical_work",
    "customer_problem_statement",
    "delivery_confirmation",
})


def is_noise(action_class: str) -> bool:
    """Return True if the action_class is considered noise."""
    return action_class in NOISE_CLASSES


def is_technical_substance(action_class: str) -> bool:
    """Return True if the action_class is considered technical substance."""
    return action_class in TECHNICAL_SUBSTANCE_CLASSES
