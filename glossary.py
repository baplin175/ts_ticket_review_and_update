"""
Centralized glossary definitions used across the application.

Each section is a dict with:
    title   — display heading
    icon    — Tabler icon name (for Dash/Mantine rendering)
    color   — theme color
    items   — list of (term, definition) tuples

Sections are grouped by topic.  Import GLOSSARY to iterate all sections,
or import individual section constants by name.
"""

# ── Pipeline Passes ──────────────────────────────────────────────────

PIPELINE_PASSES = {
    "title": "Pipeline Passes",
    "icon": "tabler:arrows-right",
    "color": "blue",
    "items": [
        ("Pass 1 — Phenomenon",
         "Extracts the single observable system behavior from the ticket thread. "
         "Also decomposes it into Component + Operation + Unexpected State (the canonical failure grammar)."),
        ("Pass 3 — Mechanism",
         "Infers the most plausible internal system mechanism that would produce "
         "the observed failure — e.g. a validation bug, config mismatch, or integration error."),
        ("Pass 4 — Intervention",
         "Maps the mechanism to a normalized mechanism class and recommends a "
         "specific intervention type and action to fix the root cause."),
    ],
}

# ── Canonical Failure Grammar ────────────────────────────────────────

CANONICAL_FAILURE_GRAMMAR = {
    "title": "Canonical Failure Grammar",
    "icon": "tabler:vocabulary",
    "color": "indigo",
    "items": [
        ("Component",
         "The system module, subsystem, workflow, report, or integration involved. "
         "Examples: Billing module, Invoice Cloud integration, Meter import process."),
        ("Operation",
         "What the component was doing when it failed, normalized to a fixed 12-verb vocabulary: "
         "generate, calculate, import, export, validate, update, delete, load, create, process, sync, query."),
        ("Unexpected State",
         "The observable deviation from correct behavior — what went wrong. "
         "Examples: returns incorrect total, silently drops records, displays stale data."),
        ("Canonical Failure",
         "The full reconstructed sentence: Component + Operation + Unexpected State. "
         "Used for cross-ticket comparison and clustering."),
        ("Phenomenon",
         "A single normalized sentence describing what the system did or failed to do, "
         "as visible to the user. Not diagnosis — only the observable behavior."),
    ],
}

# ── Mechanism Classes ────────────────────────────────────────────────

MECHANISM_CLASSES = {
    "title": "Mechanism Classes",
    "icon": "tabler:category",
    "color": "violet",
    "items": [
        ("Calculation Logic Error",
         "A formula, rounding rule, aggregation, or arithmetic operation produces an incorrect result."),
        ("Schema Mismatch",
         "Database columns, table structures, or data types don't match what the code expects."),
        ("Data Validation Failure",
         "Input data that should be rejected is accepted (or vice versa), "
         "leading to corrupt or nonsensical records."),
        ("Configuration Mismatch",
         "A setting, flag, or environment variable is wrong or missing, "
         "causing the system to behave differently than intended."),
        ("Authentication Failure",
         "Login, token exchange, session management, or credential validation breaks down."),
        ("Integration Mapping Error",
         "Field mapping between two systems is wrong — data lands in the wrong place or is transformed incorrectly."),
        ("Integration Communication Failure",
         "An API call, file transfer, or message exchange between systems fails "
         "(timeout, connection refused, protocol error)."),
        ("State Inconsistency",
         "The system's internal state becomes contradictory — e.g. a record marked 'complete' "
         "still has pending steps."),
        ("Synchronization Failure",
         "Two systems or processes that should stay in sync have drifted apart."),
        ("Dependency Missing",
         "A required library, service, file, or prerequisite record is absent."),
        ("Field Mapping Error",
         "A single field within a system is read from or written to the wrong location."),
        ("Cache Inconsistency",
         "Cached data is stale or invalid, causing the system to act on outdated information."),
        ("Permission Error",
         "A user or service account lacks the required permissions to perform an operation."),
        ("Other",
         "None of the above classes fit. A 'proposed_class' is submitted for taxonomy review."),
    ],
}

# ── Intervention Types ───────────────────────────────────────────────

INTERVENTION_TYPES = {
    "title": "Intervention Types",
    "icon": "tabler:tools",
    "color": "teal",
    "items": [
        ("Software Fix",
         "A code change — bug fix, logic correction, or new feature — deployed to production."),
        ("Configuration Change",
         "Adjusting a setting, flag, connection string, or environment variable without code changes."),
        ("Validation Guardrail",
         "Adding or strengthening input validation to prevent bad data from entering the system."),
        ("Integration Fix",
         "Correcting the interface between two systems — field mapping, protocol, or API contract."),
        ("Data Repair",
         "One-time correction of existing bad data in the database (manual SQL, migration script, etc.)."),
        ("Documentation",
         "Updating or creating user-facing or internal documentation to prevent recurrence."),
        ("Customer Training",
         "Educating the customer on correct usage, workflows, or configuration to avoid the issue."),
        ("Other",
         "None of the above types fit. A 'proposed_type' is submitted for taxonomy review."),
    ],
}

# ── Dashboard Metrics ────────────────────────────────────────────────

DASHBOARD_METRICS = {
    "title": "Dashboard Metrics",
    "icon": "tabler:chart-bar",
    "color": "orange",
    "items": [
        ("Tickets Analyzed",
         "Number of tickets that have a successful Pass 1 (phenomenon extraction) result."),
        ("Mechanisms Found",
         "Number of tickets with a successful Pass 3 (mechanism inference) result."),
        ("Interventions Mapped",
         "Number of tickets with a successful Pass 4 (intervention mapping) result."),
        ("Pipeline Completion",
         "Percentage of Pass 1 tickets that made it all the way through to Pass 4."),
        ("Top Mechanism",
         "The mechanism class with the highest ticket count across all analyzed tickets."),
        ("Sankey Diagram",
         "A flow visualization showing how tickets move from Component → Mechanism Class → Intervention Type, "
         "with link thickness proportional to ticket count."),
        ("Engineering ROI",
         "The Top Engineering Fixes table ranks mechanism × intervention combinations by ticket count, "
         "highlighting where a single fix would resolve the most tickets."),
    ],
}

# ── All sections (ordered) ───────────────────────────────────────────

GLOSSARY = [
    PIPELINE_PASSES,
    CANONICAL_FAILURE_GRAMMAR,
    MECHANISM_CLASSES,
    INTERVENTION_TYPES,
    DASHBOARD_METRICS,
]
