"""
Pass 4 — Normalized intervention type taxonomy.

A fixed set of intervention types describing the category of corrective
action that eliminates a failure mechanism.  When none of the named types
fit, use "other" and supply a proposed_type for taxonomy review.
"""

INTERVENTION_TYPES = frozenset({
    "software_fix",
    "configuration_change",
    "validation_guardrail",
    "integration_fix",
    "data_repair",
    "documentation",
    "customer_training",
    "other",
})
