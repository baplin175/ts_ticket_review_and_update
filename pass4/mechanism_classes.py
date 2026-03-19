"""
Pass 4 — Normalized mechanism class taxonomy.

A fixed set of mechanism classes used to categorize failure mechanisms.
No free-form classes are allowed; every mechanism must map to one of these.
"""

MECHANISM_CLASSES = frozenset({
    "calculation_logic_error",
    "schema_mismatch",
    "data_validation_failure",
    "configuration_mismatch",
    "authentication_failure",
    "integration_mapping_error",
    "integration_communication_failure",
    "state_inconsistency",
    "synchronization_failure",
    "dependency_missing",
    "field_mapping_error",
    "cache_inconsistency",
    "permission_error",
})
