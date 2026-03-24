"""User-facing numbering for the active RCA stages.

Internal pass identifiers remain unchanged for backward compatibility.
"""

from __future__ import annotations


STAGES = {
    "phenomenon": {"sequence": 1, "name": "Phenomenon", "internal": "pass1_phenomenon"},
    "grammar_legacy": {"sequence": None, "name": "Grammar", "internal": "pass2_grammar"},
    "mechanism": {"sequence": 2, "name": "Mechanism", "internal": "pass3_mechanism"},
    "intervention": {"sequence": 3, "name": "Intervention", "internal": "pass4_intervention"},
    "cluster_key": {"sequence": 4, "name": "Cluster Key", "internal": "pass5_cluster_key"},
}


def stage_label(stage_key: str) -> str:
    stage = STAGES[stage_key]
    if stage["sequence"] is None:
        return f"Legacy {stage['name']}"
    return f"Pass {stage['sequence']}"


def stage_title(stage_key: str) -> str:
    stage = STAGES[stage_key]
    if stage["sequence"] is None:
        return f"Legacy {stage['name']}"
    return f"Pass {stage['sequence']} — {stage['name']}"
