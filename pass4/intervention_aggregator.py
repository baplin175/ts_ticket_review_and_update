"""
Pass 3 — Intervention Aggregator.

Computes engineering ROI metrics from Pass 3 results stored in the DB:
  - tickets per mechanism class
  - tickets per intervention type
  - top engineering fixes (ranked by ticket count)

Also supports JSON-file-based aggregation for standalone / export use.
"""

import json
import os
from collections import Counter
from typing import Any, Dict, List, Optional


def aggregate_from_results(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Compute aggregation metrics from a list of Pass 3 result dicts.

    Each dict must have 'mechanism_class', 'intervention_type',
    'intervention_action', and 'status' keys.

    Returns a dict with three keys:
      - mechanism_class_counts: {class: count}
      - intervention_type_counts: {type: count}
      - top_engineering_fixes: [{intervention_type, mechanism_class, ticket_count, recommended_fix}]
    """
    success_results = [r for r in results if r.get("status") == "success"]

    mechanism_class_counts: Counter = Counter()
    intervention_type_counts: Counter = Counter()
    # Composite key: (intervention_type, mechanism_class) → (count, representative action)
    fix_tracker: Dict[tuple, Dict[str, Any]] = {}

    for r in success_results:
        mc = r.get("mechanism_class")
        it = r.get("intervention_type")
        ia = r.get("intervention_action")

        if mc:
            mechanism_class_counts[mc] += 1
        if it:
            intervention_type_counts[it] += 1

        if mc and it:
            key = (it, mc)
            if key not in fix_tracker:
                fix_tracker[key] = {"count": 0, "action": ia}
            fix_tracker[key]["count"] += 1

    # Build top fixes, sorted by ticket count descending
    top_fixes = []
    for (it, mc), data in sorted(
        fix_tracker.items(), key=lambda x: x[1]["count"], reverse=True
    ):
        top_fixes.append({
            "intervention_type": it,
            "mechanism_class": mc,
            "ticket_count": data["count"],
            "recommended_fix": data["action"],
        })

    return {
        "mechanism_class_counts": dict(
            mechanism_class_counts.most_common()
        ),
        "intervention_type_counts": dict(
            intervention_type_counts.most_common()
        ),
        "top_engineering_fixes": top_fixes,
    }


def aggregate_from_db() -> Dict[str, Any]:
    """Compute aggregation metrics from all successful Pass 3 rows in the DB.

    Returns the same structure as aggregate_from_results.
    """
    import db

    rows = db.fetch_all("""
        SELECT mechanism_class, intervention_type, intervention_action
        FROM ticket_llm_pass_results
        WHERE pass_name = 'pass3_intervention'
          AND status = 'success'
          AND mechanism_class IS NOT NULL;
    """)

    results = [
        {
            "status": "success",
            "mechanism_class": row[0],
            "intervention_type": row[1],
            "intervention_action": row[2],
        }
        for row in rows
    ]
    return aggregate_from_results(results)


def write_artifacts(
    aggregation: Dict[str, Any],
    output_dir: str,
    *,
    interventions: Optional[List[Dict[str, Any]]] = None,
) -> List[str]:
    """Write Pass 3 JSON artifacts to the output directory.

    Returns a list of file paths written.
    """
    os.makedirs(output_dir, exist_ok=True)
    written = []

    if interventions is not None:
        path = os.path.join(output_dir, "interventions.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(interventions, f, indent=2, ensure_ascii=False)
        written.append(path)

    path = os.path.join(output_dir, "mechanism_class_counts.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(aggregation["mechanism_class_counts"], f, indent=2, ensure_ascii=False)
    written.append(path)

    path = os.path.join(output_dir, "intervention_type_counts.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(aggregation["intervention_type_counts"], f, indent=2, ensure_ascii=False)
    written.append(path)

    path = os.path.join(output_dir, "top_engineering_fixes.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(aggregation["top_engineering_fixes"], f, indent=2, ensure_ascii=False)
    written.append(path)

    return written
