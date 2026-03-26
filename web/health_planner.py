"""Customer health improvement plan generation via Matcha."""

from __future__ import annotations

import json
from typing import Any

from config import MATCHA_MISSION_ID
from matcha_client import call_matcha
from prompt_store import get_prompt
from rollups.customer_health import simulate_improvement_to_band

from . import data

PROMPT_NAME = "customer_health_improvement_plan"
MODEL_NAME = f"matcha-{MATCHA_MISSION_ID}"


def _build_group_filter_label(selected_groups: list[str], available_groups: list[str]) -> str:
    selected = [g for g in selected_groups if g]
    available = [g for g in available_groups if g]
    if selected and available and set(selected) == set(available):
        return "All groups except Marketing and Sales"
    if not selected:
        return "No groups selected"
    return ", ".join(selected)


def generate_customer_health_plan(
    customer: str,
    as_of_date: str,
    selected_groups: list[str],
    available_groups: list[str],
    target_band: str,
) -> dict[str, Any]:
    history = data.get_customer_health_history(customer, selected_groups)
    current_row = next(
        (row for row in history if str(row.get("as_of_date")) == as_of_date), None
    )
    if not current_row:
        raise ValueError(
            "No health history found for the selected customer/date/groups."
        )

    contributors = data.get_customer_health_contributors(
        customer, as_of_date, selected_groups
    )
    current_score = float(current_row.get("customer_health_score") or 0)
    simulation = simulate_improvement_to_band(contributors, current_score, target_band)
    group_filter_label = _build_group_filter_label(selected_groups, available_groups)

    prompt_record = get_prompt(PROMPT_NAME, allow_fallback=False)
    prompt_template = prompt_record["content"]
    payload = {
        "customer": customer,
        "as_of_date": as_of_date,
        "group_filter_label": group_filter_label,
        "selected_groups": selected_groups,
        "current_metrics": current_row,
        "simulation": {
            "target_band": simulation["target_band"],
            "current_score": simulation["current_score"],
            "projected_score": simulation["projected_score"],
            "projected_band": simulation["projected_band"],
            "score_reduction": simulation["score_reduction"],
            "tickets_to_resolve_count": len(simulation["tickets_to_resolve"]),
            "already_at_or_better": simulation["already_at_or_better"],
        },
        "tickets_to_resolve": [
            {
                "ticket_number": t.get("ticket_number"),
                "ticket_name": t.get("ticket_name"),
                "product_name": t.get("product_name"),
                "cluster_id": t.get("cluster_id"),
                "assignee": t.get("assignee"),
                "days_opened": t.get("days_opened"),
                "priority": t.get("priority"),
                "frustrated": t.get("frustrated"),
                "total_contribution": t.get("total_contribution"),
                "pressure_contribution": t.get("pressure_contribution"),
                "aging_contribution": t.get("aging_contribution"),
                "friction_contribution": t.get("friction_contribution"),
            }
            for t in simulation["tickets_to_resolve"]
        ],
    }
    prompt = prompt_template.replace(
        "{{DATA_JSON}}", json.dumps(payload, indent=2, default=str)
    ).strip()
    response_text = call_matcha(prompt, timeout=600)
    return data.save_customer_health_plan(
        customer=customer,
        as_of_date=as_of_date,
        group_filter_json=selected_groups,
        group_filter_label=group_filter_label,
        target_band=target_band,
        projected_score=simulation["projected_score"],
        projected_band=simulation["projected_band"],
        tickets_to_resolve=simulation["tickets_to_resolve"],
        model_name=MODEL_NAME,
        prompt_version=prompt_record["version"],
        plan_text=response_text.strip(),
        raw_context_json=payload,
        raw_response_text=response_text,
    )
