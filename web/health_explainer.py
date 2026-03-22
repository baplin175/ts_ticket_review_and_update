"""Customer health explanation generation via Matcha."""

from __future__ import annotations

import json
import os
from typing import Any

from config import MATCHA_MISSION_ID
from matcha_client import call_matcha

from . import data

PROMPT_VERSION = "1"
MODEL_NAME = f"matcha-{MATCHA_MISSION_ID}"
PROMPT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "prompts",
    "customer_health_explanation.md",
)


def build_group_filter_label(selected_groups: list[str], available_groups: list[str]) -> str:
    selected = [g for g in selected_groups if g]
    available = [g for g in available_groups if g]
    if selected and available and set(selected) == set(available):
        return "All groups except Marketing and Sales"
    if not selected:
        return "No groups selected"
    return ", ".join(selected)


def _find_previous_row(history: list[dict[str, Any]], as_of_date: str) -> dict[str, Any] | None:
    prior = None
    for row in history:
        if row.get("as_of_date") == as_of_date:
            return prior
        prior = row
    return prior


def _load_prompt() -> str:
    with open(PROMPT_PATH, "r", encoding="utf-8") as f:
        return f.read()


def _build_prompt(
    customer: str,
    as_of_date: str,
    selected_groups: list[str],
    group_filter_label: str,
    history: list[dict[str, Any]],
    current_row: dict[str, Any],
    previous_row: dict[str, Any] | None,
    contributors: list[dict[str, Any]],
) -> str:
    top_contributors = contributors[:10]
    payload = {
        "customer": customer,
        "as_of_date": as_of_date,
        "group_filter_label": group_filter_label,
        "selected_groups": selected_groups,
        "current_metrics": current_row,
        "previous_snapshot": previous_row,
        "history_last_10": history[-10:],
        "top_contributors": top_contributors,
    }
    prompt_template = _load_prompt()
    return prompt_template.replace(
        "{{DATA_JSON}}",
        json.dumps(payload, indent=2, default=str),
    ).strip()


def generate_customer_health_explanation(
    customer: str,
    as_of_date: str,
    selected_groups: list[str],
    available_groups: list[str],
) -> dict[str, Any]:
    history = data.get_customer_health_history(customer, selected_groups)
    current_row = next((row for row in history if row.get("as_of_date") == as_of_date), None)
    if not current_row:
        raise ValueError("No health history found for the selected customer/date/groups.")

    previous_row = _find_previous_row(history, as_of_date)
    contributors = data.get_customer_health_contributors(customer, as_of_date, selected_groups)
    group_filter_label = build_group_filter_label(selected_groups, available_groups)
    prompt = _build_prompt(
        customer=customer,
        as_of_date=as_of_date,
        selected_groups=selected_groups,
        group_filter_label=group_filter_label,
        history=history,
        current_row=current_row,
        previous_row=previous_row,
        contributors=contributors,
    )
    response_text = call_matcha(prompt, timeout=600)
    return data.save_customer_health_explanation(
        customer=customer,
        as_of_date=as_of_date,
        group_filter_json=selected_groups,
        group_filter_label=group_filter_label,
        model_name=MODEL_NAME,
        prompt_version=PROMPT_VERSION,
        explanation_text=response_text.strip(),
        raw_context_json={
            "current_row": current_row,
            "previous_row": previous_row,
            "selected_groups": selected_groups,
            "available_groups": available_groups,
            "contributors": contributors[:10],
            "history_last_10": history[-10:],
        },
        raw_response_text=response_text,
    )
