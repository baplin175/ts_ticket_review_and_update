"""
Pass 3 — Intervention Mapper.

Orchestrates the LLM call for a single ticket: builds the prompt,
calls Matcha, parses and validates the response, and stores results in the DB.
"""

import os

from config import MATCHA_MISSION_ID
from passes.runtime import load_prompt_template, process_ticket_pass
from pass4.mechanism_classifier import (
    parse_pass4_response,
    validate_intervention_action,
)

PROMPT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "prompts",
    "pass4_intervention.txt",
)

PASS_NAME = "pass3_intervention"
PROMPT_VERSION = "2"
MODEL_NAME = f"matcha-{MATCHA_MISSION_ID}"


def _load_prompt_template() -> str:
    return load_prompt_template(PROMPT_PATH)


def _build_prompt(template: str, mechanism: str) -> str:
    """Replace {{mechanism}} placeholder in the prompt template."""
    return template.replace("{{mechanism}}", mechanism)


def process_ticket(
    ticket_id: int,
    mechanism: str,
    prompt_template: str,
    *,
    force: bool = False,
) -> dict:
    """Process a single ticket through Pass 3.

    Returns a result dict with status, parsed fields, timing, etc.
    """
    full_prompt = _build_prompt(prompt_template, mechanism)

    def _success_update(parsed_output):
        parsed_json, mechanism_class, intervention_type, intervention_action = parsed_output
        return {
            "parsed_json": parsed_json,
            "mechanism_class": mechanism_class,
            "intervention_type": intervention_type,
            "intervention_action": intervention_action,
        }

    def _result_update(parsed_output):
        _, mechanism_class, intervention_type, intervention_action = parsed_output
        return {
            "mechanism_class": mechanism_class,
            "intervention_type": intervention_type,
            "intervention_action": intervention_action,
        }

    def _validate_parsed(parsed_output):
        _, _, _, intervention_action = parsed_output
        validate_intervention_action(intervention_action, mechanism)

    return process_ticket_pass(
        ticket_id,
        pass_name=PASS_NAME,
        prompt_version=PROMPT_VERSION,
        model_name=MODEL_NAME,
        input_text=mechanism,
        prompt_text=full_prompt,
        force=force,
        initial_result={
            "mechanism_class": None,
            "intervention_type": None,
            "intervention_action": None,
        },
        parse_response=parse_pass4_response,
        build_success_update=_success_update,
        build_result_update=_result_update,
        validate_parsed=_validate_parsed,
    )
