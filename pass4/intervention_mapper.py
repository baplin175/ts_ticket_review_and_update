"""Pass 4 — Intervention Mapper."""

from config import MATCHA_MISSION_ID
from matcha_client import call_matcha
from passes.runtime import process_ticket_pass
from pass4.mechanism_classifier import (
    parse_pass4_response,
    validate_intervention_action,
)
from prompt_store import get_prompt

PASS_NAME = "pass4_intervention"
DEFAULT_PROMPT_VERSION = "2"
MODEL_NAME = f"matcha-{MATCHA_MISSION_ID}"


def load_prompt_record() -> dict:
    return get_prompt(PASS_NAME, allow_fallback=False)


def _load_prompt_template() -> str:
    return load_prompt_record()["content"]


def _build_prompt(template: str, mechanism: str) -> str:
    """Replace {{mechanism}} placeholder in the prompt template."""
    return template.replace("{{mechanism}}", mechanism)


def process_ticket(
    ticket_id: int,
    mechanism: str,
    prompt_template: str,
    prompt_version: str = DEFAULT_PROMPT_VERSION,
    *,
    force: bool = False,
) -> dict:
    """Process a single ticket through Pass 4.

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
        prompt_version=prompt_version,
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
        call_matcha_fn=call_matcha,
    )
