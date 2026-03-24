"""Pass 5 — Cluster Key Mapper."""

from config import MATCHA_MISSION_ID
from matcha_client import call_matcha
from passes.runtime import process_ticket_pass
from pass5.cluster_key_parser import parse_pass5_response
from prompt_store import get_prompt

PASS_NAME = "pass5_cluster_key"
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
    """Process a single ticket through Pass 5.

    Returns a result dict with status, parsed fields, timing, etc.
    """
    full_prompt = _build_prompt(prompt_template, mechanism)

    def _success_update(parsed_output):
        parsed_json, cluster_key = parsed_output
        return {
            "parsed_json": parsed_json,
            "cluster_key": cluster_key,
        }

    def _result_update(parsed_output):
        _, cluster_key = parsed_output
        return {"cluster_key": cluster_key}

    return process_ticket_pass(
        ticket_id,
        pass_name=PASS_NAME,
        prompt_version=prompt_version,
        model_name=MODEL_NAME,
        input_text=mechanism,
        prompt_text=full_prompt,
        force=force,
        initial_result={"cluster_key": None},
        parse_response=parse_pass5_response,
        build_success_update=_success_update,
        build_result_update=_result_update,
        call_matcha_fn=call_matcha,
    )
