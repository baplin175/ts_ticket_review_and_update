"""
Pass 4 — Intervention Mapper.

Orchestrates the LLM call for a single ticket: builds the prompt,
calls Matcha, parses and validates the response, and stores results in the DB.
"""

import os
import time
from datetime import datetime, timezone

from config import MATCHA_MISSION_ID
from matcha_client import call_matcha
from pass4.mechanism_classifier import (
    parse_pass4_response,
    validate_intervention_action,
    Pass4ParseError,
)

PROMPT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "prompts",
    "pass4_intervention.txt",
)

PASS_NAME = "pass4_intervention"
PROMPT_VERSION = "1"
MODEL_NAME = f"matcha-{MATCHA_MISSION_ID}"


def _load_prompt_template() -> str:
    with open(PROMPT_PATH, "r", encoding="utf-8") as f:
        return f.read()


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
    """Process a single ticket through Pass 4.

    Returns a result dict with status, parsed fields, timing, etc.
    """
    import db

    result = {
        "ticket_id": ticket_id,
        "status": "pending",
        "mechanism_class": None,
        "intervention_type": None,
        "intervention_action": None,
        "error": None,
        "elapsed_s": 0.0,
    }

    started_at = datetime.now(timezone.utc)
    start_time = time.monotonic()

    # Clean up prior rows
    if force:
        db.delete_prior_failed_pass(ticket_id, PASS_NAME, PROMPT_VERSION)
        conn = db.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    DELETE FROM ticket_llm_pass_results
                     WHERE ticket_id = %s
                       AND pass_name = %s
                       AND prompt_version = %s
                       AND status = 'success';
                """, (ticket_id, PASS_NAME, PROMPT_VERSION))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            db.put_conn(conn)
    else:
        db.delete_prior_failed_pass(ticket_id, PASS_NAME, PROMPT_VERSION)

    # Build prompt
    full_prompt = _build_prompt(prompt_template, mechanism)

    # Insert pending row
    row_id = db.insert_pass_result(
        ticket_id,
        pass_name=PASS_NAME,
        prompt_version=PROMPT_VERSION,
        model_name=MODEL_NAME,
        input_text=mechanism,
        status="pending",
        started_at=started_at,
    )

    raw_response = None
    try:
        # Call Matcha
        raw_response = call_matcha(full_prompt)

        # Parse + validate JSON structure
        parsed_json, mechanism_class, intervention_type, intervention_action = (
            parse_pass4_response(raw_response)
        )

        # Validate intervention_action content
        validate_intervention_action(intervention_action, mechanism)

        completed_at = datetime.now(timezone.utc)
        elapsed = time.monotonic() - start_time

        # Update row to success
        db.update_pass_result(
            row_id,
            status="success",
            raw_response_text=raw_response,
            parsed_json=parsed_json,
            mechanism_class=mechanism_class,
            intervention_type=intervention_type,
            intervention_action=intervention_action,
            completed_at=completed_at,
        )

        result["status"] = "success"
        result["mechanism_class"] = mechanism_class
        result["intervention_type"] = intervention_type
        result["intervention_action"] = intervention_action
        result["elapsed_s"] = round(elapsed, 2)

    except Pass4ParseError as exc:
        completed_at = datetime.now(timezone.utc)
        elapsed = time.monotonic() - start_time

        db.update_pass_result(
            row_id,
            status="failed",
            raw_response_text=raw_response,
            error_message=str(exc),
            completed_at=completed_at,
        )

        result["status"] = "failed"
        result["error"] = str(exc)
        result["elapsed_s"] = round(elapsed, 2)

    except Exception as exc:
        completed_at = datetime.now(timezone.utc)
        elapsed = time.monotonic() - start_time

        db.update_pass_result(
            row_id,
            status="failed",
            raw_response_text=raw_response,
            error_message=str(exc),
            completed_at=completed_at,
        )

        result["status"] = "failed"
        result["error"] = str(exc)
        result["elapsed_s"] = round(elapsed, 2)

    return result
