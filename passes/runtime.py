"""Shared runtime helpers for LLM ticket passes."""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional


def load_prompt_template(path: str) -> str:
    """Read a prompt template from disk."""
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def _delete_prior_success(db: Any, ticket_id: int, pass_name: str, prompt_version: str) -> None:
    """Delete a prior successful row so a forced rerun can insert a fresh result."""
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM ticket_llm_pass_results
                 WHERE ticket_id = %s
                   AND pass_name = %s
                   AND prompt_version = %s
                   AND status = 'success';
                """,
                (ticket_id, pass_name, prompt_version),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        db.put_conn(conn)


def cleanup_prior_attempts(
    db: Any,
    *,
    ticket_id: int,
    pass_name: str,
    prompt_version: str,
    force: bool,
) -> None:
    """Remove stale rows for a pass attempt, optionally including prior success."""
    db.delete_prior_failed_pass(ticket_id, pass_name, prompt_version)
    if force:
        _delete_prior_success(db, ticket_id, pass_name, prompt_version)


def process_ticket_pass(
    ticket_id: int,
    *,
    pass_name: str,
    prompt_version: str,
    model_name: str,
    input_text: str,
    prompt_text: str,
    force: bool = False,
    initial_result: Optional[Dict[str, Any]] = None,
    parse_response: Callable[[str], Any],
    build_success_update: Callable[[Any], Dict[str, Any]],
    build_result_update: Optional[Callable[[Any], Dict[str, Any]]] = None,
    validate_parsed: Optional[Callable[[Any], None]] = None,
) -> Dict[str, Any]:
    """Run a single ticket through the standard LLM pass lifecycle."""
    import db

    result = {
        "ticket_id": ticket_id,
        "status": "pending",
        "error": None,
        "elapsed_s": 0.0,
    }
    if initial_result:
        result.update(initial_result)

    started_at = datetime.now(timezone.utc)
    start_time = time.monotonic()

    cleanup_prior_attempts(
        db,
        ticket_id=ticket_id,
        pass_name=pass_name,
        prompt_version=prompt_version,
        force=force,
    )

    row_id = db.insert_pass_result(
        ticket_id,
        pass_name=pass_name,
        prompt_version=prompt_version,
        model_name=model_name,
        input_text=input_text,
        status="pending",
        started_at=started_at,
    )

    raw_response = None
    try:
        from matcha_client import call_matcha

        raw_response = call_matcha(prompt_text)
        parsed_output = parse_response(raw_response)
        if validate_parsed:
            validate_parsed(parsed_output)

        completed_at = datetime.now(timezone.utc)
        elapsed = round(time.monotonic() - start_time, 2)
        success_update = build_success_update(parsed_output)

        db.update_pass_result(
            row_id,
            status="success",
            raw_response_text=raw_response,
            completed_at=completed_at,
            **success_update,
        )

        result["status"] = "success"
        result["elapsed_s"] = elapsed
        if build_result_update:
            result.update(build_result_update(parsed_output))
        else:
            result.update(success_update)
        return result

    except Exception as exc:
        completed_at = datetime.now(timezone.utc)
        elapsed = round(time.monotonic() - start_time, 2)

        db.update_pass_result(
            row_id,
            status="failed",
            raw_response_text=raw_response,
            error_message=str(exc),
            completed_at=completed_at,
        )

        result["status"] = "failed"
        result["error"] = str(exc)
        result["elapsed_s"] = elapsed
        return result
