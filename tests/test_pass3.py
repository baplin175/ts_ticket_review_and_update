"""
Tests for Pass 3 — failure mechanism inference pipeline.

Covers:
  - Response parsing (valid, malformed, edge cases)
  - Mechanism validation rules (restatement, admin text)
  - Selection logic (only tickets with successful Pass 2 canonical_failure)
  - Idempotent reruns
  - DB persistence
  - Malformed Matcha JSON handling
  - Successful extraction flow
"""

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call

import pytest


# ── Fixtures ─────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def _mock_db_enabled(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://test:test@localhost/test")
    import config
    monkeypatch.setattr(config, "DATABASE_URL", "postgresql://test:test@localhost/test")


@pytest.fixture
def mock_conn():
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn, cur


@pytest.fixture
def patch_pool(mock_conn):
    conn, cur = mock_conn
    with patch("db.get_conn", return_value=conn), \
         patch("db.put_conn"):
        yield conn, cur


# ── Parser tests ─────────────────────────────────────────────────────

class TestPass3Parser:
    def test_valid_json(self):
        from pass3_parser import parse_pass3_response
        raw = '{"mechanism": "Billing calculation logic applies service charge rule twice during bill generation"}'
        parsed, mechanism = parse_pass3_response(raw)
        assert mechanism == "Billing calculation logic applies service charge rule twice during bill generation"
        assert parsed["mechanism"] == mechanism

    def test_valid_json_with_whitespace(self):
        from pass3_parser import parse_pass3_response
        raw = '{"mechanism": "  Import parser rejects input due to schema mismatch  "}'
        parsed, mechanism = parse_pass3_response(raw)
        assert mechanism == "Import parser rejects input due to schema mismatch"

    def test_valid_json_with_code_fence(self):
        from pass3_parser import parse_pass3_response
        raw = '```json\n{"mechanism": "Map service returns invalid response"}\n```'
        parsed, mechanism = parse_pass3_response(raw)
        assert mechanism == "Map service returns invalid response"

    def test_empty_response_raises(self):
        from pass3_parser import parse_pass3_response, Pass3ParseError
        with pytest.raises(Pass3ParseError, match="Empty response"):
            parse_pass3_response("")

    def test_whitespace_only_raises(self):
        from pass3_parser import parse_pass3_response, Pass3ParseError
        with pytest.raises(Pass3ParseError, match="Empty response"):
            parse_pass3_response("   \n  ")

    def test_invalid_json_raises(self):
        from pass3_parser import parse_pass3_response, Pass3ParseError
        with pytest.raises(Pass3ParseError, match="Invalid JSON"):
            parse_pass3_response("not json at all")

    def test_missing_mechanism_key_raises(self):
        from pass3_parser import parse_pass3_response, Pass3ParseError
        with pytest.raises(Pass3ParseError, match="Missing 'mechanism'"):
            parse_pass3_response('{"result": "something"}')

    def test_empty_mechanism_raises(self):
        from pass3_parser import parse_pass3_response, Pass3ParseError
        with pytest.raises(Pass3ParseError, match="empty after trimming"):
            parse_pass3_response('{"mechanism": ""}')

    def test_whitespace_only_mechanism_raises(self):
        from pass3_parser import parse_pass3_response, Pass3ParseError
        with pytest.raises(Pass3ParseError, match="empty after trimming"):
            parse_pass3_response('{"mechanism": "   "}')

    def test_non_string_mechanism_raises(self):
        from pass3_parser import parse_pass3_response, Pass3ParseError
        with pytest.raises(Pass3ParseError, match="must be a string"):
            parse_pass3_response('{"mechanism": 42}')

    def test_null_mechanism_raises(self):
        from pass3_parser import parse_pass3_response, Pass3ParseError
        with pytest.raises(Pass3ParseError, match="must not be null"):
            parse_pass3_response('{"mechanism": null}')

    def test_array_response_raises(self):
        from pass3_parser import parse_pass3_response, Pass3ParseError
        with pytest.raises(Pass3ParseError, match="Expected JSON object"):
            parse_pass3_response('[{"mechanism": "test"}]')

    def test_extra_fields_preserved(self):
        from pass3_parser import parse_pass3_response
        raw = '{"mechanism": "Validation failure on input schema", "confidence": 0.95}'
        parsed, mechanism = parse_pass3_response(raw)
        assert mechanism == "Validation failure on input schema"
        assert parsed["confidence"] == 0.95


# ── Mechanism validation tests ───────────────────────────────────────

class TestMechanismValidation:
    def test_rejects_exact_restatement(self):
        from pass3_parser import validate_mechanism, Pass3ParseError
        cf = "Billing module + print + service charge prints twice"
        with pytest.raises(Pass3ParseError, match="exact restatement"):
            validate_mechanism(cf, cf)

    def test_rejects_case_insensitive_restatement(self):
        from pass3_parser import validate_mechanism, Pass3ParseError
        cf = "Billing module + print + service charge prints twice"
        with pytest.raises(Pass3ParseError, match="exact restatement"):
            validate_mechanism(cf.upper(), cf)

    def test_rejects_ticket_word(self):
        from pass3_parser import validate_mechanism, Pass3ParseError
        with pytest.raises(Pass3ParseError, match="administrative language"):
            validate_mechanism(
                "The ticket was not handled correctly",
                "Billing module + print + fails"
            )

    def test_rejects_customer_word(self):
        from pass3_parser import validate_mechanism, Pass3ParseError
        with pytest.raises(Pass3ParseError, match="administrative language"):
            validate_mechanism(
                "Customer reported the issue multiple times",
                "Billing module + print + fails"
            )

    def test_rejects_agent_word(self):
        from pass3_parser import validate_mechanism, Pass3ParseError
        with pytest.raises(Pass3ParseError, match="administrative language"):
            validate_mechanism(
                "Agent needs to escalate the request",
                "Billing module + print + fails"
            )

    def test_rejects_troubleshoot_word(self):
        from pass3_parser import validate_mechanism, Pass3ParseError
        with pytest.raises(Pass3ParseError, match="administrative language"):
            validate_mechanism(
                "Troubleshooting steps indicate a system issue",
                "Billing module + print + fails"
            )

    def test_rejects_support_team_phrase(self):
        from pass3_parser import validate_mechanism, Pass3ParseError
        with pytest.raises(Pass3ParseError, match="administrative language"):
            validate_mechanism(
                "Support team needs to review the configuration",
                "Billing module + print + fails"
            )

    def test_accepts_valid_mechanism(self):
        from pass3_parser import validate_mechanism
        result = validate_mechanism(
            "Billing calculation logic applies service charge rule twice during bill generation",
            "Billing module + print + service charge prints twice"
        )
        assert result == "Billing calculation logic applies service charge rule twice during bill generation"

    def test_accepts_technical_mechanism(self):
        from pass3_parser import validate_mechanism
        result = validate_mechanism(
            "Integration API rejects authentication token due to expired credentials",
            "Invoice Cloud integration + upload + files fail to upload"
        )
        assert result == "Integration API rejects authentication token due to expired credentials"


# ── Selection logic tests ────────────────────────────────────────────

class TestSelectionLogic:
    def test_basic_selection_excludes_success(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchall.return_value = [(1, "comp + op + state")]
        import db
        db.fetch_pending_pass3_tickets("1")
        sql = cur.execute.call_args[0][0]
        assert "canonical_failure IS NOT NULL" in sql
        assert "NOT EXISTS" in sql
        assert "pass3_mechanism" in sql

    def test_force_skips_success_check(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchall.return_value = []
        import db
        db.fetch_pending_pass3_tickets("1", force=True)
        sql = cur.execute.call_args[0][0]
        assert "NOT EXISTS" not in sql

    def test_failed_only_filters(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchall.return_value = []
        import db
        db.fetch_pending_pass3_tickets("1", failed_only=True)
        sql = cur.execute.call_args[0][0]
        assert "NOT EXISTS" in sql
        assert "status = 'failed'" in sql

    def test_ticket_ids_filter(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchall.return_value = []
        import db
        db.fetch_pending_pass3_tickets("1", ticket_ids=[100, 200])
        sql = cur.execute.call_args[0][0]
        assert "ticket_id IN" in sql
        params = cur.execute.call_args[0][1]
        assert 100 in params
        assert 200 in params

    def test_limit_applied(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchall.return_value = []
        import db
        db.fetch_pending_pass3_tickets("1", limit=50)
        sql = cur.execute.call_args[0][0]
        assert "LIMIT 50" in sql

    def test_requires_pass2_success(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchall.return_value = []
        import db
        db.fetch_pending_pass3_tickets("1")
        sql = cur.execute.call_args[0][0]
        assert "p2.status = 'success'" in sql
        params = cur.execute.call_args[0][1]
        assert "pass2_grammar" in params


# ── Idempotency tests ────────────────────────────────────────────────

class TestIdempotency:
    def test_second_run_skips_successful_ticket(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchall.return_value = []
        import db
        rows = db.fetch_pending_pass3_tickets("1", force=False)
        sql = cur.execute.call_args[0][0]
        assert "NOT EXISTS" in sql
        assert "status = 'success'" in sql
        assert rows == []

    def test_force_rerun_includes_successful(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchall.return_value = [(1, "comp + op + state")]
        import db
        rows = db.fetch_pending_pass3_tickets("1", force=True)
        sql = cur.execute.call_args[0][0]
        assert "NOT EXISTS" not in sql
        assert len(rows) == 1

    def test_delete_prior_failed_called(self, patch_pool):
        conn, cur = patch_pool
        cur.rowcount = 1
        import db
        deleted = db.delete_prior_failed_pass(123, "pass3_mechanism", "1")
        sql = cur.execute.call_args[0][0]
        assert "DELETE FROM ticket_llm_pass_results" in sql
        assert "status IN ('pending', 'failed')" in sql


# ── DB persistence tests ─────────────────────────────────────────────

class TestDBPersistence:
    def test_insert_pass_result_sql(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchone.return_value = (42,)
        import db
        now = datetime(2026, 3, 15, 8, 0, 0, tzinfo=timezone.utc)
        row_id = db.insert_pass_result(
            ticket_id=100, pass_name="pass3_mechanism", prompt_version="1",
            model_name="matcha-27301", input_text="comp + op + state",
            status="pending", started_at=now,
        )
        assert row_id == 42
        sql = cur.execute.call_args[0][0]
        assert "INSERT INTO ticket_llm_pass_results" in sql
        assert "RETURNING id" in sql
        conn.commit.assert_called_once()

    def test_update_pass_result_to_success_with_mechanism(self, patch_pool):
        conn, cur = patch_pool
        import db
        now = datetime(2026, 3, 15, 8, 1, 0, tzinfo=timezone.utc)
        db.update_pass_result(
            row_id=42, status="success",
            raw_response_text='{"mechanism": "Validation failure on schema"}',
            parsed_json={"mechanism": "Validation failure on schema"},
            mechanism="Validation failure on schema",
            completed_at=now,
        )
        sql = cur.execute.call_args[0][0]
        assert "UPDATE ticket_llm_pass_results" in sql
        assert "mechanism" in sql
        params = cur.execute.call_args[0][1]
        assert "success" in params
        assert "Validation failure on schema" in params
        conn.commit.assert_called_once()

    def test_update_pass_result_to_failed(self, patch_pool):
        conn, cur = patch_pool
        import db
        now = datetime(2026, 3, 15, 8, 1, 0, tzinfo=timezone.utc)
        db.update_pass_result(
            row_id=42, status="failed", raw_response_text="malformed garbage",
            error_message="Invalid JSON: ...", completed_at=now,
        )
        sql = cur.execute.call_args[0][0]
        assert "UPDATE ticket_llm_pass_results" in sql
        params = cur.execute.call_args[0][1]
        assert "failed" in params
        assert "Invalid JSON: ..." in params
        conn.commit.assert_called_once()


# ── Malformed handling tests ─────────────────────────────────────────

class TestMalformedHandling:
    def test_process_ticket_stores_malformed_response(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchone.return_value = (99,)
        cur.rowcount = 0
        import db
        with patch("run_ticket_pass3.call_matcha", return_value="not valid json"):
            with patch("db.migrate", return_value=[]):
                from run_ticket_pass3 import process_ticket
                result = process_ticket(
                    ticket_id=100,
                    canonical_failure="Billing module + print + service charge prints twice",
                    prompt_template="Test {{input_text}}", force=False,
                )
        assert result["status"] == "failed"
        assert result["error"] is not None

    def test_process_ticket_stores_empty_mechanism(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchone.return_value = (99,)
        cur.rowcount = 0
        import db
        with patch("run_ticket_pass3.call_matcha", return_value='{"mechanism": ""}'):
            with patch("db.migrate", return_value=[]):
                from run_ticket_pass3 import process_ticket
                result = process_ticket(
                    ticket_id=100,
                    canonical_failure="Billing module + print + service charge prints twice",
                    prompt_template="Test {{input_text}}", force=False,
                )
        assert result["status"] == "failed"
        assert "empty" in result["error"].lower()


# ── Success flow tests ───────────────────────────────────────────────

class TestSuccessFlow:
    def test_successful_extraction(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchone.return_value = (99,)
        cur.rowcount = 0
        import db
        matcha_response = '{"mechanism": "Billing calculation logic applies service charge rule twice during bill generation"}'
        with patch("run_ticket_pass3.call_matcha", return_value=matcha_response):
            with patch("db.migrate", return_value=[]):
                from run_ticket_pass3 import process_ticket
                result = process_ticket(
                    ticket_id=100,
                    canonical_failure="Billing module + print + service charge prints twice",
                    prompt_template="Test {{input_text}}", force=False,
                )
        assert result["status"] == "success"
        assert result["mechanism"] == "Billing calculation logic applies service charge rule twice during bill generation"
        assert result["elapsed_s"] >= 0

    def test_matcha_error_results_in_failed(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchone.return_value = (99,)
        cur.rowcount = 0
        import db
        with patch("run_ticket_pass3.call_matcha", side_effect=Exception("Timeout")):
            with patch("db.migrate", return_value=[]):
                from run_ticket_pass3 import process_ticket
                result = process_ticket(
                    ticket_id=100,
                    canonical_failure="Billing module + print + service charge prints twice",
                    prompt_template="Test {{input_text}}", force=False,
                )
        assert result["status"] == "failed"
        assert "Timeout" in result["error"]

    def test_validation_failure_results_in_failed(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchone.return_value = (99,)
        cur.rowcount = 0
        import db
        # Return mechanism that restates the canonical failure exactly
        canonical = "Billing module + print + service charge prints twice"
        matcha_response = json.dumps({"mechanism": canonical})
        with patch("run_ticket_pass3.call_matcha", return_value=matcha_response):
            with patch("db.migrate", return_value=[]):
                from run_ticket_pass3 import process_ticket
                result = process_ticket(
                    ticket_id=100,
                    canonical_failure=canonical,
                    prompt_template="Test {{input_text}}", force=False,
                )
        assert result["status"] == "failed"
        assert "restatement" in result["error"].lower()


# ── Prompt template tests ────────────────────────────────────────────

class TestPromptTemplate:
    def test_prompt_file_exists(self):
        import os
        prompt_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "prompts", "pass3_mechanism.txt"
        )
        assert os.path.isfile(prompt_path), f"Prompt file not found: {prompt_path}"
