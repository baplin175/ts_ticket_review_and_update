"""
Tests for Pass 1 — phenomenon extraction pipeline.

Covers:
  - Response parsing (valid, malformed, edge cases)
  - Selection logic (pending, already-succeeded, force, failed-only)
  - Idempotent reruns
  - DB persistence
  - Malformed Matcha JSON handling
  - Successful extraction flow
"""

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call

import pytest


# ── Helpers to mock the DB connection ────────────────────────────────

@pytest.fixture(autouse=True)
def _mock_db_enabled(monkeypatch):
    """Ensure db module thinks a DATABASE_URL is configured."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://test:test@localhost/test")
    import config
    monkeypatch.setattr(config, "DATABASE_URL", "postgresql://test:test@localhost/test")


@pytest.fixture
def mock_conn():
    """Return a mock connection + cursor that db.get_conn() will return."""
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__ = MagicMock(return_value=cur)
    conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
    return conn, cur


@pytest.fixture
def patch_pool(mock_conn):
    """Patch get_conn/put_conn so no real Postgres is needed."""
    conn, cur = mock_conn
    with patch("db.get_conn", return_value=conn), \
         patch("db.put_conn"):
        yield conn, cur


# ════════════════════════════════════════════════════════════════════
# 1. Pass 1 response parsing
# ════════════════════════════════════════════════════════════════════

class TestPass1Parser:
    """Tests for pass1_parser.parse_pass1_response."""

    def test_valid_json(self):
        from pass1_parser import parse_pass1_response
        raw = '{"phenomenon": "Meter import overwrites current readings"}'
        parsed, phenomenon = parse_pass1_response(raw)
        assert phenomenon == "Meter import overwrites current readings"
        assert parsed["phenomenon"] == phenomenon

    def test_valid_json_with_whitespace(self):
        from pass1_parser import parse_pass1_response
        raw = '{"phenomenon": "  Bill prints deposit in credit section  "}'
        parsed, phenomenon = parse_pass1_response(raw)
        assert phenomenon == "Bill prints deposit in credit section"

    def test_valid_json_with_code_fence(self):
        from pass1_parser import parse_pass1_response
        raw = '```json\n{"phenomenon": "Crystal reports fail to generate"}\n```'
        parsed, phenomenon = parse_pass1_response(raw)
        assert phenomenon == "Crystal reports fail to generate"

    def test_empty_response_raises(self):
        from pass1_parser import parse_pass1_response, Pass1ParseError
        with pytest.raises(Pass1ParseError, match="Empty response"):
            parse_pass1_response("")

    def test_whitespace_only_raises(self):
        from pass1_parser import parse_pass1_response, Pass1ParseError
        with pytest.raises(Pass1ParseError, match="Empty response"):
            parse_pass1_response("   \n  ")

    def test_invalid_json_raises(self):
        from pass1_parser import parse_pass1_response, Pass1ParseError
        with pytest.raises(Pass1ParseError, match="Invalid JSON"):
            parse_pass1_response("not json at all")

    def test_missing_phenomenon_key_raises(self):
        from pass1_parser import parse_pass1_response, Pass1ParseError
        with pytest.raises(Pass1ParseError, match="Missing 'phenomenon'"):
            parse_pass1_response('{"result": "something"}')

    def test_empty_phenomenon_raises(self):
        from pass1_parser import parse_pass1_response, Pass1ParseError
        with pytest.raises(Pass1ParseError, match="empty after trimming"):
            parse_pass1_response('{"phenomenon": ""}')

    def test_whitespace_only_phenomenon_raises(self):
        from pass1_parser import parse_pass1_response, Pass1ParseError
        with pytest.raises(Pass1ParseError, match="empty after trimming"):
            parse_pass1_response('{"phenomenon": "   "}')

    def test_non_string_phenomenon_raises(self):
        from pass1_parser import parse_pass1_response, Pass1ParseError
        with pytest.raises(Pass1ParseError, match="must be a string"):
            parse_pass1_response('{"phenomenon": 42}')

    def test_null_phenomenon_raises(self):
        from pass1_parser import parse_pass1_response, Pass1ParseError
        with pytest.raises(Pass1ParseError, match="must be a string"):
            parse_pass1_response('{"phenomenon": null}')

    def test_array_response_raises(self):
        from pass1_parser import parse_pass1_response, Pass1ParseError
        with pytest.raises(Pass1ParseError, match="Expected JSON object"):
            parse_pass1_response('[{"phenomenon": "test"}]')

    def test_extra_fields_preserved(self):
        from pass1_parser import parse_pass1_response
        raw = '{"phenomenon": "Test value", "confidence": 0.95}'
        parsed, phenomenon = parse_pass1_response(raw)
        assert phenomenon == "Test value"
        assert parsed["confidence"] == 0.95


# ════════════════════════════════════════════════════════════════════
# 2. Selection logic — fetch_pending_pass1_tickets
# ════════════════════════════════════════════════════════════════════

class TestSelectionLogic:
    """Tests for db.fetch_pending_pass1_tickets SQL generation."""

    def test_basic_selection_excludes_success(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchall.return_value = [(1, "thread text")]
        import db

        db.fetch_pending_pass1_tickets("1")

        sql = cur.execute.call_args[0][0]
        assert "full_thread_text IS NOT NULL" in sql
        assert "NOT EXISTS" in sql
        assert "pass1_phenomenon" in sql

    def test_force_skips_success_check(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchall.return_value = []
        import db

        db.fetch_pending_pass1_tickets("1", force=True)

        sql = cur.execute.call_args[0][0]
        assert "NOT EXISTS" not in sql

    def test_failed_only_filters(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchall.return_value = []
        import db

        db.fetch_pending_pass1_tickets("1", failed_only=True)

        sql = cur.execute.call_args[0][0]
        # Should have both NOT EXISTS (exclude success) and EXISTS (require failed)
        assert "NOT EXISTS" in sql
        assert "status = 'failed'" in sql

    def test_ticket_ids_filter(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchall.return_value = []
        import db

        db.fetch_pending_pass1_tickets("1", ticket_ids=[100, 200])

        sql = cur.execute.call_args[0][0]
        assert "ticket_id IN" in sql
        params = cur.execute.call_args[0][1]
        assert 100 in params
        assert 200 in params

    def test_limit_applied(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchall.return_value = []
        import db

        db.fetch_pending_pass1_tickets("1", limit=50)

        sql = cur.execute.call_args[0][0]
        assert "LIMIT 50" in sql

    def test_since_filter(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchall.return_value = []
        import db

        db.fetch_pending_pass1_tickets("1", since="2026-03-01")

        sql = cur.execute.call_args[0][0]
        assert "date_created >= %s" in sql
        params = cur.execute.call_args[0][1]
        assert "2026-03-01" in params


# ════════════════════════════════════════════════════════════════════
# 3. Idempotent reruns
# ════════════════════════════════════════════════════════════════════

class TestIdempotency:
    """Verify that reruns don't create duplicate success rows."""

    def test_second_run_skips_successful_ticket(self, patch_pool):
        """When force=False, a ticket with an existing success row is excluded."""
        conn, cur = patch_pool
        cur.fetchall.return_value = []  # No eligible tickets
        import db

        rows = db.fetch_pending_pass1_tickets("1", force=False)
        sql = cur.execute.call_args[0][0]
        # The NOT EXISTS clause ensures already-successful tickets are skipped
        assert "NOT EXISTS" in sql
        assert "status = 'success'" in sql
        assert rows == []

    def test_force_rerun_includes_successful(self, patch_pool):
        """When force=True, successful tickets are re-included."""
        conn, cur = patch_pool
        cur.fetchall.return_value = [(1, "text")]
        import db

        rows = db.fetch_pending_pass1_tickets("1", force=True)
        sql = cur.execute.call_args[0][0]
        assert "NOT EXISTS" not in sql
        assert len(rows) == 1

    def test_delete_prior_failed_called(self, patch_pool):
        """delete_prior_failed_pass removes old failed rows."""
        conn, cur = patch_pool
        cur.rowcount = 1
        import db

        deleted = db.delete_prior_failed_pass(123, "pass1_phenomenon", "1")

        sql = cur.execute.call_args[0][0]
        assert "DELETE FROM ticket_llm_pass_results" in sql
        assert "status IN ('pending', 'failed')" in sql


# ════════════════════════════════════════════════════════════════════
# 4. DB persistence — insert_pass_result / update_pass_result
# ════════════════════════════════════════════════════════════════════

class TestDBPersistence:
    """Tests for db.insert_pass_result and db.update_pass_result."""

    def test_insert_pass_result_sql(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchone.return_value = (42,)
        import db

        now = datetime(2026, 3, 15, 8, 0, 0, tzinfo=timezone.utc)
        row_id = db.insert_pass_result(
            ticket_id=100,
            pass_name="pass1_phenomenon",
            prompt_version="1",
            model_name="matcha-27301",
            input_text="test input",
            status="pending",
            started_at=now,
        )

        assert row_id == 42
        sql = cur.execute.call_args[0][0]
        assert "INSERT INTO ticket_llm_pass_results" in sql
        assert "RETURNING id" in sql
        conn.commit.assert_called_once()

    def test_update_pass_result_to_success(self, patch_pool):
        conn, cur = patch_pool
        import db

        now = datetime(2026, 3, 15, 8, 1, 0, tzinfo=timezone.utc)
        db.update_pass_result(
            row_id=42,
            status="success",
            raw_response_text='{"phenomenon": "test"}',
            parsed_json={"phenomenon": "test"},
            phenomenon="test",
            completed_at=now,
        )

        sql = cur.execute.call_args[0][0]
        assert "UPDATE ticket_llm_pass_results" in sql
        assert "status" in sql
        conn.commit.assert_called_once()

    def test_update_pass_result_to_failed(self, patch_pool):
        conn, cur = patch_pool
        import db

        now = datetime(2026, 3, 15, 8, 1, 0, tzinfo=timezone.utc)
        db.update_pass_result(
            row_id=42,
            status="failed",
            raw_response_text="malformed garbage",
            error_message="Invalid JSON: ...",
            completed_at=now,
        )

        sql = cur.execute.call_args[0][0]
        assert "UPDATE ticket_llm_pass_results" in sql
        params = cur.execute.call_args[0][1]
        assert "failed" in params
        assert "Invalid JSON: ..." in params
        conn.commit.assert_called_once()


# ════════════════════════════════════════════════════════════════════
# 5. Malformed Matcha JSON handling
# ════════════════════════════════════════════════════════════════════

class TestMalformedHandling:
    """Verify that bad Matcha responses are stored but result in failure."""

    def test_process_ticket_stores_malformed_response(self, patch_pool):
        """A malformed Matcha response should be stored with status=failed."""
        conn, cur = patch_pool
        cur.fetchone.return_value = (99,)  # row_id from insert
        cur.rowcount = 0  # for delete_prior_failed
        import db

        with patch("run_ticket_pass1.call_matcha", return_value="not valid json"):
            with patch("db.migrate"):
                from run_ticket_pass1 import process_ticket
                result = process_ticket(
                    ticket_id=100,
                    full_thread_text="Customer reports crash",
                    prompt_template="Test {{input_text}}",
                    force=False,
                )

        assert result["status"] == "failed"
        assert result["error"] is not None

    def test_process_ticket_stores_empty_phenomenon(self, patch_pool):
        """A response with empty phenomenon should fail."""
        conn, cur = patch_pool
        cur.fetchone.return_value = (99,)
        cur.rowcount = 0
        import db

        with patch("run_ticket_pass1.call_matcha", return_value='{"phenomenon": ""}'):
            with patch("db.migrate"):
                from run_ticket_pass1 import process_ticket
                result = process_ticket(
                    ticket_id=100,
                    full_thread_text="Customer reports crash",
                    prompt_template="Test {{input_text}}",
                    force=False,
                )

        assert result["status"] == "failed"
        assert "empty" in result["error"].lower()


# ════════════════════════════════════════════════════════════════════
# 6. Successful extraction flow
# ════════════════════════════════════════════════════════════════════

class TestSuccessFlow:
    """End-to-end success path for process_ticket."""

    def test_successful_extraction(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchone.return_value = (99,)
        cur.rowcount = 0
        import db

        matcha_response = '{"phenomenon": "AutoPay payments remain in web tables"}'
        with patch("run_ticket_pass1.call_matcha", return_value=matcha_response):
            with patch("db.migrate"):
                from run_ticket_pass1 import process_ticket
                result = process_ticket(
                    ticket_id=100,
                    full_thread_text="Customer says autopay not posting...",
                    prompt_template="Analyze: {{input_text}}",
                    force=False,
                )

        assert result["status"] == "success"
        assert result["phenomenon"] == "AutoPay payments remain in web tables"
        assert result["elapsed_s"] >= 0

    def test_matcha_error_results_in_failed(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchone.return_value = (99,)
        cur.rowcount = 0
        import db

        with patch("run_ticket_pass1.call_matcha", side_effect=Exception("Timeout")):
            with patch("db.migrate"):
                from run_ticket_pass1 import process_ticket
                result = process_ticket(
                    ticket_id=100,
                    full_thread_text="Thread text",
                    prompt_template="Test {{input_text}}",
                    force=False,
                )

        assert result["status"] == "failed"
        assert "Timeout" in result["error"]


# ════════════════════════════════════════════════════════════════════
# 7. Prompt template
# ════════════════════════════════════════════════════════════════════

class TestPromptTemplate:
    """Verify prompt loading and placeholder substitution."""

    def test_prompt_file_exists(self):
        import os
        prompt_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "prompts", "pass1_phenomenon.txt"
        )
        assert os.path.isfile(prompt_path), f"Prompt file not found: {prompt_path}"

    def test_prompt_contains_placeholder(self):
        import os
        prompt_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "prompts", "pass1_phenomenon.txt"
        )
        with open(prompt_path) as f:
            content = f.read()
        assert "{{input_text}}" in content

    def test_build_prompt_substitution(self):
        from run_ticket_pass1 import _build_prompt
        template = "Analyze this: {{input_text}}"
        result = _build_prompt(template, "Some thread text here")
        assert result == "Analyze this: Some thread text here"
        assert "{{input_text}}" not in result


# ════════════════════════════════════════════════════════════════════
# 8. get_latest_pass_result
# ════════════════════════════════════════════════════════════════════

class TestGetLatestPassResult:

    def test_returns_success_row(self):
        import db
        with patch("db.fetch_one", return_value=(42, "success", "The phenomenon", None, None)):
            result = db.get_latest_pass_result(100, "pass1_phenomenon", "1")
        assert result is not None
        assert result["status"] == "success"
        assert result["phenomenon"] == "The phenomenon"

    def test_returns_none_when_empty(self):
        import db
        with patch("db.fetch_one", return_value=None):
            result = db.get_latest_pass_result(100, "pass1_phenomenon", "1")
        assert result is None

    def test_without_prompt_version(self):
        import db
        with patch("db.fetch_one", return_value=(42, "failed", None, "error msg", None)):
            result = db.get_latest_pass_result(100, "pass1_phenomenon")
        assert result is not None
        assert result["status"] == "failed"
        assert result["error_message"] == "error msg"
