"""
Tests for Pass 2 — canonical failure grammar extraction pipeline.

Covers:
  - Response parsing (valid, malformed, edge cases)
  - Operation normalization and rejection
  - Canonical failure reconstruction
  - Selection logic (only tickets with successful Pass 1 phenomenon)
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
# 1. Pass 2 response parsing
# ════════════════════════════════════════════════════════════════════

class TestPass2Parser:
    """Tests for pass2_parser.parse_pass2_response."""

    def test_valid_json(self):
        from pass2_parser import parse_pass2_response
        raw = json.dumps({
            "component": "WebShare AutoPay transfer job",
            "operation": "transfer",
            "unexpected_state": "payments remain in web tables",
            "canonical_failure": "WebShare AutoPay transfer job + transfer + payments remain in web tables"
        })
        parsed, component, operation, unexpected_state, canonical_failure = parse_pass2_response(raw)
        assert component == "WebShare AutoPay transfer job"
        assert operation == "transfer"
        assert unexpected_state == "payments remain in web tables"
        assert canonical_failure == "WebShare AutoPay transfer job + transfer + payments remain in web tables"

    def test_valid_json_with_whitespace(self):
        from pass2_parser import parse_pass2_response
        raw = json.dumps({
            "component": "  Invoice Cloud data pump  ",
            "operation": "  import  ",
            "unexpected_state": "  deposit counted twice  ",
            "canonical_failure": "Invoice Cloud data pump + import + deposit counted twice"
        })
        parsed, component, operation, unexpected_state, canonical_failure = parse_pass2_response(raw)
        assert component == "Invoice Cloud data pump"
        assert operation == "import"
        assert unexpected_state == "deposit counted twice"

    def test_valid_json_with_code_fence(self):
        from pass2_parser import parse_pass2_response
        raw = '```json\n' + json.dumps({
            "component": "Crystal Reports runtime",
            "operation": "generate",
            "unexpected_state": "report includes wrong employee",
            "canonical_failure": "Crystal Reports runtime + generate + report includes wrong employee"
        }) + '\n```'
        parsed, component, operation, unexpected_state, canonical_failure = parse_pass2_response(raw)
        assert component == "Crystal Reports runtime"
        assert operation == "generate"

    def test_empty_response_raises(self):
        from pass2_parser import parse_pass2_response, Pass2ParseError
        with pytest.raises(Pass2ParseError, match="Empty response"):
            parse_pass2_response("")

    def test_whitespace_only_raises(self):
        from pass2_parser import parse_pass2_response, Pass2ParseError
        with pytest.raises(Pass2ParseError, match="Empty response"):
            parse_pass2_response("   \n  ")

    def test_invalid_json_raises(self):
        from pass2_parser import parse_pass2_response, Pass2ParseError
        with pytest.raises(Pass2ParseError, match="Invalid JSON"):
            parse_pass2_response("not json at all")

    def test_missing_component_raises(self):
        from pass2_parser import parse_pass2_response, Pass2ParseError
        raw = json.dumps({
            "operation": "import",
            "unexpected_state": "data lost",
            "canonical_failure": "X + import + data lost"
        })
        with pytest.raises(Pass2ParseError, match="Missing 'component'"):
            parse_pass2_response(raw)

    def test_missing_operation_raises(self):
        from pass2_parser import parse_pass2_response, Pass2ParseError
        raw = json.dumps({
            "component": "Module X",
            "unexpected_state": "data lost",
            "canonical_failure": "Module X + ? + data lost"
        })
        with pytest.raises(Pass2ParseError, match="Missing 'operation'"):
            parse_pass2_response(raw)

    def test_missing_unexpected_state_raises(self):
        from pass2_parser import parse_pass2_response, Pass2ParseError
        raw = json.dumps({
            "component": "Module X",
            "operation": "import",
            "canonical_failure": "Module X + import + ?"
        })
        with pytest.raises(Pass2ParseError, match="Missing 'unexpected_state'"):
            parse_pass2_response(raw)

    def test_missing_canonical_failure_raises(self):
        from pass2_parser import parse_pass2_response, Pass2ParseError
        raw = json.dumps({
            "component": "Module X",
            "operation": "import",
            "unexpected_state": "data lost"
        })
        with pytest.raises(Pass2ParseError, match="Missing 'canonical_failure'"):
            parse_pass2_response(raw)

    def test_empty_component_raises(self):
        from pass2_parser import parse_pass2_response, Pass2ParseError
        raw = json.dumps({
            "component": "",
            "operation": "import",
            "unexpected_state": "data lost",
            "canonical_failure": " + import + data lost"
        })
        with pytest.raises(Pass2ParseError, match="'component' value is empty"):
            parse_pass2_response(raw)

    def test_empty_operation_raises(self):
        from pass2_parser import parse_pass2_response, Pass2ParseError
        raw = json.dumps({
            "component": "Module X",
            "operation": "   ",
            "unexpected_state": "data lost",
            "canonical_failure": "Module X +  + data lost"
        })
        with pytest.raises(Pass2ParseError, match="'operation' value is empty"):
            parse_pass2_response(raw)

    def test_non_string_component_raises(self):
        from pass2_parser import parse_pass2_response, Pass2ParseError
        raw = json.dumps({
            "component": 42,
            "operation": "import",
            "unexpected_state": "data lost",
            "canonical_failure": "42 + import + data lost"
        })
        with pytest.raises(Pass2ParseError, match="must be a string"):
            parse_pass2_response(raw)

    def test_null_operation_raises(self):
        from pass2_parser import parse_pass2_response, Pass2ParseError
        raw = '{"component": "X", "operation": null, "unexpected_state": "Y", "canonical_failure": "X + null + Y"}'
        with pytest.raises(Pass2ParseError, match="must be a string"):
            parse_pass2_response(raw)

    def test_array_response_raises(self):
        from pass2_parser import parse_pass2_response, Pass2ParseError
        raw = '[{"component": "X", "operation": "import", "unexpected_state": "Y", "canonical_failure": "X + import + Y"}]'
        with pytest.raises(Pass2ParseError, match="Expected JSON object"):
            parse_pass2_response(raw)

    def test_extra_fields_preserved(self):
        from pass2_parser import parse_pass2_response
        raw = json.dumps({
            "component": "Module X",
            "operation": "import",
            "unexpected_state": "data lost",
            "canonical_failure": "Module X + import + data lost",
            "confidence": 0.95
        })
        parsed, component, operation, unexpected_state, canonical_failure = parse_pass2_response(raw)
        assert parsed["confidence"] == 0.95
        assert component == "Module X"


# ════════════════════════════════════════════════════════════════════
# 2. Operation normalization
# ════════════════════════════════════════════════════════════════════

class TestOperationNormalization:
    """Tests for pass2_parser.normalize_operation."""

    def test_valid_operation_passes_through(self):
        from pass2_parser import normalize_operation
        for op in ["post", "import", "export", "print", "load", "transfer",
                    "calculate", "attach", "generate", "recover", "create", "update"]:
            assert normalize_operation(op) == op

    def test_case_insensitive(self):
        from pass2_parser import normalize_operation
        assert normalize_operation("Import") == "import"
        assert normalize_operation("TRANSFER") == "transfer"
        assert normalize_operation("Generate") == "generate"

    def test_synonym_mapping(self):
        from pass2_parser import normalize_operation
        assert normalize_operation("upload") == "import"
        assert normalize_operation("download") == "export"
        assert normalize_operation("send") == "transfer"
        assert normalize_operation("build") == "generate"
        assert normalize_operation("compute") == "calculate"
        assert normalize_operation("insert") == "create"
        assert normalize_operation("modify") == "update"
        assert normalize_operation("save") == "update"
        assert normalize_operation("restore") == "recover"
        assert normalize_operation("link") == "attach"

    def test_unknown_operation_raises(self):
        from pass2_parser import normalize_operation, Pass2ParseError
        with pytest.raises(Pass2ParseError, match="Unknown operation"):
            normalize_operation("fly")

    def test_empty_after_strip_raises(self):
        from pass2_parser import normalize_operation, Pass2ParseError
        with pytest.raises(Pass2ParseError, match="Unknown operation"):
            normalize_operation("   ")

    def test_synonym_case_insensitive(self):
        from pass2_parser import normalize_operation
        assert normalize_operation("Upload") == "import"
        assert normalize_operation("COMPUTE") == "calculate"

    def test_operation_normalized_in_parse(self):
        """Synonym is normalized when going through full parse."""
        from pass2_parser import parse_pass2_response
        raw = json.dumps({
            "component": "Module X",
            "operation": "upload",
            "unexpected_state": "data lost",
            "canonical_failure": "Module X + upload + data lost"
        })
        parsed, component, operation, unexpected_state, canonical_failure = parse_pass2_response(raw)
        assert operation == "import"
        assert canonical_failure == "Module X + import + data lost"

    def test_invalid_operation_fails_parse(self):
        """Unknown operation causes full parse to fail."""
        from pass2_parser import parse_pass2_response, Pass2ParseError
        raw = json.dumps({
            "component": "Module X",
            "operation": "fly",
            "unexpected_state": "data lost",
            "canonical_failure": "Module X + fly + data lost"
        })
        with pytest.raises(Pass2ParseError, match="Unknown operation"):
            parse_pass2_response(raw)


# ════════════════════════════════════════════════════════════════════
# 3. Canonical failure reconstruction
# ════════════════════════════════════════════════════════════════════

class TestCanonicalFailureReconstruction:
    """Verify canonical_failure is reconstructed from parsed fields."""

    def test_matching_model_output_accepted(self):
        from pass2_parser import parse_pass2_response
        raw = json.dumps({
            "component": "Payment Plan module",
            "operation": "calculate",
            "unexpected_state": "balance incorrect",
            "canonical_failure": "Payment Plan module + calculate + balance incorrect"
        })
        _, _, _, _, canonical_failure = parse_pass2_response(raw)
        assert canonical_failure == "Payment Plan module + calculate + balance incorrect"

    def test_mismatched_model_output_overwritten(self):
        """Model returns a different canonical_failure — it should be reconstructed."""
        from pass2_parser import parse_pass2_response
        raw = json.dumps({
            "component": "Payment Plan module",
            "operation": "calculate",
            "unexpected_state": "balance incorrect",
            "canonical_failure": "Something completely different"
        })
        parsed, _, _, _, canonical_failure = parse_pass2_response(raw)
        assert canonical_failure == "Payment Plan module + calculate + balance incorrect"
        assert parsed["canonical_failure"] == canonical_failure

    def test_synonym_normalized_in_canonical(self):
        """When operation is a synonym, canonical_failure uses the normalized form."""
        from pass2_parser import parse_pass2_response
        raw = json.dumps({
            "component": "Report Engine",
            "operation": "build",
            "unexpected_state": "PDF empty",
            "canonical_failure": "Report Engine + build + PDF empty"
        })
        _, _, operation, _, canonical_failure = parse_pass2_response(raw)
        assert operation == "generate"
        assert canonical_failure == "Report Engine + generate + PDF empty"

    def test_whitespace_in_fields_normalized(self):
        from pass2_parser import parse_pass2_response
        raw = json.dumps({
            "component": "  Report Engine  ",
            "operation": "  generate  ",
            "unexpected_state": "  PDF empty  ",
            "canonical_failure": "Report Engine + generate + PDF empty"
        })
        _, component, _, unexpected_state, canonical_failure = parse_pass2_response(raw)
        assert component == "Report Engine"
        assert unexpected_state == "PDF empty"
        assert canonical_failure == "Report Engine + generate + PDF empty"


# ════════════════════════════════════════════════════════════════════
# 4. Selection logic — fetch_pending_pass2_tickets
# ════════════════════════════════════════════════════════════════════

class TestPass2SelectionLogic:
    """Tests for db.fetch_pending_pass2_tickets SQL generation."""

    def test_basic_selection_requires_pass1_success(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchall.return_value = [(1, "phenomenon text")]
        import db

        db.fetch_pending_pass2_tickets("1")

        sql = cur.execute.call_args[0][0]
        params = cur.execute.call_args[0][1]
        assert "p1.pass_name = %s" in sql
        assert "pass1_phenomenon" in params  # pass_name passed as param
        assert "status = 'success'" in sql
        assert "phenomenon IS NOT NULL" in sql
        assert "NOT EXISTS" in sql
        assert "pass2_grammar" in sql

    def test_force_skips_success_check(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchall.return_value = []
        import db

        db.fetch_pending_pass2_tickets("1", force=True)

        sql = cur.execute.call_args[0][0]
        assert "NOT EXISTS" not in sql

    def test_failed_only_filters(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchall.return_value = []
        import db

        db.fetch_pending_pass2_tickets("1", failed_only=True)

        sql = cur.execute.call_args[0][0]
        assert "NOT EXISTS" in sql
        assert "status = 'failed'" in sql

    def test_ticket_ids_filter(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchall.return_value = []
        import db

        db.fetch_pending_pass2_tickets("1", ticket_ids=[100, 200])

        sql = cur.execute.call_args[0][0]
        assert "ticket_id IN" in sql
        params = cur.execute.call_args[0][1]
        assert 100 in params
        assert 200 in params

    def test_limit_applied(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchall.return_value = []
        import db

        db.fetch_pending_pass2_tickets("1", limit=50)

        sql = cur.execute.call_args[0][0]
        assert "LIMIT 50" in sql

    def test_returns_ticket_id_and_phenomenon(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchall.return_value = [
            (100, "AutoPay payments remain in web tables"),
            (200, "Meter import overwrites readings"),
        ]
        import db

        rows = db.fetch_pending_pass2_tickets("1")
        assert len(rows) == 2
        assert rows[0] == (100, "AutoPay payments remain in web tables")
        assert rows[1] == (200, "Meter import overwrites readings")

    def test_custom_pass1_version(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchall.return_value = []
        import db

        db.fetch_pending_pass2_tickets("1", pass1_prompt_version="2")

        params = cur.execute.call_args[0][1]
        assert "2" in params


# ════════════════════════════════════════════════════════════════════
# 5. Idempotent reruns
# ════════════════════════════════════════════════════════════════════

class TestPass2Idempotency:
    """Verify that reruns don't create duplicate success rows."""

    def test_second_run_skips_successful_ticket(self, patch_pool):
        """When force=False, a ticket with an existing success row is excluded."""
        conn, cur = patch_pool
        cur.fetchall.return_value = []
        import db

        rows = db.fetch_pending_pass2_tickets("1", force=False)
        sql = cur.execute.call_args[0][0]
        assert "NOT EXISTS" in sql
        assert "status = 'success'" in sql
        assert rows == []

    def test_force_rerun_includes_successful(self, patch_pool):
        """When force=True, successful tickets are re-included."""
        conn, cur = patch_pool
        cur.fetchall.return_value = [(1, "phenomenon")]
        import db

        rows = db.fetch_pending_pass2_tickets("1", force=True)
        sql = cur.execute.call_args[0][0]
        assert "NOT EXISTS" not in sql
        assert len(rows) == 1

    def test_delete_prior_failed_called(self, patch_pool):
        """delete_prior_failed_pass removes old failed rows for pass2_grammar."""
        conn, cur = patch_pool
        cur.rowcount = 1
        import db

        deleted = db.delete_prior_failed_pass(123, "pass2_grammar", "1")

        sql = cur.execute.call_args[0][0]
        assert "DELETE FROM ticket_llm_pass_results" in sql
        assert "status IN ('pending', 'failed')" in sql
        params = cur.execute.call_args[0][1]
        assert "pass2_grammar" in params


# ════════════════════════════════════════════════════════════════════
# 6. DB persistence — insert_pass_result / update_pass_result
# ════════════════════════════════════════════════════════════════════

class TestPass2DBPersistence:
    """Tests for Pass 2 DB persistence via insert/update_pass_result."""

    def test_insert_pass_result_for_pass2(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchone.return_value = (42,)
        import db

        now = datetime(2026, 3, 15, 8, 0, 0, tzinfo=timezone.utc)
        row_id = db.insert_pass_result(
            ticket_id=100,
            pass_name="pass2_grammar",
            prompt_version="1",
            model_name="matcha-27301",
            input_text="AutoPay payments remain in web tables",
            status="pending",
            started_at=now,
        )

        assert row_id == 42
        sql = cur.execute.call_args[0][0]
        assert "INSERT INTO ticket_llm_pass_results" in sql
        assert "RETURNING id" in sql
        conn.commit.assert_called_once()

    def test_update_pass_result_with_pass2_fields(self, patch_pool):
        conn, cur = patch_pool
        import db

        now = datetime(2026, 3, 15, 8, 1, 0, tzinfo=timezone.utc)
        db.update_pass_result(
            row_id=42,
            status="success",
            raw_response_text='{"component":"X","operation":"import","unexpected_state":"Y","canonical_failure":"X + import + Y"}',
            parsed_json={"component": "X", "operation": "import", "unexpected_state": "Y", "canonical_failure": "X + import + Y"},
            component="X",
            operation="import",
            unexpected_state="Y",
            canonical_failure="X + import + Y",
            completed_at=now,
        )

        sql = cur.execute.call_args[0][0]
        assert "UPDATE ticket_llm_pass_results" in sql
        assert "component" in sql
        assert "operation" in sql
        assert "unexpected_state" in sql
        assert "canonical_failure" in sql
        params = cur.execute.call_args[0][1]
        assert "X" in params
        assert "import" in params
        assert "Y" in params
        assert "X + import + Y" in params
        conn.commit.assert_called_once()

    def test_update_pass_result_to_failed(self, patch_pool):
        conn, cur = patch_pool
        import db

        now = datetime(2026, 3, 15, 8, 1, 0, tzinfo=timezone.utc)
        db.update_pass_result(
            row_id=42,
            status="failed",
            raw_response_text="malformed garbage",
            error_message="Unknown operation 'fly'",
            completed_at=now,
        )

        sql = cur.execute.call_args[0][0]
        assert "UPDATE ticket_llm_pass_results" in sql
        params = cur.execute.call_args[0][1]
        assert "failed" in params
        assert "Unknown operation 'fly'" in params
        conn.commit.assert_called_once()

    def test_update_pass_result_backward_compatible(self, patch_pool):
        """Pass 1 callers still work — Pass 2 columns default to None."""
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

        params = cur.execute.call_args[0][1]
        # component, operation, unexpected_state, canonical_failure should be None
        # They appear after completed_at and before row_id in the params
        assert params[-1] == 42  # row_id is last
        conn.commit.assert_called_once()


# ════════════════════════════════════════════════════════════════════
# 7. Malformed Matcha JSON handling
# ════════════════════════════════════════════════════════════════════

class TestPass2MalformedHandling:
    """Verify that bad Matcha responses are stored but result in failure."""

    def test_process_ticket_stores_malformed_response(self, patch_pool):
        """A malformed Matcha response should be stored with status=failed."""
        conn, cur = patch_pool
        cur.fetchone.return_value = (99,)
        cur.rowcount = 0
        import db

        with patch("run_ticket_pass2.call_matcha", return_value="not valid json"):
            with patch("db.migrate", return_value=[]):
                from run_ticket_pass2 import process_ticket
                result = process_ticket(
                    ticket_id=100,
                    phenomenon="AutoPay payments remain in web tables",
                    prompt_template="Test {{input_text}}",
                    force=False,
                )

        assert result["status"] == "failed"
        assert result["error"] is not None

    def test_process_ticket_stores_invalid_operation(self, patch_pool):
        """A response with an unknown operation should fail."""
        conn, cur = patch_pool
        cur.fetchone.return_value = (99,)
        cur.rowcount = 0
        import db

        bad_response = json.dumps({
            "component": "Module X",
            "operation": "fly",
            "unexpected_state": "data lost",
            "canonical_failure": "Module X + fly + data lost"
        })
        with patch("run_ticket_pass2.call_matcha", return_value=bad_response):
            with patch("db.migrate", return_value=[]):
                from run_ticket_pass2 import process_ticket
                result = process_ticket(
                    ticket_id=100,
                    phenomenon="Module X data lost",
                    prompt_template="Test {{input_text}}",
                    force=False,
                )

        assert result["status"] == "failed"
        assert "Unknown operation" in result["error"]

    def test_process_ticket_stores_empty_component(self, patch_pool):
        """A response with empty component should fail."""
        conn, cur = patch_pool
        cur.fetchone.return_value = (99,)
        cur.rowcount = 0
        import db

        bad_response = json.dumps({
            "component": "",
            "operation": "import",
            "unexpected_state": "data lost",
            "canonical_failure": " + import + data lost"
        })
        with patch("run_ticket_pass2.call_matcha", return_value=bad_response):
            with patch("db.migrate", return_value=[]):
                from run_ticket_pass2 import process_ticket
                result = process_ticket(
                    ticket_id=100,
                    phenomenon="Something happened",
                    prompt_template="Test {{input_text}}",
                    force=False,
                )

        assert result["status"] == "failed"
        assert "empty" in result["error"].lower()


# ════════════════════════════════════════════════════════════════════
# 8. Successful extraction flow
# ════════════════════════════════════════════════════════════════════

class TestPass2SuccessFlow:
    """End-to-end success path for process_ticket."""

    def test_successful_extraction(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchone.return_value = (99,)
        cur.rowcount = 0
        import db

        matcha_response = json.dumps({
            "component": "WebShare AutoPay transfer job",
            "operation": "transfer",
            "unexpected_state": "payments remain in web tables",
            "canonical_failure": "WebShare AutoPay transfer job + transfer + payments remain in web tables"
        })
        with patch("run_ticket_pass2.call_matcha", return_value=matcha_response):
            with patch("db.migrate", return_value=[]):
                from run_ticket_pass2 import process_ticket
                result = process_ticket(
                    ticket_id=100,
                    phenomenon="AutoPay payments remain in web tables after transfer job runs",
                    prompt_template="Analyze: {{input_text}}",
                    force=False,
                )

        assert result["status"] == "success"
        assert result["component"] == "WebShare AutoPay transfer job"
        assert result["operation"] == "transfer"
        assert result["unexpected_state"] == "payments remain in web tables"
        assert result["canonical_failure"] == "WebShare AutoPay transfer job + transfer + payments remain in web tables"
        assert result["elapsed_s"] >= 0

    def test_synonym_normalized_in_result(self, patch_pool):
        """A synonym operation is normalized in the result."""
        conn, cur = patch_pool
        cur.fetchone.return_value = (99,)
        cur.rowcount = 0
        import db

        matcha_response = json.dumps({
            "component": "Neptune meter module",
            "operation": "upload",
            "unexpected_state": "meter readings overwritten",
            "canonical_failure": "Neptune meter module + upload + meter readings overwritten"
        })
        with patch("run_ticket_pass2.call_matcha", return_value=matcha_response):
            with patch("db.migrate", return_value=[]):
                from run_ticket_pass2 import process_ticket
                result = process_ticket(
                    ticket_id=200,
                    phenomenon="Neptune meter readings overwritten during upload",
                    prompt_template="Test: {{input_text}}",
                    force=False,
                )

        assert result["status"] == "success"
        assert result["operation"] == "import"  # "upload" normalized to "import"
        assert "import" in result["canonical_failure"]

    def test_matcha_error_results_in_failed(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchone.return_value = (99,)
        cur.rowcount = 0
        import db

        with patch("run_ticket_pass2.call_matcha", side_effect=Exception("Timeout")):
            with patch("db.migrate", return_value=[]):
                from run_ticket_pass2 import process_ticket
                result = process_ticket(
                    ticket_id=100,
                    phenomenon="Some phenomenon",
                    prompt_template="Test {{input_text}}",
                    force=False,
                )

        assert result["status"] == "failed"
        assert "Timeout" in result["error"]


# ════════════════════════════════════════════════════════════════════
# 9. Prompt template
# ════════════════════════════════════════════════════════════════════

class TestPass2PromptTemplate:
    """Verify prompt loading and placeholder substitution."""

    def test_prompt_file_exists(self):
        import os
        prompt_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "prompts", "pass2_grammar.txt"
        )
        assert os.path.isfile(prompt_path), f"Prompt file not found: {prompt_path}"

    def test_prompt_contains_placeholder(self):
        import os
        prompt_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "prompts", "pass2_grammar.txt"
        )
        with open(prompt_path) as f:
            content = f.read()
        assert "{{input_text}}" in content

    def test_build_prompt_substitution(self):
        from run_ticket_pass2 import _build_prompt
        template = "Convert this phenomenon: {{input_text}}"
        result = _build_prompt(template, "AutoPay payments remain in web tables")
        assert result == "Convert this phenomenon: AutoPay payments remain in web tables"
        assert "{{input_text}}" not in result
