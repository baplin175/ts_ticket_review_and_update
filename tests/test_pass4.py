"""
Tests for Pass 4 — intervention mapping pipeline.

Covers:
  - Response parsing (valid, malformed, edge cases)
  - Taxonomy validation (mechanism_class, intervention_type)
  - Intervention action validation rules
  - Selection logic (only tickets with successful Pass 3 mechanism)
  - Idempotent reruns
  - DB persistence
  - Malformed Matcha JSON handling
  - Successful extraction flow
  - Aggregation logic
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

class TestPass4Parser:
    def test_valid_json(self):
        from pass4.mechanism_classifier import parse_pass4_response
        raw = json.dumps({
            "mechanism_class": "schema_mismatch",
            "intervention_type": "validation_guardrail",
            "intervention_action": "add strict schema validation to the import parser"
        })
        parsed, mc, it, ia = parse_pass4_response(raw)
        assert mc == "schema_mismatch"
        assert it == "validation_guardrail"
        assert ia == "add strict schema validation to the import parser"

    def test_valid_json_with_whitespace(self):
        from pass4.mechanism_classifier import parse_pass4_response
        raw = json.dumps({
            "mechanism_class": "  schema_mismatch  ",
            "intervention_type": "  validation_guardrail  ",
            "intervention_action": "  add schema validation  "
        })
        parsed, mc, it, ia = parse_pass4_response(raw)
        assert mc == "schema_mismatch"
        assert it == "validation_guardrail"
        assert ia == "add schema validation"

    def test_valid_json_with_code_fence(self):
        from pass4.mechanism_classifier import parse_pass4_response
        raw = '```json\n{"mechanism_class": "schema_mismatch", "intervention_type": "software_fix", "intervention_action": "fix parser"}\n```'
        parsed, mc, it, ia = parse_pass4_response(raw)
        assert mc == "schema_mismatch"
        assert it == "software_fix"

    def test_empty_response_raises(self):
        from pass4.mechanism_classifier import parse_pass4_response, Pass4ParseError
        with pytest.raises(Pass4ParseError, match="Empty response"):
            parse_pass4_response("")

    def test_whitespace_only_raises(self):
        from pass4.mechanism_classifier import parse_pass4_response, Pass4ParseError
        with pytest.raises(Pass4ParseError, match="Empty response"):
            parse_pass4_response("   \n  ")

    def test_invalid_json_raises(self):
        from pass4.mechanism_classifier import parse_pass4_response, Pass4ParseError
        with pytest.raises(Pass4ParseError, match="Invalid JSON"):
            parse_pass4_response("not json at all")

    def test_missing_mechanism_class_raises(self):
        from pass4.mechanism_classifier import parse_pass4_response, Pass4ParseError
        raw = json.dumps({
            "intervention_type": "software_fix",
            "intervention_action": "fix it"
        })
        with pytest.raises(Pass4ParseError, match="Missing 'mechanism_class'"):
            parse_pass4_response(raw)

    def test_missing_intervention_type_raises(self):
        from pass4.mechanism_classifier import parse_pass4_response, Pass4ParseError
        raw = json.dumps({
            "mechanism_class": "schema_mismatch",
            "intervention_action": "fix it"
        })
        with pytest.raises(Pass4ParseError, match="Missing 'intervention_type'"):
            parse_pass4_response(raw)

    def test_missing_intervention_action_raises(self):
        from pass4.mechanism_classifier import parse_pass4_response, Pass4ParseError
        raw = json.dumps({
            "mechanism_class": "schema_mismatch",
            "intervention_type": "software_fix"
        })
        with pytest.raises(Pass4ParseError, match="Missing 'intervention_action'"):
            parse_pass4_response(raw)

    def test_null_mechanism_class_raises(self):
        from pass4.mechanism_classifier import parse_pass4_response, Pass4ParseError
        raw = json.dumps({
            "mechanism_class": None,
            "intervention_type": "software_fix",
            "intervention_action": "fix it"
        })
        with pytest.raises(Pass4ParseError, match="must not be null"):
            parse_pass4_response(raw)

    def test_empty_mechanism_class_raises(self):
        from pass4.mechanism_classifier import parse_pass4_response, Pass4ParseError
        raw = json.dumps({
            "mechanism_class": "",
            "intervention_type": "software_fix",
            "intervention_action": "fix it"
        })
        with pytest.raises(Pass4ParseError, match="empty after trimming"):
            parse_pass4_response(raw)

    def test_empty_intervention_action_raises(self):
        from pass4.mechanism_classifier import parse_pass4_response, Pass4ParseError
        raw = json.dumps({
            "mechanism_class": "schema_mismatch",
            "intervention_type": "software_fix",
            "intervention_action": "   "
        })
        with pytest.raises(Pass4ParseError, match="empty after trimming"):
            parse_pass4_response(raw)

    def test_non_string_mechanism_class_raises(self):
        from pass4.mechanism_classifier import parse_pass4_response, Pass4ParseError
        raw = json.dumps({
            "mechanism_class": 42,
            "intervention_type": "software_fix",
            "intervention_action": "fix it"
        })
        with pytest.raises(Pass4ParseError, match="must be a string"):
            parse_pass4_response(raw)

    def test_array_response_raises(self):
        from pass4.mechanism_classifier import parse_pass4_response, Pass4ParseError
        with pytest.raises(Pass4ParseError, match="Expected JSON object"):
            parse_pass4_response('[{"mechanism_class": "test"}]')

    def test_extra_fields_preserved(self):
        from pass4.mechanism_classifier import parse_pass4_response
        raw = json.dumps({
            "mechanism_class": "schema_mismatch",
            "intervention_type": "software_fix",
            "intervention_action": "fix it",
            "confidence": 0.95
        })
        parsed, mc, it, ia = parse_pass4_response(raw)
        assert parsed["confidence"] == 0.95

    def test_case_insensitive_class(self):
        from pass4.mechanism_classifier import parse_pass4_response
        raw = json.dumps({
            "mechanism_class": "Schema_Mismatch",
            "intervention_type": "Software_Fix",
            "intervention_action": "fix the parser"
        })
        parsed, mc, it, ia = parse_pass4_response(raw)
        assert mc == "schema_mismatch"
        assert it == "software_fix"


# ── Taxonomy validation tests ────────────────────────────────────────

class TestTaxonomyValidation:
    def test_unknown_mechanism_class_raises(self):
        from pass4.mechanism_classifier import parse_pass4_response, Pass4ParseError
        raw = json.dumps({
            "mechanism_class": "invented_class",
            "intervention_type": "software_fix",
            "intervention_action": "fix it"
        })
        with pytest.raises(Pass4ParseError, match="Unknown mechanism_class"):
            parse_pass4_response(raw)

    def test_unknown_intervention_type_raises(self):
        from pass4.mechanism_classifier import parse_pass4_response, Pass4ParseError
        raw = json.dumps({
            "mechanism_class": "schema_mismatch",
            "intervention_type": "invented_type",
            "intervention_action": "fix it"
        })
        with pytest.raises(Pass4ParseError, match="Unknown intervention_type"):
            parse_pass4_response(raw)

    def test_all_mechanism_classes_accepted(self):
        from pass4.mechanism_classifier import parse_pass4_response
        from pass4.mechanism_classes import MECHANISM_CLASSES
        for mc in sorted(MECHANISM_CLASSES):
            payload = {
                "mechanism_class": mc,
                "intervention_type": "software_fix",
                "intervention_action": "fix it",
            }
            if mc == "other":
                payload["proposed_class"] = "new_class_name"
            raw = json.dumps(payload)
            _, got_mc, _, _ = parse_pass4_response(raw)
            assert got_mc == mc

    def test_all_intervention_types_accepted(self):
        from pass4.mechanism_classifier import parse_pass4_response
        from pass4.intervention_types import INTERVENTION_TYPES
        for it in sorted(INTERVENTION_TYPES):
            payload = {
                "mechanism_class": "schema_mismatch",
                "intervention_type": it,
                "intervention_action": "fix it",
            }
            if it == "other":
                payload["proposed_type"] = "new_type_name"
            raw = json.dumps(payload)
            _, _, got_it, _ = parse_pass4_response(raw)
            assert got_it == it

    def test_mechanism_classes_count(self):
        from pass4.mechanism_classes import MECHANISM_CLASSES
        assert len(MECHANISM_CLASSES) == 14

    def test_intervention_types_count(self):
        from pass4.intervention_types import INTERVENTION_TYPES
        assert len(INTERVENTION_TYPES) == 8

    def test_other_mechanism_class_requires_proposed(self):
        from pass4.mechanism_classifier import parse_pass4_response, Pass4ParseError
        raw = json.dumps({
            "mechanism_class": "other",
            "intervention_type": "software_fix",
            "intervention_action": "fix it"
        })
        with pytest.raises(Pass4ParseError, match="proposed_class.*required"):
            parse_pass4_response(raw)

    def test_other_mechanism_class_with_proposed(self):
        from pass4.mechanism_classifier import parse_pass4_response
        raw = json.dumps({
            "mechanism_class": "other",
            "intervention_type": "software_fix",
            "intervention_action": "fix it",
            "proposed_class": "rate_calculation_drift"
        })
        parsed, mc, it, ia = parse_pass4_response(raw)
        assert mc == "other"
        assert parsed["proposed_class"] == "rate_calculation_drift"

    def test_other_intervention_type_requires_proposed(self):
        from pass4.mechanism_classifier import parse_pass4_response, Pass4ParseError
        raw = json.dumps({
            "mechanism_class": "schema_mismatch",
            "intervention_type": "other",
            "intervention_action": "fix it"
        })
        with pytest.raises(Pass4ParseError, match="proposed_type.*required"):
            parse_pass4_response(raw)

    def test_other_intervention_type_with_proposed(self):
        from pass4.mechanism_classifier import parse_pass4_response
        raw = json.dumps({
            "mechanism_class": "schema_mismatch",
            "intervention_type": "other",
            "intervention_action": "fix it",
            "proposed_type": "process_redesign"
        })
        parsed, mc, it, ia = parse_pass4_response(raw)
        assert it == "other"
        assert parsed["proposed_type"] == "process_redesign"

    def test_other_proposed_class_normalized(self):
        from pass4.mechanism_classifier import parse_pass4_response
        raw = json.dumps({
            "mechanism_class": "other",
            "intervention_type": "software_fix",
            "intervention_action": "fix it",
            "proposed_class": "  Rate_Drift  "
        })
        parsed, _, _, _ = parse_pass4_response(raw)
        assert parsed["proposed_class"] == "rate_drift"


# ── Intervention action validation tests ─────────────────────────────

class TestInterventionActionValidation:
    def test_rejects_exact_restatement(self):
        from pass4.mechanism_classifier import validate_intervention_action, Pass4ParseError
        mechanism = "Import parser rejects input file due to schema mismatch"
        with pytest.raises(Pass4ParseError, match="exact restatement"):
            validate_intervention_action(mechanism, mechanism)

    def test_rejects_case_insensitive_restatement(self):
        from pass4.mechanism_classifier import validate_intervention_action, Pass4ParseError
        mechanism = "Import parser rejects input file"
        with pytest.raises(Pass4ParseError, match="exact restatement"):
            validate_intervention_action(mechanism.upper(), mechanism)

    def test_rejects_ticket_word(self):
        from pass4.mechanism_classifier import validate_intervention_action, Pass4ParseError
        with pytest.raises(Pass4ParseError, match="administrative language"):
            validate_intervention_action(
                "Create a ticket to track this fix",
                "Schema mismatch error"
            )

    def test_rejects_support_agent_phrase(self):
        from pass4.mechanism_classifier import validate_intervention_action, Pass4ParseError
        with pytest.raises(Pass4ParseError, match="administrative language"):
            validate_intervention_action(
                "Support agent should update the config",
                "Configuration mismatch error"
            )

    def test_rejects_troubleshooting_phrase(self):
        from pass4.mechanism_classifier import validate_intervention_action, Pass4ParseError
        with pytest.raises(Pass4ParseError, match="administrative language"):
            validate_intervention_action(
                "Troubleshoot the integration issue",
                "Integration failure"
            )

    def test_accepts_valid_action(self):
        from pass4.mechanism_classifier import validate_intervention_action
        result = validate_intervention_action(
            "Add strict schema validation and descriptive error messages to the import parser",
            "Import parser rejects input file due to schema mismatch"
        )
        assert "schema validation" in result

    def test_accepts_technical_action(self):
        from pass4.mechanism_classifier import validate_intervention_action
        result = validate_intervention_action(
            "Implement retry logic with exponential backoff for API authentication requests",
            "Authentication token expires during long-running integration sync"
        )
        assert "retry logic" in result


# ── Selection logic tests ────────────────────────────────────────────

class TestSelectionLogic:
    def test_basic_selection_excludes_success(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchall.return_value = [(1, "some mechanism")]
        import db
        db.fetch_pending_pass4_tickets("1")
        sql = cur.execute.call_args[0][0]
        assert "mechanism IS NOT NULL" in sql
        assert "NOT EXISTS" in sql
        assert "pass4_intervention" in sql

    def test_force_skips_success_check(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchall.return_value = []
        import db
        db.fetch_pending_pass4_tickets("1", force=True)
        sql = cur.execute.call_args[0][0]
        assert "NOT EXISTS" not in sql

    def test_failed_only_filters(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchall.return_value = []
        import db
        db.fetch_pending_pass4_tickets("1", failed_only=True)
        sql = cur.execute.call_args[0][0]
        assert "NOT EXISTS" in sql
        assert "status = 'failed'" in sql

    def test_ticket_ids_filter(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchall.return_value = []
        import db
        db.fetch_pending_pass4_tickets("1", ticket_ids=[100, 200])
        sql = cur.execute.call_args[0][0]
        assert "ticket_id IN" in sql
        params = cur.execute.call_args[0][1]
        assert 100 in params
        assert 200 in params

    def test_limit_applied(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchall.return_value = []
        import db
        db.fetch_pending_pass4_tickets("1", limit=50)
        sql = cur.execute.call_args[0][0]
        assert "LIMIT 50" in sql

    def test_requires_pass3_success(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchall.return_value = []
        import db
        db.fetch_pending_pass4_tickets("1")
        sql = cur.execute.call_args[0][0]
        assert "p3.status = 'success'" in sql
        params = cur.execute.call_args[0][1]
        assert "pass3_mechanism" in params


# ── Idempotency tests ────────────────────────────────────────────────

class TestIdempotency:
    def test_second_run_skips_successful_ticket(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchall.return_value = []
        import db
        rows = db.fetch_pending_pass4_tickets("1", force=False)
        sql = cur.execute.call_args[0][0]
        assert "NOT EXISTS" in sql
        assert "status = 'success'" in sql
        assert rows == []

    def test_force_rerun_includes_successful(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchall.return_value = [(1, "some mechanism")]
        import db
        rows = db.fetch_pending_pass4_tickets("1", force=True)
        sql = cur.execute.call_args[0][0]
        assert "NOT EXISTS" not in sql
        assert len(rows) == 1

    def test_delete_prior_failed_called(self, patch_pool):
        conn, cur = patch_pool
        cur.rowcount = 1
        import db
        deleted = db.delete_prior_failed_pass(123, "pass4_intervention", "1")
        sql = cur.execute.call_args[0][0]
        assert "DELETE FROM ticket_llm_pass_results" in sql
        assert "status IN ('pending', 'failed')" in sql


# ── DB persistence tests ─────────────────────────────────────────────

class TestDBPersistence:
    def test_insert_pass_result_sql(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchone.return_value = (42,)
        import db
        row_id = db.insert_pass_result(
            99999,
            pass_name="pass4_intervention",
            prompt_version="1",
            model_name="matcha-27301",
            input_text="some mechanism",
            status="pending",
        )
        sql = cur.execute.call_args[0][0]
        assert "INSERT INTO ticket_llm_pass_results" in sql
        assert "ticket_number" in sql
        assert row_id == 42

    def test_update_pass_result_with_pass4_fields(self, patch_pool):
        conn, cur = patch_pool
        import db
        db.update_pass_result(
            42,
            status="success",
            raw_response_text='{"mechanism_class":"schema_mismatch"}',
            mechanism_class="schema_mismatch",
            intervention_type="validation_guardrail",
            intervention_action="add schema validation",
        )
        sql = cur.execute.call_args[0][0]
        assert "mechanism_class" in sql
        assert "intervention_type" in sql
        assert "intervention_action" in sql
        params = cur.execute.call_args[0][1]
        assert "schema_mismatch" in params
        assert "validation_guardrail" in params
        assert "add schema validation" in params


# ── Malformed response handling ───────────────────────────────────────

class TestMalformedHandling:
    def test_process_ticket_stores_failed_on_bad_json(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchone.return_value = (1,)
        with patch("pass4.intervention_mapper.call_matcha", return_value="not json"):
            from pass4.intervention_mapper import process_ticket
            result = process_ticket(
                99999,
                "some mechanism",
                "template {{mechanism}}",
            )
        assert result["status"] == "failed"
        assert "Invalid JSON" in result["error"]

    def test_process_ticket_stores_failed_on_unknown_class(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchone.return_value = (1,)
        bad_response = json.dumps({
            "mechanism_class": "invented_class",
            "intervention_type": "software_fix",
            "intervention_action": "fix it"
        })
        with patch("pass4.intervention_mapper.call_matcha", return_value=bad_response):
            from pass4.intervention_mapper import process_ticket
            result = process_ticket(
                99999,
                "some mechanism",
                "template {{mechanism}}",
            )
        assert result["status"] == "failed"
        assert "Unknown mechanism_class" in result["error"]


# ── Success flow tests ────────────────────────────────────────────────

class TestSuccessFlow:
    def test_process_ticket_success(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchone.return_value = (1,)
        good_response = json.dumps({
            "mechanism_class": "schema_mismatch",
            "intervention_type": "validation_guardrail",
            "intervention_action": "add strict schema validation to the import parser"
        })
        with patch("pass4.intervention_mapper.call_matcha", return_value=good_response):
            from pass4.intervention_mapper import process_ticket
            result = process_ticket(
                99999,
                "Import parser rejects input due to schema mismatch",
                "template {{mechanism}}",
            )
        assert result["status"] == "success"
        assert result["mechanism_class"] == "schema_mismatch"
        assert result["intervention_type"] == "validation_guardrail"
        assert "schema validation" in result["intervention_action"]

    def test_process_ticket_with_force(self, patch_pool):
        conn, cur = patch_pool
        cur.fetchone.return_value = (1,)
        good_response = json.dumps({
            "mechanism_class": "calculation_logic_error",
            "intervention_type": "software_fix",
            "intervention_action": "correct the billing calculation logic"
        })
        with patch("pass4.intervention_mapper.call_matcha", return_value=good_response):
            from pass4.intervention_mapper import process_ticket
            result = process_ticket(
                99999,
                "Billing module applies charge twice",
                "template {{mechanism}}",
                force=True,
            )
        assert result["status"] == "success"
        assert result["mechanism_class"] == "calculation_logic_error"


# ── Aggregation tests ────────────────────────────────────────────────

class TestAggregation:
    def test_aggregate_from_results_basic(self):
        from pass4.intervention_aggregator import aggregate_from_results
        results = [
            {"status": "success", "mechanism_class": "schema_mismatch", "intervention_type": "validation_guardrail", "intervention_action": "add validation"},
            {"status": "success", "mechanism_class": "schema_mismatch", "intervention_type": "validation_guardrail", "intervention_action": "add validation"},
            {"status": "success", "mechanism_class": "calculation_logic_error", "intervention_type": "software_fix", "intervention_action": "fix calc"},
            {"status": "failed", "mechanism_class": None, "intervention_type": None, "intervention_action": None},
        ]
        agg = aggregate_from_results(results)
        assert agg["mechanism_class_counts"]["schema_mismatch"] == 2
        assert agg["mechanism_class_counts"]["calculation_logic_error"] == 1
        assert agg["intervention_type_counts"]["validation_guardrail"] == 2
        assert agg["intervention_type_counts"]["software_fix"] == 1
        assert len(agg["top_engineering_fixes"]) == 2
        assert agg["top_engineering_fixes"][0]["ticket_count"] == 2
        assert agg["top_engineering_fixes"][0]["mechanism_class"] == "schema_mismatch"

    def test_aggregate_empty_results(self):
        from pass4.intervention_aggregator import aggregate_from_results
        agg = aggregate_from_results([])
        assert agg["mechanism_class_counts"] == {}
        assert agg["intervention_type_counts"] == {}
        assert agg["top_engineering_fixes"] == []

    def test_aggregate_skips_failed(self):
        from pass4.intervention_aggregator import aggregate_from_results
        results = [
            {"status": "failed", "mechanism_class": "schema_mismatch", "intervention_type": "software_fix", "intervention_action": "fix"},
        ]
        agg = aggregate_from_results(results)
        assert agg["mechanism_class_counts"] == {}

    def test_write_artifacts(self, tmp_path):
        from pass4.intervention_aggregator import write_artifacts
        agg = {
            "mechanism_class_counts": {"schema_mismatch": 2},
            "intervention_type_counts": {"software_fix": 2},
            "top_engineering_fixes": [{"mechanism_class": "schema_mismatch", "intervention_type": "software_fix", "ticket_count": 2, "recommended_fix": "fix"}],
        }
        interventions = [{"ticket_id": "1", "mechanism_class": "schema_mismatch", "intervention_type": "software_fix", "intervention_action": "fix"}]
        written = write_artifacts(agg, str(tmp_path), interventions=interventions)
        assert len(written) == 4
        for path in written:
            assert os.path.exists(path)


# ── Stale P4 invalidation tests ──────────────────────────────────────

class TestStaleInvalidation:
    def test_invalidate_stale_pass4_sql(self, patch_pool):
        conn, cur = patch_pool
        cur.rowcount = 2
        import db
        updated = db.invalidate_stale_pass4(
            [100, 200, 300],
            pass3_pass_name="pass3_mechanism",
            pass3_prompt_version="3",
        )
        sql = cur.execute.call_args[0][0]
        assert "UPDATE ticket_llm_pass_results" in sql
        assert "status = 'skipped'" in sql
        assert "pass4_intervention" in sql
        assert "NOT EXISTS" in sql
        params = cur.execute.call_args[0][1]
        assert 100 in params
        assert 200 in params
        assert 300 in params
        assert "pass3_mechanism" in params
        assert "3" in params
        assert updated == 2

    def test_invalidate_stale_pass4_empty_list(self, patch_pool):
        conn, cur = patch_pool
        import db
        updated = db.invalidate_stale_pass4([])
        assert updated == 0
        cur.execute.assert_not_called()

    def test_main_invalidates_stale_when_tickets_missing_p3(self, patch_pool):
        """Tickets requested but filtered out (no P3) trigger stale invalidation."""
        conn, cur = patch_pool
        cur.fetchall.return_value = [(101, "valid mechanism")]
        cur.rowcount = 1
        import db
        with patch("db.invalidate_stale_pass4", return_value=1) as mock_inv:
            with patch("run_pass4._load_prompt_template", return_value="template {{mechanism}}"):
                with patch("run_pass4.process_ticket", return_value={
                    "status": "success", "ticket_id": 101,
                    "mechanism_class": "schema_mismatch",
                    "intervention_type": "software_fix",
                    "intervention_action": "fix it",
                    "elapsed_s": 0.1,
                }) as mock_proc:
                    with patch("run_pass4.aggregate_from_db", return_value={
                        "mechanism_class_counts": {}, "intervention_type_counts": {},
                        "top_engineering_fixes": [],
                    }):
                        with patch("run_pass4.write_artifacts", return_value=[]):
                            with patch("db.migrate", return_value=[]):
                                import run_pass4
                                run_pass4.main(ticket_ids=[101, 200, 300], force=True)
            mock_inv.assert_called_once_with(
                [200, 300],
                pass3_pass_name="pass3_mechanism",
                pass3_prompt_version="3",
            )


import os
