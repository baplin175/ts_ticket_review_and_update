"""Tests for the CSV pipeline runner (pipeline/csv_runner.py).

All LLM calls are mocked — no network or DB dependency.
"""

import csv
import json
import os
import tempfile
from unittest.mock import patch

import pytest

from pipeline.csv_runner import (
    run_pass1_csv,
    run_pass3_csv,
    run_pass4_csv,
    run_full_pipeline,
    PASS1_COLUMNS,
    PASS3_COLUMNS,
    PASS4_COLUMNS,
)


# ── Fixtures ─────────────────────────────────────────────────────────

@pytest.fixture
def tmp_dir(tmp_path):
    return str(tmp_path)


def _write_csv(path, fieldnames, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def _read_csv(path):
    with open(path, "r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


# ── Mock LLM responses ──────────────────────────────────────────────

PASS1_GOOD_RESPONSE = json.dumps({
    "phenomenon": "Billing screen crashes when saving payment plan",
    "confidence": "HIGH",
    "component": "Billing screen",
    "operation": "save",
    "unexpected_state": "application crashes on payment plan submission",
})

PASS1_LOW_RESPONSE = json.dumps({
    "phenomenon": None,
    "confidence": "LOW",
})

PASS3_GOOD_RESPONSE = json.dumps({
    "mechanism": "Null pointer in payment plan serializer when plan array is empty",
    "category": "software_defect",
    "evidence": "from_thread",
})

PASS4_GOOD_RESPONSE = json.dumps({
    "mechanism_class": "data_validation_failure",
    "intervention_type": "software_fix",
    "intervention_action": "add null-check guard in payment plan serializer before array iteration",
})


# ── Pass 1 tests ─────────────────────────────────────────────────────

class TestPass1Csv:
    def test_basic(self, tmp_dir):
        input_csv = os.path.join(tmp_dir, "input.csv")
        output_csv = os.path.join(tmp_dir, "pass1.csv")
        _write_csv(input_csv, ["ticket_id", "ticket_name", "full_thread_text"], [
            {"ticket_id": "100", "ticket_name": "Billing crash", "full_thread_text": "The billing screen crashes."},
        ])

        with patch("pipeline.csv_runner.call_matcha", return_value=PASS1_GOOD_RESPONSE):
            run_pass1_csv(input_csv, output_csv)

        rows = _read_csv(output_csv)
        assert len(rows) == 1
        assert rows[0]["ticket_id"] == "100"
        assert rows[0]["status"] == "success"
        assert rows[0]["phenomenon"] == "Billing screen crashes when saving payment plan"
        assert rows[0]["component"] == "Billing screen"
        assert rows[0]["operation"] == "update"  # normalize_operation maps "save" → "update"
        assert rows[0]["canonical_failure"] == "Billing screen + update + application crashes on payment plan submission"
        assert rows[0]["confidence"] == "HIGH"

    def test_columns(self, tmp_dir):
        input_csv = os.path.join(tmp_dir, "input.csv")
        output_csv = os.path.join(tmp_dir, "pass1.csv")
        _write_csv(input_csv, ["ticket_id", "ticket_name", "full_thread_text"], [
            {"ticket_id": "1", "ticket_name": "t", "full_thread_text": "text"},
        ])

        with patch("pipeline.csv_runner.call_matcha", return_value=PASS1_GOOD_RESPONSE):
            run_pass1_csv(input_csv, output_csv)

        with open(output_csv) as f:
            reader = csv.reader(f)
            header = next(reader)
        assert header == PASS1_COLUMNS

    def test_error_isolation(self, tmp_dir):
        """A failing row should not stop the pipeline."""
        input_csv = os.path.join(tmp_dir, "input.csv")
        output_csv = os.path.join(tmp_dir, "pass1.csv")
        _write_csv(input_csv, ["ticket_id", "ticket_name", "full_thread_text"], [
            {"ticket_id": "1", "ticket_name": "ok", "full_thread_text": "text1"},
            {"ticket_id": "2", "ticket_name": "bad", "full_thread_text": "text2"},
            {"ticket_id": "3", "ticket_name": "ok2", "full_thread_text": "text3"},
        ])

        call_count = 0

        def _mock_matcha(prompt, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise RuntimeError("LLM timeout")
            return PASS1_GOOD_RESPONSE

        with patch("pipeline.csv_runner.call_matcha", side_effect=_mock_matcha):
            run_pass1_csv(input_csv, output_csv)

        rows = _read_csv(output_csv)
        assert len(rows) == 3
        assert rows[0]["status"] == "success"
        assert rows[1]["status"] == "failed"
        assert "LLM timeout" in rows[1]["error"]
        assert rows[2]["status"] == "success"

    def test_progress_callback(self, tmp_dir):
        input_csv = os.path.join(tmp_dir, "input.csv")
        output_csv = os.path.join(tmp_dir, "pass1.csv")
        _write_csv(input_csv, ["ticket_id", "ticket_name", "full_thread_text"], [
            {"ticket_id": "1", "ticket_name": "t", "full_thread_text": "x"},
            {"ticket_id": "2", "ticket_name": "t", "full_thread_text": "y"},
        ])

        calls = []
        with patch("pipeline.csv_runner.call_matcha", return_value=PASS1_GOOD_RESPONSE):
            run_pass1_csv(input_csv, output_csv, progress_cb=lambda p, i, t: calls.append((p, i, t)))

        assert calls == [(1, 1, 2), (1, 2, 2)]


# ── Pass 3 tests ─────────────────────────────────────────────────────

class TestPass3Csv:
    def _setup_pass1(self, tmp_dir, rows):
        p1 = os.path.join(tmp_dir, "pass1.csv")
        _write_csv(p1, PASS1_COLUMNS, rows)
        return p1

    def _setup_input(self, tmp_dir, rows):
        inp = os.path.join(tmp_dir, "input.csv")
        _write_csv(inp, ["ticket_id", "ticket_name", "full_thread_text"], rows)
        return inp

    def test_basic(self, tmp_dir):
        p1 = self._setup_pass1(tmp_dir, [{
            "ticket_id": "100", "ticket_name": "Billing crash",
            "phenomenon": "Billing screen crashes", "component": "Billing screen",
            "operation": "save", "unexpected_state": "crashes",
            "canonical_failure": "Billing screen + save + crashes",
            "confidence": "HIGH", "status": "success", "error": "",
        }])
        inp = self._setup_input(tmp_dir, [{
            "ticket_id": "100", "ticket_name": "Billing crash",
            "full_thread_text": "The billing screen crashes when saving.",
        }])
        out = os.path.join(tmp_dir, "pass3.csv")

        with patch("pipeline.csv_runner.call_matcha", return_value=PASS3_GOOD_RESPONSE):
            run_pass3_csv(p1, inp, out)

        rows = _read_csv(out)
        assert len(rows) == 1
        assert rows[0]["status"] == "success"
        assert rows[0]["mechanism"] == "Null pointer in payment plan serializer when plan array is empty"
        assert rows[0]["category"] == "software_defect"
        assert rows[0]["evidence"] == "from_thread"

    def test_skip_low_confidence(self, tmp_dir):
        p1 = self._setup_pass1(tmp_dir, [{
            "ticket_id": "200", "ticket_name": "No issue",
            "phenomenon": "", "component": "", "operation": "", "unexpected_state": "",
            "canonical_failure": "", "confidence": "LOW",
            "status": "success", "error": "",
        }])
        inp = self._setup_input(tmp_dir, [{
            "ticket_id": "200", "ticket_name": "No issue", "full_thread_text": "All fine.",
        }])
        out = os.path.join(tmp_dir, "pass3.csv")

        with patch("pipeline.csv_runner.call_matcha") as mock:
            run_pass3_csv(p1, inp, out)
            mock.assert_not_called()

        rows = _read_csv(out)
        assert len(rows) == 1
        assert rows[0]["status"] == "skipped"

    def test_skip_empty_canonical_failure(self, tmp_dir):
        p1 = self._setup_pass1(tmp_dir, [{
            "ticket_id": "300", "ticket_name": "X",
            "phenomenon": "something", "component": "C", "operation": "O",
            "unexpected_state": "S", "canonical_failure": "",
            "confidence": "HIGH", "status": "success", "error": "",
        }])
        inp = self._setup_input(tmp_dir, [
            {"ticket_id": "300", "ticket_name": "X", "full_thread_text": "thread"},
        ])
        out = os.path.join(tmp_dir, "pass3.csv")

        with patch("pipeline.csv_runner.call_matcha") as mock:
            run_pass3_csv(p1, inp, out)
            mock.assert_not_called()

        rows = _read_csv(out)
        assert rows[0]["status"] == "skipped"

    def test_columns(self, tmp_dir):
        p1 = self._setup_pass1(tmp_dir, [{
            "ticket_id": "1", "ticket_name": "", "phenomenon": "x",
            "component": "C", "operation": "O", "unexpected_state": "S",
            "canonical_failure": "C + O + S", "confidence": "HIGH",
            "status": "success", "error": "",
        }])
        inp = self._setup_input(tmp_dir, [
            {"ticket_id": "1", "ticket_name": "", "full_thread_text": "t"},
        ])
        out = os.path.join(tmp_dir, "pass3.csv")

        with patch("pipeline.csv_runner.call_matcha", return_value=PASS3_GOOD_RESPONSE):
            run_pass3_csv(p1, inp, out)

        with open(out) as f:
            header = next(csv.reader(f))
        assert header == PASS3_COLUMNS


# ── Pass 4 tests ─────────────────────────────────────────────────────

class TestPass4Csv:
    def _setup_pass3(self, tmp_dir, rows):
        p3 = os.path.join(tmp_dir, "pass3.csv")
        _write_csv(p3, PASS3_COLUMNS, rows)
        return p3

    def test_basic(self, tmp_dir):
        p3 = self._setup_pass3(tmp_dir, [{
            "ticket_id": "100", "mechanism": "Null pointer in serializer",
            "evidence": "from_thread", "category": "software_defect",
            "status": "success", "error": "",
        }])
        out = os.path.join(tmp_dir, "pass4.csv")

        with patch("pipeline.csv_runner.call_matcha", return_value=PASS4_GOOD_RESPONSE):
            run_pass4_csv(p3, out)

        rows = _read_csv(out)
        assert len(rows) == 1
        assert rows[0]["status"] == "success"
        assert rows[0]["mechanism_class"] == "data_validation_failure"
        assert rows[0]["intervention_type"] == "software_fix"
        assert "null-check" in rows[0]["intervention_action"]

    def test_skip_non_success(self, tmp_dir):
        p3 = self._setup_pass3(tmp_dir, [{
            "ticket_id": "200", "mechanism": "",
            "evidence": "", "category": "",
            "status": "skipped", "error": "",
        }])
        out = os.path.join(tmp_dir, "pass4.csv")

        with patch("pipeline.csv_runner.call_matcha") as mock:
            run_pass4_csv(p3, out)
            mock.assert_not_called()

        rows = _read_csv(out)
        assert rows[0]["status"] == "skipped"

    def test_proposed_class_and_type(self, tmp_dir):
        response = json.dumps({
            "mechanism_class": "other",
            "proposed_class": "user_knowledge_gap",
            "intervention_type": "other",
            "proposed_type": "guided_walkthrough",
            "intervention_action": "add guided onboarding wizard to billing module",
        })
        p3 = self._setup_pass3(tmp_dir, [{
            "ticket_id": "400", "mechanism": "User unfamiliar with billing workflow",
            "evidence": "from_thread", "category": "user_training",
            "status": "success", "error": "",
        }])
        out = os.path.join(tmp_dir, "pass4.csv")

        with patch("pipeline.csv_runner.call_matcha", return_value=response):
            run_pass4_csv(p3, out)

        rows = _read_csv(out)
        assert rows[0]["status"] == "success"
        assert rows[0]["proposed_class"] == "user_knowledge_gap"
        assert rows[0]["proposed_type"] == "guided_walkthrough"

    def test_columns(self, tmp_dir):
        p3 = self._setup_pass3(tmp_dir, [{
            "ticket_id": "1", "mechanism": "Bug",
            "evidence": "inferred", "category": "software_defect",
            "status": "success", "error": "",
        }])
        out = os.path.join(tmp_dir, "pass4.csv")

        with patch("pipeline.csv_runner.call_matcha", return_value=PASS4_GOOD_RESPONSE):
            run_pass4_csv(p3, out)

        with open(out) as f:
            header = next(csv.reader(f))
        assert header == PASS4_COLUMNS


# ── Full pipeline test ───────────────────────────────────────────────

class TestFullPipeline:
    def test_end_to_end(self, tmp_dir):
        input_csv = os.path.join(tmp_dir, "input.csv")
        output_dir = os.path.join(tmp_dir, "output")
        _write_csv(input_csv, ["ticket_id", "ticket_name", "full_thread_text"], [
            {"ticket_id": "1", "ticket_name": "Crash", "full_thread_text": "App crashes on save."},
            {"ticket_id": "2", "ticket_name": "No issue", "full_thread_text": "All fine."},
        ])

        call_count = 0

        def _mock_matcha(prompt, **kwargs):
            nonlocal call_count
            call_count += 1
            # Pass 1: first row HIGH, second row LOW
            if call_count == 1:
                return PASS1_GOOD_RESPONSE
            elif call_count == 2:
                return PASS1_LOW_RESPONSE
            # Pass 3: only called for first row (second is LOW)
            elif call_count == 3:
                return PASS3_GOOD_RESPONSE
            # Pass 4: only called for first row
            elif call_count == 4:
                return PASS4_GOOD_RESPONSE
            return "{}"

        with patch("pipeline.csv_runner.call_matcha", side_effect=_mock_matcha):
            result = run_full_pipeline(input_csv, output_dir)

        # Verify all 3 output CSVs exist
        assert os.path.isfile(result["pass1"])
        assert os.path.isfile(result["pass3"])
        assert os.path.isfile(result["pass4"])

        # Pass 1: 2 rows
        p1_rows = _read_csv(result["pass1"])
        assert len(p1_rows) == 2
        assert p1_rows[0]["status"] == "success"
        assert p1_rows[1]["confidence"] == "LOW"

        # Pass 3: 2 rows (1 success, 1 skipped)
        p3_rows = _read_csv(result["pass3"])
        assert len(p3_rows) == 2
        assert p3_rows[0]["status"] == "success"
        assert p3_rows[1]["status"] == "skipped"

        # Pass 4: 2 rows (1 success, 1 skipped)
        p4_rows = _read_csv(result["pass4"])
        assert len(p4_rows) == 2
        assert p4_rows[0]["status"] == "success"
        assert p4_rows[1]["status"] == "skipped"

        # Only 4 LLM calls total (not 6)
        assert call_count == 4

    def test_progress_callback(self, tmp_dir):
        input_csv = os.path.join(tmp_dir, "input.csv")
        output_dir = os.path.join(tmp_dir, "output")
        _write_csv(input_csv, ["ticket_id", "ticket_name", "full_thread_text"], [
            {"ticket_id": "1", "ticket_name": "t", "full_thread_text": "x"},
        ])

        calls = []

        with patch("pipeline.csv_runner.call_matcha") as mock:
            mock.return_value = PASS1_GOOD_RESPONSE

            def _side_effect(prompt, **kwargs):
                if "phenomenon" in prompt.lower() or "observable" in prompt.lower():
                    return PASS1_GOOD_RESPONSE
                elif "mechanism" in prompt.lower() and "intervention" not in prompt.lower():
                    return PASS3_GOOD_RESPONSE
                else:
                    return PASS4_GOOD_RESPONSE

            mock.side_effect = _side_effect
            run_full_pipeline(input_csv, output_dir, progress_cb=lambda p, i, t: calls.append((p, i, t)))

        # Should have progress calls from all 3 passes
        pass_nums = [c[0] for c in calls]
        assert 1 in pass_nums
        assert 3 in pass_nums
        assert 4 in pass_nums
