"""
Tests for hash-based enrichment skip logic.

Verifies that:
  - Unchanged content (hash match) → skip
  - Changed content (hash mismatch) → recompute
  - force=True → always recompute
  - No prior hash → recompute (first run)
"""

from unittest.mock import patch, MagicMock

import pytest


@pytest.fixture(autouse=True)
def _no_real_db(monkeypatch):
    """Prevent any real DB connections during these tests."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://test:test@localhost/test")
    import config
    monkeypatch.setattr(config, "DATABASE_URL", "postgresql://test:test@localhost/test")


# ── Priority skip logic ──────────────────────────────────────────────

def test_priority_skip_when_hash_matches():
    from run_priority import _should_skip
    with patch("db._is_enabled", return_value=True), \
         patch("db.get_current_hashes", return_value={"thread_hash": "aaa", "technical_core_hash": "bbb"}), \
         patch("db.get_latest_enrichment_hash", return_value="aaa"):
        assert _should_skip(123, force=False) is True


def test_priority_no_skip_when_hash_differs():
    from run_priority import _should_skip
    with patch("db._is_enabled", return_value=True), \
         patch("db.get_current_hashes", return_value={"thread_hash": "aaa", "technical_core_hash": "bbb"}), \
         patch("db.get_latest_enrichment_hash", return_value="old_hash"):
        assert _should_skip(123, force=False) is False


def test_priority_no_skip_when_force():
    from run_priority import _should_skip
    # force=True should bypass hash check entirely
    assert _should_skip(123, force=True) is False


def test_priority_no_skip_when_no_rollup():
    from run_priority import _should_skip
    with patch("db._is_enabled", return_value=True), \
         patch("db.get_current_hashes", return_value={"thread_hash": None, "technical_core_hash": None}):
        assert _should_skip(123, force=False) is False


# ── Complexity skip logic ────────────────────────────────────────────

def test_complexity_skip_when_hash_matches():
    from run_complexity import _should_skip
    with patch("db._is_enabled", return_value=True), \
         patch("db.get_current_hashes", return_value={"thread_hash": "aaa", "technical_core_hash": "bbb"}), \
         patch("db.get_latest_enrichment_hash", return_value="bbb"):
        assert _should_skip(123, force=False) is True


def test_complexity_no_skip_when_hash_differs():
    from run_complexity import _should_skip
    with patch("db._is_enabled", return_value=True), \
         patch("db.get_current_hashes", return_value={"thread_hash": "aaa", "technical_core_hash": "bbb"}), \
         patch("db.get_latest_enrichment_hash", return_value="old_hash"):
        assert _should_skip(123, force=False) is False


def test_complexity_no_skip_when_force():
    from run_complexity import _should_skip
    assert _should_skip(123, force=True) is False


# ── Sentiment skip logic ────────────────────────────────────────────

def test_sentiment_skip_when_hash_matches():
    from run_sentiment import _should_skip
    with patch("db._is_enabled", return_value=True), \
         patch("db.get_current_hashes", return_value={"thread_hash": "aaa", "technical_core_hash": "bbb"}), \
         patch("db.get_latest_enrichment_hash", return_value="aaa"):
        assert _should_skip(123, force=False) is True


def test_sentiment_no_skip_when_hash_differs():
    from run_sentiment import _should_skip
    with patch("db._is_enabled", return_value=True), \
         patch("db.get_current_hashes", return_value={"thread_hash": "aaa", "technical_core_hash": "bbb"}), \
         patch("db.get_latest_enrichment_hash", return_value="old_hash"):
        assert _should_skip(123, force=False) is False


def test_sentiment_no_skip_when_force():
    from run_sentiment import _should_skip
    assert _should_skip(123, force=True) is False
