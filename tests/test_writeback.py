"""
Tests for ts_client.py — write-back logic including LastInhComment/LastCustComment.
"""

from ts_client import _last_comment_timestamps


def test_last_comment_timestamps_basic():
    activities = [
        {"party": "cust", "created_at": "2026-01-10T10:00:00Z"},
        {"party": "inh", "created_at": "2026-01-11T12:00:00Z"},
        {"party": "cust", "created_at": "2026-01-12T08:00:00Z"},
        {"party": "inh", "created_at": "2026-01-09T06:00:00Z"},
    ]
    last_inh, last_cust = _last_comment_timestamps(activities)
    assert last_inh == "2026-01-11T12:00:00Z"
    assert last_cust == "2026-01-12T08:00:00Z"


def test_last_comment_timestamps_empty():
    last_inh, last_cust = _last_comment_timestamps([])
    assert last_inh == ""
    assert last_cust == ""


def test_last_comment_timestamps_only_cust():
    activities = [
        {"party": "cust", "created_at": "2026-01-10T10:00:00Z"},
    ]
    last_inh, last_cust = _last_comment_timestamps(activities)
    assert last_inh == ""
    assert last_cust == "2026-01-10T10:00:00Z"


def test_last_comment_timestamps_only_inh():
    activities = [
        {"party": "inh", "created_at": "2026-01-11T12:00:00Z"},
    ]
    last_inh, last_cust = _last_comment_timestamps(activities)
    assert last_inh == "2026-01-11T12:00:00Z"
    assert last_cust == ""


def test_last_comment_timestamps_missing_created_at():
    """Activities without created_at should be safely skipped."""
    activities = [
        {"party": "inh", "created_at": ""},
        {"party": "cust"},
    ]
    last_inh, last_cust = _last_comment_timestamps(activities)
    assert last_inh == ""
    assert last_cust == ""
