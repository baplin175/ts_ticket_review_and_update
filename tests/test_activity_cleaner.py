"""
Tests for activity_cleaner.py — text cleaning pipeline.
"""

from activity_cleaner import clean_activity, html_to_text, clean_activity_dict


# ── HTML to text ─────────────────────────────────────────────────────

def test_html_to_text_strips_tags():
    result = html_to_text("<p>Hello <b>world</b></p>")
    assert "Hello" in result
    assert "world" in result
    assert "<" not in result


def test_html_to_text_handles_br():
    result = html_to_text("Line 1<br>Line 2")
    assert "Line 1" in result
    assert "Line 2" in result


def test_html_to_text_empty():
    assert html_to_text("") == ""
    assert html_to_text(None) == ""


# ── clean_activity ───────────────────────────────────────────────────

def test_clean_activity_removes_boilerplate():
    text = "Hello, this is a real message.\nCAUTION: This email originated from outside of the organization. Do not click links or open attachments unless you recognize the sender and know the content is safe."
    result = clean_activity(text)
    assert "real message" in result
    assert "CAUTION" not in result


def test_clean_activity_preserves_substance():
    text = "The stored procedure needs to be updated to handle the new field mapping."
    result = clean_activity(text)
    assert "stored procedure" in result


def test_clean_activity_empty():
    assert clean_activity("") == ""
    assert clean_activity(None) == ""


def test_clean_activity_deduplicates_lines():
    text = "Hello there\nHello there\nSomething new"
    result = clean_activity(text)
    assert result.count("Hello there") == 1
    assert "Something new" in result


# ── clean_activity_dict ──────────────────────────────────────────────

def test_clean_activity_dict_extracts_fields(monkeypatch):
    """Verify that clean_activity_dict produces expected keys."""
    # Mock is_inhance_user to avoid API calls
    import activity_cleaner
    import ts_client
    monkeypatch.setattr(ts_client, "_INHANCE_IDS", {"12345"})

    action = {
        "ID": "999",
        "DateCreated": "2026-01-15T10:00:00Z",
        "ActionType": "Comment",
        "CreatorID": "12345",
        "CreatorName": "Support Agent",
        "IsVisibleOnPortal": "True",
        "Description": "Checking the SQL query results.",
    }
    result = clean_activity_dict(action)

    assert result["action_id"] == "999"
    assert result["party"] == "inh"
    assert result["creator_name"] == "Support Agent"
    assert result["is_visible"] is True
    assert "SQL" in result["description"]


def test_clean_activity_dict_customer_party(monkeypatch):
    """Non-inHANCE user should be classified as cust."""
    import ts_client
    monkeypatch.setattr(ts_client, "_INHANCE_IDS", {"12345"})

    action = {
        "ID": "1000",
        "DateCreated": "2026-01-15T10:00:00Z",
        "CreatorID": "99999",
        "CreatorName": "External User",
        "IsVisibleOnPortal": "True",
        "Description": "I need help with my report.",
    }
    result = clean_activity_dict(action)
    assert result["party"] == "cust"
