"""
Tests for action_classifier.py — deterministic action classification.
"""

from action_classifier import classify_action, is_noise, is_technical_substance


# ── system_noise ─────────────────────────────────────────────────────

def test_empty_description_is_system_noise():
    assert classify_action("") == "system_noise"
    assert classify_action(None) == "system_noise"
    assert classify_action("   ") == "system_noise"


def test_is_empty_flag_is_system_noise():
    assert classify_action("some text", is_empty=True) == "system_noise"


# ── administrative_noise ─────────────────────────────────────────────

def test_thanks_only_is_admin_noise():
    assert classify_action("Thanks!") == "administrative_noise"
    assert classify_action("Thank you") == "administrative_noise"


def test_ok_only_is_admin_noise():
    assert classify_action("Ok") == "administrative_noise"
    assert classify_action("Sounds good") == "administrative_noise"


# ── technical_work ───────────────────────────────────────────────────

def test_sql_mention_is_technical():
    assert classify_action("The SQL query is running slowly on the server.") == "technical_work"


def test_stored_proc_is_technical():
    assert classify_action("Updated the stored proc to handle null values.") == "technical_work"


def test_bug_fix_is_technical():
    assert classify_action("Applied a hotfix for the null reference bug.") == "technical_work"


def test_api_mention_is_technical():
    assert classify_action("The REST API endpoint returns HTTP 500.") == "technical_work"


def test_deploy_is_technical():
    assert classify_action("We will deploy the fix to staging for testing.") == "technical_work"


# ── scheduling ───────────────────────────────────────────────────────

def test_meeting_scheduling():
    assert classify_action("Can we schedule a meeting on Monday at 2:00 PM?") == "scheduling"


# ── waiting_on_customer ──────────────────────────────────────────────

def test_waiting_on_customer_from_inh():
    assert classify_action(
        "We are waiting on your file to proceed.",
        party="inh",
    ) == "waiting_on_customer"


def test_waiting_pattern_from_cust_is_not_waiting():
    # "waiting on your file" from a customer is not "waiting_on_customer"
    result = classify_action(
        "We are waiting on your file to proceed.",
        party="cust",
    )
    assert result != "waiting_on_customer"


# ── delivery_confirmation ────────────────────────────────────────────

def test_delivery_confirmation():
    assert classify_action("The fix has been delivered to production.") == "delivery_confirmation"


# ── customer_problem_statement ───────────────────────────────────────

def test_long_customer_text_is_problem_statement():
    text = "We are unable to access our account and our staff cannot see the dashboard properly since last week."
    assert classify_action(text, party="cust") == "customer_problem_statement"


# ── status_update ────────────────────────────────────────────────────

def test_checking_in_is_status_update():
    assert classify_action("Just checking in on the status of this ticket.") == "status_update"


# ── unknown ──────────────────────────────────────────────────────────

def test_short_ambiguous_text_is_unknown():
    assert classify_action("Please advise.") == "unknown"


# ── Noise / substance helpers ────────────────────────────────────────

def test_is_noise():
    assert is_noise("system_noise") is True
    assert is_noise("administrative_noise") is True
    assert is_noise("scheduling") is True
    assert is_noise("technical_work") is False
    assert is_noise("unknown") is False


def test_is_technical_substance():
    assert is_technical_substance("technical_work") is True
    assert is_technical_substance("customer_problem_statement") is True
    assert is_technical_substance("delivery_confirmation") is True
    assert is_technical_substance("status_update") is False
    assert is_technical_substance("system_noise") is False
