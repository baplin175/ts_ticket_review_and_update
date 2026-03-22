from datetime import date

from rollups.customer_health import SCORE_FORMULA_VERSION, build_customer_health_model, health_band


def test_build_customer_health_model_creates_snapshot_and_contributors():
    rows = [
        {
            "customer": "Acme",
            "ticket_id": 1,
            "ticket_number": "1001",
            "ticket_name": "Billing issue",
            "product_name": "Billing",
            "status": "Working",
            "severity": "1 - High",
            "assignee": "Engineer",
            "date_modified": "2026-03-21T00:00:00",
            "days_opened": 95,
            "days_since_modified": 15,
            "open_flag": True,
            "priority": 2,
            "overall_complexity": 4,
            "frustrated": "Yes",
            "customer_message_count": 6,
            "handoff_count": 3,
            "cluster_id": "configuration_mismatch",
            "mechanism_class": "configuration_mismatch",
            "intervention_type": "configuration_change",
            "component": "Billing",
        },
        {
            "customer": "Acme",
            "ticket_id": 2,
            "ticket_number": "1002",
            "ticket_name": "Print issue",
            "product_name": "Receipts",
            "status": "Working",
            "severity": "2 - Medium",
            "assignee": "Engineer",
            "date_modified": "2026-03-21T00:00:00",
            "days_opened": 10,
            "days_since_modified": 3,
            "open_flag": True,
            "priority": 4,
            "overall_complexity": 2,
            "frustrated": "No",
            "customer_message_count": 1,
            "handoff_count": 0,
            "cluster_id": "configuration_mismatch",
            "mechanism_class": "configuration_mismatch",
            "intervention_type": "configuration_change",
            "component": "Printing",
        },
    ]

    snapshots, contributors = build_customer_health_model(rows, date(2026, 3, 21))

    assert len(snapshots) == 1
    assert len(contributors) == 2

    snapshot = snapshots[0]
    assert snapshot["customer"] == "Acme"
    assert snapshot["score_formula_version"] == SCORE_FORMULA_VERSION
    assert snapshot["pressure_score"] == round(sum(item["pressure_contribution"] for item in contributors), 2)
    assert snapshot["aging_score"] == round(sum(item["aging_contribution"] for item in contributors), 2)
    assert snapshot["customer_health_score"] == round(
        snapshot["pressure_score"]
        + snapshot["aging_score"]
        + snapshot["friction_score"]
        + snapshot["concentration_score"]
        + snapshot["breadth_score"],
        2,
    )
    assert snapshot["top_cluster_ids"] == ["configuration_mismatch"]
    assert sorted(snapshot["top_products"]) == ["Billing", "Receipts"]


def test_contributors_capture_factor_breakdown():
    rows = [
        {
            "customer": "Acme",
            "ticket_id": 1,
            "ticket_number": "1001",
            "ticket_name": "Billing issue",
            "product_name": "Billing",
            "status": "Working",
            "severity": "1 - High",
            "assignee": "Engineer",
            "date_modified": "2026-03-21T00:00:00",
            "days_opened": 95,
            "days_since_modified": 35,
            "open_flag": True,
            "priority": 2,
            "overall_complexity": 4,
            "frustrated": "Yes",
            "customer_message_count": 10,
            "handoff_count": 6,
            "cluster_id": "state_inconsistency",
            "mechanism_class": "state_inconsistency",
            "intervention_type": "software_fix",
            "component": "UI",
        }
    ]

    snapshots, contributors = build_customer_health_model(rows, date(2026, 3, 21))
    contributor = contributors[0]

    assert contributor["pressure_contribution"] == 7.5
    assert contributor["aging_contribution"] == 4.5
    assert contributor["friction_contribution"] == 3.5
    assert contributor["concentration_contribution"] == 0.0
    assert contributor["total_contribution"] == 15.5
    assert snapshots[0]["customer_health_band"] == health_band(snapshots[0]["customer_health_score"])


def test_health_band_thresholds():
    assert health_band(10) == "healthy"
    assert health_band(20) == "watch"
    assert health_band(40) == "at_risk"
    assert health_band(60) == "critical"
