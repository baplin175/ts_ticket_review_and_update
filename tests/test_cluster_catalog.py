import pandas as pd

import build_cluster_catalog as cluster_catalog


def test_filter_success_keeps_only_successful_pass4_rows():
    df = pd.DataFrame(
        [
            {"ticket_id": 1, "pass_4": "success", "mechanism_class": "alpha"},
            {"ticket_id": 2, "pass_4": "failed", "mechanism_class": "alpha"},
            {"ticket_id": 3, "pass_4": "success", "mechanism_class": None},
        ]
    )

    result = cluster_catalog.filter_success(df)

    assert result["ticket_id"].tolist() == [1]


def test_compute_dominant_is_frequency_based_and_deterministic_on_ties():
    df = pd.DataFrame(
        [
            {"cluster_id": "alpha", "component": "meter"},
            {"cluster_id": "alpha", "component": "cloud"},
            {"cluster_id": "alpha", "component": "cloud"},
            {"cluster_id": "beta", "component": "zebra"},
            {"cluster_id": "beta", "component": "apple"},
        ]
    )

    result = cluster_catalog.compute_dominant(df, "cluster_id", "component")

    assert result["alpha"] == "cloud"
    assert result["beta"] == "apple"


def test_build_cluster_catalog_and_mapping_produce_expected_outputs():
    df = pd.DataFrame(
        [
            {
                "ticket_id": 10,
                "product": "Prod A",
                "customer": "Cust 1",
                "component": "Meter",
                "operation": "Create",
                "mechanism": "The tab state is stale.",
                "pass_4": "success",
                "mechanism_class": "state_inconsistency",
                "intervention_type": "software_fix",
                "created_at": "2026-03-01T00:00:00Z",
            },
            {
                "ticket_id": 11,
                "product": "Prod A",
                "customer": "Cust 2",
                "component": "Meter",
                "operation": "Create",
                "mechanism": "Initialization logic keeps the wrong view active.",
                "pass_4": "success",
                "mechanism_class": "state_inconsistency",
                "intervention_type": "software_fix",
                "created_at": "2026-03-02T00:00:00Z",
            },
            {
                "ticket_id": 12,
                "product": "Prod B",
                "customer": "Cust 1",
                "component": "Print",
                "operation": "Persist",
                "mechanism": "Wrong API mapping drops page range state.",
                "pass_4": "success",
                "mechanism_class": "integration_mapping_error",
                "intervention_type": "software_fix",
                "created_at": "2026-03-03T00:00:00Z",
            },
        ]
    )

    successful = cluster_catalog.filter_success(df)
    catalog = cluster_catalog.build_cluster_catalog(successful)
    mapping = cluster_catalog.build_ticket_mapping(successful)

    first = catalog.iloc[0].to_dict()
    assert first["cluster_id"] == "state_inconsistency"
    assert first["ticket_count"] == 2
    assert first["customer_count"] == 2
    assert first["product_count"] == 1
    assert first["dominant_component"] == "Meter"
    assert first["dominant_operation"] == "Create"
    assert first["dominant_intervention_type"] == "software_fix"
    assert first["example_ticket_ids"] == [10, 11]
    assert len(first["example_mechanisms"]) == 2
    assert first["subclusters"][0]["component"] == "Meter"
    assert first["subclusters"][0]["operation"] == "Create"

    assert mapping.to_dict(orient="records") == [
        {
            "ticket_id": 12,
            "cluster_id": "integration_mapping_error",
            "component": "Print",
            "operation": "Persist",
            "intervention_type": "software_fix",
        },
        {
            "ticket_id": 10,
            "cluster_id": "state_inconsistency",
            "component": "Meter",
            "operation": "Create",
            "intervention_type": "software_fix",
        },
        {
            "ticket_id": 11,
            "cluster_id": "state_inconsistency",
            "component": "Meter",
            "operation": "Create",
            "intervention_type": "software_fix",
        },
    ]


def test_generate_llm_label_input_returns_top_3_subclusters():
    payload = cluster_catalog.generate_llm_label_input(
        {
            "cluster_id": "state_inconsistency",
            "dominant_component": "Meter",
            "dominant_operation": "Create",
            "example_mechanisms": ["m1", "m2"],
            "subclusters": [{"component": str(i), "operation": "op"} for i in range(5)],
        }
    )

    assert payload == {
        "mechanism_class": "state_inconsistency",
        "dominant_component": "Meter",
        "dominant_operation": "Create",
        "example_mechanisms": ["m1", "m2"],
        "top_3_subclusters": [
            {"component": "0", "operation": "op"},
            {"component": "1", "operation": "op"},
            {"component": "2", "operation": "op"},
        ],
    }


def test_db_record_serializers_preserve_catalog_and_mapping_payloads():
    catalog = pd.DataFrame(
        [
            {
                "cluster_id": "state_inconsistency",
                "ticket_count": 2,
                "percent_of_total": 0.5,
                "customer_count": 2,
                "product_count": 1,
                "dominant_component": "Meter",
                "dominant_operation": "Create",
                "dominant_intervention_type": "software_fix",
                "example_ticket_ids": [10, 11],
                "example_mechanisms": ["m1", "m2"],
                "subclusters": [{"component": "Meter", "operation": "Create", "ticket_count": 2, "percent_within_cluster": 1.0}],
            }
        ]
    )
    mapping = pd.DataFrame(
        [
            {
                "ticket_id": 10,
                "cluster_id": "state_inconsistency",
                "component": "Meter",
                "operation": "Create",
                "intervention_type": "software_fix",
            }
        ]
    )

    catalog_records = cluster_catalog.cluster_catalog_records_for_db(catalog, "run-123")
    mapping_records = cluster_catalog.ticket_cluster_records_for_db(mapping, "run-123")

    assert catalog_records == [
        {
            "cluster_run_id": "run-123",
            "cluster_id": "state_inconsistency",
            "cluster_label": "State Inconsistency",
            "cluster_description": "Deterministic mechanism-class cluster for state_inconsistency derived from successful pass 4 results.",
            "representative_tickets": "[10, 11]",
            "common_issue_pattern": "Meter / Create",
            "common_mechanism_pattern": "m1",
            "suggested_intervention_type": "software_fix",
            "ticket_count": 2,
            "percent_of_total": 0.5,
            "customer_count": 2,
            "product_count": 1,
            "dominant_component": "Meter",
            "dominant_operation": "Create",
            "dominant_intervention_type": "software_fix",
            "example_ticket_ids": "[10, 11]",
            "example_mechanisms": "[\"m1\", \"m2\"]",
            "subclusters": "[{\"component\": \"Meter\", \"operation\": \"Create\", \"ticket_count\": 2, \"percent_within_cluster\": 1.0}]",
        }
    ]
    assert mapping_records == [
        {
            "ticket_id": 10,
            "cluster_run_id": "run-123",
            "cluster_id": "state_inconsistency",
            "cluster_label": "State Inconsistency",
            "cluster_confidence": 1.0,
            "cluster_method": "mechanism_class_catalog",
        }
    ]
