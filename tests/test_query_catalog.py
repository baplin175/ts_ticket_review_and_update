from unittest.mock import MagicMock, patch

import pytest

import web.query_catalog as query_catalog


def test_list_queries_returns_sorted_metadata():
    queries = query_catalog.list_queries()
    labels = [item["label"] for item in queries]
    assert labels == sorted(labels)
    assert any(item["key"] == "open_ticket_stats" for item in queries)


def test_unknown_query_rejected():
    with pytest.raises(ValueError, match="Unknown dashboard query"):
        query_catalog.get_query_definition("does_not_exist")


def test_query_widget_type_compatibility_enforced():
    with pytest.raises(ValueError, match="cannot be used with widget type"):
        query_catalog.validate_widget_query("ticket_list", "stat_row")


def test_unknown_parameter_rejected():
    with pytest.raises(ValueError, match="Unknown parameters"):
        query_catalog.validate_widget_query(
            "component_distribution",
            "chart",
            {"nope": 1},
        )


def test_missing_required_params_not_needed_when_defaults_exist():
    normalized = query_catalog.validate_widget_query("component_distribution", "chart")
    assert normalized == {"limit": 20}


def test_integer_params_are_coerced_and_bounded():
    normalized = query_catalog.validate_widget_query(
        "component_distribution",
        "chart",
        {"limit": "15"},
    )
    assert normalized == {"limit": 15}

    with pytest.raises(ValueError, match="must be <="):
        query_catalog.validate_widget_query(
            "component_distribution",
            "chart",
            {"limit": 500},
        )


def test_run_query_executes_catalog_handler_with_normalized_params():
    original = query_catalog.QUERY_CATALOG["component_distribution"]
    mocked = MagicMock(return_value=[])
    patched = query_catalog.QueryDefinition(
        key=original.key,
        label=original.label,
        description=original.description,
        handler=mocked,
        result_kind=original.result_kind,
        allowed_widget_types=original.allowed_widget_types,
        params=original.params,
    )
    with patch.dict(query_catalog.QUERY_CATALOG, {"component_distribution": patched}):
        result = query_catalog.run_query("component_distribution", "chart", {"limit": "12"})
        assert result == []
        mocked.assert_called_once_with(limit=12)
