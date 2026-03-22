from unittest.mock import MagicMock, patch

import psycopg2

import web.data as data


def test_create_dashboard_uses_returning_insert():
    with patch.object(data, "_execute_returning", return_value={"id": 1, "name": "Ops"}) as mocked:
        result = data.create_dashboard("Ops", "ops")
    assert result["id"] == 1
    sql = mocked.call_args[0][0]
    assert "INSERT INTO dashboards" in sql
    assert "RETURNING" in sql


def test_list_dashboards_filters_for_global_active():
    with patch.object(data, "query", return_value=[]) as mocked:
        data.list_dashboards()
    sql = mocked.call_args[0][0]
    params = mocked.call_args[0][1]
    assert "FROM dashboards" in sql
    assert "owner_type = %s" in sql
    assert "owner_id IS NULL" in sql
    assert "is_active = TRUE" in sql
    assert params == ("global",)


def test_get_dashboard_tree_assembles_sections_and_widgets():
    dashboard = {"id": 10, "name": "Ops"}
    sections = [
        {"id": 100, "dashboard_id": 10, "title": "Summary"},
        {"id": 101, "dashboard_id": 10, "title": "Details"},
    ]
    widgets = [
        {"id": 1000, "section_id": 100, "widget_type": "stat_row"},
        {"id": 1001, "section_id": 101, "widget_type": "grid"},
    ]

    with patch.object(data, "query_one", return_value=dashboard), \
         patch.object(data, "query", side_effect=[sections, widgets]):
        result = data.get_dashboard_tree(10)

    assert result["sections"][0]["widgets"] == [widgets[0]]
    assert result["sections"][1]["widgets"] == [widgets[1]]


def test_delete_dashboard_widget_delegates_to_execute():
    with patch.object(data, "_execute") as mocked:
        data.delete_dashboard_widget(123)
    mocked.assert_called_once()
    assert "DELETE FROM dashboard_widgets" in mocked.call_args[0][0]


def test_list_dashboards_returns_empty_when_tables_are_missing():
    with patch.object(data, "query", side_effect=psycopg2.errors.UndefinedTable()):
        result = data.list_dashboards()
    assert result == []


def test_get_dashboard_by_slug_returns_none_when_tables_are_missing():
    with patch.object(data, "query_one", side_effect=psycopg2.errors.UndefinedTable()):
        result = data.get_dashboard_by_slug("ops")
    assert result is None


def test_get_dashboard_tree_returns_none_when_tables_are_missing():
    with patch.object(data, "query_one", side_effect=psycopg2.errors.UndefinedTable()):
        result = data.get_dashboard_tree(1)
    assert result is None


def test_list_dashboards_returns_empty_when_db_is_unavailable():
    with patch.object(data, "query", side_effect=psycopg2.OperationalError()):
        result = data.list_dashboards()
    assert result == []


def test_get_dashboard_by_slug_returns_none_when_db_is_unavailable():
    with patch.object(data, "query_one", side_effect=psycopg2.OperationalError()):
        result = data.get_dashboard_by_slug("ops")
    assert result is None


def test_root_cause_ticket_list_uses_latest_cluster_view():
    with patch.object(data, "query", return_value=[]) as mocked:
        data.get_root_cause_tickets()
    sql = mocked.call_args[0][0]
    assert "FROM vw_latest_mechanism_ticket_clusters tc" in sql
    assert "latest_p4" in sql


def test_root_cause_mechanism_distribution_uses_latest_catalog_view():
    with patch.object(data, "query", return_value=[]) as mocked:
        data.get_mechanism_class_distribution()
    sql = mocked.call_args[0][0]
    assert "FROM vw_latest_mechanism_cluster_catalog" in sql
    assert "cluster_id AS mechanism_class" in sql


def test_root_cause_cluster_catalog_uses_latest_catalog_view():
    with patch.object(data, "query", return_value=[]) as mocked:
        data.get_root_cause_cluster_catalog()
    sql = mocked.call_args[0][0]
    assert "FROM vw_latest_mechanism_cluster_catalog" in sql
    assert "subclusters" in sql


def test_root_cause_fix_drilldown_uses_latest_cluster_view():
    with patch.object(data, "query", return_value=[]) as mocked:
        data.get_tickets_by_fixes([("configuration_mismatch", "configuration_change")])
    sql = mocked.call_args[0][0]
    params = mocked.call_args[0][1]
    assert "FROM vw_latest_mechanism_ticket_clusters tc" in sql
    assert "latest_p4" in sql
    assert params == ("configuration_mismatch", "configuration_change")


def test_customer_health_history_uses_model_version_and_orders_by_date():
    with patch.object(data, "query", return_value=[]) as mocked:
        data.get_customer_health_history("Acme", ["Customer Support (CS)"])
    sql = mocked.call_args[0][0]
    params = mocked.call_args[0][1]
    assert "FROM customer_health_ticket_contributors" in sql
    assert "COALESCE(group_name, '') IN (%s)" in sql
    assert "score_formula_version = %s" in sql
    assert "ORDER BY as_of_date" in sql
    assert params == ("Acme", "v1", "Customer Support (CS)")


def test_customer_health_contributors_orders_by_total_contribution():
    with patch.object(data, "query", return_value=[]) as mocked:
        data.get_customer_health_contributors("Acme", "2026-03-21", ["Customer Support (CS)"])
    sql = mocked.call_args[0][0]
    params = mocked.call_args[0][1]
    assert "FROM customer_health_ticket_contributors" in sql
    assert "COALESCE(group_name, '') IN (%s)" in sql
    assert "score_formula_version = %s" in sql
    assert "ORDER BY total_contribution DESC" in sql
    assert params == ("Acme", "2026-03-21", "v1", "Customer Support (CS)")


def test_customer_health_excludes_marketing_and_sales():
    with patch.object(data, "query", return_value=[]) as mocked:
        data.get_customer_health()
    sql = mocked.call_args[0][0]
    params = mocked.call_args[0][1]
    assert "COALESCE(group_name, '') NOT IN (%s,%s)" in sql
    assert params == ("v1", "v1", "Marketing", "Sales (S)", "v1")


def test_product_health_excludes_marketing_and_sales():
    with patch.object(data, "query", return_value=[]) as mocked:
        data.get_product_health()
    sql = mocked.call_args[0][0]
    params = mocked.call_args[0][1]
    assert "COALESCE(group_name, '') NOT IN (%s,%s)" in sql
    assert params == ("Marketing", "Sales (S)")


def test_get_customer_health_explanations_orders_newest_first():
    with patch.object(data, "query", return_value=[]) as mocked:
        data.get_customer_health_explanations("Acme")
    sql = mocked.call_args[0][0]
    params = mocked.call_args[0][1]
    assert "FROM customer_health_explanations" in sql
    assert "ORDER BY created_at DESC" in sql
    assert params == ("Acme",)
