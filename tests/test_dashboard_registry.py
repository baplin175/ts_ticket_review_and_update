from unittest.mock import patch

import web.dashboard_registry as dashboard_registry


def test_dashboard_tree_to_definition_maps_query_and_display_config():
    tree = {
        "id": 1,
        "name": "Ops",
        "slug": "ops",
        "description": "Operations view",
        "icon": "tabler:chart-bar",
        "sections": [
            {
                "id": 10,
                "title": "Summary",
                "description": None,
                "layout_columns": 2,
                "widgets": [
                    {
                        "id": 100,
                        "widget_type": "chart",
                        "title": "Backlog",
                        "query_key": "backlog_daily",
                        "query_params_json": {"limit": 10},
                        "display_config_json": {"x": "snapshot_date", "y": "open_backlog"},
                    }
                ],
            }
        ],
    }

    definition = dashboard_registry.dashboard_tree_to_definition(tree)

    widget = definition["sections"][0]["widgets"][0]
    assert widget["query"] == {"key": "backlog_daily", "params": {"limit": 10}}
    assert widget["x"] == "snapshot_date"
    assert widget["y"] == "open_backlog"
    assert definition["route"] == "/dashboards/ops"


def test_build_nav_items_includes_runtime_dashboards():
    pages = [{"label": "Overview", "route": "/", "icon": "tabler:dashboard"}]
    dashboards = [{"name": "Ops", "slug": "ops", "icon": None}]

    with patch.object(dashboard_registry.data, "list_dashboards", return_value=dashboards):
        items = dashboard_registry.build_nav_items(pages)

    assert items[0]["kind"] == "static"
    assert items[1]["kind"] == "dashboard"
    assert items[1]["href"] == "/dashboards/ops"


def test_nav_item_active_handles_static_aliases_and_dashboard_routes():
    static_item = {
        "kind": "static",
        "href": "/",
        "aliases": ["/overview"],
        "match_prefix": None,
    }
    dashboard_item = {
        "kind": "dashboard",
        "href": "/dashboards/ops",
    }

    assert dashboard_registry.nav_item_active(static_item, "/overview") is True
    assert dashboard_registry.nav_item_active(dashboard_item, "/dashboards/ops") is True
