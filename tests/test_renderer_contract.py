from unittest.mock import patch

import dash_mantine_components as dmc

import web.renderer as renderer


def test_render_dashboard_renders_canonical_sections():
    definition = {
        "title": "Operations",
        "description": "Shared dashboard",
        "sections": [
            {
                "id": "summary",
                "title": "Summary",
                "widgets": [
                    {
                        "id": "kpis",
                        "type": "stat_row",
                        "query": {"key": "open_ticket_stats", "params": {}},
                        "items": [
                            {"field": "total_open", "title": "Open Tickets", "color": "blue"},
                        ],
                    }
                ],
            }
        ],
    }

    with patch.object(
        renderer.query_catalog,
        "run_query",
        return_value={"total_open": 42},
    ):
        component = renderer.render_dashboard(definition)

    assert isinstance(component, dmc.Stack)
    assert len(component.children) == 3


def test_render_widget_returns_alert_for_invalid_widget():
    component = renderer.render_widget(
        {
            "id": "broken",
            "type": "chart",
            "query": {"key": "backlog_daily", "params": {}},
            "y": "open_backlog",
        }
    )

    assert isinstance(component, dmc.Alert)
    assert component.title == "chart"


def test_render_page_supports_legacy_yaml_shape():
    page_def = {
        "title": "Health",
        "components": [
            {
                "type": "stat_row",
                "query": {"key": "open_ticket_stats", "params": {}},
                "items": [
                    {"field": "total_open", "title": "Open Tickets"},
                ],
            }
        ],
    }

    with patch.object(
        renderer.query_catalog,
        "run_query",
        return_value={"total_open": 7},
    ):
        component = renderer.render_page(page_def)

    assert isinstance(component, dmc.Stack)
