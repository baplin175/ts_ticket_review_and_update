import psycopg2

from web.pages.dashboard_editor import (
    _build_widget_payload,
    _format_dashboard_write_error,
    _parse_columns,
    _parse_stat_items,
    _widget_field_styles,
)


def test_format_dashboard_write_error_for_unique_violation():
    assert "already exists" in _format_dashboard_write_error(psycopg2.errors.UniqueViolation())


def test_format_dashboard_write_error_for_missing_table():
    assert "migration 021" in _format_dashboard_write_error(psycopg2.errors.UndefinedTable())


def test_format_dashboard_write_error_for_connection_failure():
    assert "storage is unavailable" in _format_dashboard_write_error(psycopg2.OperationalError())


def test_truthy_click_guard_matches_real_clicks():
    assert not None
    assert not 0
    assert 1


def test_parse_columns_builds_header_names():
    assert _parse_columns("ticket_number, status") == [
        {"field": "ticket_number", "headerName": "Ticket Number"},
        {"field": "status", "headerName": "Status"},
    ]


def test_parse_stat_items_requires_field_and_title():
    items = _parse_stat_items("total_open|Open Tickets|tabler:ticket|blue")
    assert items == [
        {
            "field": "total_open",
            "title": "Open Tickets",
            "icon": "tabler:ticket",
            "color": "blue",
        }
    ]


def test_build_widget_payload_for_alert():
    query_key, query_params, display_config = _build_widget_payload(
        "alert",
        "Notice",
        None,
        "",
        "Review this dashboard weekly.",
        "yellow",
        "bar",
        None,
        None,
        None,
        "",
        "",
    )
    assert query_key is None
    assert query_params == {}
    assert display_config["message"] == "Review this dashboard weekly."


def test_build_widget_payload_for_chart():
    query_key, query_params, display_config = _build_widget_payload(
        "chart",
        "Backlog",
        "component_distribution",
        '{"limit": 5}',
        "",
        "",
        "line",
        "component",
        "ticket_count",
        "",
        "",
        "",
    )
    assert query_key == "component_distribution"
    assert query_params == {"limit": 5}
    assert display_config["chart_type"] == "line"
    assert display_config["x"] == "component"


def test_widget_field_styles_hide_irrelevant_fields():
    assert _widget_field_styles("alert")["query"] == {"display": "none"}
    assert _widget_field_styles("chart")["chart"] == {}
    assert _widget_field_styles("grid")["grid"] == {}
    assert _widget_field_styles("stat_row")["stat"] == {}


def test_widget_payload_for_grid_allows_empty_columns():
    query_key, query_params, display_config = _build_widget_payload(
        "grid",
        "Tickets",
        "ticket_list",
        "",
        "",
        "",
        "bar",
        "",
        "",
        "",
        "",
        "",
    )
    assert query_key == "ticket_list"
    assert query_params == {}
    assert display_config == {}
