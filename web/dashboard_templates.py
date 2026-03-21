"""Opinionated starter templates for runtime dashboards."""

from . import data


TEMPLATES = {
    "executive_summary": {
        "label": "Executive Summary",
        "description": "Top-line KPIs, backlog trend, and current ticket explorer.",
        "sections": [
            {
                "title": "Summary",
                "description": "Top-line ticket metrics",
                "layout_columns": 1,
                "widgets": [
                    {
                        "widget_type": "stat_row",
                        "title": "Open Ticket KPIs",
                        "query_key": "open_ticket_stats",
                        "display_config": {
                            "items": [
                                {"field": "total_open", "title": "Open Tickets", "icon": "tabler:ticket", "color": "blue"},
                                {"field": "high_priority", "title": "High Priority", "icon": "tabler:alert-circle", "color": "red"},
                                {"field": "high_complexity", "title": "High Complexity", "icon": "tabler:stack-2", "color": "orange"},
                                {"field": "frustrated", "title": "Frustrated", "icon": "tabler:mood-angry", "color": "grape"},
                            ]
                        },
                    }
                ],
            },
            {
                "title": "Trends",
                "description": "Trend and aging views",
                "layout_columns": 2,
                "widgets": [
                    {
                        "widget_type": "chart",
                        "title": "Open Backlog Trend",
                        "query_key": "backlog_daily",
                        "display_config": {
                            "chart_type": "line",
                            "x": "snapshot_date",
                            "y": "open_backlog",
                            "title": "Open Backlog Trend",
                        },
                    },
                    {
                        "widget_type": "chart",
                        "title": "Current Aging",
                        "query_key": "backlog_aging",
                        "display_config": {
                            "chart_type": "horizontal_bar",
                            "x": "age_bucket",
                            "y": "ticket_count",
                            "title": "Current Aging",
                        },
                    },
                ],
            },
            {
                "title": "Tickets",
                "description": "Current operational queue",
                "layout_columns": 1,
                "widgets": [
                    {
                        "widget_type": "grid",
                        "title": "Ticket Explorer",
                        "query_key": "ticket_list",
                        "display_config": {
                            "columns": [
                                {"field": "ticket_number", "headerName": "Ticket Number"},
                                {"field": "ticket_name", "headerName": "Ticket Name"},
                                {"field": "status", "headerName": "Status"},
                                {"field": "product_name", "headerName": "Product"},
                                {"field": "priority", "headerName": "Priority"},
                            ]
                        },
                    }
                ],
            },
        ],
    },
    "root_cause_watch": {
        "label": "Root Cause Watch",
        "description": "Coverage and distribution view for root cause analysis.",
        "sections": [
            {
                "title": "Coverage",
                "description": "Pipeline KPI coverage",
                "layout_columns": 1,
                "widgets": [
                    {
                        "widget_type": "stat_row",
                        "title": "Root Cause KPIs",
                        "query_key": "root_cause_stats",
                        "display_config": {
                            "items": [
                                {"field": "pass1_success", "title": "Pass 1", "icon": "tabler:search", "color": "blue"},
                                {"field": "pass3_success", "title": "Pass 2", "icon": "tabler:bulb", "color": "violet"},
                                {"field": "pass4_success", "title": "Pass 3", "icon": "tabler:tools", "color": "teal"},
                                {"field": "distinct_mechanism_classes", "title": "Mechanisms", "icon": "tabler:git-branch", "color": "orange"},
                            ]
                        },
                    }
                ],
            },
            {
                "title": "Distribution",
                "description": "High-level root cause breakdowns",
                "layout_columns": 2,
                "widgets": [
                    {
                        "widget_type": "chart",
                        "title": "Mechanism Class Distribution",
                        "query_key": "mechanism_class_distribution",
                        "display_config": {
                            "chart_type": "bar",
                            "x": "mechanism_class",
                            "y": "ticket_count",
                            "title": "Mechanism Class Distribution",
                        },
                    },
                    {
                        "widget_type": "chart",
                        "title": "Intervention Type Distribution",
                        "query_key": "intervention_type_distribution",
                        "display_config": {
                            "chart_type": "bar",
                            "x": "intervention_type",
                            "y": "ticket_count",
                            "title": "Intervention Type Distribution",
                        },
                    },
                ],
            },
        ],
    },
}


def list_templates():
    return [{"key": key, "label": value["label"], "description": value["description"]} for key, value in TEMPLATES.items()]


def apply_template(dashboard_id, template_key):
    template = TEMPLATES.get(template_key)
    if not template:
        raise ValueError(f"Unknown template '{template_key}'.")
    tree = data.get_dashboard_tree(dashboard_id) or {"sections": []}
    next_section_order = len(tree.get("sections", []))
    for section_offset, section in enumerate(template["sections"]):
        created_section = data.create_dashboard_section(
            dashboard_id=dashboard_id,
            title=section.get("title"),
            description=section.get("description"),
            layout_columns=section.get("layout_columns", 1),
            sort_order=next_section_order + section_offset,
        )
        for widget_offset, widget in enumerate(section.get("widgets", [])):
            data.create_dashboard_widget(
                section_id=created_section["id"],
                widget_type=widget["widget_type"],
                title=widget.get("title"),
                query_key=widget.get("query_key"),
                query_params=widget.get("query_params", {}),
                display_config=widget.get("display_config", {}),
                sort_order=widget_offset,
            )
