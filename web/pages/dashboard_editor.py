"""Dashboard editor page for DB-backed runtime dashboards."""

import json
import re
from urllib.parse import parse_qs

import dash
import dash_mantine_components as dmc
from dash import Input, Output, State, dcc, html, no_update
from dash_iconify import DashIconify
import psycopg2

from .. import dashboard_registry
from .. import dashboard_templates
from .. import data
from .. import query_catalog
from .. import renderer


_SLUG_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
_WIDGET_TYPE_OPTIONS = [
    {"label": "Alert", "value": "alert"},
    {"label": "Grid", "value": "grid"},
    {"label": "Chart", "value": "chart"},
    {"label": "Stat Row", "value": "stat_row"},
]
_CHART_TYPE_OPTIONS = [
    {"label": "Bar", "value": "bar"},
    {"label": "Stacked Bar", "value": "stacked_bar"},
    {"label": "Line", "value": "line"},
    {"label": "Horizontal Bar", "value": "horizontal_bar"},
    {"label": "Area", "value": "area"},
]


def _format_dashboard_write_error(exc):
    if isinstance(exc, psycopg2.errors.UniqueViolation):
        return "A dashboard with that slug already exists."
    if isinstance(exc, psycopg2.errors.UndefinedTable):
        return "Dashboard tables are not installed yet. Apply migration 021 first."
    if isinstance(exc, psycopg2.OperationalError):
        return "Dashboard storage is unavailable right now. Check the database connection."
    cause = getattr(exc, "__cause__", None)
    if isinstance(cause, psycopg2.errors.UniqueViolation):
        return "A dashboard with that slug already exists."
    if isinstance(cause, psycopg2.errors.UndefinedTable):
        return "Dashboard tables are not installed yet. Apply migration 021 first."
    if isinstance(cause, psycopg2.OperationalError):
        return "Dashboard storage is unavailable right now. Check the database connection."
    return f"Unable to save dashboard: {exc}"


def _safe_int(value, default=0):
    if value in (None, ""):
        return default
    return int(value)


def _parse_json_object(value, field_name):
    if not value:
        return {}
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise ValueError(f"{field_name} must be a JSON object.")
    return parsed


def _parse_columns(value):
    if not value:
        return []
    columns = []
    for raw in value.split(","):
        field = raw.strip()
        if not field:
            continue
        columns.append(
            {
                "field": field,
                "headerName": field.replace("_", " ").title(),
            }
        )
    return columns


def _parse_stat_items(value):
    items = []
    if not value:
        raise ValueError("Stat row items are required.")
    for line in value.splitlines():
        line = line.strip()
        if not line:
            continue
        parts = [part.strip() for part in line.split("|")]
        if len(parts) < 2:
            raise ValueError("Each stat item line must be: field|title|icon|color")
        item = {"field": parts[0], "title": parts[1]}
        if len(parts) >= 3 and parts[2]:
            item["icon"] = parts[2]
        if len(parts) >= 4 and parts[3]:
            item["color"] = parts[3]
        items.append(item)
    if not items:
        raise ValueError("Stat row items are required.")
    return items


def _build_widget_payload(
    widget_type,
    title,
    query_key,
    query_params_text,
    alert_message,
    alert_color,
    chart_type,
    x_field,
    y_field,
    color_field,
    grid_columns_text,
    stat_items_text,
):
    widget_type = widget_type or "grid"
    query_params = _parse_json_object(query_params_text, "Query params")
    display_config = {}
    normalized_title = (title or "").strip() or None

    if widget_type == "alert":
        display_config["message"] = (alert_message or "").strip()
        if not display_config["message"]:
            raise ValueError("Alert message is required.")
        if alert_color:
            display_config["color"] = alert_color.strip()
        return None, {}, display_config

    if not query_key:
        raise ValueError("Query is required for this widget type.")
    query_catalog.validate_widget_query(query_key, widget_type, query_params)

    if widget_type == "chart":
        if not x_field or not y_field:
            raise ValueError("Chart widgets require x and y fields.")
        display_config.update(
            {
                "chart_type": chart_type or "bar",
                "x": x_field.strip(),
                "y": y_field.strip(),
            }
        )
        if normalized_title:
            display_config["title"] = normalized_title
        if color_field:
            display_config["color"] = color_field.strip()
    elif widget_type == "grid":
        columns = _parse_columns(grid_columns_text)
        if columns:
            display_config["columns"] = columns
    elif widget_type == "stat_row":
        display_config["items"] = _parse_stat_items(stat_items_text)
    else:
        raise ValueError(f"Unsupported widget type '{widget_type}'.")

    return query_key, query_params, display_config


def _query_options_for_widget(widget_type):
    widget_type = widget_type or "grid"
    options = []
    for query in query_catalog.list_queries():
        if widget_type in query["allowed_widget_types"]:
            options.append({"label": query["label"], "value": query["key"]})
    return options


def _widget_field_styles(widget_type):
    widget_type = widget_type or "grid"
    return {
        "query": {"display": "none"} if widget_type == "alert" else {},
        "chart": {} if widget_type == "chart" else {"display": "none"},
        "grid": {} if widget_type == "grid" else {"display": "none"},
        "alert": {} if widget_type == "alert" else {"display": "none"},
        "stat": {} if widget_type == "stat_row" else {"display": "none"},
    }


def _increment_refresh(current_refresh):
    return (current_refresh or 0) + 1


def _dashboard_card(dashboard):
    return dmc.Paper(
        [
            dmc.Group(
                [
                    dmc.Stack(
                        [
                            dmc.Text(dashboard["name"], fw=600),
                            dmc.Text(f"/dashboards/{dashboard['slug']}", size="sm", c="dimmed"),
                        ],
                        gap=2,
                    ),
                    dmc.Group(
                        [
                            dmc.Button(
                                "Edit",
                                id={"type": "dashboard-edit-btn", "index": dashboard["id"]},
                                variant="light",
                                size="compact-sm",
                            ),
                            dmc.ActionIcon(
                                DashIconify(icon="tabler:trash", width=16),
                                id={"type": "dashboard-delete-btn", "index": dashboard["id"]},
                                color="red",
                                variant="light",
                            ),
                        ],
                        gap="xs",
                    ),
                ],
                justify="space-between",
            ),
            dmc.Text(dashboard.get("description") or "No description", size="sm", c="dimmed", mt="xs"),
        ],
        withBorder=True,
        radius="md",
        p="md",
    )


def _widget_summary(widget):
    bits = [widget["widget_type"]]
    if widget.get("query_key"):
        bits.append(widget["query_key"])
    return " | ".join(bits)


def _section_editor_card(section, query_options):
    widgets = section.get("widgets", [])
    styles = _widget_field_styles("grid")
    return dmc.Paper(
        [
            dmc.Group(
                [
                    dmc.Stack(
                        [
                            dmc.Text(section.get("title") or "Untitled Section", fw=600),
                            dmc.Text(
                                section.get("description") or f"{len(widgets)} widget(s)",
                                size="sm",
                                c="dimmed",
                            ),
                        ],
                        gap=2,
                    ),
                    dmc.Group(
                        [
                            dmc.ActionIcon(
                                DashIconify(icon="tabler:arrow-up", width=16),
                                id={"type": "move-section-up-btn", "index": section["id"]},
                                variant="light",
                            ),
                            dmc.ActionIcon(
                                DashIconify(icon="tabler:arrow-down", width=16),
                                id={"type": "move-section-down-btn", "index": section["id"]},
                                variant="light",
                            ),
                            dmc.ActionIcon(
                                DashIconify(icon="tabler:trash", width=16),
                                id={"type": "delete-section-btn", "index": section["id"]},
                                color="red",
                                variant="light",
                            ),
                        ],
                        gap=4,
                    ),
                ],
                justify="space-between",
                mb="sm",
            ),
            dmc.Stack(
                [
                    dmc.Paper(
                        [
                            dmc.Group(
                                [
                                    dmc.Stack(
                                        [
                                            dmc.Text(widget.get("title") or "Untitled Widget", fw=500),
                                            dmc.Text(_widget_summary(widget), size="xs", c="dimmed"),
                                        ],
                                        gap=1,
                                    ),
                                    dmc.Group(
                                        [
                                            dmc.ActionIcon(
                                                DashIconify(icon="tabler:arrow-up", width=16),
                                                id={"type": "move-widget-up-btn", "index": widget["id"]},
                                                variant="subtle",
                                            ),
                                            dmc.ActionIcon(
                                                DashIconify(icon="tabler:arrow-down", width=16),
                                                id={"type": "move-widget-down-btn", "index": widget["id"]},
                                                variant="subtle",
                                            ),
                                            dmc.ActionIcon(
                                                DashIconify(icon="tabler:trash", width=16),
                                                id={"type": "delete-widget-btn", "index": widget["id"]},
                                                color="red",
                                                variant="subtle",
                                            ),
                                        ],
                                        gap=4,
                                    ),
                                ],
                                justify="space-between",
                            ),
                        ],
                        withBorder=True,
                        p="sm",
                        radius="sm",
                    )
                    for widget in widgets
                ] or [dmc.Text("No widgets yet.", c="dimmed", size="sm")],
                gap="xs",
            ),
            dmc.Divider(my="md"),
            dmc.Title("Add Widget", order=5, mb="sm"),
            dmc.SimpleGrid(
                cols={"base": 1, "md": 2},
                children=[
                    dmc.Select(
                        id={"type": "widget-type-input", "index": section["id"]},
                        label="Widget Type",
                        data=_WIDGET_TYPE_OPTIONS,
                        value="grid",
                        allowDeselect=False,
                    ),
                    dmc.TextInput(
                        id={"type": "widget-title-input", "index": section["id"]},
                        label="Widget Title",
                        placeholder="Open Tickets",
                    ),
                    dmc.Select(
                        id={"type": "widget-query-input", "index": section["id"]},
                        label="Query",
                        data=query_options,
                        searchable=True,
                        style=styles["query"],
                    ),
                    dmc.TextInput(
                        id={"type": "widget-query-params-input", "index": section["id"]},
                        label="Query Params JSON",
                        placeholder='{"limit": 10}',
                        style=styles["query"],
                    ),
                    dmc.Select(
                        id={"type": "widget-chart-type-input", "index": section["id"]},
                        label="Chart Type",
                        data=_CHART_TYPE_OPTIONS,
                        value="bar",
                        allowDeselect=False,
                        style=styles["chart"],
                    ),
                    dmc.TextInput(
                        id={"type": "widget-x-input", "index": section["id"]},
                        label="Chart X Field",
                        placeholder="snapshot_date",
                        style=styles["chart"],
                    ),
                    dmc.TextInput(
                        id={"type": "widget-y-input", "index": section["id"]},
                        label="Chart Y Field",
                        placeholder="open_backlog",
                        style=styles["chart"],
                    ),
                    dmc.TextInput(
                        id={"type": "widget-color-input", "index": section["id"]},
                        label="Chart Color Group Field",
                        placeholder="severity_tier",
                        style=styles["chart"],
                    ),
                    dmc.TextInput(
                        id={"type": "widget-columns-input", "index": section["id"]},
                        label="Grid Columns",
                        placeholder="ticket_number, ticket_name, status",
                        style=styles["grid"],
                    ),
                    dmc.TextInput(
                        id={"type": "widget-alert-message-input", "index": section["id"]},
                        label="Alert Message",
                        placeholder="Use this dashboard for weekly review.",
                        style=styles["alert"],
                    ),
                    dmc.TextInput(
                        id={"type": "widget-alert-color-input", "index": section["id"]},
                        label="Alert Color",
                        placeholder="blue",
                        style=styles["alert"],
                    ),
                    dmc.Textarea(
                        id={"type": "widget-stat-items-input", "index": section["id"]},
                        label="Stat Row Items",
                        placeholder="total_open|Open Tickets|tabler:ticket|blue",
                        autosize=True,
                        minRows=3,
                        style=styles["stat"],
                    ),
                ],
            ),
            dmc.Button(
                "Add Widget",
                id={"type": "add-widget-btn", "index": section["id"]},
                leftSection=DashIconify(icon="tabler:plus", width=16),
                mt="md",
            ),
        ],
        withBorder=True,
        p="md",
        radius="md",
    )


def _editor_panel(tree):
    query_options = [{"label": q["label"], "value": q["key"]} for q in query_catalog.list_queries()]
    templates = dashboard_templates.list_templates()
    return dmc.Stack(
        [
            dmc.Group(
                [
                    dmc.Stack(
                        [
                            dmc.Title(f"Edit: {tree['name']}", order=4),
                            dmc.Text(tree.get("description") or tree["slug"], c="dimmed", size="sm"),
                        ],
                        gap=2,
                    ),
                    dmc.Badge(f"{len(tree.get('sections', []))} sections", variant="light"),
                ],
                justify="space-between",
            ),
            dmc.Paper(
                [
                    dmc.Title("Dashboard Settings", order=5, mb="sm"),
                    dmc.SimpleGrid(
                        cols={"base": 1, "md": 2},
                        children=[
                            dmc.TextInput(id="dashboard-edit-name-input", label="Name", value=tree["name"]),
                            dmc.TextInput(id="dashboard-edit-slug-input", label="Slug", value=tree["slug"]),
                            dmc.TextInput(id="dashboard-edit-icon-input", label="Icon", value=tree.get("icon") or ""),
                            dmc.TextInput(
                                id="dashboard-edit-description-input",
                                label="Description",
                                value=tree.get("description") or "",
                            ),
                        ],
                    ),
                    dmc.Button(
                        "Save Dashboard",
                        id="save-dashboard-metadata-btn",
                        leftSection=DashIconify(icon="tabler:device-floppy", width=16),
                        mt="md",
                    ),
                ],
                withBorder=True,
                p="md",
                radius="md",
            ),
            dmc.Paper(
                [
                    dmc.Title("Starter Templates", order=5, mb="sm"),
                    dmc.Group(
                        [
                            dmc.Button(
                                template["label"],
                                id={"type": "apply-template-btn", "index": template["key"]},
                                variant="light",
                            )
                            for template in templates
                        ],
                        gap="sm",
                    ),
                    dmc.Text(
                        "Templates append starter sections and widgets to the selected dashboard.",
                        c="dimmed",
                        size="sm",
                        mt="sm",
                    ),
                ],
                withBorder=True,
                p="md",
                radius="md",
            ),
            dmc.Paper(
                [
                    dmc.Title("Add Section", order=5, mb="sm"),
                    dmc.SimpleGrid(
                        cols={"base": 1, "md": 3},
                        children=[
                            dmc.TextInput(id="section-title-input", label="Section Title", placeholder="Summary"),
                            dmc.TextInput(id="section-description-input", label="Description", placeholder="Top-line metrics"),
                            dmc.NumberInput(
                                id="section-columns-input",
                                label="Layout Columns",
                                value=1,
                                min=1,
                                max=4,
                                step=1,
                            ),
                        ],
                    ),
                    dmc.Button(
                        "Add Section",
                        id="add-section-btn",
                        leftSection=DashIconify(icon="tabler:plus", width=16),
                        mt="md",
                    ),
                ],
                withBorder=True,
                p="md",
                radius="md",
            ),
            dmc.Stack(
                [_section_editor_card(section, query_options) for section in tree.get("sections", [])]
                or [dmc.Text("No sections yet. Add one to start building the dashboard.", c="dimmed")],
                gap="md",
            ),
        ],
        gap="md",
    )


def dashboard_editor_layout():
    dashboards = data.list_dashboards(include_inactive=True)
    queries = query_catalog.list_queries()
    return dmc.Stack(
        [
            dmc.Group(
                [
                    dmc.Stack(
                        [
                            dmc.Title("Dashboard Editor", order=2),
                            dmc.Text(
                                "Manage shared runtime dashboards. Add sections, then add widgets backed by the curated query catalogue.",
                                c="dimmed",
                                size="sm",
                            ),
                        ],
                        gap=2,
                    ),
                ],
                justify="space-between",
            ),
            dmc.Paper(
                [
                    dmc.Title("Create Dashboard", order=4, mb="sm"),
                    dmc.SimpleGrid(
                        cols={"base": 1, "md": 2},
                        children=[
                            dmc.TextInput(id="dashboard-name-input", label="Name", placeholder="Operations"),
                            dmc.TextInput(id="dashboard-slug-input", label="Slug", placeholder="operations"),
                            dmc.TextInput(id="dashboard-icon-input", label="Icon", placeholder="tabler:layout-dashboard"),
                            dmc.TextInput(id="dashboard-description-input", label="Description", placeholder="Shared operational dashboard"),
                        ],
                    ),
                    dmc.Group(
                        [
                            dmc.Button(
                                "Create Dashboard",
                                id="dashboard-create-btn",
                                leftSection=DashIconify(icon="tabler:plus", width=16),
                            ),
                            dmc.Text(id="dashboard-create-message", size="sm"),
                        ],
                        mt="md",
                    ),
                ],
                withBorder=True,
                p="md",
                radius="md",
            ),
            dmc.Paper(
                [
                    dmc.Group(
                        [
                            dmc.Title("Dashboards", order=4),
                            dmc.Badge(id="dashboard-count-badge", children=f"{len(dashboards)} total", variant="light"),
                        ],
                        justify="space-between",
                        mb="sm",
                    ),
                    html.Div(
                        children=[_dashboard_card(d) for d in dashboards] or [dmc.Text("No dashboards yet.", c="dimmed")],
                        id="dashboard-editor-list",
                    ),
                ],
                withBorder=True,
                p="md",
                radius="md",
            ),
            dmc.Paper(
                html.Div(dmc.Text("Select a dashboard to edit.", c="dimmed"), id="dashboard-structure-editor"),
                withBorder=True,
                p="md",
                radius="md",
            ),
            dmc.Paper(
                [
                    dmc.Group(
                        [
                            dmc.Title("Preview", order=4),
                            dmc.Text(id="dashboard-preview-label", size="sm", c="dimmed"),
                        ],
                        justify="space-between",
                        mb="sm",
                    ),
                    html.Div(dmc.Text("Select a dashboard to preview.", c="dimmed"), id="dashboard-preview-content"),
                ],
                withBorder=True,
                p="md",
                radius="md",
            ),
            dmc.Group(
                [
                    dmc.Text(id="section-action-message", size="sm"),
                    dmc.Text(id="widget-action-message", size="sm"),
                    dmc.Text(id="template-action-message", size="sm"),
                ],
                gap="xl",
            ),
            dmc.Paper(
                [
                    dmc.Title("Available Queries", order=4, mb="sm"),
                    dmc.Table(
                        striped=True,
                        highlightOnHover=True,
                        withTableBorder=True,
                        data={
                            "head": ["Label", "Key", "Result", "Widgets"],
                            "body": [
                                [q["label"], q["key"], q["result_kind"], ", ".join(q["allowed_widget_types"])]
                                for q in queries
                            ],
                        },
                    ),
                ],
                withBorder=True,
                p="md",
                radius="md",
            ),
            dcc.Store(id="selected-dashboard-id", storage_type="session"),
            dcc.Store(id="dashboard-editor-refresh", data=0),
        ],
        gap="md",
    )


def register_callbacks(app):
    @app.callback(
        Output("url", "pathname", allow_duplicate=True),
        Output("url", "search", allow_duplicate=True),
        Input({"type": "dashboard-edit-btn", "index": dash.ALL}, "n_clicks"),
        prevent_initial_call=True,
    )
    def navigate_to_dashboard_editor(_clicks):
        from dash import ctx
        if not ctx.triggered_id or not ctx.triggered or not ctx.triggered[0].get("value"):
            return no_update, no_update
        dashboard_id = ctx.triggered_id["index"]
        tree = data.get_dashboard_tree(dashboard_id)
        if not tree:
            return no_update, no_update
        return "/dashboards/manage", f"?dashboard={tree['slug']}"

    @app.callback(
        Output("dashboard-create-message", "children"),
        Output("selected-dashboard-id", "data", allow_duplicate=True),
        Output("dashboard-editor-refresh", "data", allow_duplicate=True),
        Input("dashboard-create-btn", "n_clicks"),
        State("dashboard-name-input", "value"),
        State("dashboard-slug-input", "value"),
        State("dashboard-description-input", "value"),
        State("dashboard-icon-input", "value"),
        State("dashboard-editor-refresh", "data"),
        prevent_initial_call=True,
    )
    def create_dashboard(n_clicks, name, slug, description, icon, refresh):
        if not n_clicks:
            return no_update, no_update, no_update
        if not name or not slug:
            return "Name and slug are required.", no_update, no_update
        slug = slug.strip().lower()
        if not _SLUG_RE.match(slug):
            return "Slug must use lowercase letters, numbers, and hyphens only.", no_update, no_update
        try:
            dashboard = data.create_dashboard(
                name=name.strip(),
                slug=slug,
                description=(description or "").strip() or None,
                icon=(icon or "").strip() or None,
            )
        except Exception as exc:
            return _format_dashboard_write_error(exc), no_update, no_update
        return f"Created dashboard '{name.strip()}'.", dashboard["id"], _increment_refresh(refresh)

    @app.callback(
        Output("dashboard-editor-list", "children"),
        Output("dashboard-count-badge", "children"),
        Input("dashboard-editor-refresh", "data"),
    )
    def render_dashboard_list(_refresh):
        dashboards = data.list_dashboards(include_inactive=True)
        children = [_dashboard_card(d) for d in dashboards] or [dmc.Text("No dashboards yet.", c="dimmed")]
        return children, f"{len(dashboards)} total"

    @app.callback(
        Output("selected-dashboard-id", "data"),
        Input("url", "pathname"),
        Input("url", "search"),
        Input({"type": "dashboard-edit-btn", "index": dash.ALL}, "n_clicks"),
        prevent_initial_call=False,
    )
    def set_selected_dashboard(pathname, search, _clicks):
        from dash import ctx

        if isinstance(ctx.triggered_id, dict) and ctx.triggered_id.get("type") == "dashboard-edit-btn":
            return ctx.triggered_id["index"]

        if pathname != "/dashboards/manage":
            return no_update

        params = parse_qs((search or "").lstrip("?"))
        dashboard_slug = params.get("dashboard", [None])[0]
        if not dashboard_slug:
            return no_update

        dashboard = data.get_dashboard_by_slug(dashboard_slug)
        if not dashboard:
            return no_update
        return dashboard["id"]

    @app.callback(
        Output("dashboard-structure-editor", "children"),
        Input("selected-dashboard-id", "data"),
        Input("dashboard-editor-refresh", "data"),
    )
    def render_structure_editor(dashboard_id, _refresh):
        if not dashboard_id:
            return dmc.Text("Select a dashboard to edit.", c="dimmed")
        tree = data.get_dashboard_tree(dashboard_id)
        if not tree:
            return dmc.Text("Dashboard not found.", c="red")
        return _editor_panel(tree)

    @app.callback(
        Output("dashboard-preview-content", "children"),
        Output("dashboard-preview-label", "children"),
        Input("selected-dashboard-id", "data"),
        Input("dashboard-editor-refresh", "data"),
    )
    def render_preview(dashboard_id, _refresh):
        if not dashboard_id:
            return dmc.Text("Select a dashboard to preview.", c="dimmed"), ""
        tree = data.get_dashboard_tree(dashboard_id)
        if not tree:
            return dmc.Text("Dashboard not found.", c="red"), ""
        definition = dashboard_registry.dashboard_tree_to_definition(tree)
        return renderer.render_dashboard(definition), definition["route"]

    @app.callback(
        Output("selected-dashboard-id", "data", allow_duplicate=True),
        Output("dashboard-editor-refresh", "data", allow_duplicate=True),
        Input({"type": "dashboard-delete-btn", "index": dash.ALL}, "n_clicks"),
        State("selected-dashboard-id", "data"),
        State("dashboard-editor-refresh", "data"),
        prevent_initial_call=True,
    )
    def delete_dashboard(_clicks, selected_dashboard_id, refresh):
        from dash import ctx
        if not ctx.triggered_id or not ctx.triggered or not ctx.triggered[0].get("value"):
            return no_update, no_update
        deleted_id = ctx.triggered_id["index"]
        try:
            data.delete_dashboard(deleted_id)
        except Exception:
            return no_update, no_update
        next_selected = None if selected_dashboard_id == deleted_id else selected_dashboard_id
        return next_selected, _increment_refresh(refresh)

    @app.callback(
        Output("dashboard-create-message", "children", allow_duplicate=True),
        Output("dashboard-editor-refresh", "data", allow_duplicate=True),
        Input("save-dashboard-metadata-btn", "n_clicks"),
        State("selected-dashboard-id", "data"),
        State("dashboard-edit-name-input", "value"),
        State("dashboard-edit-slug-input", "value"),
        State("dashboard-edit-description-input", "value"),
        State("dashboard-edit-icon-input", "value"),
        State("dashboard-editor-refresh", "data"),
        prevent_initial_call=True,
    )
    def save_dashboard_metadata(n_clicks, dashboard_id, name, slug, description, icon, refresh):
        if not n_clicks:
            return no_update, no_update
        if not dashboard_id:
            return "Select a dashboard first.", no_update
        if not name or not slug:
            return "Name and slug are required.", no_update
        slug = slug.strip().lower()
        if not _SLUG_RE.match(slug):
            return "Slug must use lowercase letters, numbers, and hyphens only.", no_update
        tree = data.get_dashboard_tree(dashboard_id)
        if not tree:
            return "Dashboard not found.", no_update
        try:
            data.update_dashboard(
                dashboard_id=dashboard_id,
                name=name.strip(),
                slug=slug,
                description=(description or "").strip() or None,
                icon=(icon or "").strip() or None,
                sort_order=tree.get("sort_order", 0),
                is_default=tree.get("is_default", False),
                is_active=tree.get("is_active", True),
            )
        except Exception as exc:
            return _format_dashboard_write_error(exc), no_update
        return "Dashboard saved.", _increment_refresh(refresh)

    @app.callback(
        Output("dashboard-editor-refresh", "data", allow_duplicate=True),
        Output("template-action-message", "children"),
        Input({"type": "apply-template-btn", "index": dash.ALL}, "n_clicks"),
        State("selected-dashboard-id", "data"),
        State("dashboard-editor-refresh", "data"),
        prevent_initial_call=True,
    )
    def apply_template(_clicks, dashboard_id, refresh):
        from dash import ctx
        if not ctx.triggered_id or not ctx.triggered or not ctx.triggered[0].get("value"):
            return no_update, no_update
        if not dashboard_id:
            return no_update, "Select a dashboard first."
        try:
            dashboard_templates.apply_template(dashboard_id, ctx.triggered_id["index"])
        except Exception as exc:
            return no_update, str(exc)
        return _increment_refresh(refresh), "Template applied."

    @app.callback(
        Output("dashboard-editor-refresh", "data", allow_duplicate=True),
        Output("section-action-message", "children"),
        Input("add-section-btn", "n_clicks"),
        State("selected-dashboard-id", "data"),
        State("section-title-input", "value"),
        State("section-description-input", "value"),
        State("section-columns-input", "value"),
        State("dashboard-editor-refresh", "data"),
        prevent_initial_call=True,
    )
    def add_section(n_clicks, dashboard_id, title, description, columns, refresh):
        if not n_clicks:
            return no_update, no_update
        if not dashboard_id:
            return no_update, "Select a dashboard first."
        try:
            tree = data.get_dashboard_tree(dashboard_id) or {"sections": []}
            data.create_dashboard_section(
                dashboard_id=dashboard_id,
                title=(title or "").strip() or None,
                description=(description or "").strip() or None,
                layout_columns=_safe_int(columns, 1),
                sort_order=len(tree.get("sections", [])),
            )
        except Exception as exc:
            return no_update, _format_dashboard_write_error(exc)
        return _increment_refresh(refresh), "Section added."

    @app.callback(
        Output("dashboard-editor-refresh", "data", allow_duplicate=True),
        Output("section-action-message", "children", allow_duplicate=True),
        Input({"type": "delete-section-btn", "index": dash.ALL}, "n_clicks"),
        State("dashboard-editor-refresh", "data"),
        prevent_initial_call=True,
    )
    def delete_section(_clicks, refresh):
        from dash import ctx
        if not ctx.triggered_id or not ctx.triggered or not ctx.triggered[0].get("value"):
            return no_update, no_update
        try:
            data.delete_dashboard_section(ctx.triggered_id["index"])
        except Exception as exc:
            return no_update, _format_dashboard_write_error(exc)
        return _increment_refresh(refresh), "Section deleted."

    @app.callback(
        Output("dashboard-editor-refresh", "data", allow_duplicate=True),
        Output("section-action-message", "children", allow_duplicate=True),
        Input({"type": "move-section-up-btn", "index": dash.ALL}, "n_clicks"),
        Input({"type": "move-section-down-btn", "index": dash.ALL}, "n_clicks"),
        State("selected-dashboard-id", "data"),
        State("dashboard-editor-refresh", "data"),
        prevent_initial_call=True,
    )
    def move_section(_up_clicks, _down_clicks, dashboard_id, refresh):
        from dash import ctx
        if not ctx.triggered_id or not ctx.triggered or not ctx.triggered[0].get("value"):
            return no_update, no_update
        tree = data.get_dashboard_tree(dashboard_id)
        if not tree:
            return no_update, "Dashboard not found."
        sections = list(tree.get("sections", []))
        section_id = ctx.triggered_id["index"]
        idx = next((i for i, section in enumerate(sections) if section["id"] == section_id), None)
        if idx is None:
            return no_update, "Section not found."
        swap_idx = idx - 1 if ctx.triggered_id["type"] == "move-section-up-btn" else idx + 1
        if swap_idx < 0 or swap_idx >= len(sections):
            return no_update, no_update
        sections[idx], sections[swap_idx] = sections[swap_idx], sections[idx]
        for sort_order, section in enumerate(sections):
            data.update_dashboard_section(
                section_id=section["id"],
                title=section.get("title"),
                description=section.get("description"),
                layout_columns=section.get("layout_columns", 1),
                sort_order=sort_order,
                is_active=section.get("is_active", True),
            )
        return _increment_refresh(refresh), "Section order updated."

    @app.callback(
        Output({"type": "widget-query-input", "index": dash.ALL}, "data"),
        Output({"type": "widget-query-input", "index": dash.ALL}, "placeholder"),
        Output({"type": "widget-query-input", "index": dash.ALL}, "disabled"),
        Output({"type": "widget-query-input", "index": dash.ALL}, "style"),
        Output({"type": "widget-query-params-input", "index": dash.ALL}, "style"),
        Output({"type": "widget-chart-type-input", "index": dash.ALL}, "style"),
        Output({"type": "widget-x-input", "index": dash.ALL}, "style"),
        Output({"type": "widget-y-input", "index": dash.ALL}, "style"),
        Output({"type": "widget-color-input", "index": dash.ALL}, "style"),
        Output({"type": "widget-columns-input", "index": dash.ALL}, "style"),
        Output({"type": "widget-alert-message-input", "index": dash.ALL}, "style"),
        Output({"type": "widget-alert-color-input", "index": dash.ALL}, "style"),
        Output({"type": "widget-stat-items-input", "index": dash.ALL}, "style"),
        Input({"type": "widget-type-input", "index": dash.ALL}, "value"),
    )
    def sync_widget_form_visibility(widget_types):
        widget_types = widget_types or []
        if not widget_types:
            return (no_update,) * 13
        query_data = []
        query_placeholders = []
        query_disabled = []
        query_styles = []
        params_styles = []
        chart_type_styles = []
        x_styles = []
        y_styles = []
        color_styles = []
        grid_styles = []
        alert_message_styles = []
        alert_color_styles = []
        stat_styles = []
        for widget_type in widget_types:
            styles = _widget_field_styles(widget_type)
            query_data.append(_query_options_for_widget(widget_type))
            query_placeholders.append("Choose a query" if widget_type != "alert" else "Not used for alerts")
            query_disabled.append(widget_type == "alert")
            query_styles.append(styles["query"])
            params_styles.append(styles["query"])
            chart_type_styles.append(styles["chart"])
            x_styles.append(styles["chart"])
            y_styles.append(styles["chart"])
            color_styles.append(styles["chart"])
            grid_styles.append(styles["grid"])
            alert_message_styles.append(styles["alert"])
            alert_color_styles.append(styles["alert"])
            stat_styles.append(styles["stat"])
        return (
            query_data,
            query_placeholders,
            query_disabled,
            query_styles,
            params_styles,
            chart_type_styles,
            x_styles,
            y_styles,
            color_styles,
            grid_styles,
            alert_message_styles,
            alert_color_styles,
            stat_styles,
        )

    @app.callback(
        Output("dashboard-editor-refresh", "data", allow_duplicate=True),
        Output("widget-action-message", "children"),
        Input({"type": "add-widget-btn", "index": dash.ALL}, "n_clicks"),
        State({"type": "widget-type-input", "index": dash.ALL}, "value"),
        State({"type": "widget-type-input", "index": dash.ALL}, "id"),
        State({"type": "widget-title-input", "index": dash.ALL}, "value"),
        State({"type": "widget-query-input", "index": dash.ALL}, "value"),
        State({"type": "widget-query-params-input", "index": dash.ALL}, "value"),
        State({"type": "widget-chart-type-input", "index": dash.ALL}, "value"),
        State({"type": "widget-x-input", "index": dash.ALL}, "value"),
        State({"type": "widget-y-input", "index": dash.ALL}, "value"),
        State({"type": "widget-color-input", "index": dash.ALL}, "value"),
        State({"type": "widget-columns-input", "index": dash.ALL}, "value"),
        State({"type": "widget-alert-message-input", "index": dash.ALL}, "value"),
        State({"type": "widget-alert-color-input", "index": dash.ALL}, "value"),
        State({"type": "widget-stat-items-input", "index": dash.ALL}, "value"),
        State("dashboard-editor-refresh", "data"),
        prevent_initial_call=True,
    )
    def add_widget(
        _clicks,
        widget_types,
        widget_type_ids,
        titles,
        query_keys,
        query_params_texts,
        chart_types,
        x_fields,
        y_fields,
        color_fields,
        grid_columns_texts,
        alert_messages,
        alert_colors,
        stat_items_texts,
        refresh,
    ):
        from dash import ctx
        if not ctx.triggered_id or not ctx.triggered or not ctx.triggered[0].get("value"):
            return no_update, no_update
        section_id = ctx.triggered_id["index"]
        position = next((idx for idx, item in enumerate(widget_type_ids) if item["index"] == section_id), None)
        if position is None:
            return no_update, "Unable to resolve widget form."
        try:
            query_key, query_params, display_config = _build_widget_payload(
                widget_types[position],
                titles[position],
                query_keys[position],
                query_params_texts[position],
                alert_messages[position],
                alert_colors[position],
                chart_types[position],
                x_fields[position],
                y_fields[position],
                color_fields[position],
                grid_columns_texts[position],
                stat_items_texts[position],
            )
            count_row = data.query_one(
                """
                SELECT COUNT(*) AS count
                FROM dashboard_widgets
                WHERE section_id = %s
                """,
                (section_id,),
            )
            sort_order = int(count_row["count"]) if count_row and count_row.get("count") is not None else 0
            data.create_dashboard_widget(
                section_id=section_id,
                widget_type=widget_types[position],
                title=(titles[position] or "").strip() or None,
                query_key=query_key,
                query_params=query_params,
                display_config=display_config,
                sort_order=sort_order,
            )
        except Exception as exc:
            if isinstance(exc, ValueError):
                return no_update, str(exc)
            return no_update, _format_dashboard_write_error(exc)
        return _increment_refresh(refresh), "Widget added."

    @app.callback(
        Output("dashboard-editor-refresh", "data", allow_duplicate=True),
        Output("widget-action-message", "children", allow_duplicate=True),
        Input({"type": "delete-widget-btn", "index": dash.ALL}, "n_clicks"),
        State("dashboard-editor-refresh", "data"),
        prevent_initial_call=True,
    )
    def delete_widget(_clicks, refresh):
        from dash import ctx
        if not ctx.triggered_id or not ctx.triggered or not ctx.triggered[0].get("value"):
            return no_update, no_update
        try:
            data.delete_dashboard_widget(ctx.triggered_id["index"])
        except Exception as exc:
            return no_update, _format_dashboard_write_error(exc)
        return _increment_refresh(refresh), "Widget deleted."

    @app.callback(
        Output("dashboard-editor-refresh", "data", allow_duplicate=True),
        Output("widget-action-message", "children", allow_duplicate=True),
        Input({"type": "move-widget-up-btn", "index": dash.ALL}, "n_clicks"),
        Input({"type": "move-widget-down-btn", "index": dash.ALL}, "n_clicks"),
        State("selected-dashboard-id", "data"),
        State("dashboard-editor-refresh", "data"),
        prevent_initial_call=True,
    )
    def move_widget(_up_clicks, _down_clicks, dashboard_id, refresh):
        from dash import ctx
        if not ctx.triggered_id or not ctx.triggered or not ctx.triggered[0].get("value"):
            return no_update, no_update
        tree = data.get_dashboard_tree(dashboard_id)
        if not tree:
            return no_update, "Dashboard not found."
        widget_id = ctx.triggered_id["index"]
        for section in tree.get("sections", []):
            widgets = list(section.get("widgets", []))
            idx = next((i for i, widget in enumerate(widgets) if widget["id"] == widget_id), None)
            if idx is None:
                continue
            swap_idx = idx - 1 if ctx.triggered_id["type"] == "move-widget-up-btn" else idx + 1
            if swap_idx < 0 or swap_idx >= len(widgets):
                return no_update, no_update
            widgets[idx], widgets[swap_idx] = widgets[swap_idx], widgets[idx]
            for sort_order, widget in enumerate(widgets):
                data.update_dashboard_widget(
                    widget_id=widget["id"],
                    widget_type=widget["widget_type"],
                    title=widget.get("title"),
                    query_key=widget.get("query_key"),
                    query_params=widget.get("query_params_json") or {},
                    display_config=widget.get("display_config_json") or {},
                    sort_order=sort_order,
                    is_active=widget.get("is_active", True),
                )
            return _increment_refresh(refresh), "Widget order updated."
        return no_update, "Widget not found."
