"""
YAML-driven dashboard renderer.

Reads web/dashboard.yaml and converts page definitions into Dash layouts.
Custom (code-driven) pages use 'module' + 'layout_func' keys.
YAML-driven pages use 'components' or 'tabs' keys.
"""

import importlib
import os

import dash_ag_grid as dag
import dash_mantine_components as dmc
from dash import dcc, html
from dash_iconify import DashIconify
import plotly.graph_objects as go
import yaml

from . import data
from . import query_catalog

# ── Config loading (auto-reloads on file change) ────────────────────

_DIR = os.path.dirname(os.path.abspath(__file__))
_CONFIG_PATH = os.path.join(_DIR, "dashboard.yaml")
_config_cache = None
_config_mtime = 0


def load_config():
    global _config_cache, _config_mtime
    mtime = os.path.getmtime(_CONFIG_PATH)
    if _config_cache is None or mtime > _config_mtime:
        with open(_CONFIG_PATH) as f:
            _config_cache = yaml.safe_load(f)
        _config_mtime = mtime
    return _config_cache


def get_pages():
    return load_config().get("pages", [])


def get_queries():
    return load_config().get("queries", {})


# ── Custom page import ──────────────────────────────────────────────

_custom_layouts = {}


def import_custom_layouts():
    """Import all custom page modules and cache their layout functions."""
    for page in get_pages():
        module_name = page.get("module")
        func_name = page.get("layout_func")
        if module_name and func_name:
            mod = importlib.import_module(module_name)
            _custom_layouts[page["route"]] = getattr(mod, func_name)


def get_custom_layout(route):
    return _custom_layouts.get(route)


# ── Page renderer ───────────────────────────────────────────────────

def render_page(page_def):
    """Render a YAML-driven page definition into Dash components."""
    return render_dashboard(_legacy_page_to_dashboard(page_def))


def render_dashboard(definition):
    """Render a canonical dashboard definition into Dash components."""
    children = []

    title = definition.get("title")
    description = definition.get("description")
    sections = definition.get("sections", [])

    if title:
        children.append(dmc.Title(title, order=2))
    if description:
        children.append(dmc.Text(description, c="dimmed", size="sm"))

    for section in sections:
        children.append(render_section(section))

    return dmc.Stack(children, gap="md")


def render_section(section_def):
    """Render one canonical dashboard section."""
    try:
        validate_section(section_def)
    except ValueError as exc:
        return _render_error(str(exc), title=section_def.get("title") or "Invalid section")

    widgets = [render_widget(widget_def) for widget_def in section_def.get("widgets", [])]
    stack_children = []
    if section_def.get("title"):
        stack_children.append(dmc.Text(section_def["title"], fw=600, size="lg"))
    if section_def.get("description"):
        stack_children.append(dmc.Text(section_def["description"], c="dimmed", size="sm"))
    stack_children.extend(widgets)
    return dmc.Stack(stack_children, gap="md")


def render_widget(widget_def):
    """Render one canonical widget with isolated validation/error handling."""
    try:
        validate_widget(widget_def)
        return _render_widget_impl(widget_def)
    except Exception as exc:
        title = widget_def.get("title") or widget_def.get("type") or "Invalid widget"
        return _render_error(str(exc), title=title)


# ── Tabs ────────────────────────────────────────────────────────────

def _render_tabs(tabs_def):
    tabs_list = []
    panels = []
    for i, tab in enumerate(tabs_def):
        value = tab.get("value", f"tab-{i}")
        tabs_list.append(dmc.TabsTab(tab["label"], value=value))
        content = _render_components(tab.get("components", []))
        panels.append(
            dmc.TabsPanel(dmc.Stack(content, gap="md"), value=value, pt="md")
        )

    default = tabs_def[0].get("value", "tab-0") if tabs_def else None
    return dmc.Tabs(
        [dmc.TabsList(tabs_list)] + panels,
        value=default,
    )


# ── Component dispatcher ───────────────────────────────────────────

def _render_components(components):
    rendered = []
    for comp in components:
        rendered.append(render_widget(_legacy_component_to_widget(comp)))
    return rendered


def _render_widget_impl(widget_def):
    ctype = widget_def.get("type")
    if ctype == "alert":
        return _render_alert(widget_def)
    if ctype == "grid":
        return _render_grid(widget_def)
    if ctype == "chart":
        return _render_chart(widget_def)
    if ctype == "stat_row":
        return _render_stat_row(widget_def)
    raise ValueError(f"Unsupported widget type '{ctype}'.")


# ── Alert ───────────────────────────────────────────────────────────

def _render_alert(comp):
    icon = None
    if comp.get("icon"):
        icon = DashIconify(icon=comp["icon"], width=24)
    return dmc.Alert(
        comp.get("message", ""),
        title=comp.get("title"),
        color=comp.get("color", "blue"),
        variant=comp.get("variant", "light"),
        icon=icon,
        radius="md",
    )


# ── AG Grid ─────────────────────────────────────────────────────────

_COLUMN_ALIASES = {
    "header": "headerName",
    "header_name": "headerName",
    "min_width": "minWidth",
    "max_width": "maxWidth",
    "cell_style": "cellStyle",
    "value_formatter": "valueFormatter",
    "floating_filter": "floatingFilter",
    "cell_renderer": "cellRenderer",
}

_TICKET_NUMBER_CELL_STYLE = {
    "function": """
        params.data && params.data.ticket_id
            ? {
                'color': '#1c7ed6',
                'textDecoration': 'underline',
                'cursor': 'pointer',
                'fontWeight': '600'
              }
            : {}
    """
}


def ticket_number_column(width=110, header_name="Ticket #", pinned="left", **kwargs):
    """Return a standard ticket-number column with link-like styling."""
    col = {
        "field": "ticket_number",
        "headerName": header_name,
        "width": width,
        "cellStyle": _TICKET_NUMBER_CELL_STYLE,
        "type": "numericColumn",
        "valueGetter": {"function": "Number(params.data.ticket_number)"},
    }
    if pinned is not None:
        col["pinned"] = pinned
    col.update(kwargs)
    return col


def _decorate_ticket_number_column(col):
    if col.get("field") != "ticket_number":
        return col
    decorated = dict(col)
    decorated.setdefault("cellStyle", _TICKET_NUMBER_CELL_STYLE)
    return decorated


def _normalize_columns(col_defs):
    """Convert YAML column definitions to AG Grid format."""
    result = []
    for col in col_defs:
        ag_col = {}
        for k, v in col.items():
            ag_col[_COLUMN_ALIASES.get(k, k)] = v
        result.append(_decorate_ticket_number_column(ag_col))
    return result


def _render_grid(comp):
    rows = _run_query(comp["query"], "grid")
    raw_columns = comp.get("columns", [])
    columns = _normalize_columns(raw_columns) if raw_columns else _infer_columns(rows)

    grid_id = comp.get("id", f"yaml-grid-{comp['query']}")
    height = comp.get("height", "calc(100vh - 280px)")

    default_col_def = comp.get("default_col_def", {
        "sortable": True, "filter": True, "resizable": True,
        "filterParams": {"caseSensitive": False},
    })
    grid_options = comp.get("grid_options", {
        "pagination": True, "paginationPageSize": 25, "animateRows": True,
    })

    if not rows:
        return dmc.Text(
            comp.get("empty_message", "No data available."),
            c="dimmed", ta="center", py="xl",
        )

    grid = dag.AgGrid(
        id=grid_id,
        rowData=rows,
        columnDefs=columns,
        defaultColDef=default_col_def,
        dashGridOptions=grid_options,
        style={"height": height},
        className="ag-theme-quartz",
    )
    return grid_with_export(grid, grid_id)


def _infer_columns(rows):
    """Infer AG Grid columns from the first row when none are configured."""
    if not rows:
        return []
    result = []
    for field in rows[0].keys():
        result.append(
            _decorate_ticket_number_column({
                "field": field,
                "headerName": field.replace("_", " ").title(),
            })
        )
    return result


def grid_with_export(grid_component, grid_id):
    """Wrap an AG Grid with a CSV export button."""
    btn_id = f"{grid_id}-csv-btn"
    return dmc.Stack(
        [
            dmc.Group(
                [
                    dmc.Button(
                        "Export CSV",
                        id=btn_id,
                        leftSection=DashIconify(icon="tabler:download", width=16),
                        variant="light",
                        color="gray",
                        size="compact-sm",
                    ),
                ],
                justify="flex-end",
            ),
            grid_component,
        ],
        gap="xs",
    )


# ── Charts ──────────────────────────────────────────────────────────

def _render_chart(comp):
    rows = _run_query(comp["query"], "chart")

    if not rows:
        return dmc.Text(
            comp.get("empty_message", "No chart data available."),
            c="dimmed", ta="center", py="xl",
        )

    chart_type = comp.get("chart_type", "bar")
    x = comp.get("x")
    y = comp.get("y")
    color = comp.get("color")
    title = comp.get("title", "")
    height = comp.get("height", 400)
    chart_id = comp.get("id", f"yaml-chart-{comp['query']}")

    fig = _build_figure(rows, chart_type, x, y, color, height)

    if title:
        fig.update_layout(title=title)

    return dcc.Graph(
        id=chart_id,
        figure=fig,
        config={"displayModeBar": False},
    )


def _build_figure(rows, chart_type, x, y, color, height):
    fig = go.Figure()
    y_fields = y if isinstance(y, list) else [y]

    if color:
        groups = {}
        for r in rows:
            key = r.get(color, "Unknown")
            groups.setdefault(key, []).append(r)
        for group_name, group_rows in groups.items():
            x_vals = [r[x] for r in group_rows]
            y_vals = [r[y_fields[0]] for r in group_rows]
            _add_trace(fig, chart_type, x_vals, y_vals, group_name)
    else:
        x_vals = [r[x] for r in rows]
        for yf in y_fields:
            y_vals = [r[yf] for r in rows]
            name = yf if len(y_fields) > 1 else None
            _add_trace(fig, chart_type, x_vals, y_vals, name)

    fig.update_layout(
        template="plotly_white",
        margin=dict(l=40, r=20, t=10, b=40),
        height=height,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        barmode="stack" if chart_type == "stacked_bar" else None,
    )
    return fig


def _add_trace(fig, chart_type, x_vals, y_vals, name):
    if chart_type in ("bar", "stacked_bar"):
        fig.add_trace(go.Bar(x=x_vals, y=y_vals, name=name))
    elif chart_type == "horizontal_bar":
        fig.add_trace(go.Bar(x=y_vals, y=x_vals, orientation="h", name=name))
    elif chart_type == "line":
        fig.add_trace(go.Scatter(x=x_vals, y=y_vals, mode="lines", name=name))
    elif chart_type == "area":
        fig.add_trace(go.Scatter(
            x=x_vals, y=y_vals, fill="tozeroy", name=name,
        ))


# ── Stat Row ────────────────────────────────────────────────────────

def _render_stat_row(comp):
    query_name = comp.get("query")
    row = {}
    if query_name:
        row = _run_query_one(query_name, "stat_row") or {}

    cards = []
    for item in comp.get("items", []):
        value = item.get("value")
        if value is None and item.get("field"):
            value = row.get(item["field"], "\u2014")
        cards.append(_stat_card(
            title=item.get("title", ""),
            value=value,
            icon=item.get("icon", "tabler:chart-bar"),
            color=item.get("color", "blue"),
        ))

    return dmc.SimpleGrid(
        cols={"base": 1, "sm": 2, "lg": len(cards)},
        children=cards,
    )


def _stat_card(title, value, icon, color):
    return dmc.Paper(
        [
            dmc.Group(
                [
                    dmc.Stack(
                        [
                            dmc.Text(title, size="xs", c="dimmed", tt="uppercase", fw=700),
                            dmc.Title(str(value), order=3),
                        ],
                        gap=0,
                    ),
                    dmc.ThemeIcon(
                        DashIconify(icon=icon, width=28),
                        variant="light",
                        color=color,
                        size=50,
                        radius="md",
                    ),
                ],
                justify="space-between",
                align="flex-start",
            ),
        ],
        withBorder=True,
        p="md",
        radius="md",
        shadow="sm",
    )


# ── Query execution ────────────────────────────────────────────────

def _run_query(query_name, widget_type=None):
    return _execute_query(query_name, widget_type)


def _run_query_one(query_name, widget_type=None):
    return _execute_query(query_name, widget_type, expect_one=True)


def _execute_query(query_ref, widget_type, expect_one=False):
    if isinstance(query_ref, dict):
        query_key = query_ref.get("key")
        params = query_ref.get("params", {})
        result = query_catalog.run_query(query_key, widget_type, params)
        return result

    catalog_query = query_catalog.QUERY_CATALOG.get(query_ref)
    if catalog_query and widget_type:
        return query_catalog.run_query(query_ref, widget_type, {})

    q = get_queries().get(query_ref)
    if not q:
        return None if expect_one else []
    if expect_one:
        return data.query_one(q["sql"], tuple(q.get("params", [])))
    return data.query(q["sql"], tuple(q.get("params", [])))


def validate_section(section_def):
    widgets = section_def.get("widgets")
    if widgets is None:
        raise ValueError("Section is missing 'widgets'.")
    if not isinstance(widgets, list):
        raise ValueError("Section 'widgets' must be a list.")


def validate_widget(widget_def):
    ctype = widget_def.get("type")
    if ctype not in {"alert", "grid", "chart", "stat_row"}:
        raise ValueError(f"Unsupported widget type '{ctype}'.")
    if ctype == "alert":
        return

    query_ref = widget_def.get("query")
    if not query_ref:
        raise ValueError("Widget is missing 'query'.")

    if ctype == "chart":
        if not widget_def.get("x"):
            raise ValueError("Chart widget is missing 'x'.")
        if not widget_def.get("y"):
            raise ValueError("Chart widget is missing 'y'.")

    if ctype == "stat_row":
        items = widget_def.get("items", [])
        if not items:
            raise ValueError("Stat row widget requires at least one item.")

    if isinstance(query_ref, dict):
        query_catalog.validate_widget_query(
            query_ref.get("key"),
            ctype,
            query_ref.get("params", {}),
        )
    elif query_ref in query_catalog.QUERY_CATALOG:
        query_catalog.validate_widget_query(query_ref, ctype, {})
    elif query_ref not in get_queries():
        raise ValueError(f"Unknown widget query '{query_ref}'.")


def _legacy_page_to_dashboard(page_def):
    sections = []
    if "tabs" in page_def:
        for idx, tab in enumerate(page_def["tabs"]):
            sections.append(
                {
                    "id": tab.get("value", f"tab-{idx}"),
                    "title": tab.get("label"),
                    "widgets": [_legacy_component_to_widget(comp) for comp in tab.get("components", [])],
                }
            )
    elif "components" in page_def:
        sections.append(
            {
                "id": page_def.get("route", "main"),
                "widgets": [_legacy_component_to_widget(comp) for comp in page_def.get("components", [])],
            }
        )

    return {
        "title": page_def.get("title"),
        "description": page_def.get("description"),
        "sections": sections,
    }


def _legacy_component_to_widget(comp):
    return dict(comp)


def _render_error(message, title="Widget Error"):
    return dmc.Alert(
        message,
        title=title,
        color="red",
        variant="light",
        radius="md",
        icon=DashIconify(icon="tabler:alert-triangle", width=20),
    )
