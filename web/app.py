"""
TS Ticket Analytics — Dash + Mantine web dashboard.

Entry point.  Run with:
    python -m web.app
    python web/app.py

Reads from the existing Postgres database via db.py.
Does NOT modify any existing project code or write to TeamSupport.
"""

import os
import traceback
from urllib.parse import parse_qs, quote

from dash import Dash, html, callback, Input, Output, State, no_update, ALL, callback_context  # noqa: E402
import dash_mantine_components as dmc                              # noqa: E402
from dash import dcc                                               # noqa: E402
from dash_iconify import DashIconify                               # noqa: E402

if __package__ in (None, ""):
    import web.dashboard_registry as dashboard_registry            # noqa: E402
    import web.data as data                                        # noqa: E402
    import web.renderer as renderer                               # noqa: E402
    from web.pages.dashboard_editor import register_callbacks as dashboard_editor_callbacks  # noqa: E402
    from web.pages.health import register_health_callbacks as health_callbacks  # noqa: E402
    from web.pages.overview import register_overview_callbacks as ov_callbacks  # noqa: E402
    from web.pages.root_cause import register_callbacks as rc_callbacks  # noqa: E402
    from web.pages.ticket_detail import ticket_detail_layout, register_callbacks as td_callbacks  # noqa: E402
else:
    from . import data                                             # noqa: E402
    from . import dashboard_registry                               # noqa: E402
    from . import renderer                                         # noqa: E402
    from .pages.dashboard_editor import register_callbacks as dashboard_editor_callbacks  # noqa: E402
    from .pages.health import register_health_callbacks as health_callbacks  # noqa: E402
    from .pages.overview import register_overview_callbacks as ov_callbacks  # noqa: E402
    from .pages.root_cause import register_callbacks as rc_callbacks  # noqa: E402
    from .pages.ticket_detail import ticket_detail_layout, register_callbacks as td_callbacks  # noqa: E402

# Import custom page modules declared in dashboard.yaml
renderer.import_custom_layouts()
_PAGES = renderer.get_pages()

# ── Dash app ─────────────────────────────────────────────────────────

app = Dash(
    __name__,
    suppress_callback_exceptions=True,
    external_stylesheets=[
        "https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap",
    ],
)
app.title = "CS Analytics"
server = app.server  # for gunicorn: gunicorn web.app:server

# Register page-specific callbacks
rc_callbacks(app)
td_callbacks(app)
ov_callbacks(app)
health_callbacks(app)
dashboard_editor_callbacks(app)

# ── Navigation items (static + DB-backed) ────────────────────────────

STATIC_NAV_ITEMS = dashboard_registry.build_static_nav_items(_PAGES)

# ── Layout ───────────────────────────────────────────────────────────

app.layout = dmc.MantineProvider(
    forceColorScheme="light",
    theme={
        "primaryColor": "blue",
        "fontFamily": "'Inter', sans-serif",
        "headings": {"fontFamily": "'Inter', sans-serif"},
    },
    children=[
        dcc.Location(id="url", refresh=False),
        dcc.Store(id="ticket-filter-session", storage_type="session"),
        dcc.Store(id="dark-mode-store", storage_type="local", data=False),
        dmc.AppShell(
            [
                dmc.AppShellHeader(
                    dmc.Group(
                        [
                            dmc.Group(
                                [
                                    DashIconify(icon="tabler:chart-dots-3", width=28, color="#1c7ed6"),
                                    dmc.Text("CS Analytics", fw=700, size="lg"),
                                ],
                                gap="xs",
                            ),
                            dmc.Group(
                                [
                                    DashIconify(icon="tabler:sun", width=18, id="dark-mode-sun-icon"),
                                    dmc.Switch(
                                        id="dark-mode-toggle",
                                        size="md",
                                        offLabel="",
                                        onLabel="",
                                    ),
                                    DashIconify(icon="tabler:moon", width=18, id="dark-mode-moon-icon"),
                                ],
                                gap=6,
                            ),
                        ],
                        h="100%",
                        px="md",
                        justify="space-between",
                    ),
                ),
                dmc.AppShellNavbar(
                    dmc.Stack(id="sidebar-nav", gap=2, p="sm"),
                ),
                dmc.AppShellMain(
                    html.Div(id="page-content", style={"padding": "1.5rem"}),
                ),
            ],
            header={"height": 60},
            navbar={"width": 240, "breakpoint": "sm"},
            padding="md",
        ),
    ],
)


# ── Dark mode toggle ─────────────────────────────────────────────────

app.clientside_callback(
    """
    function(checked) {
        if (checked) {
            document.body.classList.add('dark-mode');
        } else {
            document.body.classList.remove('dark-mode');
        }
        return checked;
    }
    """,
    Output("dark-mode-store", "data"),
    Input("dark-mode-toggle", "checked"),
)

# ── URL routing ──────────────────────────────────────────────────────

def _page_error(message: str):
    return dmc.Alert(
        title="Page Load Error",
        color="red",
        children=message,
    )

@callback(Output("page-content", "children"), Input("url", "pathname"), State("url", "search"))
def display_page(pathname, search):
    try:
        if pathname is None:
            pathname = "/"

        # Dynamic route: ticket detail
        if pathname and pathname.startswith("/ticket/"):
            try:
                ticket_id = int(pathname.split("/")[-1])
                params = parse_qs((search or "").lstrip("?"))
                back_tab = params.get("back_tab", [None])[0]
                back_href = "/tickets"
                if back_tab:
                    back_href = f"/tickets?tab={quote(back_tab)}"
                return ticket_detail_layout(ticket_id, back_href=back_href)
            except (ValueError, IndexError):
                return dmc.Text("Invalid ticket ID.", c="red")

        # Match against YAML-defined pages
        for page in _PAGES:
            if pathname == page["route"] or pathname in page.get("aliases", []):
                custom_fn = renderer.get_custom_layout(page["route"])
                if custom_fn:
                    return custom_fn()
                return renderer.render_page(page)

        if pathname.startswith(dashboard_registry.DASHBOARD_ROUTE_PREFIX):
            slug = pathname[len(dashboard_registry.DASHBOARD_ROUTE_PREFIX):]
            definition = dashboard_registry.get_runtime_dashboard_definition(slug)
            if definition:
                return renderer.render_dashboard(definition)
            return dmc.Text("Dashboard not found.", c="red")

        # Default to first page
        first = _PAGES[0] if _PAGES else None
        if first:
            custom_fn = renderer.get_custom_layout(first["route"])
            return custom_fn() if custom_fn else renderer.render_page(first)
        return dmc.Text("No pages configured.")
    except Exception as exc:
        print(f"[web] page-content fallback for {pathname}: {exc}", flush=True)
        traceback.print_exc()
        return _page_error(str(exc))


# ── Nav active state ─────────────────────────────────────────────────

@callback(
    Output("sidebar-nav", "children"),
    Input("url", "pathname"),
)
def render_sidebar_nav(pathname):
    try:
        items = dashboard_registry.build_nav_items(_PAGES)
    except Exception as exc:
        print(f"[web] sidebar nav fallback: {exc}", flush=True)
        traceback.print_exc()
        items = dashboard_registry.build_static_nav_items(_PAGES)
    return [
        dmc.NavLink(
            label=item["label"],
            leftSection=DashIconify(icon=item["icon"], width=20),
            href=item["href"],
            variant="light",
            active=dashboard_registry.nav_item_active(item, pathname),
        )
        for item in items
    ]


# ── Ticket grid row click → navigate ────────────────────────────────

@callback(
    Output("url", "pathname", allow_duplicate=True),
    Output("url", "search", allow_duplicate=True),
    Input("ticket-grid", "selectedRows"),
    State("ticket-view-tabs", "value"),
    prevent_initial_call=True,
)
def navigate_to_ticket(selected_rows, current_tab):
    if selected_rows and len(selected_rows) > 0:
        tid = selected_rows[0].get("ticket_id")
        if tid is not None:
            return f"/ticket/{tid}", f"?back_tab={quote(str(current_tab or 'open'))}"
    return no_update, no_update


def _ticket_id_from_cell_click(cell_event):
    if not isinstance(cell_event, dict):
        return None
    row = cell_event.get("data") or {}
    col_id = cell_event.get("colId") or ((cell_event.get("colDef") or {}).get("field"))
    if col_id is None:
        value = cell_event.get("value")
        ticket_number = row.get("ticket_number")
        if value is not None and ticket_number is not None and str(value) == str(ticket_number):
            col_id = "ticket_number"
    if col_id != "ticket_number":
        return None
    return row.get("ticket_id")


@callback(
    Output("url", "pathname", allow_duplicate=True),
    Output("url", "search", allow_duplicate=True),
    Input("ticket-grid", "cellClicked", allow_optional=True),
    State("ticket-view-tabs", "value"),
    prevent_initial_call=True,
)
def navigate_from_ticket_grid_ticket_number(cell_event, current_tab):
    tid = _ticket_id_from_cell_click(cell_event)
    if tid is None:
        return no_update, no_update
    return f"/ticket/{tid}", f"?back_tab={quote(str(current_tab or 'open'))}"


# ── Aging detail expand/collapse ─────────────────────────────────────

@callback(
    Output("aging-detail-collapse", "style"),
    Output("aging-toggle-btn", "children"),
    Input("aging-toggle-btn", "n_clicks"),
    prevent_initial_call=True,
)
def toggle_aging_detail(n_clicks):
    if n_clicks and n_clicks % 2 == 1:
        return {"display": "block"}, "Hide per-product breakdown"
    return {"display": "none"}, "Show per-product breakdown"


# ── Chart drill-down ─────────────────────────────────────────────────


_KPI_CARDS = {
    "kpi-total-open":      {"kpi_filter": "total_open",      "label": "All Open Tickets"},
    "kpi-high-priority":   {"kpi_filter": "high_priority",   "label": "High Priority Tickets"},
    "kpi-high-complexity": {"kpi_filter": "high_complexity", "label": "High Complexity Tickets"},
    "kpi-frustrated":      {"kpi_filter": "frustrated",      "label": "Frustrated Tickets"},
}


@callback(
    Output("drilldown-store", "data"),
    Input("aging-chart", "clickData"),
    Input("product-chart", "clickData"),
    Input({"type": "aging-product-chart", "index": ALL}, "clickData"),
    [Input(card_id, "n_clicks") for card_id in _KPI_CARDS],
    prevent_initial_call=True,
)
def chart_click_to_store(aging_click, product_click, aging_product_clicks, *kpi_clicks):
    """Translate a Plotly clickData event into a drilldown filter dict."""
    from dash import ctx
    trigger = ctx.triggered_id

    if trigger == "aging-chart" and aging_click:
        pt = aging_click["points"][0]
        bucket = pt.get("y") or pt.get("label")
        return {"age_bucket": bucket, "label": f"Age {bucket} days"}

    if trigger == "product-chart" and product_click:
        pt = product_click["points"][0]
        product = pt.get("y") or pt.get("label")
        severity_tier = pt.get("data", {}).get("name") or pt.get("fullData", {}).get("name")
        label = f"{product}"
        if severity_tier:
            label += f" — {severity_tier} severity"
        return {"product": product, "severity_tier": severity_tier, "label": label}

    # Per-product aging charts (pattern-matching IDs)
    if isinstance(trigger, dict) and trigger.get("type") == "aging-product-chart":
        idx = trigger["index"]
        click_data = aging_product_clicks[idx] if idx < len(aging_product_clicks) else None
        if click_data:
            pt = click_data["points"][0]
            bucket = pt.get("y") or pt.get("label")
            product = pt.get("customdata")
            label = f"{product} — Age {bucket} days" if product else f"Age {bucket} days"
            result = {"age_bucket": bucket, "label": label}
            if product:
                result["product"] = product
            return result

    if trigger in _KPI_CARDS:
        info = _KPI_CARDS[trigger]
        return {"kpi_filter": info["kpi_filter"], "label": info["label"]}

    return no_update


@callback(
    Output("drilldown-modal", "opened"),
    Output("drilldown-modal", "title"),
    Output("drilldown-subtitle", "children"),
    Output("drilldown-grid", "rowData"),
    Input("drilldown-store", "data"),
    State("overview-active-filters", "data"),
    State("overview-ticket-store", "data"),
    prevent_initial_call=True,
)
def open_drilldown_modal(filter_data, active_filters, ticket_store):
    """Fetch matching tickets and open the drill-down modal."""
    if not filter_data:
        return False, no_update, no_update, no_update

    # When overview filters are active, use the ticket store (same data as charts)
    if active_filters and ticket_store:
        rows = _drilldown_from_store(filter_data, active_filters, ticket_store)
    else:
        rows = data.get_drilldown_tickets(
            product=filter_data.get("product"),
            severity_tier=filter_data.get("severity_tier"),
            age_bucket=filter_data.get("age_bucket"),
            kpi_filter=filter_data.get("kpi_filter"),
        )

    label = filter_data.get("label", "Tickets")
    subtitle = f"{len(rows)} ticket{'s' if len(rows) != 1 else ''} found"
    return True, f"Drill-down: {label}", subtitle, rows


# ── Age bucket ranges matching _build_aging_from_tickets in overview.py ──

_AGE_BUCKET_RANGES = {
    "0-6":   (0, 7),
    "7-13":  (7, 14),
    "14-29": (14, 30),
    "30-59": (30, 60),
    "60-89": (60, 90),
    "90+":   (90, None),
}


def _ticket_age_bucket(days):
    """Return age bucket string for a given days_opened value."""
    if days is None:
        return None
    d = float(days)
    if d < 7:
        return "0-6"
    if d < 14:
        return "7-13"
    if d < 30:
        return "14-29"
    if d < 60:
        return "30-59"
    if d < 90:
        return "60-89"
    return "90+"


def _consolidate_product(name):
    """Consolidate PM/Power* variants into 'PowerMan'."""
    p = (name or "").strip().lower()
    if p.startswith("pm") or "power" in p:
        return "PowerMan"
    return (name or "").strip() or "Unknown"


def _severity_tier(severity_text):
    """Map severity string to tier."""
    s = (severity_text or "").lower()
    if s.startswith("1") or "high" in s:
        return "High"
    if s.startswith("3") or "low" in s:
        return "Low"
    return "Medium"


def _drilldown_from_store(filter_data, active_filters, ticket_store):
    """Filter ticket store data to match drill-down + overview filters."""
    # Start with open tickets only
    rows = [r for r in ticket_store if (r.get("status") or "").lower() != "closed"]

    # Apply overview multi-select filters
    for field, values in active_filters.items():
        if values:
            val_set = set(values)
            rows = [r for r in rows if str(r.get(field, "")) in val_set]

    # Apply drill-down chart filter
    kpi_filter = filter_data.get("kpi_filter")
    if kpi_filter == "total_open":
        pass  # already filtered to open
    elif kpi_filter == "high_priority":
        rows = [r for r in rows if r.get("priority") is not None and r["priority"] <= 3]
    elif kpi_filter == "high_complexity":
        rows = [r for r in rows if r.get("overall_complexity") is not None and r["overall_complexity"] >= 4]
    elif kpi_filter == "frustrated":
        rows = [r for r in rows if r.get("frustrated") == "Yes"]

    product = filter_data.get("product")
    if product:
        if product == "PowerMan":
            rows = [r for r in rows if _consolidate_product(r.get("product_name")) == "PowerMan"]
        else:
            rows = [r for r in rows if r.get("product_name") == product]

    sev = filter_data.get("severity_tier")
    if sev:
        rows = [r for r in rows if _severity_tier(r.get("severity")) == sev]

    age_bucket = filter_data.get("age_bucket")
    if age_bucket:
        rows = [r for r in rows if _ticket_age_bucket(r.get("days_opened")) == age_bucket]

    return rows


@callback(
    Output("url", "pathname", allow_duplicate=True),
    Input("drilldown-grid", "selectedRows"),
    prevent_initial_call=True,
)
def drilldown_navigate(selected_rows):
    """Row-click in the drilldown grid navigates to ticket detail."""
    if selected_rows and len(selected_rows) > 0:
        tid = selected_rows[0].get("ticket_id")
        if tid is not None:
            return f"/ticket/{tid}"
    return no_update


# ── Saved reports — open/close modal ─────────────────────────────────

@callback(
    Output("save-report-modal", "opened"),
    Input("save-report-btn", "n_clicks"),
    Input("cancel-save-report-btn", "n_clicks"),
    Input("confirm-save-report-btn", "n_clicks"),
    prevent_initial_call=True,
)
def toggle_save_modal(open_clicks, cancel_clicks, confirm_clicks):
    from dash import ctx
    trigger = ctx.triggered_id
    if trigger == "save-report-btn":
        return True
    return False


# ── Saved reports — persist to DB and update tabs live ───────────────

@callback(
    Output("saved-reports-store", "data", allow_duplicate=True),
    Output("ticket-view-tabs", "value", allow_duplicate=True),
    Output("report-name-input", "value"),
    Input("confirm-save-report-btn", "n_clicks"),
    State("report-name-input", "value"),
    State("ticket-grid", "filterModel"),
    prevent_initial_call=True,
)
def save_report(n_clicks, name, filter_model):
    if not n_clicks or not name or not name.strip():
        return no_update, no_update, no_update
    fm = filter_model or {}
    if not fm:
        return no_update, no_update, no_update
    saved = data.save_report(name.strip(), fm)
    reports = {str(r["id"]): r for r in data.get_saved_reports()}
    selected_tab = f"report:{saved['id']}" if saved else no_update
    return reports, selected_tab, ""


# ── Saved reports — apply filters from chip click ────────────────────

@callback(
    Output("report-filter-store", "data"),
    Output("ticket-view-tabs", "value", allow_duplicate=True),
    Input({"type": "report-chip", "index": ALL}, "n_clicks"),
    State("saved-reports-store", "data"),
    prevent_initial_call=True,
)
def apply_report_filter(n_clicks_list, saved_reports):
    from dash import ctx
    if not ctx.triggered_id or not any(n_clicks_list):
        return no_update, no_update
    saved_reports = saved_reports or {}
    report_id = str(ctx.triggered_id["index"])
    report = saved_reports.get(report_id) or saved_reports.get(int(report_id), {})
    fm = (report or {}).get("filter_model") or {}
    if not fm:
        return no_update, no_update
    return fm, f"report:{report_id}"


# Apply the filter model from the store to the grid via clientside callback
app.clientside_callback(
    """function(filterData) {
        if (!filterData || Object.keys(filterData).length === 0) {
            return window.dash_clientside.no_update;
        }
        // Find the grid API and set the filter model
        const gridEl = document.querySelector('#ticket-grid .ag-root-wrapper');
        if (gridEl && gridEl.__agComponent) {
            gridEl.__agComponent.api.setFilterModel(filterData);
        }
        return filterData;
    }""",
    Output("ticket-grid", "filterModel", allow_duplicate=True),
    Input("report-filter-store", "data"),
    prevent_initial_call=True,
)


# ── Saved reports — clear all filters ────────────────────────────────

app.clientside_callback(
    """function(n) {
        if (!n) { return [window.dash_clientside.no_update, {}]; }
        window.location.href = '/tickets';
        return [window.dash_clientside.no_update, {}];
    }""",
    Output("clear-filters-btn", "loading"),
    Output("ticket-filter-session", "data"),
    Input("clear-filters-btn", "n_clicks"),
    prevent_initial_call=True,
)


# ── Persist ticket filters to session store ──────────────────────────

app.clientside_callback(
    """function(filterModel, tabValue) {
        if (!filterModel && !tabValue) {
            return window.dash_clientside.no_update;
        }
        return {filterModel: filterModel || {}, tab: tabValue || 'open'};
    }""",
    Output("ticket-filter-session", "data", allow_duplicate=True),
    Input("ticket-grid", "filterModel"),
    Input("ticket-view-tabs", "value"),
    prevent_initial_call=True,
)


# ── Restore ticket filters from session store on page load ───────────

@callback(
    Output("ticket-grid", "filterModel", allow_duplicate=True),
    Output("ticket-view-tabs", "value", allow_duplicate=True),
    Input("filter-restore-trigger", "n_intervals"),
    State("ticket-filter-session", "data"),
    State("url", "search"),
    prevent_initial_call=True,
)
def restore_ticket_filters(_n, session_data, search):
    open_filter = {
        "status": {"filterType": "text", "type": "doesNotContain", "filter": "Closed"}
    }
    params = parse_qs((search or "").lstrip("?"))
    explicit_tab = params.get("tab", [None])[0]
    if explicit_tab:
        if explicit_tab == "open":
            return open_filter, "open"
        filter_model = (session_data or {}).get("filterModel") or {}
        return filter_model, explicit_tab

    if not session_data:
        return open_filter, "open"

    filter_model = session_data.get("filterModel") or {}
    tab_value = session_data.get("tab") or "open"
    if tab_value == "open":
        return open_filter, "open"
    return filter_model, tab_value


# ── Saved reports — delete active saved-report tab ───────────────────

@callback(
    Output("saved-reports-store", "data", allow_duplicate=True),
    Output("ticket-view-tabs", "value", allow_duplicate=True),
    Input("delete-report-btn", "n_clicks"),
    State("ticket-view-tabs", "value"),
    State("saved-reports-store", "data"),
    prevent_initial_call=True,
)
def delete_report(n_clicks, active_tab, saved_reports):
    if not n_clicks or not active_tab or not str(active_tab).startswith("report:"):
        return no_update, no_update
    report_id = str(active_tab).split(":", 1)[1]
    report = (saved_reports or {}).get(report_id) or (saved_reports or {}).get(int(report_id), {})
    if not report:
        return no_update, no_update
    data.delete_report(report_id)
    reports = {str(r["id"]): r for r in data.get_saved_reports()}
    return reports, "open"


# ── CSV export callbacks (one per grid) ──────────────────────────────

def _collect_grid_ids():
    """Gather all AG Grid IDs from YAML config and known code-driven pages."""
    ids = set()
    # YAML-driven grids
    for page in _PAGES:
        for tab in page.get("tabs", []):
            for comp in tab.get("components", []):
                if comp.get("type") == "grid":
                    ids.add(comp.get("id", f"yaml-grid-{comp['query']}"))
        for comp in page.get("components", []):
            if comp.get("type") == "grid":
                ids.add(comp.get("id", f"yaml-grid-{comp['query']}"))
    # Code-driven grids
    ids.update([
        "ticket-grid",
        "drilldown-grid",
        "root-cause-grid",
        "health-drilldown-grid",
        "health-contributors-grid",
        "rc-fixes-drilldown-grid",
    ])
    return ids


_TICKET_NUMBER_CLICK_GRID_IDS = [
    grid_id for grid_id in sorted(_collect_grid_ids())
    if grid_id != "ticket-grid"
]


@callback(
    Output("url", "pathname", allow_duplicate=True),
    Output("url", "search", allow_duplicate=True),
    [Input(grid_id, "cellClicked", allow_optional=True) for grid_id in _TICKET_NUMBER_CLICK_GRID_IDS],
    prevent_initial_call=True,
)
def navigate_from_ticket_number_cells(*cell_events):
    trigger_id = callback_context.triggered_id
    if not trigger_id or trigger_id not in _TICKET_NUMBER_CLICK_GRID_IDS:
        return no_update, no_update
    event = cell_events[_TICKET_NUMBER_CLICK_GRID_IDS.index(trigger_id)]
    tid = _ticket_id_from_cell_click(event)
    if tid is None:
        return no_update, no_update
    return f"/ticket/{tid}", ""


for _grid_id in _collect_grid_ids():
    app.clientside_callback(
        "function(n) { return true; }",
        Output(_grid_id, "exportDataAsCsv"),
        Input(f"{_grid_id}-csv-btn", "n_clicks"),
        prevent_initial_call=True,
    )


# ── Run ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    port = int(os.getenv("WEB_PORT", "8050"))
    debug = os.getenv("WEB_DEBUG", "1").strip() == "1"
    print(f"[web] Starting CS Analytics on http://localhost:{port}", flush=True)
    app.run(debug=debug, port=port)
