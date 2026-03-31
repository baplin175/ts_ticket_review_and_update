"""Ticket list page — AG Grid explorer with filters, row-click navigation, and saved reports."""

import subprocess
import sys
import os
import threading
import time

import dash_ag_grid as dag
import dash_mantine_components as dmc
from dash import callback, ctx, dcc, html, Input, Output, State, no_update
from dash.exceptions import PreventUpdate
from dash_iconify import DashIconify

from .. import data
from ..renderer import grid_with_export, ticket_number_column

# Path to run_ingest.py in the project root (web/pages/ → web/ → project root)
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_INGEST_SCRIPT = os.path.join(_PROJECT_ROOT, "run_ingest.py")

# ── Shared sync state (module-level, single-process safe) ────────────

_sync_state = {
    "running": False,
    "lines": [],
    "finished": False,
    "return_code": None,
}
_sync_lock = threading.Lock()


def _run_sync_in_background():
    """Launch run_ingest.py sync as a subprocess, capture output line by line."""
    with _sync_lock:
        _sync_state["running"] = True
        _sync_state["lines"] = []
        _sync_state["finished"] = False
        _sync_state["return_code"] = None

    try:
        env = os.environ.copy()
        proc = subprocess.Popen(
            [sys.executable, _INGEST_SCRIPT, "sync", "--verbose", "--enrich-new"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=_PROJECT_ROOT,
            env=env,
        )
        for line in proc.stdout:
            with _sync_lock:
                _sync_state["lines"].append(line.rstrip())
        proc.wait()
        with _sync_lock:
            _sync_state["return_code"] = proc.returncode
    except Exception as exc:
        with _sync_lock:
            _sync_state["lines"].append(f"[error] {exc}")
            _sync_state["return_code"] = -1
    finally:
        with _sync_lock:
            _sync_state["running"] = False
            _sync_state["finished"] = True


# ── Column definitions ───────────────────────────────────────────────

COLUMN_DEFS = [
    ticket_number_column(width=110, pinned="left"),
    {
        "field": "flag_review",
        "headerName": "🚩",
        "width": 55,
        "cellStyle": {
            "function": "params.value ? {'color': '#e8590c', 'fontWeight': 'bold', 'textAlign': 'center'} : {'textAlign': 'center', 'color': '#ced4da'}"
        },
        "valueFormatter": {"function": "params.value ? '🚩' : ''"},
    },
    {
        "field": "ticket_name",
        "headerName": "Name",
        "minWidth": 200,
        "flex": 1,
        "tooltipField": "ticket_name",
    },
    {"field": "status", "headerName": "Status", "width": 120},
    {"field": "severity", "headerName": "Severity", "width": 140},
    {"field": "product_name", "headerName": "Product", "width": 140},
    {"field": "assignee", "headerName": "Assignee", "width": 130},
    {"field": "customer", "headerName": "Customer", "width": 140},
    {
        "field": "days_opened",
        "headerName": "Age (d)",
        "width": 90,
        "type": "numericColumn",
        "valueFormatter": {"function": "params.value != null ? Math.round(params.value) : ''"},
    },
    {
        "field": "days_since_modified",
        "headerName": "Days Idle",
        "width": 100,
        "type": "numericColumn",
        "valueFormatter": {"function": "params.value != null ? Math.round(params.value) : ''"},
    },
    {
        "field": "priority",
        "headerName": "Priority",
        "width": 95,
        "type": "numericColumn",
        "cellStyle": {
            "function": """
                params.value != null && params.value <= 3
                    ? {'color': '#e03131', 'fontWeight': 'bold'}
                    : {}
            """
        },
    },
    {
        "field": "overall_complexity",
        "headerName": "Complexity",
        "width": 110,
        "type": "numericColumn",
        "cellStyle": {
            "function": """
                params.value != null && params.value >= 4
                    ? {'color': '#e8590c', 'fontWeight': 'bold'}
                    : {}
            """
        },
    },
    {
        "field": "frustrated",
        "headerName": "Frustrated",
        "width": 105,
        "cellStyle": {
            "function": """
                params.value === 'Yes'
                    ? {'color': '#c2255c', 'fontWeight': 'bold'}
                    : {'color': '#868e96'}
            """
        },
    },
    {
        "field": "action_count",
        "headerName": "Messages",
        "width": 100,
        "type": "numericColumn",
    },
    {
        "field": "date_modified",
        "headerName": "Last Modified",
        "width": 130,
        "valueFormatter": {
            "function": "params.value ? new Date(params.value).toLocaleDateString() : ''"
        },
        "sort": "desc",
    },
    {"field": "do_number", "headerName": "DO #", "width": 90,
     "cellRenderer": "DOLink"},
    {"field": "do_status", "headerName": "DO Status", "width": 110},
    {
        "field": "do_mismatch_label",
        "headerName": "DO Align",
        "width": 160,
        "cellStyle": {
            "function": (
                "params.value === 'aligned' || params.value == null ? {} : "
                "params.value === 'do_stalled_or_abandoned' ? {'backgroundColor': '#fff3cd', 'color': '#856404'} : "
                "{'backgroundColor': '#f8d7da', 'color': '#721c24'}"
            )
        },
        "valueFormatter": {
            "function": (
                "({'ticket_open_do_closed': 'Open / DO Closed',"
                "  'ticket_closed_do_active': 'Closed / DO Active',"
                "  'do_stalled_or_abandoned': 'DO Stalled',"
                "  'do_scope_mismatch': 'Scope Mismatch',"
                "  'unclear': 'Unclear',"
                "  'aligned': 'Aligned'})[params.value] || params.value || '—'"
            )
        },
        "tooltipField": "do_alignment_explanation",
    },
]

DEFAULT_COL_DEF = {
    "sortable": True,
    "filter": True,
    "resizable": True,
    "floatingFilter": True,
    "filterParams": {"caseSensitive": False, "maxNumConditions": 10},
}


# ── Open-ticket filter model for AG Grid ─────────────────────────────

_OPEN_FILTER_MODEL = {
    "status": {
        "filterType": "text",
        "type": "notContains",
        "filter": "Closed",
    }
}


# ── Layout ───────────────────────────────────────────────────────────

def _build_report_chips(reports):
    """Build a row of clickable report chips with delete buttons."""
    return []


def normalize_saved_reports(reports):
    if not reports:
        return []
    if isinstance(reports, dict):
        reports = list(reports.values())
    return sorted(reports, key=lambda r: (r.get("sort_order") or 0, (r.get("name") or "").lower()))


def build_report_tabs(reports):
    report_tabs = [
        dmc.TabsTab(report["name"], value=f"report:{report['id']}")
        for report in normalize_saved_reports(reports)
    ]
    return [
        dmc.TabsList([
            dmc.TabsTab("Open", value="open"),
            dmc.TabsTab("All Tickets", value="all"),
            *report_tabs,
        ]),
    ]


def tickets_layout():
    rows = data.get_ticket_list()
    reports = data.get_saved_reports('tickets')
    open_count = sum(1 for r in rows if (r.get("status") or "").lower() != "closed")

    return dmc.Stack(
        [
            dmc.Group(
                [
                    dmc.Title("Tickets", order=2),
                    dmc.Group(
                        [
                            dmc.Badge(f"{open_count} open tickets", id="ticket-count-badge", size="lg", variant="light"),
                            dmc.Button(
                                "Refresh from TeamSupport",
                                id="refresh-tickets-btn",
                                leftSection=DashIconify(icon="tabler:refresh", width=16),
                                variant="light",
                                size="compact-sm",
                            ),
                        ],
                        gap="xs",
                    ),
                ],
                justify="space-between",
            ),
            dmc.Text(
                "Click any row to view full ticket detail.",
                size="sm", c="dimmed",
            ),
            # Saved reports bar
            dmc.Group(
                [
                    html.Div(id="report-chip-group", style={"display": "none"}),
                    dmc.Group(
                        [
                            dmc.Button(
                                "Save Report",
                                id="save-report-btn",
                                leftSection=DashIconify(icon="tabler:device-floppy", width=16),
                                variant="light",
                                size="compact-sm",
                            ),
                            dmc.Button(
                                "Delete Report",
                                id="delete-report-btn",
                                leftSection=DashIconify(icon="tabler:trash", width=16),
                                variant="subtle",
                                color="red",
                                size="compact-sm",
                                disabled=True,
                            ),
                            dmc.ActionIcon(
                                DashIconify(icon="tabler:chevron-left", width=16),
                                id="move-tab-left-btn",
                                variant="subtle",
                                color="gray",
                                size="sm",
                                disabled=True,
                            ),
                            dmc.ActionIcon(
                                DashIconify(icon="tabler:chevron-right", width=16),
                                id="move-tab-right-btn",
                                variant="subtle",
                                color="gray",
                                size="sm",
                                disabled=True,
                            ),
                            dmc.Button(
                                "Clear Filters",
                                id="clear-filters-btn",
                                leftSection=DashIconify(icon="tabler:filter-off", width=16),
                                variant="subtle",
                                color="gray",
                                size="compact-sm",
                            ),
                        ],
                        gap="xs",
                    ),
                ],
                justify="space-between",
            ),
            # Save-report modal
            dmc.Modal(
                id="save-report-modal",
                title="Save Current Filters as Report",
                centered=True,
                children=[
                    dmc.Stack(
                        [
                            dmc.TextInput(
                                id="report-name-input",
                                label="Report name",
                                placeholder="e.g. High Priority Frustrated",
                            ),
                            dmc.Group(
                                [
                                    dmc.Button("Save", id="confirm-save-report-btn", color="blue"),
                                    dmc.Button("Cancel", id="cancel-save-report-btn", variant="subtle", color="gray"),
                                ],
                                justify="flex-end",
                            ),
                        ],
                        gap="md",
                    ),
                ],
            ),
            # Hidden stores
            dcc.Store(id="report-filter-store", data={}),
            dcc.Store(id="saved-reports-store", data={str(r["id"]): r for r in reports}),
            # One-shot trigger to restore persisted filters after mount
            dcc.Interval(id="filter-restore-trigger", interval=200, max_intervals=1),
            # Sync progress panel (above the grid so it's visible)
            html.Div(id="sync-progress-panel", style={"display": "none"}),
            dcc.Interval(id="sync-poll-interval", interval=800, disabled=True),
            dcc.Interval(id="sync-dismiss-interval", interval=5000, disabled=True),
            dmc.Tabs(
                build_report_tabs(reports),
                id="ticket-view-tabs",
                value="open",
            ),
            grid_with_export(
                dag.AgGrid(
                    id="ticket-grid",
                    rowData=rows,
                    columnDefs=COLUMN_DEFS,
                    defaultColDef=DEFAULT_COL_DEF,
                    getRowId="params.data.ticket_id",
                    dashGridOptions={
                        "rowSelection": "single",
                        "pagination": True,
                        "paginationPageSize": 50,
                        "animateRows": False,
                        "enableCellTextSelection": True,
                    },
                    style={"height": "calc(100vh - 250px)", "cursor": "pointer"},
                    className="ag-theme-quartz",
                ),
                "ticket-grid",
            ),
        ],
        gap="sm",
    )


# ── Refresh callbacks — subprocess with live progress ────────────────

@callback(
    Output("sync-progress-panel", "children"),
    Output("sync-progress-panel", "style"),
    Output("sync-poll-interval", "disabled"),
    Output("sync-dismiss-interval", "disabled"),
    Output("refresh-tickets-btn", "disabled"),
    Output("ticket-grid", "rowData"),
    Output("ticket-count-badge", "children"),
    Input("refresh-tickets-btn", "n_clicks"),
    Input("sync-poll-interval", "n_intervals"),
    prevent_initial_call=True,
)
def handle_sync(_n_clicks, _n_intervals):
    """Start sync on button click, poll progress on interval tick."""
    trigger = ctx.triggered_id

    # ── Button click: kick off background sync ───────────────────────
    if trigger == "refresh-tickets-btn":
        with _sync_lock:
            if _sync_state["running"]:
                return no_update, no_update, no_update, no_update, no_update, no_update, no_update

        thread = threading.Thread(target=_run_sync_in_background, daemon=True)
        thread.start()

        initial_panel = dmc.Paper(
            dmc.Stack([
                dmc.Group([
                    dmc.Loader(size="xs", type="dots"),
                    dmc.Text("Syncing from TeamSupport…", size="sm", fw=500),
                ], gap="xs"),
                dmc.Code(
                    "[ingest] Starting…",
                    block=True,
                    style={
                        "maxHeight": "200px",
                        "overflowY": "auto",
                        "fontSize": "12px",
                        "whiteSpace": "pre-wrap",
                    },
                ),
            ], gap="xs"),
            shadow="xs",
            p="sm",
            withBorder=True,
        )
        return initial_panel, {}, False, True, True, no_update, no_update

    # ── Interval tick: poll for progress ─────────────────────────────
    with _sync_lock:
        lines = list(_sync_state["lines"])
        finished = _sync_state["finished"]
        return_code = _sync_state["return_code"]

    log_text = "\n".join(lines[-50:]) if lines else "[ingest] Starting…"

    if not finished:
        panel = dmc.Paper(
            dmc.Stack([
                dmc.Group([
                    dmc.Loader(size="xs", type="dots"),
                    dmc.Text("Syncing from TeamSupport…", size="sm", fw=500),
                ], gap="xs"),
                dmc.Code(
                    log_text,
                    block=True,
                    style={
                        "maxHeight": "200px",
                        "overflowY": "auto",
                        "fontSize": "12px",
                        "whiteSpace": "pre-wrap",
                    },
                ),
            ], gap="xs"),
            shadow="xs",
            p="sm",
            withBorder=True,
        )
        # Sync still running — don't touch grid data
        return panel, {}, False, True, True, no_update, no_update
    # Treat as success if exit code is 0 OR the log shows completion markers
    # (Python 3.13 can produce a non-zero exit on stdout pipe flush)
    log_has_done = any("] Done" in ln for ln in lines)
    ok = return_code == 0 or log_has_done
    color = "green" if ok else "red"
    icon = "tabler:check" if ok else "tabler:x"
    title = "Sync complete" if ok else "Sync failed"

    panel = dmc.Paper(
        dmc.Stack([
            dmc.Group([
                DashIconify(icon=icon, width=18, color=color),
                dmc.Text(title, size="sm", fw=500, c=color),
            ], gap="xs"),
            dmc.Code(
                log_text,
                block=True,
                style={
                    "maxHeight": "200px",
                    "overflowY": "auto",
                    "fontSize": "12px",
                    "whiteSpace": "pre-wrap",
                },
            ),
        ], gap="xs"),
        shadow="xs",
        p="sm",
        withBorder=True,
    )

    rows = data.get_ticket_list()
    # Stop polling, enable dismiss timer, re-enable button, refresh grid
    return panel, {}, True, False, False, rows, f"{len(rows)} tickets"


@callback(
    Output("sync-progress-panel", "children", allow_duplicate=True),
    Output("sync-progress-panel", "style", allow_duplicate=True),
    Output("ticket-grid", "rowData", allow_duplicate=True),
    Output("ticket-count-badge", "children", allow_duplicate=True),
    Output("sync-dismiss-interval", "disabled", allow_duplicate=True),
    Input("sync-dismiss-interval", "n_intervals"),
    prevent_initial_call=True,
)
def auto_dismiss_sync_panel(_n):
    """Auto-clear the sync panel after 5 seconds and refresh grid."""
    rows = data.get_ticket_list()
    return None, {"display": "none"}, rows, f"{len(rows)} tickets", True


@callback(
    Output("ticket-grid", "filterModel", allow_duplicate=True),
    Input("ticket-view-tabs", "value"),
    State("ticket-grid", "rowData"),
    State("ticket-grid", "filterModel"),
    State("saved-reports-store", "data"),
    prevent_initial_call=True,
)
def switch_tab(tab_value, all_rows, current_filter, saved_reports):
    """Apply open filter on initial load and when switching tabs.

    Tabs define their own baseline filter models. Saved report tabs apply the
    saved filter set exactly; the built-in Open and All Tickets tabs reset to
    their own defaults instead of inheriting filters from the previously
    selected report tab.

    The badge count is updated separately by the update_badge_from_grid
    callback which watches virtualRowData.
    """
    saved_reports = saved_reports or {}
    if tab_value == "open":
        return dict(_OPEN_FILTER_MODEL)
    if tab_value and str(tab_value).startswith("report:"):
        report_id = str(tab_value).split(":", 1)[1]
        report = saved_reports.get(report_id) or saved_reports.get(int(report_id), {})
        filter_model = (report or {}).get("filter_model") or {}
        return filter_model
    # "all" tab — clear any report-specific or prior-tab filters
    return {}


@callback(
    Output("ticket-count-badge", "children", allow_duplicate=True),
    Input("ticket-grid", "virtualRowData"),
    prevent_initial_call=True,
)
def update_badge_from_grid(virtual_rows):
    """Keep the ticket-count badge in sync with the grid's filtered row count."""
    count = len(virtual_rows) if virtual_rows else 0
    return f"{count} tickets"


@callback(
    Output("report-chip-group", "children"),
    Output("ticket-view-tabs", "children"),
    Input("saved-reports-store", "data"),
)
def render_saved_report_navigation(saved_reports):
    reports = normalize_saved_reports(saved_reports)
    return _build_report_chips(reports), build_report_tabs(reports)


@callback(
    Output("delete-report-btn", "disabled"),
    Output("delete-report-btn", "children"),
    Output("move-tab-left-btn", "disabled"),
    Output("move-tab-right-btn", "disabled"),
    Input("ticket-view-tabs", "value"),
    State("saved-reports-store", "data"),
)
def update_delete_report_button(tab_value, saved_reports):
    if not tab_value or not str(tab_value).startswith("report:"):
        return True, "Delete Report", True, True
    report_id = str(tab_value).split(":", 1)[1]
    report = (saved_reports or {}).get(report_id) or (saved_reports or {}).get(int(report_id), {})
    report_name = (report or {}).get("name")
    if not report_name:
        return True, "Delete Report", True, True
    return False, f"Delete {report_name}", False, False


@callback(
    Output("saved-reports-store", "data", allow_duplicate=True),
    Input("move-tab-left-btn", "n_clicks"),
    Input("move-tab-right-btn", "n_clicks"),
    State("ticket-view-tabs", "value"),
    State("saved-reports-store", "data"),
    prevent_initial_call=True,
)
def reorder_tab(left_clicks, right_clicks, tab_value, saved_reports):
    if not tab_value or not str(tab_value).startswith("report:"):
        raise PreventUpdate
    triggered = ctx.triggered_id
    if triggered == "move-tab-left-btn":
        direction = "left"
    elif triggered == "move-tab-right-btn":
        direction = "right"
    else:
        raise PreventUpdate
    report_id = int(str(tab_value).split(":", 1)[1])
    data.reorder_report(report_id, direction)
    # Refresh store with updated order
    rows = data.get_saved_reports()
    return {str(r["id"]): r for r in rows}
