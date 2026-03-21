"""Ticket list page — AG Grid explorer with filters, row-click navigation, and saved reports."""

import subprocess
import sys
import os
import threading
import time

import dash_ag_grid as dag
import dash_mantine_components as dmc
from dash import callback, ctx, dcc, html, Input, Output, State, no_update
from dash_iconify import DashIconify

import data
from renderer import grid_with_export

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
    {
        "field": "ticket_number",
        "headerName": "Ticket #",
        "width": 110,
        "pinned": "left",
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
]

DEFAULT_COL_DEF = {
    "sortable": True,
    "filter": True,
    "resizable": True,
    "floatingFilter": True,
    "filterParams": {"caseSensitive": False},
}


# ── Open-ticket filter model for AG Grid ─────────────────────────────

_OPEN_FILTER_MODEL = {
    "status": {
        "filterType": "text",
        "type": "notEqual",
        "filter": "Closed",
    }
}


# ── Layout ───────────────────────────────────────────────────────────

def _build_report_chips(reports):
    """Build a row of clickable report chips with delete buttons."""
    if not reports:
        return []
    chips = []
    for r in reports:
        chips.append(
            dmc.Group(
                [
                    dmc.Button(
                        r["name"],
                        id={"type": "report-chip", "index": r["id"]},
                        variant="light",
                        color="violet",
                        size="compact-sm",
                        radius="xl",
                    ),
                    dmc.ActionIcon(
                        DashIconify(icon="tabler:x", width=12),
                        id={"type": "report-delete", "index": r["id"]},
                        variant="subtle",
                        color="gray",
                        size="xs",
                        radius="xl",
                    ),
                ],
                gap=2,
            )
        )
    return chips


def tickets_layout():
    rows = data.get_ticket_list()
    reports = data.get_saved_reports()
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
                    dmc.Group(
                        _build_report_chips(reports),
                        id="report-chip-group",
                        gap="xs",
                    ),
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
            dcc.Store(id="saved-reports-store", data={str(r["id"]): r["filter_model"] for r in reports}),
            # One-shot trigger to restore persisted filters after mount
            dcc.Interval(id="filter-restore-trigger", interval=200, max_intervals=1),
            # Sync progress panel (above the grid so it's visible)
            html.Div(id="sync-progress-panel", style={"display": "none"}),
            dcc.Interval(id="sync-poll-interval", interval=800, disabled=True),
            dcc.Interval(id="sync-dismiss-interval", interval=5000, disabled=True),
            dmc.Tabs(
                [
                    dmc.TabsList([
                        dmc.TabsTab("Open", value="open"),
                        dmc.TabsTab("All Tickets", value="all"),
                    ]),
                ],
                id="ticket-view-tabs",
                value="open",
            ),
            grid_with_export(
                dag.AgGrid(
                    id="ticket-grid",
                    rowData=rows,
                    columnDefs=COLUMN_DEFS,
                    defaultColDef=DEFAULT_COL_DEF,
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
    Output("ticket-count-badge", "children", allow_duplicate=True),
    Input("ticket-view-tabs", "value"),
    State("ticket-grid", "rowData"),
    State("ticket-grid", "filterModel"),
    prevent_initial_call=True,
)
def switch_tab(tab_value, all_rows, current_filter):
    """Apply open filter on initial load and when switching tabs.

    Merges the status filter into (or removes it from) the existing
    filterModel so that user-applied column filters are preserved.
    """
    merged = dict(current_filter) if current_filter else {}
    if tab_value == "open":
        merged["status"] = _OPEN_FILTER_MODEL["status"]
        count = sum(1 for r in (all_rows or []) if (r.get("status") or "").lower() != "closed")
        return merged, f"{count} open tickets"
    # "all" tab — remove the status filter but keep everything else
    merged.pop("status", None)
    count = len(all_rows or [])
    return merged, f"{count} tickets"
