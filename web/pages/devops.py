"""DOs page — Azure DevOps open work items (Bugs, Features, Tasks, Epics)."""

import os
import subprocess
import sys
import threading

import dash_ag_grid as dag
import dash_mantine_components as dmc
from dash import callback, ctx, dcc, Input, Output, no_update
from dash_iconify import DashIconify

from .. import data
from ..renderer import grid_with_export

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_IMPORT_SCRIPT = os.path.join(_PROJECT_ROOT, "run_import_work_items.py")

# ── Shared sync state ────────────────────────────────────────────────

_sync_state = {"running": False, "finished": False, "return_code": None, "lines": []}
_sync_lock = threading.Lock()


def _run_import_in_background():
    with _sync_lock:
        _sync_state["running"] = True
        _sync_state["finished"] = False
        _sync_state["return_code"] = None
        _sync_state["lines"] = []
    try:
        proc = subprocess.Popen(
            [sys.executable, _IMPORT_SCRIPT],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, cwd=_PROJECT_ROOT, env=os.environ.copy(),
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

_STATE_COLORS = {
    "New": "#228be6",
    "Active": "#40c057",
    "Code Complete": "#fab005",
    "Test Complete": "#be4bdb",
    "Rejected": "#fa5252",
    "On Hold": "#868e96",
    "Done": "#12b886",
    "In Testing": "#7950f2",
    "In Progress": "#15aabf",
}

COLUMN_DEFS = [
    {
        "field": "work_item_id",
        "headerName": "ID",
        "width": 90,
        "pinned": "left",
        "type": "numericColumn",
        "cellRenderer": "DOLink",
    },
    {"field": "project", "headerName": "Project", "width": 130},
    {"field": "work_item_type", "headerName": "Type", "width": 120},
    {
        "field": "title",
        "headerName": "Title",
        "minWidth": 250,
        "flex": 1,
        "tooltipField": "title",
    },
    {
        "field": "state",
        "headerName": "State",
        "width": 130,
        "cellStyle": {
            "function": """
                (() => {
                    const colors = {
                        'New': '#228be6', 'Active': '#40c057',
                        'Code Complete': '#fab005', 'Test Complete': '#be4bdb',
                        'Rejected': '#fa5252', 'On Hold': '#868e96',
                        'Done': '#12b886', 'In Testing': '#7950f2',
                        'In Progress': '#15aabf'
                    };
                    const c = colors[params.value];
                    return c ? {color: c, fontWeight: 'bold'} : {};
                })()
            """
        },
    },
    {"field": "assigned_to", "headerName": "Assigned To", "width": 150},
    {
        "field": "priority",
        "headerName": "Priority",
        "width": 95,
        "type": "numericColumn",
        "cellStyle": {
            "function": "params.value != null && params.value <= 1 ? {'color': '#e03131', 'fontWeight': 'bold'} : {}"
        },
    },
    {"field": "iteration_path", "headerName": "Iteration", "width": 180},
    {"field": "board_column", "headerName": "Board Column", "width": 130},
    {"field": "work_type", "headerName": "Work Type", "width": 120},
    {
        "field": "changed_date",
        "headerName": "Last Changed",
        "width": 140,
        "valueFormatter": {"function": "params.value ? new Date(params.value).toLocaleDateString() : ''"},
        "sort": "desc",
    },
    {
        "field": "created_date",
        "headerName": "Created",
        "width": 130,
        "valueFormatter": {"function": "params.value ? new Date(params.value).toLocaleDateString() : ''"},
    },
    {
        "field": "completed_work",
        "headerName": "Done (h)",
        "width": 95,
        "type": "numericColumn",
        "valueFormatter": {"function": "params.value != null ? params.value.toFixed(1) : ''"},
    },
    {
        "field": "remaining_work",
        "headerName": "Left (h)",
        "width": 95,
        "type": "numericColumn",
        "valueFormatter": {"function": "params.value != null ? params.value.toFixed(1) : ''"},
    },
]

DEFAULT_COL_DEF = {
    "sortable": True,
    "filter": True,
    "resizable": True,
    "floatingFilter": True,
}


# ── KPI stat card helper ────────────────────────────────────────────

def _stat_card(label, value, color="blue", icon=None):
    icon_el = DashIconify(icon=icon, width=24, color=f"var(--mantine-color-{color}-6)") if icon else None
    return dmc.Paper(
        dmc.Group(
            [
                dmc.Stack(
                    [
                        dmc.Text(label, size="xs", c="dimmed", fw=500),
                        dmc.Title(str(value), order=3),
                    ],
                    gap=0,
                ),
                icon_el,
            ] if icon_el else [
                dmc.Stack(
                    [
                        dmc.Text(label, size="xs", c="dimmed", fw=500),
                        dmc.Title(str(value), order=3),
                    ],
                    gap=0,
                ),
            ],
            justify="space-between",
        ),
        p="md",
        radius="md",
        withBorder=True,
        style={"minWidth": 160},
    )


# ── Layout ──────────────────────────────────────────────────────────

def devops_layout():
    rows = data.get_open_work_items()
    kpis = data.get_work_item_kpis() or {}

    grid = dag.AgGrid(
        id="devops-grid",
        rowData=rows,
        columnDefs=COLUMN_DEFS,
        defaultColDef=DEFAULT_COL_DEF,
        dashGridOptions={
            "pagination": True,
            "paginationPageSize": 50,
            "rowSelection": "single",
            "animateRows": True,
            "domLayout": "autoHeight",
        },
        style={"width": "100%"},
        className="ag-theme-quartz",
    )

    return dmc.Stack(
        [
            dmc.Group(
                [
                    dmc.Title("DevOps Work Items", order=2),
                    dmc.Button(
                        "Refresh from Azure DevOps",
                        id="refresh-devops-btn",
                        leftSection=DashIconify(icon="tabler:refresh", width=18),
                        variant="light",
                        size="sm",
                    ),
                ],
                justify="space-between",
                align="center",
            ),
            dmc.Box(id="devops-sync-panel", children=[]),
            dcc.Interval(id="devops-poll-interval", interval=1500, disabled=True),
            dmc.SimpleGrid(
                [
                    _stat_card("Open Items", kpis.get("total_open", 0), color="blue", icon="tabler:list-check"),
                    _stat_card("Bugs", kpis.get("bugs", 0), color="red", icon="tabler:bug"),
                    _stat_card("Features", kpis.get("features", 0), color="violet", icon="tabler:sparkles"),
                    _stat_card("Tasks", kpis.get("tasks", 0), color="teal", icon="tabler:subtask"),
                    _stat_card("Epics", kpis.get("epics", 0), color="orange", icon="tabler:trophy"),
                ],
                cols={"base": 2, "sm": 3, "lg": 5},
            ),
            grid_with_export(grid, "devops-grid"),
        ],
        gap="md",
    )


# ── Refresh callback ────────────────────────────────────────────────

@callback(
    Output("devops-sync-panel", "children"),
    Output("devops-poll-interval", "disabled"),
    Output("refresh-devops-btn", "disabled"),
    Output("devops-grid", "rowData"),
    Input("refresh-devops-btn", "n_clicks"),
    Input("devops-poll-interval", "n_intervals"),
    prevent_initial_call=True,
)
def handle_devops_sync(_n_clicks, _n_intervals):
    trigger = ctx.triggered_id

    if trigger == "refresh-devops-btn":
        with _sync_lock:
            if _sync_state["running"]:
                return no_update, no_update, no_update, no_update
        threading.Thread(target=_run_import_in_background, daemon=True).start()
        panel = dmc.Paper(
            dmc.Group([
                dmc.Loader(size="xs", type="dots"),
                dmc.Text("Importing work items from Azure DevOps…", size="sm", fw=500),
            ], gap="xs"),
            shadow="xs", p="sm", withBorder=True,
        )
        return panel, False, True, no_update

    # Interval tick — poll progress
    with _sync_lock:
        lines = list(_sync_state["lines"])
        finished = _sync_state["finished"]
        return_code = _sync_state["return_code"]

    if not finished:
        log_text = "\n".join(lines[-30:]) if lines else "Starting…"
        panel = dmc.Paper(
            dmc.Stack([
                dmc.Group([
                    dmc.Loader(size="xs", type="dots"),
                    dmc.Text("Importing work items from Azure DevOps…", size="sm", fw=500),
                ], gap="xs"),
                dmc.Code(log_text, block=True, style={
                    "maxHeight": "150px", "overflowY": "auto",
                    "fontSize": "12px", "whiteSpace": "pre-wrap",
                }),
            ], gap="xs"),
            shadow="xs", p="sm", withBorder=True,
        )
        return panel, False, True, no_update

    # Finished
    ok = return_code == 0
    log_text = "\n".join(lines[-30:]) if lines else ""
    panel = dmc.Paper(
        dmc.Stack([
            dmc.Group([
                DashIconify(
                    icon="tabler:check" if ok else "tabler:x",
                    width=18,
                    color="green" if ok else "red",
                ),
                dmc.Text(
                    "Import complete" if ok else "Import failed",
                    size="sm", fw=500,
                    c="green" if ok else "red",
                ),
            ], gap="xs"),
            dmc.Code(log_text, block=True, style={
                "maxHeight": "150px", "overflowY": "auto",
                "fontSize": "12px", "whiteSpace": "pre-wrap",
            }),
        ], gap="xs"),
        shadow="xs", p="sm", withBorder=True,
    )
    new_rows = data.get_open_work_items() if ok else no_update
    return panel, True, False, new_rows
