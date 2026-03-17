"""Ticket list page — AG Grid explorer with filters, row-click navigation, and saved reports."""

import dash_ag_grid as dag
import dash_mantine_components as dmc
from dash import dcc, html
from dash_iconify import DashIconify

import data
from renderer import grid_with_export


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
    {"field": "status", "headerName": "Status", "width": 120, "filter": "agSetColumnFilter"},
    {"field": "severity", "headerName": "Severity", "width": 140, "filter": "agSetColumnFilter"},
    {"field": "product_name", "headerName": "Product", "width": 140, "filter": "agSetColumnFilter"},
    {"field": "assignee", "headerName": "Assignee", "width": 130, "filter": "agSetColumnFilter"},
    {"field": "customer", "headerName": "Customer", "width": 140, "filter": "agSetColumnFilter"},
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
        "filter": "agSetColumnFilter",
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

    return dmc.Stack(
        [
            dmc.Group(
                [
                    dmc.Title("Tickets", order=2),
                    dmc.Badge(f"{len(rows)} tickets", size="lg", variant="light"),
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
            grid_with_export(
                dag.AgGrid(
                    id="ticket-grid",
                    rowData=rows,
                    columnDefs=COLUMN_DEFS,
                    defaultColDef=DEFAULT_COL_DEF,
                    getRowId="String(params.data.ticket_id)",
                    dashGridOptions={
                        "rowSelection": "single",
                        "pagination": True,
                        "paginationPageSize": 50,
                        "animateRows": True,
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
