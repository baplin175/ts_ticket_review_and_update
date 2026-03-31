"""Key Account Tickets — open tickets for key accounts with health and DO context."""

import dash_ag_grid as dag
import dash_mantine_components as dmc
from dash import html
from dash_iconify import DashIconify

from .. import data
from ..renderer import grid_with_export, ticket_number_column


# ── Column definitions ───────────────────────────────────────────────

_HEALTH_BAND_COLORS = {
    "critical": "#e03131",
    "at_risk": "#f08c00",
    "watch": "#fab005",
    "healthy": "#2f9e44",
}

COLS = [
    ticket_number_column(width=100, pinned="left"),
    {"field": "customer", "headerName": "Customer", "width": 160, "pinned": "left",
     "rowGroup": False},
    {"field": "health_score", "headerName": "Health", "width": 90, "type": "numericColumn",
     "valueFormatter": {"function": "params.value != null ? Number(params.value).toFixed(1) : '—'"},
     "cellStyle": {"function": """
        var band = params.data && params.data.health_band;
        var colors = {critical: '#e03131', at_risk: '#f08c00', watch: '#fab005', healthy: '#2f9e44'};
        return {color: colors[band] || '#868e96', fontWeight: 'bold'};
     """}},
    {"field": "health_band", "headerName": "Band", "width": 90,
     "cellStyle": {"function": """
        var colors = {critical: '#e03131', at_risk: '#f08c00', watch: '#fab005', healthy: '#2f9e44'};
        return {color: colors[params.value] || '#868e96', fontWeight: '500'};
     """}},
    {"field": "ticket_name", "headerName": "Name", "minWidth": 200, "flex": 1,
     "tooltipField": "ticket_name"},
    {"field": "status", "headerName": "Status", "width": 130},
    {"field": "severity", "headerName": "Severity", "width": 110},
    {"field": "assignee", "headerName": "Assignee", "width": 130},
    {"field": "product_name", "headerName": "Product", "width": 130},
    {"field": "group_name", "headerName": "Group", "width": 140},
    {"field": "days_opened", "headerName": "Age (d)", "width": 85, "type": "numericColumn",
     "valueFormatter": {"function": "params.value != null ? Math.round(params.value) : ''"}},
    {"field": "priority", "headerName": "Pri", "width": 60, "type": "numericColumn"},
    {"field": "do_number", "headerName": "DO #", "width": 90,
     "cellRenderer": "DOLink"},
    {"field": "do_status", "headerName": "DO Status", "width": 110},
    {"field": "do_aligned", "headerName": "DO Align", "width": 100,
     "cellStyle": {"function": """
        if (params.value === 'yes') return {color: '#2f9e44', fontWeight: 'bold'};
        if (params.value === 'no') return {color: '#e03131', fontWeight: 'bold'};
        return {};
     """}},
    {"field": "do_mismatch_label", "headerName": "Mismatch", "width": 130},
    {"field": "frustrated", "headerName": "Frustrated", "width": 95},
    {"field": "date_created", "headerName": "Created", "width": 110},
]

DEFAULT_COL_DEF = {
    "sortable": True,
    "filter": True,
    "resizable": True,
    "floatingFilter": True,
    "filterParams": {"caseSensitive": False, "maxNumConditions": 10},
}


# ── Layout ───────────────────────────────────────────────────────────

def key_accounts_layout():
    rows = data.get_key_account_tickets()
    customer_count = len(set(r["customer"] for r in rows)) if rows else 0

    return dmc.Stack(
        [
            dmc.Group(
                [
                    dmc.Title("Key Account Tickets", order=2),
                    dmc.Badge(
                        f"{len(rows)} tickets across {customer_count} key accounts",
                        size="lg", variant="light",
                    ),
                ],
                justify="space-between",
            ),
            dmc.Text(
                "Open tickets for key accounts — includes health score, DO status, "
                "and alignment. Click a ticket number to view details.",
                size="sm", c="dimmed",
            ),
            grid_with_export(
                dag.AgGrid(
                    id="key-account-grid",
                    rowData=rows,
                    columnDefs=COLS,
                    defaultColDef=DEFAULT_COL_DEF,
                    dashGridOptions={
                        "animateRows": True,
                        "tooltipShowDelay": 200,
                    },
                    style={"height": "calc(100vh - 220px)", "width": "100%"},
                ),
                "key-account-grid",
            ),
        ],
        gap="md",
    )
