"""Health dashboards — Customer and Product health AG Grid tables."""

import dash_ag_grid as dag
import dash_mantine_components as dmc
from dash import callback, dcc, html, Input, Output, State, no_update
from dash_iconify import DashIconify

from .. import data
from ..renderer import grid_with_export


# ── Customer health columns ──────────────────────────────────────────

CUSTOMER_COLS = [
    {"field": "customer", "headerName": "Customer", "minWidth": 160, "flex": 1, "pinned": "left",
     "checkboxSelection": True, "headerCheckboxSelection": True},
    {"field": "open_ticket_count", "headerName": "Open", "width": 80, "type": "numericColumn"},
    {"field": "high_priority_count", "headerName": "High Pri", "width": 90, "type": "numericColumn",
     "cellStyle": {"function": "params.value > 0 ? {'color': '#e03131', 'fontWeight': 'bold'} : {}"}},
    {"field": "high_complexity_count", "headerName": "High Cmplx", "width": 100, "type": "numericColumn",
     "cellStyle": {"function": "params.value > 0 ? {'color': '#e8590c', 'fontWeight': 'bold'} : {}"}},
    {"field": "avg_complexity", "headerName": "Avg Complexity", "width": 130, "type": "numericColumn",
     "valueFormatter": {"function": "params.value != null ? Number(params.value).toFixed(1) : ''"}},
    {"field": "frustration_count_90d", "headerName": "Frustrated (90d)", "width": 140, "type": "numericColumn",
     "cellStyle": {"function": "params.value > 0 ? {'color': '#c2255c', 'fontWeight': 'bold'} : {}"}},
    {"field": "ticket_load_pressure_score", "headerName": "Pressure Score", "width": 130, "type": "numericColumn",
     "valueFormatter": {"function": "params.value != null ? Number(params.value).toFixed(2) : ''"},
     "cellStyle": {"function": "params.value != null && params.value > 5 ? {'color': '#e03131', 'fontWeight': 'bold'} : {}"}},
    {"field": "as_of_date", "headerName": "As Of", "width": 110,
     "valueFormatter": {"function": "params.value ? new Date(params.value).toLocaleDateString() : ''"}},
]

# ── Product health columns ───────────────────────────────────────────

PRODUCT_COLS = [
    {"field": "product_name", "headerName": "Product", "minWidth": 160, "flex": 1, "pinned": "left"},
    {"field": "ticket_volume", "headerName": "Volume", "width": 90, "type": "numericColumn"},
    {"field": "avg_complexity", "headerName": "Avg Complexity", "width": 130, "type": "numericColumn",
     "valueFormatter": {"function": "params.value != null ? Number(params.value).toFixed(1) : ''"}},
    {"field": "avg_coordination_load", "headerName": "Avg Coord Load", "width": 140, "type": "numericColumn",
     "valueFormatter": {"function": "params.value != null ? Number(params.value).toFixed(1) : ''"}},
    {"field": "avg_elapsed_drag", "headerName": "Avg Elapsed Drag", "width": 140, "type": "numericColumn",
     "valueFormatter": {"function": "params.value != null ? Number(params.value).toFixed(1) : ''"}},
    {"field": "dev_touched_rate", "headerName": "Dev Touched %", "width": 130, "type": "numericColumn",
     "valueFormatter": {"function": "params.value != null ? (Number(params.value) * 100).toFixed(1) + '%' : ''"}},
    {"field": "customer_wait_rate", "headerName": "Cust Wait %", "width": 120, "type": "numericColumn",
     "valueFormatter": {"function": "params.value != null ? (Number(params.value) * 100).toFixed(1) + '%' : ''"}},
    {"field": "as_of_date", "headerName": "As Of", "width": 110,
     "valueFormatter": {"function": "params.value ? new Date(params.value).toLocaleDateString() : ''"}},
]

DEFAULT_COL_DEF = {
    "sortable": True,
    "filter": True,
    "resizable": True,
    "filterParams": {"caseSensitive": False},
}

# ── Drill-down column defs ───────────────────────────────────────────

DRILLDOWN_COL_DEFS = [
    {"field": "ticket_number", "headerName": "Ticket #", "width": 110, "pinned": "left"},
    {"field": "ticket_name", "headerName": "Name", "minWidth": 200, "flex": 1,
     "tooltipField": "ticket_name"},
    {"field": "status", "headerName": "Status", "width": 120},
    {"field": "severity", "headerName": "Severity", "width": 140},
    {"field": "product_name", "headerName": "Product", "width": 140},
    {"field": "assignee", "headerName": "Assignee", "width": 130},
    {"field": "customer", "headerName": "Customer", "width": 140},
    {"field": "days_opened", "headerName": "Age (d)", "width": 90, "type": "numericColumn",
     "valueFormatter": {"function": "params.value != null ? Math.round(params.value) : ''"}},
    {"field": "priority", "headerName": "Priority", "width": 95, "type": "numericColumn"},
    {"field": "overall_complexity", "headerName": "Complexity", "width": 110, "type": "numericColumn"},
    {"field": "frustrated", "headerName": "Frustrated", "width": 105},
    {"field": "date_modified", "headerName": "Last Modified", "width": 130,
     "valueFormatter": {"function": "params.value ? new Date(params.value).toLocaleDateString() : ''"},
     "sort": "desc"},
]


# ── Layout ───────────────────────────────────────────────────────────

def health_layout():
    customers = data.get_customer_health()
    products = data.get_product_health()

    customer_grid = dag.AgGrid(
        id="customer-health-grid",
        rowData=customers,
        columnDefs=CUSTOMER_COLS,
        defaultColDef=DEFAULT_COL_DEF,
        dashGridOptions={
            "pagination": True,
            "paginationPageSize": 25,
            "animateRows": True,
            "rowSelection": "multiple",
            "suppressRowClickSelection": True,
        },
        style={"height": "calc(100vh - 340px)"},
        className="ag-theme-quartz",
    ) if customers else dmc.Text("No customer health data available.", c="dimmed", ta="center", py="xl")

    product_grid = dag.AgGrid(
        id="product-health-grid",
        rowData=products,
        columnDefs=PRODUCT_COLS,
        defaultColDef=DEFAULT_COL_DEF,
        dashGridOptions={"pagination": True, "paginationPageSize": 25, "animateRows": True},
        style={"height": "calc(100vh - 280px)"},
        className="ag-theme-quartz",
    ) if products else dmc.Text("No product health data available.", c="dimmed", ta="center", py="xl")

    return dmc.Stack(
        [
            dmc.Title("Health Dashboards", order=2),
            dmc.Tabs(
                [
                    dmc.TabsList([
                        dmc.TabsTab("Customer Health", value="customer"),
                        dmc.TabsTab("Product Health", value="product"),
                    ]),
                    dmc.TabsPanel(
                        dmc.Stack([
                            dmc.Group(
                                [
                                    dmc.Text(
                                        "Select one or more customers, then click View Tickets.",
                                        size="sm", c="dimmed",
                                    ),
                                    dmc.Button(
                                        "View Tickets",
                                        id="health-drilldown-btn",
                                        leftSection=DashIconify(icon="tabler:list-search", width=16),
                                        variant="light",
                                        size="compact-sm",
                                        disabled=True,
                                    ),
                                ],
                                justify="space-between",
                            ),
                            customer_grid,
                        ], gap="xs"),
                        value="customer", pt="md",
                    ),
                    dmc.TabsPanel(product_grid, value="product", pt="md"),
                ],
                value="customer",
            ),
            # Drill-down modal for customer tickets
            dmc.Modal(
                id="health-drilldown-modal",
                title="Customer Tickets",
                size="90%",
                centered=True,
                children=[
                    dmc.Text(id="health-drilldown-subtitle", size="sm", c="dimmed", mb="sm"),
                    grid_with_export(
                        dag.AgGrid(
                            id="health-drilldown-grid",
                            rowData=[],
                            columnDefs=DRILLDOWN_COL_DEFS,
                            defaultColDef={
                                "sortable": True, "filter": True,
                                "resizable": True, "floatingFilter": True,
                                "filterParams": {"caseSensitive": False},
                            },
                            dashGridOptions={
                                "rowSelection": "single",
                                "pagination": True,
                                "paginationPageSize": 25,
                                "animateRows": True,
                                "enableCellTextSelection": True,
                            },
                            style={"height": "60vh", "cursor": "pointer"},
                            className="ag-theme-quartz",
                        ),
                        "health-drilldown-grid",
                    ),
                ],
            ),
        ],
        gap="sm",
    )


# ── Callbacks ────────────────────────────────────────────────────────

def register_health_callbacks(app):
    """Register health page callbacks. Called from app.py."""

    @app.callback(
        Output("health-drilldown-btn", "disabled"),
        Output("health-drilldown-btn", "children"),
        Input("customer-health-grid", "selectedRows"),
    )
    def toggle_drilldown_btn(selected):
        if not selected:
            return True, "View Tickets"
        n = len(selected)
        return False, f"View Tickets ({n} customer{'s' if n != 1 else ''})"

    @app.callback(
        Output("health-drilldown-modal", "opened"),
        Output("health-drilldown-grid", "rowData"),
        Output("health-drilldown-subtitle", "children"),
        Input("health-drilldown-btn", "n_clicks"),
        State("customer-health-grid", "selectedRows"),
        prevent_initial_call=True,
    )
    def open_drilldown(n_clicks, selected):
        if not n_clicks or not selected:
            return no_update, no_update, no_update
        names = [r["customer"] for r in selected]
        tickets = data.get_tickets_by_customers(names)
        label = ", ".join(names)
        subtitle = f"{len(tickets)} open ticket{'s' if len(tickets) != 1 else ''} for: {label}"
        return True, tickets, subtitle
