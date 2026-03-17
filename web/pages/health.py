"""Health dashboards — Customer and Product health AG Grid tables."""

import dash_ag_grid as dag
import dash_mantine_components as dmc

import data


# ── Customer health columns ──────────────────────────────────────────

CUSTOMER_COLS = [
    {"field": "customer", "headerName": "Customer", "minWidth": 160, "flex": 1, "pinned": "left"},
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


# ── Layout ───────────────────────────────────────────────────────────

def health_layout():
    customers = data.get_customer_health()
    products = data.get_product_health()

    customer_grid = dag.AgGrid(
        id="customer-health-grid",
        rowData=customers,
        columnDefs=CUSTOMER_COLS,
        defaultColDef=DEFAULT_COL_DEF,
        dashGridOptions={"pagination": True, "paginationPageSize": 25, "animateRows": True},
        style={"height": "calc(100vh - 280px)"},
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
                    dmc.TabsPanel(customer_grid, value="customer", pt="md"),
                    dmc.TabsPanel(product_grid, value="product", pt="md"),
                ],
                value="customer",
            ),
        ],
        gap="sm",
    )
