"""Overview dashboard page — KPI cards, backlog trend, aging, product split."""

import dash_ag_grid as dag
import dash_mantine_components as dmc
from dash import dcc, html
from dash_iconify import DashIconify
import plotly.graph_objects as go

import data
from renderer import grid_with_export


AGE_BUCKET_ORDER = ["0-6", "7-13", "14-29", "30-59", "60-89", "90+"]
AGE_BUCKET_COLORS = ["#1c7ed6", "#339af0", "#74c0fc", "#f59f00", "#e8590c", "#e03131"]


# ── Helpers ──────────────────────────────────────────────────────────

def _stat_card(title, value, icon, color, card_id=None):
    paper = dmc.Paper(
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
    if card_id:
        return html.Div(paper, id=card_id, n_clicks=0,
                         style={"cursor": "pointer"})
    return paper


def _backlog_chart(rows, severity_rows=None):
    if not rows:
        return dmc.Text("No backlog snapshot data available.", c="dimmed", ta="center", py="xl")
    dates = [r["snapshot_date"] for r in rows]
    open_vals = [r.get("open_backlog", 0) or 0 for r in rows]

    # Build severity time-series from severity_rows
    severity_config = [
        ("High",   "#e03131", "rgba(224, 49, 49, 0.25)"),
        ("Medium", "#f59f00", "rgba(245, 159, 0, 0.25)"),
        ("Low",    "#1c7ed6", "rgba(28, 126, 214, 0.25)"),
    ]

    fig = go.Figure()

    if severity_rows:
        # Pivot severity_rows into {date: {tier: count}}
        sev_by_date = {}
        for r in severity_rows:
            d = r["snapshot_date"]
            sev_by_date.setdefault(d, {})[r["severity_tier"]] = r["ticket_count"]

        # Use the same date axis as the total open backlog
        for tier, color, fillcolor in severity_config:
            vals = [sev_by_date.get(d, {}).get(tier, 0) for d in dates]
            fig.add_trace(go.Scatter(
                x=dates, y=vals, name=tier,
                stackgroup="severity",
                line=dict(width=0),
                fillcolor=fillcolor,
            ))

    # Total line on top (not stacked, just a line overlay)
    fig.add_trace(go.Scatter(
        x=dates, y=open_vals, name="Total Open",
        line=dict(color="#333", width=2),
        mode="lines",
    ))

    fig.update_layout(
        template="plotly_white",
        margin=dict(l=40, r=20, t=10, b=40),
        height=320,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis_title=None, yaxis_title="Tickets",
    )
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


# ── Drill-down column defs (reused for modal grid) ──────────────────

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


def _aging_chart(rows):
    if not rows:
        return dmc.Text("No aging data available.", c="dimmed", ta="center", py="xl")
    buckets = [r["age_bucket"] for r in rows]
    counts = [r["ticket_count"] for r in rows]

    fig = go.Figure(go.Bar(
        x=counts, y=buckets, orientation="h",
        marker_color=AGE_BUCKET_COLORS[: len(buckets)],
        text=counts, textposition="auto",
    ))
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=60, r=20, t=10, b=40),
        height=280,
        xaxis_title="Tickets", yaxis_title=None,
        yaxis=dict(autorange="reversed"),
    )
    return dcc.Graph(id="aging-chart", figure=fig, config={"displayModeBar": False},
                     style={"cursor": "pointer"})


def _aging_per_product_charts(rows):
    """Build a grid of small aging charts, one per product."""
    if not rows:
        return dmc.Text("No per-product aging data.", c="dimmed", ta="center", py="xl")

    # Group by product
    products = {}
    for r in rows:
        p = r["product_name"]
        products.setdefault(p, {"total": r["product_total"], "buckets": {}})
        products[p]["buckets"][r["age_bucket"]] = r["ticket_count"]

    # Sort by total descending
    sorted_names = sorted(products, key=lambda p: products[p]["total"], reverse=True)

    bucket_color_map = dict(zip(AGE_BUCKET_ORDER, AGE_BUCKET_COLORS))

    cards = []
    for idx, name in enumerate(sorted_names):
        info = products[name]
        buckets = []
        counts = []
        colors = []
        for b in AGE_BUCKET_ORDER:
            c = info["buckets"].get(b, 0)
            if c > 0:
                buckets.append(b)
                counts.append(c)
                colors.append(bucket_color_map[b])

        fig = go.Figure(go.Bar(
            x=counts, y=buckets, orientation="h",
            marker_color=colors,
            text=counts, textposition="auto",
            textfont=dict(size=10),
            customdata=[name] * len(buckets),
        ))
        fig.update_layout(
            template="plotly_white",
            margin=dict(l=50, r=10, t=6, b=20),
            height=160,
            xaxis_title=None, yaxis_title=None,
            yaxis=dict(autorange="reversed"),
            xaxis=dict(showticklabels=False),
        )

        chart_id = {"type": "aging-product-chart", "index": idx}
        cards.append(
            dmc.Paper(
                [
                    dmc.Text(f"{name} ({info['total']})", fw=600, size="sm", mb=4),
                    dcc.Graph(id=chart_id, figure=fig, config={"displayModeBar": False},
                              style={"height": "160px", "cursor": "pointer"}),
                ],
                withBorder=True, p="xs", radius="sm",
            )
        )

    return dmc.SimpleGrid(
        cols={"base": 1, "sm": 2, "lg": 3},
        children=cards,
    )


def _product_chart(rows):
    if not rows:
        return dmc.Text("No open tickets.", c="dimmed", ta="center", py="xl")

    # Pivot rows into {product: {severity_tier: count}}
    from collections import OrderedDict
    product_totals = {}
    product_data = {}
    for r in rows:
        p = r["product_name"]
        s = r["severity_tier"]
        c = r["ticket_count"]
        product_totals[p] = product_totals.get(p, 0) + c
        product_data.setdefault(p, {})[s] = c

    # Sort products by total descending, take top 12
    sorted_products = sorted(product_totals, key=lambda p: product_totals[p], reverse=True)[:12]
    # Reverse so largest is at the top of the horizontal bar chart
    sorted_products = list(reversed(sorted_products))

    severity_config = OrderedDict([
        ("High",   "#e03131"),
        ("Medium", "#f59f00"),
        ("Low",    "#1c7ed6"),
    ])

    fig = go.Figure()
    for tier, color in severity_config.items():
        values = [product_data.get(p, {}).get(tier, 0) for p in sorted_products]
        fig.add_trace(go.Bar(
            y=sorted_products, x=values, orientation="h",
            name=tier, marker_color=color,
            hovertemplate="%{y}: %{x} " + tier + "<extra></extra>",
        ))

    fig.update_layout(
        barmode="stack",
        template="plotly_white",
        margin=dict(l=120, r=20, t=10, b=40),
        height=max(280, len(sorted_products) * 28),
        xaxis_title="Open Tickets", yaxis_title=None,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return dcc.Graph(id="product-chart", figure=fig, config={"displayModeBar": False},
                     style={"cursor": "pointer"})


# ── Layout ───────────────────────────────────────────────────────────

def overview_layout():
    stats = data.get_open_ticket_stats()
    backlog = data.get_backlog_daily()
    backlog_by_severity = data.get_backlog_daily_by_severity()
    aging = data.get_backlog_aging()
    aging_by_product = data.get_aging_by_product()
    products = data.get_open_by_product()

    return dmc.Stack(
        [
            dmc.Title("Overview", order=2),

            # KPI row
            dmc.SimpleGrid(
                cols={"base": 1, "sm": 2, "lg": 4},
                children=[
                    _stat_card("Open Tickets", stats["total_open"],
                               "tabler:ticket", "blue", card_id="kpi-total-open"),
                    _stat_card("High Priority", stats["high_priority"],
                               "tabler:alert-triangle", "red", card_id="kpi-high-priority"),
                    _stat_card("High Complexity", stats["high_complexity"],
                               "tabler:brain", "orange", card_id="kpi-high-complexity"),
                    _stat_card("Frustrated", stats["frustrated"],
                               "tabler:mood-sad", "pink", card_id="kpi-frustrated"),
                ],
            ),

            # Backlog trend
            dmc.Paper(
                [
                    dmc.Text("Open Backlog Trend", fw=600, mb="xs"),
                    _backlog_chart(backlog, backlog_by_severity),
                ],
                withBorder=True, p="md", radius="md", shadow="sm",
            ),

            # Two-column row
            dmc.SimpleGrid(
                cols={"base": 1, "md": 2},
                children=[
                    dmc.Paper(
                        [
                            dmc.Text("Aging Distribution (days)", fw=600, mb="xs"),
                            _aging_chart(aging),
                            dmc.Button(
                                "Show per-product breakdown",
                                id="aging-toggle-btn",
                                variant="subtle",
                                color="gray",
                                size="compact-sm",
                                fullWidth=True,
                                mt="xs",
                                n_clicks=0,
                                leftSection=DashIconify(icon="tabler:chevron-down", width=16),
                            ),
                        ],
                        withBorder=True, p="md", radius="md", shadow="sm",
                    ),
                    dmc.Paper(
                        [
                            dmc.Text("Open by Product", fw=600, mb="xs"),
                            _product_chart(products),
                        ],
                        withBorder=True, p="md", radius="md", shadow="sm",
                    ),
                ],
            ),

            # Expandable per-product aging breakdown
            html.Div(
                dmc.Paper(
                    [
                        dmc.Text("Aging by Product (50+ open tickets)", fw=600, mb="sm"),
                        _aging_per_product_charts(aging_by_product),
                    ],
                    withBorder=True, p="md", radius="md", shadow="sm",
                ),
                id="aging-detail-collapse",
                style={"display": "none", "transition": "all 0.3s ease"},
            ),

            # Drill-down modal (hidden until a chart bar is clicked)
            dcc.Store(id="drilldown-store", data=None),
            dmc.Modal(
                id="drilldown-modal",
                title="Drill-down: Tickets",
                size="90%",
                centered=True,
                children=[
                    dmc.Text(id="drilldown-subtitle", size="sm", c="dimmed", mb="sm"),
                    grid_with_export(
                        dag.AgGrid(
                            id="drilldown-grid",
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
                        "drilldown-grid",
                    ),
                ],
            ),
        ],
        gap="md",
    )
