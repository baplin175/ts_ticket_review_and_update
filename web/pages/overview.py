"""Overview dashboard page — KPI cards, backlog trend, aging, product split."""

import dash_ag_grid as dag
import dash_mantine_components as dmc
from dash import callback, dcc, html, Input, Output, State, no_update
from dash_iconify import DashIconify
import plotly.graph_objects as go

from .. import data
from ..renderer import grid_with_export, ticket_number_column


AGE_BUCKET_ORDER = ["0-6", "7-13", "14-29", "30-59", "60-89", "90+"]
AGE_BUCKET_COLORS = ["#1c7ed6", "#339af0", "#74c0fc", "#f59f00", "#e8590c", "#e03131"]


# ── Multi-select filter config ───────────────────────────────────────

_FILTER_FIELDS = [
    {"field": "status",       "label": "Status"},
    {"field": "severity",     "label": "Severity"},
    {"field": "product_name", "label": "Product"},
    {"field": "assignee",     "label": "Assignee"},
    {"field": "customer",     "label": "Customer"},
    {"field": "group_name",   "label": "Group"},
    {"field": "frustrated",   "label": "Frustrated"},
]


# ── Helpers ──────────────────────────────────────────────────────────

def _stat_card(title, value, icon, color, card_id=None, value_id=None):
    value_el = dmc.Title(str(value), order=3, id=value_id) if value_id else dmc.Title(str(value), order=3)
    paper = dmc.Paper(
        [
            dmc.Group(
                [
                    dmc.Stack(
                        [
                            dmc.Text(title, size="xs", c="dimmed", tt="uppercase", fw=700),
                            value_el,
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
    ticket_number_column(width=110, pinned="left"),
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


def _frustrated_by_group_chart(open_tickets):
    """Horizontal bar chart: frustrated open ticket count by group."""
    from collections import Counter
    counts = Counter()
    for r in open_tickets:
        if r.get("frustrated") == "Yes":
            g = r.get("group_name") or "Unassigned"
            counts[g] += 1
    if not counts:
        return dmc.Text("No frustrated open tickets.", c="dimmed", ta="center", py="xl")
    sorted_groups = sorted(counts, key=counts.get, reverse=True)
    sorted_groups = list(reversed(sorted_groups))  # largest at top
    values = [counts[g] for g in sorted_groups]
    fig = go.Figure(go.Bar(
        y=sorted_groups, x=values, orientation="h",
        marker_color="#e03131",
        text=values, textposition="auto",
        hovertemplate="%{y}: %{x} frustrated<extra></extra>",
    ))
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=180, r=20, t=10, b=40),
        height=max(200, len(sorted_groups) * 32),
        xaxis_title="Frustrated Tickets", yaxis_title=None,
    )
    return dcc.Graph(id="frustrated-group-chart", figure=fig,
                     config={"displayModeBar": False},
                     style={"cursor": "pointer"})


# ── Filter bar helpers ───────────────────────────────────────────────

def _distinct_values(rows, field):
    """Return sorted distinct non-null values for a field."""
    return sorted({str(r.get(field)) for r in rows if r.get(field) is not None and str(r.get(field)).strip()})


def _build_filter_bar(rows):
    """Build a row of MultiSelect dropdowns for categorical ticket fields."""
    selects = []
    for f in _FILTER_FIELDS:
        options = _distinct_values(rows, f["field"])
        selects.append(
            dmc.MultiSelect(
                id={"type": "overview-filter-select", "field": f["field"]},
                label=f["label"],
                data=options,
                placeholder="All",
                searchable=True,
                clearable=True,
                size="xs",
                style={"minWidth": 150, "flex": "1 1 150px"},
            )
        )
    return dmc.Paper(
        dmc.Group(selects, gap="xs", grow=True, align="flex-end"),
        p="sm",
        withBorder=True,
        radius="md",
    )


def _compute_stats(rows):
    """Compute KPI stats from a list of open ticket dicts."""
    total = len(rows)
    high_pri = sum(1 for r in rows if r.get("priority") is not None and r["priority"] <= 3)
    high_cx = sum(1 for r in rows if r.get("overall_complexity") is not None and r["overall_complexity"] >= 4)
    frustrated = sum(1 for r in rows if r.get("frustrated") == "Yes")
    return {"total_open": total, "high_priority": high_pri, "high_complexity": high_cx, "frustrated": frustrated}


def _build_aging_from_tickets(rows):
    """Compute aging distribution from ticket-level days_opened."""
    buckets = {b: 0 for b in AGE_BUCKET_ORDER}
    for r in rows:
        d = r.get("days_opened")
        if d is None:
            continue
        d = float(d)
        if d < 7:
            buckets["0-6"] += 1
        elif d < 14:
            buckets["7-13"] += 1
        elif d < 30:
            buckets["14-29"] += 1
        elif d < 60:
            buckets["30-59"] += 1
        elif d < 90:
            buckets["60-89"] += 1
        else:
            buckets["90+"] += 1
    return [{"age_bucket": b, "ticket_count": buckets[b]} for b in AGE_BUCKET_ORDER if buckets[b] > 0]


def _severity_tier(severity_text):
    """Map severity string to tier, matching the DB view logic."""
    s = (severity_text or "").lower()
    if s.startswith("1") or "high" in s:
        return "High"
    if s.startswith("3") or "low" in s:
        return "Low"
    return "Medium"


def _consolidate_product(name):
    """Consolidate PM/Power* variants into 'PowerMan', matching the DB view."""
    p = (name or "").strip()
    pl = p.lower()
    if pl.startswith("pm") or "power" in pl:
        return "PowerMan"
    return p if p else "Unknown"


def _build_product_from_tickets(rows):
    """Compute product × severity distribution from ticket-level data."""
    from collections import defaultdict
    product_sev = defaultdict(lambda: defaultdict(int))
    for r in rows:
        p = _consolidate_product(r.get("product_name"))
        tier = _severity_tier(r.get("severity"))
        product_sev[p][tier] += 1
    result = []
    for p, tiers in product_sev.items():
        for tier, count in tiers.items():
            result.append({"product_name": p, "severity_tier": tier, "ticket_count": count})
    return result


# ── Layout ───────────────────────────────────────────────────────────

def overview_layout():
    stats = data.get_open_ticket_stats()
    backlog = data.get_backlog_daily()
    backlog_by_severity = data.get_backlog_daily_by_severity()
    aging = data.get_backlog_aging()
    aging_by_product = data.get_aging_by_product()
    products = data.get_open_by_product()
    # Fetch open tickets for filter dropdowns
    all_tickets = data.get_ticket_list()
    open_tickets = [r for r in all_tickets if (r.get("status") or "").lower() != "closed"]

    return dmc.Stack(
        [
            dmc.Title("Overview", order=2),

            # Multi-select filter bar
            _build_filter_bar(open_tickets),
            # Hidden store for open ticket data (used by filter callback)
            dcc.Store(id="overview-ticket-store", data=open_tickets),

            # KPI row (dynamic — updated by filter callback)
            html.Div(
                dmc.SimpleGrid(
                    cols={"base": 1, "sm": 2, "lg": 4},
                    children=[
                        _stat_card("Open Tickets", stats["total_open"],
                                   "tabler:ticket", "blue", card_id="kpi-total-open",
                                   value_id="kpi-val-total-open"),
                        _stat_card("High Priority", stats["high_priority"],
                                   "tabler:alert-triangle", "red", card_id="kpi-high-priority",
                                   value_id="kpi-val-high-priority"),
                        _stat_card("High Complexity", stats["high_complexity"],
                                   "tabler:brain", "orange", card_id="kpi-high-complexity",
                                   value_id="kpi-val-high-complexity"),
                        _stat_card("Frustrated", stats["frustrated"],
                                   "tabler:mood-sad", "pink", card_id="kpi-frustrated",
                                   value_id="kpi-val-frustrated"),
                    ],
                ),
                id="overview-kpi-row",
            ),

            # Backlog trend
            dmc.Paper(
                [
                    dmc.Text("Open Backlog Trend", fw=600, mb="xs"),
                    html.Div(_backlog_chart(backlog, backlog_by_severity), id="overview-backlog-chart"),
                ],
                withBorder=True, p="md", radius="md", shadow="sm",
            ),

            # Filter status indicator (hidden when no filters active)
            html.Div(id="overview-filter-indicator"),

            # Two-column row (dynamic — updated by filter callback)
            dmc.SimpleGrid(
                cols={"base": 1, "md": 2},
                children=[
                    dmc.Paper(
                        [
                            dmc.Text("Aging Distribution (days)", fw=600, mb="xs"),
                            html.Div(_aging_chart(aging), id="overview-aging-chart"),
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
                            html.Div(_product_chart(products), id="overview-product-chart"),
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

            # Frustrated by group
            dmc.Paper(
                [
                    dmc.Text("Frustrated Open Tickets by Group", fw=600, mb="xs"),
                    html.Div(_frustrated_by_group_chart(open_tickets),
                             id="overview-frustrated-group-chart"),
                ],
                withBorder=True, p="md", radius="md", shadow="sm",
            ),

            # Drill-down modal (hidden until a chart bar is clicked)
            dcc.Store(id="drilldown-store", data=None),
            dcc.Store(id="overview-active-filters", data={}),
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


# ── Multi-select filter callback ────────────────────────────────────

def register_overview_callbacks(app):
    """Register overview filter callbacks. Called from app.py."""

    @app.callback(
        Output("kpi-val-total-open", "children"),
        Output("kpi-val-high-priority", "children"),
        Output("kpi-val-high-complexity", "children"),
        Output("kpi-val-frustrated", "children"),
        Output("overview-backlog-chart", "children"),
        Output("overview-aging-chart", "children"),
        Output("overview-product-chart", "children"),
        Output("overview-frustrated-group-chart", "children"),
        Output("overview-filter-indicator", "children"),
        Output("overview-active-filters", "data"),
        *[Input({"type": "overview-filter-select", "field": f["field"]}, "value")
          for f in _FILTER_FIELDS],
        State("overview-ticket-store", "data"),
        prevent_initial_call=True,
    )
    def apply_overview_filters(*args):
        """Recompute KPIs, backlog trend, aging, and product charts from filtered data."""
        all_rows = args[-1]
        filter_values = args[:-1]

        if not all_rows:
            return (no_update,) * 10

        # Check if any filters are actually active
        any_active = any(v for v in filter_values if v)
        if not any_active:
            # No filters — revert to original DB-view data
            stats = data.get_open_ticket_stats()
            backlog = data.get_backlog_daily()
            backlog_sev = data.get_backlog_daily_by_severity()
            all_open = [r for r in all_rows if (r.get("status") or "").lower() != "closed"]
            return (
                str(stats["total_open"]),
                str(stats["high_priority"]),
                str(stats["high_complexity"]),
                str(stats["frustrated"]),
                _backlog_chart(backlog, backlog_sev),
                _aging_chart(data.get_backlog_aging()),
                _product_chart(data.get_open_by_product()),
                _frustrated_by_group_chart(all_open),
                None,
                {},
            )

        # Build filter dict for DB query and drill-down
        filter_dict = {}
        for i, f in enumerate(_FILTER_FIELDS):
            if filter_values[i]:
                filter_dict[f["field"]] = filter_values[i]

        # Apply multi-select filters to ticket store for KPIs/aging/product
        filtered = all_rows
        active_filters = []
        for i, f in enumerate(_FILTER_FIELDS):
            selected = filter_values[i]
            if selected:
                field = f["field"]
                selected_set = set(selected)
                filtered = [r for r in filtered if str(r.get(field, "")) in selected_set]
                active_filters.append(f"{f['label']}: {', '.join(selected)}")

        # Only include open tickets (exclude Closed)
        filtered = [r for r in filtered if (r.get("status") or "").lower() != "closed"]

        # Recompute KPIs
        stats = _compute_stats(filtered)

        # Recompute backlog trend from DB with filters
        backlog_rows, backlog_sev_rows = data.get_filtered_backlog_daily(filter_dict)
        backlog_chart = _backlog_chart(backlog_rows, backlog_sev_rows)

        # Recompute aging
        aging_rows = _build_aging_from_tickets(filtered)
        aging_chart = _aging_chart(aging_rows) if aging_rows else dmc.Text(
            "No tickets match the current filters.", c="dimmed", ta="center", py="xl")

        # Recompute product breakdown
        product_rows = _build_product_from_tickets(filtered)
        product_chart = _product_chart(product_rows) if product_rows else dmc.Text(
            "No tickets match the current filters.", c="dimmed", ta="center", py="xl")

        # Filter indicator
        indicator = dmc.Alert(
            f"Showing {len(filtered)} of {len(all_rows)} open tickets",
            title="Filters active",
            color="blue",
            variant="light",
            icon=DashIconify(icon="tabler:filter", width=20),
            radius="md",
            withCloseButton=False,
        ) if active_filters else None

        frustrated_chart = _frustrated_by_group_chart(filtered)

        return (
            str(stats["total_open"]),
            str(stats["high_priority"]),
            str(stats["high_complexity"]),
            str(stats["frustrated"]),
            backlog_chart,
            aging_chart,
            product_chart,
            frustrated_chart,
            indicator,
            filter_dict,
        )
