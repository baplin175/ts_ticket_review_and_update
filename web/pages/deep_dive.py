"""Deep Dive — per-analyst / per-product operational analytics."""

import dash_ag_grid as dag
import dash_mantine_components as dmc
from dash import callback, dcc, html, Input, Output, State, no_update
from dash_iconify import DashIconify
import plotly.graph_objects as go

from .. import data
from ..renderer import grid_with_export, ticket_number_column


# ── Helpers ──────────────────────────────────────────────────────────

def _stat_card(title, value, icon, color):
    return dmc.Paper(
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
        withBorder=True, p="md", radius="md", shadow="sm",
    )


_ACTION_COLORS = {
    "technical_work": "#4263eb",
    "scheduling": "#e03131",
    "customer_problem_statement": "#2b8a3e",
    "status_update": "#f59f00",
    "waiting_on_customer": "#ae3ec9",
    "delivery_confirmation": "#1098ad",
    "administrative_noise": "#868e96",
    "system_noise": "#adb5bd",
    "unknown": "#ced4da",
}

_SEVERITY_COLORS = {
    "1 - Critical": "#e03131",
    "1 - High": "#e03131",
    "1 - High Priority": "#e03131",
    "2 - Medium": "#f08c00",
    "2 - Normal": "#f08c00",
    "2 - Medium Priority": "#f08c00",
    "3 - Low": "#2b8a3e",
    "3 - Low Priority": "#2b8a3e",
    "0: System Down Office Wide": "#c92a2a",
    "Low": "#2b8a3e",
    "Unknown": "#868e96",
}


def _severity_color(label):
    """Return a color for a severity label, falling back to pattern matching."""
    if label in _SEVERITY_COLORS:
        return _SEVERITY_COLORS[label]
    low = (label or "").lower()
    if "high" in low or "critical" in low or "system down" in low:
        return "#e03131"
    if low.startswith("0"):
        return "#c92a2a"
    if "medium" in low or "normal" in low or low.startswith("2"):
        return "#f08c00"
    if "low" in low or low.startswith("3"):
        return "#2b8a3e"
    return "#868e96"


def _pretty_action(raw):
    if not raw:
        return "Unknown"
    return raw.replace("_", " ").title()


# ── Grid columns ─────────────────────────────────────────────────────

_TICKET_COLS = [
    ticket_number_column(width=110, pinned="left"),
    {"field": "ticket_name", "headerName": "Name", "minWidth": 200, "flex": 1,
     "tooltipField": "ticket_name"},
    {"field": "status", "headerName": "Status", "width": 110},
    {"field": "severity", "headerName": "Severity", "width": 130},
    {"field": "product_name", "headerName": "Product", "width": 150},
    {"field": "assignee", "headerName": "Assignee", "width": 130},
    {"field": "customer", "headerName": "Customer", "width": 150},
    {"field": "days_opened", "headerName": "Age (d)", "width": 90, "type": "numericColumn",
     "valueFormatter": {"function": "params.value != null ? Math.round(params.value) : ''"}},
    {"field": "priority", "headerName": "Priority", "width": 90, "type": "numericColumn"},
    {"field": "overall_complexity", "headerName": "Complexity", "width": 105, "type": "numericColumn"},
    {"field": "frustrated", "headerName": "Frustrated", "width": 100,
     "cellStyle": {"function": "params.value === 'Yes' ? {'color': '#e03131', 'fontWeight': 'bold'} : {}"}},
    {"field": "handoff_count", "headerName": "Handoffs", "width": 95, "type": "numericColumn"},
    {"field": "hours_to_first_response", "headerName": "1st Resp (h)", "width": 110,
     "type": "numericColumn",
     "valueFormatter": {"function": "params.value != null ? Number(params.value).toFixed(1) : ''"}},
    {"field": "closed_at", "headerName": "Closed", "width": 120,
     "valueFormatter": {"function": "params.value ? new Date(params.value).toLocaleDateString() : ''"},
     "sort": "desc"},
]


# ── Chart builders ───────────────────────────────────────────────────

def _severity_bar(rows):
    if not rows:
        return dmc.Text("No data.", c="dimmed", ta="center", py="xl")
    labels = [r["severity"] for r in rows]
    values = [r["ticket_count"] for r in rows]
    colors = [_severity_color(l) for l in labels]
    fig = go.Figure(go.Bar(
        x=labels, y=values,
        marker_color=colors,
        text=values, textposition="auto",
        hovertemplate="%{x}: %{y} tickets<extra></extra>",
    ))
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=10, r=10, t=10, b=10),
        height=280,
        xaxis_title="Severity", yaxis_title="Tickets",
    )
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _action_donut(rows):
    if not rows:
        return dmc.Text("No data.", c="dimmed", ta="center", py="xl")
    labels = [_pretty_action(r["action_class"]) for r in rows]
    values = [r["action_count"] for r in rows]
    colors = [_ACTION_COLORS.get(r["action_class"], "#868e96") for r in rows]
    fig = go.Figure(go.Pie(
        labels=labels, values=values,
        hole=0.50,
        marker=dict(colors=colors),
        textinfo="label+percent",
        textposition="outside",
        hovertemplate="%{label}: %{value} actions (%{percent})<extra></extra>",
    ))
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=10, r=10, t=10, b=10),
        height=300,
        showlegend=False,
        annotations=[dict(text="Actions", x=0.5, y=0.5,
                          font_size=14, showarrow=False, font_color="#495057")],
    )
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _volume_trend(rows):
    if not rows:
        return dmc.Text("No data.", c="dimmed", ta="center", py="xl")
    months = [r["month"] for r in rows]
    counts = [r["closed_count"] for r in rows]
    fig = go.Figure(go.Scatter(
        x=months, y=counts, mode="lines+markers",
        line=dict(color="#4263eb", width=2),
        marker=dict(size=6),
        hovertemplate="%{x}: %{y} closed<extra></extra>",
    ))
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=10, r=10, t=10, b=10),
        height=280,
        xaxis_title="Month", yaxis_title="Tickets Closed",
    )
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _avg_days_to_close_trend(rows):
    if not rows:
        return dmc.Text("No data.", c="dimmed", ta="center", py="xl")
    months = [r["month"] for r in rows]
    avg_days = [float(r["avg_days_to_close"]) for r in rows]
    ticket_counts = [r["tickets_closed"] for r in rows]
    fig = go.Figure(go.Scatter(
        x=months, y=avg_days, mode="lines+markers",
        line=dict(color="#ae3ec9", width=2),
        marker=dict(size=6),
        customdata=ticket_counts,
        hovertemplate="%{x}<br>Avg: %{y:.1f} days<br>Tickets: %{customdata}<extra></extra>",
    ))
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=10, r=10, t=10, b=10),
        height=280,
        xaxis_title="Month", yaxis_title="Avg Days to Close",
    )
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _resolution_histogram(rows):
    if not rows:
        return dmc.Text("No data.", c="dimmed", ta="center", py="xl")
    buckets = [r["bucket"] for r in rows]
    counts = [r["ticket_count"] for r in rows]
    colors = ["#2b8a3e", "#4263eb", "#ae3ec9", "#f08c00", "#e03131"][:len(buckets)]
    fig = go.Figure(go.Bar(
        x=buckets, y=counts,
        marker_color=colors,
        text=counts, textposition="auto",
        hovertemplate="%{x}: %{y} tickets<extra></extra>",
    ))
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=10, r=10, t=10, b=10),
        height=280,
        xaxis_title="Resolution Time", yaxis_title="Tickets",
    )
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _time_by_resource_chart(rows):
    """Heatmap: analyst × month, colour = total hours logged."""
    if not rows:
        return dmc.Text("No data.", c="dimmed", ta="center", py="xl")

    months = sorted(set(r["month"] for r in rows))
    analysts = sorted(set(r["assignee"] for r in rows))
    lookup = {(r["month"], r["assignee"]): float(r["total_hours"] or 0) for r in rows}

    z, hover = [], []
    for analyst in analysts:
        z_row, h_row = [], []
        for m in months:
            val = lookup.get((m, analyst), 0)
            z_row.append(val)
            h_row.append(f"<b>{analyst}</b><br>{m}: {val:.1f}h")
        z.append(z_row)
        hover.append(h_row)

    fig = go.Figure(go.Heatmap(
        z=z,
        x=months,
        y=analysts,
        colorscale="Blues",
        hoverinfo="text",
        text=hover,
        texttemplate="%{z:.0f}",
        colorbar=dict(title="Hours", thickness=12),
    ))
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=10, r=10, t=10, b=50),
        height=max(240, len(analysts) * 32 + 60),
        xaxis=dict(title="Month", tickangle=-30, automargin=True, side="bottom"),
        yaxis=dict(autorange="reversed", automargin=True),
    )
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _heatmap_chart(rows):
    if not rows:
        return dmc.Text("No data.", c="dimmed", ta="center", py="xl")

    # Build pivot: analyst × product → ticket_count
    analysts = sorted(set(r["assignee"] for r in rows))
    products = sorted(set(r["product_name"] for r in rows))
    lookup = {(r["assignee"], r["product_name"]): r["ticket_count"] for r in rows}

    z = []
    hover = []
    for a in analysts:
        z_row = []
        h_row = []
        for p in products:
            val = lookup.get((a, p), 0)
            z_row.append(val)
            detail = next((r for r in rows if r["assignee"] == a and r["product_name"] == p), None)
            if detail:
                h_row.append(
                    f"{a} × {p}<br>"
                    f"Tickets: {val}<br>"
                    f"Avg Days: {detail.get('avg_days_open', '—')}<br>"
                    f"Avg Complexity: {detail.get('avg_complexity', '—')}<br>"
                    f"Frustrated: {detail.get('frustrated', 0)}"
                )
            else:
                h_row.append(f"{a} × {p}<br>Tickets: 0")
        z.append(z_row)
        hover.append(h_row)

    fig = go.Figure(go.Heatmap(
        z=z, x=products, y=analysts,
        colorscale="Blues",
        hoverinfo="text",
        text=hover,
        texttemplate="%{z}",
    ))
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=10, r=10, t=10, b=10),
        height=max(300, len(analysts) * 32),
        xaxis=dict(automargin=True, side="top"),
        yaxis=dict(automargin=True, autorange="reversed"),
    )
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


# ── Layout ───────────────────────────────────────────────────────────

def deep_dive_layout():
    assignee_opts, product_opts = data.get_deep_dive_filter_options()

    return dmc.Stack([
        dmc.Title("Deep Dive", order=2),
        dmc.Text("Filter by analyst and/or product to explore operational patterns across closed tickets.",
                  size="sm", c="dimmed"),

        # ── Filter bar ───────────────────────────────────────────────
        dmc.Paper([
            dmc.SimpleGrid(
                cols={"base": 1, "sm": 3},
                children=[
                    dmc.MultiSelect(
                        id="dd-assignee-filter",
                        label="Analyst",
                        data=assignee_opts,
                        placeholder="All analysts",
                        searchable=True,
                        clearable=True,
                    ),
                    dmc.MultiSelect(
                        id="dd-product-filter",
                        label="Product",
                        data=product_opts,
                        placeholder="All products",
                        searchable=True,
                        clearable=True,
                    ),
                    dmc.Select(
                        id="dd-months-filter",
                        label="Time Range",
                        data=[
                            {"value": "3", "label": "Last 3 months"},
                            {"value": "6", "label": "Last 6 months"},
                            {"value": "12", "label": "Last 12 months"},
                            {"value": "24", "label": "Last 24 months"},
                        ],
                        value="12",
                    ),
                ],
            ),
        ], withBorder=True, p="md", radius="md", shadow="sm"),

        # ── Dynamic content ──────────────────────────────────────────
        html.Div(id="dd-content", children=_build_content(None, None, 12)),
    ], gap="md")


def _build_content(assignees, products, months):
    kpis = data.get_deep_dive_kpis(assignees, products, months)
    severity = data.get_deep_dive_severity_breakdown(assignees, products, months)
    action_mix = data.get_deep_dive_action_mix(assignees, products, months)
    trend = data.get_deep_dive_volume_trend(assignees, products, months)
    avg_close_trend = data.get_deep_dive_avg_days_to_close(assignees, products, months)
    resolution = data.get_deep_dive_resolution_distribution(assignees, products, months)
    time_by_resource = data.get_deep_dive_time_by_resource(assignees, products, months)
    heatmap = data.get_deep_dive_product_analyst_heatmap(assignees, products, months)
    tickets = data.get_deep_dive_tickets(assignees, products, months)

    kpis = kpis or {}

    children = []

    # ── KPIs ─────────────────────────────────────────────────────────
    children.append(
        dmc.SimpleGrid(
            cols={"base": 2, "sm": 3, "lg": 5},
            children=[
                _stat_card("Tickets Closed", kpis.get("total_closed", 0),
                           "tabler:checkbox", "blue"),
                _stat_card("Avg Resolution (d)", kpis.get("avg_days_open", "—"),
                           "tabler:clock", "violet"),
                _stat_card("Avg Complexity", kpis.get("avg_complexity", "—"),
                           "tabler:brain", "teal"),
                _stat_card(
                    "Frustrated",
                    f"{kpis.get('frustrated_count', 0)} ({kpis.get('pct_frustrated', 0) or 0}%)",
                    "tabler:mood-sad", "red",
                ),
                _stat_card("Avg 1st Response (h)", kpis.get("avg_hours_first_response", "—"),
                           "tabler:message-bolt", "orange"),
            ],
        )
    )

    # ── Row 1: Severity + Action Mix ─────────────────────────────────
    children.append(
        dmc.SimpleGrid(
            cols={"base": 1, "md": 2},
            children=[
                dmc.Paper([
                    dmc.Text("Tickets by Severity", fw=600, mb="xs"),
                    _severity_bar(severity),
                ], withBorder=True, p="md", radius="md", shadow="sm"),
                dmc.Paper([
                    dmc.Text("Action Mix (InHance)", fw=600, mb="xs"),
                    _action_donut(action_mix),
                ], withBorder=True, p="md", radius="md", shadow="sm"),
            ],
        )
    )

    # ── Row 2: Volume Trend + Resolution Distribution ────────────────
    children.append(
        dmc.SimpleGrid(
            cols={"base": 1, "md": 2},
            children=[
                dmc.Paper([
                    dmc.Text("Monthly Closure Trend", fw=600, mb="xs"),
                    _volume_trend(trend),
                ], withBorder=True, p="md", radius="md", shadow="sm"),
                dmc.Paper([
                    dmc.Text("Resolution Time Distribution", fw=600, mb="xs"),
                    _resolution_histogram(resolution),
                ], withBorder=True, p="md", radius="md", shadow="sm"),
            ],
        )
    )

    # ── Row 3: Avg Days to Close Over Time ───────────────────────────
    children.append(
        dmc.Paper([
            dmc.Text("Avg Days to Close Over Time", fw=600, mb="xs"),
            dmc.Text(
                "Monthly average days from ticket creation to close. Hover for ticket count.",
                size="xs", c="dimmed", mb="xs",
            ),
            _avg_days_to_close_trend(avg_close_trend),
        ], withBorder=True, p="md", radius="md", shadow="sm")
    )

    # ── Time by Resource ─────────────────────────────────────────────
    if time_by_resource:
        children.append(
            dmc.Paper([
                dmc.Text("Hours Entered by Analyst", fw=600, mb="xs"),
                dmc.Text(
                    "Total hours logged per analyst per month, from action time entries.",
                    size="xs", c="dimmed", mb="xs",
                ),
                _time_by_resource_chart(time_by_resource),
            ], withBorder=True, p="md", radius="md", shadow="sm")
        )

    # ── Heatmap: Analyst × Product ───────────────────────────────────
    if heatmap:
        children.append(
            dmc.Paper([
                dmc.Text("Workload Heatmap: Analyst × Product", fw=600, mb="xs"),
                dmc.Text("Ticket count per analyst/product pair. Hover for avg days, complexity, and frustrated count.",
                         size="xs", c="dimmed", mb="xs"),
                _heatmap_chart(heatmap),
            ], withBorder=True, p="md", radius="md", shadow="sm")
        )

    # ── Ticket grid ──────────────────────────────────────────────────
    grid = dag.AgGrid(
        id="dd-ticket-grid",
        rowData=tickets,
        columnDefs=_TICKET_COLS,
        defaultColDef={"sortable": True, "filter": True, "resizable": True,
                       "floatingFilter": True, "filterParams": {"caseSensitive": False}},
        dashGridOptions={
            "rowSelection": "single",
            "pagination": True,
            "paginationPageSize": 25,
            "animateRows": True,
            "enableCellTextSelection": True,
        },
        style={"height": "500px"},
        className="ag-theme-quartz",
    )
    children.append(
        dmc.Paper([
            dmc.Text(f"Matching Tickets ({len(tickets)})", fw=600, mb="xs"),
            grid_with_export(grid, "dd-ticket-grid"),
        ], withBorder=True, p="md", radius="md", shadow="sm")
    )

    return dmc.Stack(children, gap="md")


# ── Callbacks ────────────────────────────────────────────────────────

def register_deep_dive_callbacks(app):
    @app.callback(
        Output("dd-content", "children"),
        Input("dd-assignee-filter", "value"),
        Input("dd-product-filter", "value"),
        Input("dd-months-filter", "value"),
    )
    def update_content(assignees, products, months_str):
        months = int(months_str) if months_str else 12
        a = assignees if assignees else None
        p = products if products else None
        return _build_content(a, p, months)
