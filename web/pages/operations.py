"""Operations dashboard — analyst activity metrics and contribution patterns."""

import dash_ag_grid as dag
import dash_mantine_components as dmc
from dash import callback, dcc, html, Input, Output, State, no_update
from dash_iconify import DashIconify
import plotly.graph_objects as go

from .. import data
from ..renderer import grid_with_export, ticket_number_column


# ── Scorecard column defs ────────────────────────────────────────────

SCORECARD_COLS = [
    {"field": "assignee", "headerName": "Analyst", "minWidth": 150, "flex": 1.5,
     "pinned": "left"},
    {"field": "tickets_closed", "headerName": "Closed", "minWidth": 80, "flex": 1,
     "type": "numericColumn"},
    {"field": "avg_days_open", "headerName": "Avg Days Open", "minWidth": 100, "flex": 1,
     "type": "numericColumn",
     "valueFormatter": {"function": "params.value != null ? Number(params.value).toFixed(1) : '—'"}},
    {"field": "pct_high_severity", "headerName": "High Sev %", "minWidth": 90, "flex": 1,
     "type": "numericColumn",
     "valueFormatter": {"function": "params.value != null ? Number(params.value).toFixed(1) + '%' : '—'"},
     "cellStyle": {"function": "params.value != null && params.value < 15 ? {'color': '#e03131', 'fontWeight': 'bold'} : {}"}},
    {"field": "pct_technical", "headerName": "Technical %", "minWidth": 90, "flex": 1,
     "type": "numericColumn",
     "valueFormatter": {"function": "params.value != null ? Number(params.value).toFixed(1) + '%' : '—'"},
     "cellStyle": {"function": "params.value != null && params.value < 18 ? {'color': '#e03131', 'fontWeight': 'bold'} : {}"}},
    {"field": "pct_scheduling", "headerName": "Scheduling %", "minWidth": 90, "flex": 1,
     "type": "numericColumn",
     "valueFormatter": {"function": "params.value != null ? Number(params.value).toFixed(1) + '%' : '—'"},
     "cellStyle": {"function": "params.value != null && params.value > 20 ? {'color': '#f08c00', 'fontWeight': 'bold'} : {}"}},
    {"field": "high_priority_count", "headerName": "High Pri", "minWidth": 80, "flex": 1,
     "type": "numericColumn"},
    {"field": "frustrated_count", "headerName": "Frustrated", "minWidth": 80, "flex": 1,
     "type": "numericColumn"},
]

SWOOPER_DETAIL_COLS = [
    ticket_number_column(width=100, pinned="left"),
    {"field": "ticket_name", "headerName": "Name", "minWidth": 200, "flex": 1,
     "tooltipField": "ticket_name"},
    {"field": "status", "headerName": "Status", "width": 100},
    {"field": "severity", "headerName": "Severity", "width": 130},
    {"field": "product_name", "headerName": "Product", "width": 130},
    {"field": "customer", "headerName": "Customer", "width": 140},
    {"field": "days_opened", "headerName": "Age (d)", "width": 80, "type": "numericColumn",
     "valueFormatter": {"function": "params.value != null ? Math.round(params.value) : ''"}},
    {"field": "overall_complexity", "headerName": "Complexity", "width": 100,
     "type": "numericColumn"},
    {"field": "total_inh", "headerName": "Team Actions", "width": 110,
     "type": "numericColumn"},
    {"field": "own", "headerName": "Own Actions", "width": 105,
     "type": "numericColumn"},
    {"field": "own_ratio", "headerName": "Own %", "width": 80, "type": "numericColumn",
     "valueFormatter": {"function": "params.value != null ? (Number(params.value) * 100).toFixed(0) + '%' : '—'"},
     "cellStyle": {"function": "params.value != null && params.value < 0.25 ? {'color': '#e03131', 'fontWeight': 'bold'} : {}"}},
]

DEFAULT_COL_DEF = {
    "sortable": True,
    "filter": True,
    "resizable": True,
    "filterParams": {"caseSensitive": False},
}


# ── Chart builders ───────────────────────────────────────────────────

def _action_profile_chart(action_rows):
    """Horizontal grouped bar: technical work % and scheduling % per analyst."""
    if not action_rows:
        return dmc.Text("No data.", c="dimmed", ta="center", py="xl")

    rows = sorted(action_rows, key=lambda r: r.get("pct_scheduling") or 0, reverse=True)
    names = [r["assignee"] for r in rows]

    team_tech_avg = sum(r.get("pct_technical") or 0 for r in rows) / max(len(rows), 1)
    team_sched_avg = sum(r.get("pct_scheduling") or 0 for r in rows) / max(len(rows), 1)

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=names,
        x=[r.get("pct_technical") or 0 for r in rows],
        orientation="h",
        name="Technical Work %",
        marker_color="#339af0",
        text=[f"{r.get('pct_technical') or 0:.1f}%" for r in rows],
        textposition="outside",
    ))
    fig.add_trace(go.Bar(
        y=names,
        x=[r.get("pct_scheduling") or 0 for r in rows],
        orientation="h",
        name="Scheduling %",
        marker_color="#e03131",
        text=[f"{r.get('pct_scheduling') or 0:.1f}%" for r in rows],
        textposition="outside",
    ))
    fig.add_vline(x=team_tech_avg, line_dash="dash", line_color="#339af0",
                  annotation_text=f"Tech avg: {team_tech_avg:.1f}%",
                  annotation_position="top right",
                  annotation_font_color="#339af0")
    fig.update_layout(
        barmode="group",
        margin=dict(l=0, r=40, t=10, b=10),
        height=max(300, len(rows) * 40),
        xaxis_title="% of Own Actions",
        yaxis=dict(automargin=True),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    return dcc.Graph(figure=fig, id="action-profile-chart", config={"displayModeBar": False})


def _severity_chart(severity_rows):
    """Horizontal bar: high-severity % of closures per analyst."""
    if not severity_rows:
        return dmc.Text("No data.", c="dimmed", ta="center", py="xl")

    rows = sorted(severity_rows, key=lambda r: r.get("pct_high_severity") or 0)
    names = [r["assignee"] for r in rows]
    vals = [r.get("pct_high_severity") or 0 for r in rows]
    team_avg = sum(vals) / max(len(vals), 1)

    colors = [
        "#e03131" if v < team_avg * 0.65 else "#339af0" for v in vals
    ]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=names, x=vals, orientation="h",
        marker_color=colors,
        text=[f"{v:.1f}%" for v in vals],
        textposition="outside",
    ))
    fig.add_vline(x=team_avg, line_dash="dash", line_color="#868e96",
                  annotation_text=f"Team avg: {team_avg:.1f}%",
                  annotation_position="top right")
    fig.update_layout(
        margin=dict(l=0, r=20, t=10, b=10),
        height=max(300, len(rows) * 32),
        xaxis_title="High-Severity % of Closures",
        yaxis=dict(automargin=True),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    return dcc.Graph(figure=fig, id="severity-chart", config={"displayModeBar": False})


def _swooper_chart(scorecard):
    """Horizontal bar: own-work ratio per analyst."""
    if not scorecard:
        return dmc.Text("No data.", c="dimmed", ta="center", py="xl")

    rows = sorted(scorecard, key=lambda r: r.get("avg_own_work_ratio") or 1)

    names = [r["assignee"] for r in rows]
    vals = [(r.get("avg_own_work_ratio") or 1) * 100 for r in rows]
    colors = [
        "#e03131" if v < 50 else "#f08c00" if v < 70 else "#339af0"
        for v in vals
    ]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        y=names, x=vals, orientation="h",
        marker_color=colors,
        text=[f"{v:.0f}%" for v in vals],
        textposition="outside",
    ))
    fig.update_layout(
        margin=dict(l=0, r=20, t=10, b=10),
        height=max(300, len(rows) * 32),
        xaxis_title="Own Work % (actions on tickets they closed)",
        xaxis=dict(range=[0, 105]),
        yaxis=dict(automargin=True),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    return dcc.Graph(figure=fig, id="swooper-chart", config={"displayModeBar": False})


def _reassignment_chart(reassignment_rows):
    """Grouped bar: avg within-InHance handoffs per ticket by analyst and severity."""
    if not reassignment_rows:
        return dmc.Text("No data.", c="dimmed", ta="center", py="xl")

    # Pivot: {analyst: {severity: avg_handoffs}}
    by_analyst = {}
    all_sevs = sorted(set(r["severity"] or "Unknown" for r in reassignment_rows))
    for r in reassignment_rows:
        sev = r["severity"] or "Unknown"
        by_analyst.setdefault(r["assignee"], {})[sev] = r["avg_handoffs"] or 0

    # Sort analysts by overall avg desc
    analyst_avgs = [
        (a, sum(sv.values()) / max(len(sv), 1)) for a, sv in by_analyst.items()
    ]
    analyst_avgs.sort(key=lambda x: x[1], reverse=True)
    names = [a for a, _ in analyst_avgs]

    colors = ["#339af0", "#e03131", "#f08c00", "#40c057", "#be4bdb", "#868e96"]

    fig = go.Figure()
    for i, sev in enumerate(all_sevs):
        fig.add_trace(go.Bar(
            y=names,
            x=[by_analyst[a].get(sev, 0) for a in names],
            orientation="h",
            name=sev,
            marker_color=colors[i % len(colors)],
            text=[f"{by_analyst[a].get(sev, 0):.1f}" for a in names],
            textposition="outside",
        ))

    fig.update_layout(
        barmode="group",
        margin=dict(l=0, r=40, t=10, b=10),
        height=max(300, len(names) * 40),
        xaxis_title="Avg Handoffs per Ticket",
        yaxis=dict(automargin=True),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    return dcc.Graph(figure=fig, id="reassignment-chart", config={"displayModeBar": False})


def _linear_trend(xs, ys):
    """Return (slope, intercept) for a simple linear regression."""
    n = len(xs)
    if n < 2:
        return 0, (ys[0] if ys else 0)
    x_mean = sum(xs) / n
    y_mean = sum(ys) / n
    num = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys))
    den = sum((x - x_mean) ** 2 for x in xs)
    slope = num / den if den else 0
    return slope, y_mean - slope * x_mean


def _monthly_closure_chart(monthly_rows):
    """Line chart: monthly closures per analyst, with per-analyst trend lines.
    Team Total and its trend run on a secondary y-axis to preserve scale."""
    if not monthly_rows:
        return dmc.Text("No data.", c="dimmed", ta="center", py="xl")

    # Plotly default colorway (matches auto-assignment order)
    COLORS = [
        "#636EFA", "#EF553B", "#00CC96", "#AB63FA", "#FFA15A",
        "#19D3F3", "#FF6692", "#B6E880", "#FF97FF", "#FECB52",
    ]

    by_analyst = {}
    all_months = sorted(set(r["month"] for r in monthly_rows))
    for r in monthly_rows:
        by_analyst.setdefault(r["assignee"], {})[r["month"]] = r["closed_count"]

    totals = [(a, sum(m.values())) for a, m in by_analyst.items()]
    totals.sort(key=lambda x: -x[1])
    top = [a for a, _ in totals[:10]]

    fig = go.Figure()
    xs = list(range(len(all_months)))

    # Per-analyst lines + dashed trend in same colour
    for i, analyst in enumerate(top):
        color = COLORS[i % len(COLORS)]
        vals = [by_analyst[analyst].get(m, 0) for m in all_months]
        fig.add_trace(go.Scatter(
            x=all_months, y=vals, name=analyst, mode="lines+markers",
            line=dict(width=2, color=color), marker=dict(color=color),
        ))
        slope, intercept = _linear_trend(xs, vals)
        trend = [slope * x + intercept for x in xs]
        fig.add_trace(go.Scatter(
            x=all_months, y=trend,
            mode="lines",
            line=dict(width=1.5, color=color, dash="dash"),
            showlegend=False,
            hoverinfo="skip",
        ))

    # Team total on secondary y-axis (hidden by default to keep scale clean)
    team_totals = [sum(by_analyst[a].get(m, 0) for a in by_analyst) for m in all_months]
    fig.add_trace(go.Scatter(
        x=all_months, y=team_totals, name="Team Total",
        mode="lines+markers",
        line=dict(width=2, color="#1c1c1c", dash="dot"),
        marker=dict(size=5, color="#1c1c1c"),
        yaxis="y2",
        visible="legendonly",
    ))
    slope, intercept = _linear_trend(xs, team_totals)
    trend = [slope * x + intercept for x in xs]
    fig.add_trace(go.Scatter(
        x=all_months, y=trend, name="Trend (team)",
        mode="lines",
        line=dict(width=2, color="#e03131", dash="dash"),
        yaxis="y2",
        visible="legendonly",
        hovertemplate="%{x}: %{y:.0f} (trend)<extra></extra>",
    ))

    fig.update_layout(
        margin=dict(l=0, r=50, t=10, b=10),
        height=400,
        xaxis_title="Month",
        yaxis=dict(title="Tickets Closed"),
        yaxis2=dict(title="Team Total", overlaying="y", side="right", showgrid=False),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
    )
    return dcc.Graph(figure=fig, id="monthly-closure-chart", config={"displayModeBar": False})


# ── Merge helper ─────────────────────────────────────────────────────

def _merge_scorecard(scorecard, action_rows, severity_rows):
    """Merge action-profile and severity stats into scorecard rows by assignee."""
    action_map = {r["assignee"]: r for r in (action_rows or [])}
    severity_map = {r["assignee"]: r for r in (severity_rows or [])}
    for row in scorecard:
        a = row["assignee"]
        ap = action_map.get(a, {})
        row["pct_technical"] = ap.get("pct_technical")
        row["pct_scheduling"] = ap.get("pct_scheduling")
        sp = severity_map.get(a, {})
        row["pct_high_severity"] = sp.get("pct_high_severity")
    return scorecard


# ── Layout ───────────────────────────────────────────────────────────

def operations_layout():
    scorecard = data.get_analyst_scorecard(6)
    monthly = data.get_analyst_monthly_closures(12)
    action_profile = data.get_analyst_action_profile(6)
    severity_profile = data.get_analyst_severity_profile(6)
    reassignment_profile = data.get_analyst_reassignment_profile(6)
    scorecard = _merge_scorecard(scorecard, action_profile, severity_profile)

    # ── CS group KPIs
    avg_close_rows = data.get_ops_avg_days_to_close(months=6)
    backlog_snap = data.get_ops_backlog_snapshot()
    most_improved = data.get_ops_most_improved_customers(months=3)

    # Current month avg
    cur_month_avg = avg_close_rows[-1]["avg_days_to_close"] if avg_close_rows else None
    # Past 6 month overall avg
    if avg_close_rows:
        total_tickets = sum(r["tickets_closed"] for r in avg_close_rows)
        weighted = sum(
            float(r["avg_days_to_close"]) * r["tickets_closed"]
            for r in avg_close_rows
        )
        overall_avg = round(weighted / total_tickets, 1) if total_tickets else None
    else:
        overall_avg = None

    return dmc.Stack(
        [
            dmc.Title("Overview", order=2),
            dmc.Text(
                "Analyst activity metrics — team workload distribution, "
                "skill mix, and contribution patterns.",
                c="dimmed", size="sm",
            ),

            # ── CS Group KPI Cards
            dmc.SimpleGrid(
                cols={"base": 1, "sm": 2, "md": 4},
                spacing="md",
                children=[
                    # Avg days to close — current month
                    dmc.Paper(
                        [
                            dmc.Text("Avg Days to Close", c="dimmed", size="xs"),
                            dmc.Text("Current Month", c="dimmed", size="xs"),
                            dmc.Title(
                                f"{float(cur_month_avg):.1f}" if cur_month_avg else "—",
                                order=3,
                            ),
                        ],
                        withBorder=True, p="md", radius="md", shadow="sm",
                    ),
                    # Avg days to close — past 6 months
                    dmc.Paper(
                        [
                            dmc.Text("Avg Days to Close", c="dimmed", size="xs"),
                            dmc.Text("Past 6 Months", c="dimmed", size="xs"),
                            dmc.Title(
                                f"{overall_avg}" if overall_avg else "—",
                                order=3,
                            ),
                        ],
                        withBorder=True, p="md", radius="md", shadow="sm",
                    ),
                    # Backlog at Jan 1
                    dmc.Paper(
                        [
                            dmc.Text("CS Backlog", c="dimmed", size="xs"),
                            dmc.Text("Jan 1", c="dimmed", size="xs"),
                            dmc.Title(str(backlog_snap["jan1"]), order=3),
                        ],
                        withBorder=True, p="md", radius="md", shadow="sm",
                    ),
                    # Backlog now
                    dmc.Paper(
                        [
                            dmc.Text("CS Backlog", c="dimmed", size="xs"),
                            dmc.Text("Now", c="dimmed", size="xs"),
                            dmc.Group(
                                [
                                    dmc.Title(str(backlog_snap["now"]), order=3),
                                    dmc.Badge(
                                        f"{'↓' if backlog_snap['now'] <= backlog_snap['jan1'] else '↑'} "
                                        f"{abs(backlog_snap['now'] - backlog_snap['jan1'])}",
                                        color="green" if backlog_snap["now"] <= backlog_snap["jan1"] else "red",
                                        variant="light",
                                        size="lg",
                                    ),
                                ],
                                gap="xs",
                            ),
                        ],
                        withBorder=True, p="md", radius="md", shadow="sm",
                    ),
                ],
            ),

            # ── Most Improved Customers (last 3 months)
            dmc.Paper(
                [
                    dmc.Group(
                        [
                            DashIconify(icon="tabler:trending-down", width=22, color="#2f9e44"),
                            dmc.Text("Most Improved Customers (last 3 months)", fw=600, size="lg"),
                        ],
                        gap="xs", mb="xs",
                    ),
                    dmc.Text(
                        "CS-group customers whose open backlog decreased the most.",
                        c="dimmed", size="xs", mb="sm",
                    ),
                    dmc.Table(
                        [
                            dmc.TableThead(
                                dmc.TableTr([
                                    dmc.TableTh("Customer"),
                                    dmc.TableTh("3 Months Ago"),
                                    dmc.TableTh("Now"),
                                    dmc.TableTh("Reduction"),
                                ]),
                            ),
                            dmc.TableTbody(
                                [
                                    dmc.TableTr([
                                        dmc.TableTd(r["customer"]),
                                        dmc.TableTd(str(r["open_then"])),
                                        dmc.TableTd(str(r["open_now"])),
                                        dmc.TableTd(
                                            dmc.Badge(
                                                f"↓ {r['reduction']}",
                                                color="green", variant="light",
                                            ),
                                        ),
                                    ])
                                    for r in most_improved
                                ] if most_improved else [
                                    dmc.TableTr([
                                        dmc.TableTd("No improvement data available",
                                                    colSpan=4, ta="center"),
                                    ])
                                ],
                            ),
                        ],
                        striped=True, highlightOnHover=True,
                    ),
                ],
                withBorder=True, p="md", radius="md", shadow="sm",
            ),

            # ── Analyst Scorecard
            dmc.Paper(
                [
                    dmc.Group(
                        [
                            DashIconify(icon="tabler:users", width=22, color="#1c7ed6"),
                            dmc.Text("Analyst Scorecard (last 6 months)", fw=600, size="lg"),
                        ],
                        gap="xs",
                        mb="sm",
                    ),
                    dmc.Text(
                        "Highlighted rows indicate metrics that differ notably "
                        "from team averages. "
                        "Click a row to view ticket details.",
                        c="dimmed", size="xs", mb="sm",
                    ),
                    grid_with_export(
                        dag.AgGrid(
                            id="ops-scorecard-grid",
                            rowData=scorecard,
                            columnDefs=SCORECARD_COLS,
                            defaultColDef=DEFAULT_COL_DEF,
                            dashGridOptions={
                                "rowSelection": "single",
                                "animateRows": True,
                                "domLayout": "autoHeight",
                                "tooltipShowDelay": 200,
                            },
                            style={"width": "100%"},
                        ),
                        "ops-scorecard-grid",
                    ),
                ],
                withBorder=True, p="md", radius="md", shadow="sm",
            ),

            # ── Action profile + Severity side by side
            dmc.SimpleGrid(
                cols={"base": 1, "md": 2},
                spacing="md",
                children=[
                    dmc.Paper(
                        [
                            dmc.Group(
                                [
                                    DashIconify(icon="tabler:tool", width=22, color="#1c7ed6"),
                                    dmc.Text("Technical vs Scheduling Work", fw=600, size="lg"),
                                ],
                                gap="xs", mb="xs",
                            ),
                            dmc.Text(
                                "Breakdown of each analyst's actions into technical "
                                "problem-solving vs scheduling/coordination, "
                                "compared to team averages.",
                                c="dimmed", size="xs", mb="sm",
                            ),
                            _action_profile_chart(action_profile),
                        ],
                        withBorder=True, p="md", radius="md", shadow="sm",
                    ),
                    dmc.Paper(
                        [
                            dmc.Group(
                                [
                                    DashIconify(icon="tabler:alert-triangle", width=22, color="#e03131"),
                                    dmc.Text("High-Severity Closure Share", fw=600, size="lg"),
                                ],
                                gap="xs", mb="xs",
                            ),
                            dmc.Text(
                                "Percentage of each analyst's closures that were "
                                "high-severity (Sev 1) tickets, compared to "
                                "team average.",
                                c="dimmed", size="xs", mb="sm",
                            ),
                            _severity_chart(severity_profile),
                        ],
                        withBorder=True, p="md", radius="md", shadow="sm",
                    ),
                ],
            ),

            # ── Reassignment profile by severity
            dmc.Paper(
                [
                    dmc.Group(
                        [
                            DashIconify(icon="tabler:arrows-transfer-down", width=22, color="#1c7ed6"),
                            dmc.Text("Avg Handoffs per Ticket by Severity", fw=600, size="lg"),
                        ],
                        gap="xs", mb="xs",
                    ),
                    dmc.Text(
                        "Average number of times a ticket was passed between "
                        "InHance analysts before closure, broken down by severity.",
                        c="dimmed", size="xs", mb="sm",
                    ),
                    _reassignment_chart(reassignment_profile),
                ],
                withBorder=True, p="md", radius="md", shadow="sm",
            ),

            # ── Monthly trend
            dmc.Paper(
                [
                    dmc.Group(
                        [
                            DashIconify(icon="tabler:chart-line", width=22, color="#1c7ed6"),
                            dmc.Text("Monthly Closures by Analyst (12 months)", fw=600, size="lg"),
                        ],
                        gap="xs", mb="sm",
                    ),
                    _monthly_closure_chart(monthly),
                ],
                withBorder=True, p="md", radius="md", shadow="sm",
            ),

            # ── Swooper drilldown (hidden until row click)
            dmc.Modal(
                id="swooper-modal",
                title="Ticket Details",
                size="90%",
                opened=False,
                children=[
                    dmc.Text(id="swooper-modal-subtitle", c="dimmed", size="sm", mb="sm"),
                    grid_with_export(
                        dag.AgGrid(
                            id="ops-swooper-grid",
                            rowData=[],
                            columnDefs=SWOOPER_DETAIL_COLS,
                            defaultColDef=DEFAULT_COL_DEF,
                            dashGridOptions={
                                "rowSelection": "single",
                                "animateRows": True,
                                "domLayout": "autoHeight",
                                "tooltipShowDelay": 200,
                            },
                            style={"width": "100%"},
                        ),
                        "ops-swooper-grid",
                    ),
                ],
            ),
        ],
        gap="md",
    )


# ── Callbacks ────────────────────────────────────────────────────────

def register_operations_callbacks(app):

    @app.callback(
        Output("swooper-modal", "opened"),
        Output("swooper-modal", "title"),
        Output("swooper-modal-subtitle", "children"),
        Output("ops-swooper-grid", "rowData"),
        Input("ops-scorecard-grid", "selectedRows"),
        prevent_initial_call=True,
    )
    def scorecard_row_click(selected_rows):
        if not selected_rows:
            return False, no_update, no_update, no_update
        row = selected_rows[0]
        analyst = row.get("assignee")
        if not analyst:
            return False, no_update, no_update, no_update
        tickets = data.get_analyst_swooper_tickets(analyst, 6)
        subtitle = (
            f"{len(tickets)} ticket{'s' if len(tickets) != 1 else ''} "
            f"closed by {analyst} with under 25% of InHance actions"
        )
        return True, f"Ticket Details — {analyst}", subtitle, tickets
