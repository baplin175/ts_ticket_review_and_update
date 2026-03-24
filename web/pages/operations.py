"""Operations dashboard — analyst behaviour analytics for detecting gaming."""

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
    """Horizontal bar: own-work ratio per analyst (lower = more swooping)."""
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


def _monthly_closure_chart(monthly_rows):
    """Line chart: monthly closures per analyst."""
    if not monthly_rows:
        return dmc.Text("No data.", c="dimmed", ta="center", py="xl")

    # Pivot: {analyst: {month: count}}
    by_analyst = {}
    all_months = sorted(set(r["month"] for r in monthly_rows))
    for r in monthly_rows:
        by_analyst.setdefault(r["assignee"], {})[r["month"]] = r["closed_count"]

    # Sort analysts by total closures desc, take top 10
    totals = [(a, sum(m.values())) for a, m in by_analyst.items()]
    totals.sort(key=lambda x: -x[1])
    top = [a for a, _ in totals[:10]]

    fig = go.Figure()
    for analyst in top:
        vals = [by_analyst[analyst].get(m, 0) for m in all_months]
        fig.add_trace(go.Scatter(
            x=all_months, y=vals, name=analyst, mode="lines+markers",
            line=dict(width=2),
        ))

    fig.update_layout(
        margin=dict(l=0, r=0, t=10, b=10),
        height=400,
        xaxis_title="Month",
        yaxis_title="Tickets Closed",
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
    scorecard = _merge_scorecard(scorecard, action_profile, severity_profile)

    return dmc.Stack(
        [
            dmc.Title("Operations", order=2),
            dmc.Text(
                "Analyst behaviour patterns — identify free-riding, "
                "severity avoidance, and workload imbalances.",
                c="dimmed", size="sm",
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
                        "Red highlights: Technical % below 18%, High Sev % below 15%, "
                        "Own Work % < 50%, Low Contribution > 10%. "
                        "Click a row to see that analyst's low-contribution tickets.",
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
                                "What fraction of each analyst's own actions are actual "
                                "technical problem-solving vs scheduling/coordination. "
                                "Red = technical work ≥ 20% below team average. "
                                "Red = scheduling ≥ 30% above team average.",
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
                                "high-severity (Sev 1) tickets. Red bars are ≥ 35% "
                                "below team average — possible severity avoidance.",
                                c="dimmed", size="xs", mb="sm",
                            ),
                            _severity_chart(severity_profile),
                        ],
                        withBorder=True, p="md", radius="md", shadow="sm",
                    ),
                ],
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
                title="Low-Contribution Tickets",
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
            f"where {analyst} closed but contributed < 25% of InHance actions"
        )
        return True, f"Low-Contribution Closes — {analyst}", subtitle, tickets
