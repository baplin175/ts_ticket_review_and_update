"""Health dashboards — Customer and Product health AG Grid tables."""

import threading

import dash_ag_grid as dag
import dash_mantine_components as dmc
from dash import callback, dcc, html, Input, Output, State, no_update
from dash_iconify import DashIconify
import plotly.graph_objects as go

from .. import data
from ..health_explainer import generate_customer_health_explanation
from ..health_planner import generate_customer_health_plan
from ..renderer import grid_with_export, ticket_number_column


_HEALTH_BAND_COLORS = {
    "healthy": "green",
    "watch": "yellow",
    "at_risk": "orange",
    "critical": "red",
}


# ── Customer chat state ───────────────────────────────────────────────

_customer_chat_state: dict = {"running": False, "result": None, "error": None}
_customer_chat_lock = threading.Lock()


def _build_customer_context(customer_names: list[str], tickets: list[dict]) -> str:
    """Serialize open ticket summaries for one or more customers into Matcha context."""
    label = ", ".join(customer_names)
    lines = [
        f"=== CUSTOMER CONTEXT: {label} ===",
        f"Open ticket count: {len(tickets)}",
    ]
    frustrated = [t for t in tickets if str(t.get("frustrated", "")).lower() == "yes"]
    if frustrated:
        lines.append(f"Frustrated tickets: {len(frustrated)}")
    high_pri = [t for t in tickets if (t.get("priority") or 99) <= 2]
    if high_pri:
        lines.append(f"High-priority tickets (score ≤ 2): {len(high_pri)}")
    lines.append("\n=== OPEN TICKETS ===")
    for t in tickets:
        lines.append(
            f"  #{t.get('ticket_number', '?')} | {t.get('ticket_name', '—')} | "
            f"Status: {t.get('status', '—')} | Severity: {t.get('severity', '—')} | "
            f"Product: {t.get('product_name', '—')} | Assignee: {t.get('assignee', '—')} | "
            f"Age: {round(t['days_opened']) if t.get('days_opened') else '—'}d | "
            f"Priority: {t.get('priority', '—')} | Complexity: {t.get('overall_complexity', '—')} | "
            f"Frustrated: {t.get('frustrated', '—')}"
        )
    return "\n".join(lines)


def _render_customer_chat_messages(history: list[dict], pending: bool = False) -> list:
    """Render customer chat history as Dash Paper bubbles, newest first."""
    cards = []
    if pending:
        cards.append(
            dmc.Paper(
                dmc.Group([
                    dmc.Badge("Matcha", color="violet", variant="filled", size="sm"),
                    dmc.Loader(size="xs", type="dots"),
                    dmc.Text("Thinking…", size="sm", c="dimmed"),
                ], gap="xs"),
                withBorder=True, p="sm", radius="sm",
                style={"borderLeft": "3px solid #7950f2"},
            )
        )
    for msg in reversed(history):
        is_user = msg.get("role") == "user"
        content = msg.get("content", "")
        cards.append(
            dmc.Paper(
                [
                    dmc.Group(
                        [dmc.Badge("You" if is_user else "Matcha",
                                   color="blue" if is_user else "violet",
                                   variant="filled", size="sm")],
                        mb=4,
                    ),
                    dmc.Text(str(content), size="sm",
                             style={"whiteSpace": "pre-wrap", "lineHeight": 1.6}),
                ],
                withBorder=True, p="sm", radius="sm",
                style={"borderLeft": f"3px solid {'#1c7ed6' if is_user else '#7950f2'}"},
            )
        )
    return cards


def _run_customer_chat(context: str, messages: list[dict], chat_history: list[dict]) -> None:
    """Background thread: call Matcha and store the result."""
    with _customer_chat_lock:
        _customer_chat_state.update({"running": True, "result": None, "error": None})
    try:
        from matcha_client import call_matcha_chat
        reply = call_matcha_chat(context=context, messages=messages, chat_history=chat_history)
        with _customer_chat_lock:
            _customer_chat_state["result"] = reply
    except Exception as exc:
        with _customer_chat_lock:
            _customer_chat_state["error"] = str(exc)
    finally:
        with _customer_chat_lock:
            _customer_chat_state["running"] = False


def _health_band_color(band):
    return _HEALTH_BAND_COLORS.get(str(band or "").strip().lower(), "gray")


_BAND_CELL_STYLE = (
    "{'color': '#e03131', 'fontWeight': 'bold'} if params.value === 'critical' else "
    "{'color': '#f08c00', 'fontWeight': 'bold'} if params.value === 'at_risk' else "
    "{'color': '#e6a700', 'fontWeight': 'bold'} if params.value === 'watch' else "
    "{'color': '#2b8a3e', 'fontWeight': 'bold'} if params.value === 'healthy' else "
    "{'fontWeight': 'bold'}"
)

PLANS_COLS = [
    {"field": "customer", "headerName": "Customer", "minWidth": 160, "flex": 1.5, "pinned": "left"},
    {"field": "as_of_date", "headerName": "As Of", "width": 110,
     "valueFormatter": {"function": "params.value ? new Date(params.value).toLocaleDateString() : ''"}},
    {"field": "target_band", "headerName": "Target Band", "width": 120,
     "cellStyle": {"function": _BAND_CELL_STYLE}},
    {"field": "projected_band", "headerName": "Proj. Band", "width": 120,
     "cellStyle": {"function": _BAND_CELL_STYLE}},
    {"field": "projected_score", "headerName": "Proj. Score", "width": 110, "type": "numericColumn",
     "valueFormatter": {"function": "params.value != null ? Math.round(params.value) : ''"}},
    {"field": "tickets_to_resolve_count", "headerName": "Tickets", "width": 85, "type": "numericColumn"},
    {"field": "group_filter_label", "headerName": "Groups", "minWidth": 160, "flex": 1,
     "tooltipField": "group_filter_label"},
    {"field": "created_at", "headerName": "Generated", "width": 160, "sort": "desc",
     "valueFormatter": {"function": "params.value ? new Date(params.value).toLocaleDateString() : ''"}},
]

# ── Customer health columns ──────────────────────────────────────────

CUSTOMER_COLS = [
    {"field": "customer", "headerName": "Customer", "minWidth": 150, "flex": 1.5, "pinned": "left",
     "checkboxSelection": True, "headerCheckboxSelection": True},
    {"field": "customer_health_score", "headerName": "Distress", "minWidth": 80, "flex": 0.55, "type": "numericColumn",
     "valueFormatter": {"function": "params.value != null ? Math.round(params.value) : ''"}},
    {"field": "customer_health_band", "headerName": "Band", "minWidth": 70, "flex": 0.5,
     "cellStyle": {"function": "params.value === 'critical' ? {'color': '#e03131', 'fontWeight': 'bold'} : params.value === 'at_risk' ? {'color': '#f08c00', 'fontWeight': 'bold'} : params.value === 'watch' ? {'color': '#e6a700', 'fontWeight': 'bold'} : params.value === 'healthy' ? {'color': '#2b8a3e', 'fontWeight': 'bold'} : {'fontWeight': 'bold'}"}},
    {"field": "key_account", "headerName": "Key Acct", "minWidth": 85, "flex": 0.55,
     "cellStyle": {"function": "params.value ? {'color': '#2b8a3e', 'fontWeight': 'bold'} : {}"}},
    {"field": "open_ticket_count", "headerName": "Open", "minWidth": 65, "flex": 0.45, "type": "numericColumn"},
    {"field": "high_priority_count", "headerName": "High Pri", "minWidth": 75, "flex": 0.5, "type": "numericColumn",
     "cellStyle": {"function": "params.value > 0 ? {'color': '#e03131', 'fontWeight': 'bold'} : {}"}},
    {"field": "frustration_count_90d", "headerName": "Frustrated", "minWidth": 85, "flex": 0.55, "type": "numericColumn",
     "cellStyle": {"function": "params.value > 0 ? {'color': '#c2255c', 'fontWeight': 'bold'} : {}"}},
    {"field": "pressure_score", "headerName": "Pressure", "minWidth": 80, "flex": 0.55, "type": "numericColumn",
     "valueFormatter": {"function": "params.value != null ? Math.round(params.value) : ''"},
     "cellStyle": {"function": "params.value != null && params.value > 5 ? {'color': '#e03131', 'fontWeight': 'bold'} : {}"}},
    {"field": "aging_score", "headerName": "Aging", "minWidth": 70, "flex": 0.5, "type": "numericColumn",
     "valueFormatter": {"function": "params.value != null ? Math.round(params.value) : ''"},
     "cellStyle": {"function": "params.value != null && params.value > 5 ? {'color': '#f08c00', 'fontWeight': 'bold'} : {}"}},
    {"field": "friction_score", "headerName": "Friction", "minWidth": 75, "flex": 0.5, "type": "numericColumn",
     "valueFormatter": {"function": "params.value != null ? Math.round(params.value) : ''"},
     "cellStyle": {"function": "params.value != null && params.value > 5 ? {'color': '#c2255c', 'fontWeight': 'bold'} : {}"}},
    {"field": "concentration_score", "headerName": "Concentr.", "minWidth": 85, "flex": 0.55, "type": "numericColumn",
     "valueFormatter": {"function": "params.value != null ? Math.round(params.value) : ''"},
     "cellStyle": {"function": "params.value != null && params.value > 5 ? {'color': '#6741d9', 'fontWeight': 'bold'} : {}"}},
    {"field": "breadth_score", "headerName": "Breadth", "minWidth": 75, "flex": 0.5, "type": "numericColumn",
     "valueFormatter": {"function": "params.value != null ? Math.round(params.value) : ''"},
     "cellStyle": {"function": "params.value != null && params.value > 5 ? {'color': '#0b7285', 'fontWeight': 'bold'} : {}"}},
    {"field": "as_of_date", "headerName": "As Of", "minWidth": 90, "flex": 0.55,
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
    ticket_number_column(width=110, pinned="left"),
    {"field": "ticket_name", "headerName": "Name", "minWidth": 200, "flex": 1,
     "tooltipField": "ticket_name"},
    {"field": "status", "headerName": "Status", "width": 120},
    {"field": "severity", "headerName": "Severity", "width": 140},
    {"field": "group_name", "headerName": "Group", "width": 170},
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
    {"field": "do_number", "headerName": "DO #", "width": 90,
     "cellRenderer": "DOLink"},
    {"field": "do_status", "headerName": "DO Status", "width": 110},
]

CONTRIBUTOR_COL_DEFS = [
    ticket_number_column(width=110, pinned="left"),
    {"field": "ticket_name", "headerName": "Name", "minWidth": 220, "flex": 1, "tooltipField": "ticket_name"},
    {"field": "group_name", "headerName": "Group", "width": 170},
    {"field": "product_name", "headerName": "Product", "width": 140},
    {"field": "priority", "headerName": "Priority", "width": 95, "type": "numericColumn"},
    {"field": "overall_complexity", "headerName": "Complexity", "width": 110, "type": "numericColumn"},
    {"field": "frustrated", "headerName": "Frustrated", "width": 105},
    {"field": "cluster_id", "headerName": "Cluster", "width": 170},
    {"field": "mechanism_class", "headerName": "Mechanism", "width": 170},
    {"field": "total_contribution", "headerName": "Total", "width": 90, "type": "numericColumn",
     "valueFormatter": {"function": "params.value != null ? Math.round(params.value) : ''"}},
    {"field": "pressure_contribution", "headerName": "Pressure", "width": 100, "type": "numericColumn",
     "valueFormatter": {"function": "params.value != null ? Math.round(params.value) : ''"}},
    {"field": "aging_contribution", "headerName": "Aging", "width": 90, "type": "numericColumn",
     "valueFormatter": {"function": "params.value != null ? Math.round(params.value) : ''"}},
    {"field": "friction_contribution", "headerName": "Friction", "width": 100, "type": "numericColumn",
     "valueFormatter": {"function": "params.value != null ? Math.round(params.value) : ''"}},
    {"field": "concentration_contribution", "headerName": "Concentration", "width": 120, "type": "numericColumn",
     "valueFormatter": {"function": "params.value != null ? Math.round(params.value) : ''"}},
    {"field": "breadth_contribution", "headerName": "Breadth", "width": 95, "type": "numericColumn",
     "valueFormatter": {"function": "params.value != null ? Math.round(params.value) : ''"}},
    {"field": "days_opened", "headerName": "Age (d)", "width": 90, "type": "numericColumn"},
    {"field": "date_modified", "headerName": "Last Modified", "width": 130,
     "valueFormatter": {"function": "params.value ? new Date(params.value).toLocaleDateString() : ''"}},
]


def _history_stat(title, value, color):
    return dmc.Paper(
        [
            dmc.Text(title, size="xs", c="dimmed", tt="uppercase", fw=700),
            dmc.Title(value, order=4, c=color),
        ],
        withBorder=True,
        p="sm",
        radius="md",
    )


def _empty_history_figure(message):
    fig = go.Figure()
    fig.add_annotation(text=message, x=0.5, y=0.5, xref="paper", yref="paper", showarrow=False, font={"size": 16})
    fig.update_xaxes(visible=False)
    fig.update_yaxes(visible=False)
    fig.update_layout(height=320, margin=dict(l=20, r=20, t=20, b=20), template="plotly_white")
    return fig


def _trim_history_rows(history_rows):
    """Trim leading zero-signal history so the chart starts when the customer becomes active."""
    if not history_rows:
        return []

    start_idx = 0
    for idx, row in enumerate(history_rows):
        if any(
            (row.get(field) or 0) > 0
            for field in (
                "customer_health_score",
                "pressure_score",
                "aging_score",
                "friction_score",
                "concentration_score",
                "breadth_score",
                "open_ticket_count",
                "high_priority_count",
                "high_complexity_count",
                "frustration_count_90d",
            )
        ):
            start_idx = idx
            break
    return history_rows[start_idx:]


def _history_figure(history_rows):
    history_rows = _trim_history_rows(history_rows)
    if not history_rows:
        return _empty_history_figure("No customer health history has been built yet.")

    fig = go.Figure()
    fields = [
        ("customer_health_score", "Distress Score", "#1c7ed6", 4),
        ("pressure_score", "Pressure", "#e03131", 2),
        ("aging_score", "Aging", "#f08c00", 2),
        ("friction_score", "Friction", "#c2255c", 2),
        ("concentration_score", "Concentration", "#6741d9", 2),
        ("breadth_score", "Breadth", "#0b7285", 2),
    ]
    dates = [row["as_of_date"] for row in history_rows]
    for field, label, color, width in fields:
        fig.add_trace(
            go.Scatter(
                x=dates,
                y=[row.get(field) for row in history_rows],
                mode="lines+markers",
                name=label,
                line={"color": color, "width": width},
                marker={"size": 7},
                hovertemplate="%{x}<br>" + label + ": %{y:.0f}<extra></extra>",
            )
        )

    fig.update_layout(
        height=360,
        margin=dict(l=40, r=20, t=20, b=40),
        template="plotly_white",
        hovermode="x unified",
        legend={"orientation": "h", "yanchor": "bottom", "y": 1.02, "x": 0},
    )
    fig.update_xaxes(title_text="Date")
    fig.update_yaxes(title_text="Score")
    return fig


def _history_summary(latest_row):
    if not latest_row:
        return dmc.Text("No additive customer health history is available for this customer.", c="dimmed")

    band = str(latest_row.get("customer_health_band") or "—").replace("_", " ").title()
    return dmc.SimpleGrid(
        cols={"base": 2, "lg": 6},
        children=[
            _history_stat("Latest Distress Score", f'{round(latest_row.get("customer_health_score", 0))}', "blue"),
            _history_stat("Band", band, _health_band_color(latest_row.get("customer_health_band"))),
            _history_stat("Pressure", f'{round(latest_row.get("pressure_score", 0))}', "red"),
            _history_stat("Aging", f'{round(latest_row.get("aging_score", 0))}', "orange"),
            _history_stat("Friction", f'{round(latest_row.get("friction_score", 0))}', "pink"),
            _history_stat("Concentration", f'{round(latest_row.get("concentration_score", 0))}', "violet"),
        ],
    )


def _explanation_record_card(record):
    created = record.get("created_at") or "—"
    as_of_date = record.get("as_of_date") or "—"
    return dmc.Paper(
        [
            dmc.Group(
                [
                    dmc.Text(f"As of {as_of_date}", fw=700),
                    dmc.Badge(record.get("group_filter_label") or "Unknown filter", variant="light"),
                    dmc.Text(created, size="sm", c="dimmed"),
                ],
                justify="space-between",
                align="flex-start",
            ),
            dcc.Markdown(record.get("explanation_text") or "", style={"marginTop": "0.5rem"}),
        ],
        withBorder=True,
        radius="md",
        p="md",
    )


def _explanation_history(records):
    if not records:
        return dmc.Text("No saved explanations for this customer yet.", c="dimmed")
    items = []
    for idx, record in enumerate(records):
        created = record.get("created_at") or "—"
        as_of_date = record.get("as_of_date") or "—"
        label = dmc.Group(
            [
                dmc.Text(f"As of {as_of_date}", fw=700),
                dmc.Badge(record.get("group_filter_label") or "Unknown filter", variant="light"),
                dmc.Text(created, size="sm", c="dimmed"),
            ],
            justify="space-between",
            align="center",
            w="100%",
        )
        items.append(
            dmc.AccordionItem(
                [
                    dmc.AccordionControl(label),
                    dmc.AccordionPanel(_explanation_record_card(record)),
                ],
                value=f"explanation-{idx}",
            )
        )
    return dmc.Accordion(
        items,
        multiple=False,
        value=[],
        chevronPosition="left",
        variant="separated",
    )


def _plan_record_card(record):
    created = record.get("created_at") or "—"
    as_of_date = record.get("as_of_date") or "—"
    target_band = str(record.get("target_band") or "—").replace("_", " ").title()
    projected_band = str(record.get("projected_band") or "—").replace("_", " ").title()
    projected_score = record.get("projected_score")
    tickets = record.get("tickets_to_resolve") or []
    return dmc.Paper(
        [
            dmc.Group(
                [
                    dmc.Text(f"As of {as_of_date}", fw=700),
                    dmc.Badge(f"Target: {target_band}", variant="light", color="blue"),
                    dmc.Badge(
                        f"Projected: {projected_band} ({round(projected_score) if projected_score is not None else '—'})",
                        variant="light",
                        color=_health_band_color(record.get("projected_band")),
                    ),
                    dmc.Badge(record.get("group_filter_label") or "Unknown filter", variant="outline"),
                    dmc.Text(created, size="sm", c="dimmed"),
                ],
                justify="space-between",
                align="flex-start",
                wrap="wrap",
            ),
            dcc.Markdown(record.get("plan_text") or "", style={"marginTop": "0.5rem"}),
            dmc.Text(
                f"{len(tickets)} ticket(s) identified for resolution.",
                size="sm", c="dimmed", mt="xs",
            ) if tickets else None,
        ],
        withBorder=True,
        radius="md",
        p="md",
    )


def _plan_history(records):
    if not records:
        return dmc.Text("No saved improvement plans for this customer yet.", c="dimmed")
    items = []
    for idx, record in enumerate(records):
        as_of_date = record.get("as_of_date") or "—"
        target_band = str(record.get("target_band") or "—").replace("_", " ").title()
        projected_band = str(record.get("projected_band") or "—").replace("_", " ").title()
        created = record.get("created_at") or "—"
        label = dmc.Group(
            [
                dmc.Text(f"As of {as_of_date}", fw=700),
                dmc.Badge(f"Target: {target_band}", variant="light", color="blue"),
                dmc.Badge(
                    f"→ {projected_band}",
                    variant="light",
                    color=_health_band_color(record.get("projected_band")),
                ),
                dmc.Text(created, size="sm", c="dimmed"),
            ],
            justify="space-between",
            align="center",
            w="100%",
        )
        items.append(
            dmc.AccordionItem(
                [
                    dmc.AccordionControl(label),
                    dmc.AccordionPanel(_plan_record_card(record)),
                ],
                value=f"plan-{idx}",
            )
        )
    return dmc.Accordion(
        items,
        multiple=False,
        value=[],
        chevronPosition="left",
        variant="separated",
    )


def _plan_detail_card(row):
    target_band = str(row.get("target_band") or "—").replace("_", " ").title()
    projected_band = str(row.get("projected_band") or "—").replace("_", " ").title()
    projected_score = row.get("projected_score")

    tickets = row.get("tickets_to_resolve") or []
    if isinstance(tickets, str):
        import json as _json
        try:
            tickets = _json.loads(tickets)
        except Exception:
            tickets = []

    ticket_table = None
    if tickets:
        header = dmc.TableThead(
            dmc.TableTr([
                dmc.TableTh("#"),
                dmc.TableTh("Ticket"),
                dmc.TableTh("Product"),
                dmc.TableTh("Assignee"),
                dmc.TableTh("Age (d)"),
                dmc.TableTh("Pri"),
                dmc.TableTh("Frustrated"),
                dmc.TableTh("Score Δ"),
            ])
        )
        rows = []
        for i, t in enumerate(tickets, 1):
            frustrated = t.get("frustrated") or ""
            rows.append(
                dmc.TableTr([
                    dmc.TableTd(str(i)),
                    dmc.TableTd(
                        dmc.Text(
                            f"#{t.get('ticket_number') or '—'}  {t.get('ticket_name') or ''}",
                            size="sm", truncate="end",
                        ),
                        style={"maxWidth": "340px"},
                    ),
                    dmc.TableTd(dmc.Text(t.get("product_name") or "—", size="sm")),
                    dmc.TableTd(dmc.Text(t.get("assignee") or "—", size="sm")),
                    dmc.TableTd(dmc.Text(str(round(t.get("days_opened") or 0)), size="sm")),
                    dmc.TableTd(dmc.Text(str(t.get("priority") or "—"), size="sm")),
                    dmc.TableTd(
                        dmc.Badge("Yes", color="red", variant="light", size="sm")
                        if frustrated == "Yes"
                        else dmc.Text("—", size="sm", c="dimmed")
                    ),
                    dmc.TableTd(
                        dmc.Text(
                            f"−{round(t.get('total_contribution') or 0)}",
                            size="sm", c="red", fw=600,
                        )
                    ),
                ])
            )
        ticket_table = dmc.Table(
            [header, dmc.TableTbody(rows)],
            striped=True,
            highlightOnHover=True,
            withTableBorder=True,
            withColumnBorders=True,
            fz="sm",
            mt="sm",
        )

    children = [
        dmc.Group(
            [
                dmc.Badge(f"Target: {target_band}", variant="light", color="blue"),
                dmc.Badge(
                    f"Projected: {projected_band} ({round(projected_score) if projected_score is not None else '—'})",
                    variant="light",
                    color=_health_band_color(row.get("projected_band")),
                ),
                dmc.Badge(row.get("group_filter_label") or "Unknown filter", variant="outline"),
            ],
            wrap="wrap",
        ),
        dcc.Markdown(row.get("plan_text") or ""),
    ]
    if ticket_table is not None:
        children.append(dmc.Divider(my="xs"))
        children.append(dmc.Text(f"{len(tickets)} ticket(s) to resolve:", fw=600, size="sm"))
        children.append(ticket_table)

    return dmc.Stack(children, gap="sm")


def _build_plans_accordion(plans):
    """Render a list of plan rows as an Accordion."""
    if not plans:
        return dmc.Text("No plans match the current filters.", c="dimmed", ta="center", py="xl")
    items = []
    for i, row in enumerate(plans):
        customer = row.get("customer") or "—"
        as_of = str(row.get("as_of_date") or "—")
        target_band = str(row.get("target_band") or "—").replace("_", " ").title()
        projected_band = str(row.get("projected_band") or "—").replace("_", " ").title()
        current_score = row.get("current_score")
        projected_score = row.get("projected_score")
        current_str = str(round(float(current_score))) if current_score is not None else "?"
        projected_str = str(round(float(projected_score))) if projected_score is not None else "?"
        projected_band = str(row.get("projected_band") or "—").replace("_", " ").title()
        n_tickets = row.get("tickets_to_resolve_count") or 0
        groups = row.get("group_filter_label") or "—"
        created = str(row.get("created_at") or "—")[:10]
        control = dmc.Group(
            [
                dmc.Text(customer, fw=600, size="sm", style={"minWidth": "160px"}),
                dmc.Text(as_of, size="sm", c="dimmed", style={"minWidth": "95px"}),
                dmc.Badge(
                    f"{current_str} → {projected_str}",
                    color="gray", variant="light", size="sm",
                    style={"fontVariantNumeric": "tabular-nums"},
                ),
                dmc.Badge(
                    f"{projected_band}",
                    color=_health_band_color(row.get("projected_band")),
                    variant="light",
                    size="sm",
                ),
                dmc.Text(f"{n_tickets} ticket{'s' if n_tickets != 1 else ''}", size="sm", c="dimmed", style={"minWidth": "65px"}),
                dmc.Text(groups, size="xs", c="dimmed", style={"flex": "1", "overflow": "hidden", "textOverflow": "ellipsis", "whiteSpace": "nowrap"}),
                dmc.Text(created, size="xs", c="dimmed"),
            ],
            wrap="nowrap",
            gap="md",
            style={"width": "100%"},
        )
        items.append(
            dmc.AccordionItem(
                [
                    dmc.AccordionControl(control),
                    dmc.AccordionPanel(_plan_detail_card(row)),
                ],
                value=f"plan-{i}",
            )
        )
    return dmc.Accordion(
        items,
        multiple=False,
        chevronPosition="right",
        variant="separated",
    )


def plans_layout():
    import json as _json
    plans = data.get_all_health_plans()
    if not plans:
        return dmc.Stack(
            [
                dmc.Title("Improvement Plans", order=2),
                dmc.Text(
                    "No improvement plans have been generated yet. Open a customer's health history and click Generate Plan.",
                    c="dimmed", ta="center", py="xl",
                ),
            ],
            gap="sm",
        )

    customer_opts = sorted({r.get("customer") for r in plans if r.get("customer")})

    return dmc.Stack(
        [
            dmc.Title("Improvement Plans", order=2),
            dmc.Group(
                [
                    dmc.Select(
                        id="plans-customer-filter",
                        placeholder="Filter by customer…",
                        data=[{"value": c, "label": c} for c in customer_opts],
                        value=None,
                        clearable=True,
                        searchable=True,
                        w=300,
                        size="sm",
                    ),
                    dmc.Select(
                        id="plans-band-filter",
                        placeholder="Target band…",
                        data=[
                            {"value": "at_risk", "label": "At Risk"},
                            {"value": "watch",   "label": "Watch"},
                            {"value": "healthy", "label": "Healthy"},
                        ],
                        value=None,
                        clearable=True,
                        w=150,
                        size="sm",
                    ),
                    dmc.Select(
                        id="plans-sort",
                        data=[
                            {"value": "newest",   "label": "Newest first"},
                            {"value": "oldest",   "label": "Oldest first"},
                            {"value": "customer", "label": "Customer A→Z"},
                            {"value": "score",    "label": "Projected score ↑"},
                        ],
                        value="newest",
                        w=180,
                        size="sm",
                    ),
                ],
                gap="sm",
            ),
            dcc.Store(id="plans-data-store", data=_json.dumps(plans, default=str)),
            html.Div(
                id="plans-accordion-container",
                children=_build_plans_accordion(plans),
            ),
        ],
        gap="sm",
    )


# ── Saved reports helpers ────────────────────────────────────────────

def _normalize_health_reports(reports):
    if not reports:
        return []
    if isinstance(reports, dict):
        reports = list(reports.values())
    return sorted(reports, key=lambda r: (r.get("sort_order") or 0, (r.get("name") or "").lower()))


def _build_health_report_tabs(reports):
    report_tabs = [
        dmc.TabsTab(r["name"], value=f"report:{r['id']}")
        for r in _normalize_health_reports(reports)
    ]
    return [dmc.TabsList([
        dmc.TabsTab("All Customers", value="all"),
        *report_tabs,
    ])]


# ── Layout ───────────────────────────────────────────────────────────

def health_layout():
    customers = data.get_customer_health()
    products = data.get_product_health()
    health_reports = data.get_saved_reports('health')

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
                                        "Select customers to view tickets, or select one customer to view health history.",
                                        size="sm", c="dimmed",
                                    ),
                                    dmc.Group(
                                        [
                                            dmc.Button(
                                                "View Tickets",
                                                id="health-drilldown-btn",
                                                leftSection=DashIconify(icon="tabler:list-search", width=16),
                                                variant="light",
                                                size="compact-sm",
                                                disabled=True,
                                            ),
                                            dmc.Button(
                                                "View Health Trend",
                                                id="health-history-btn",
                                                leftSection=DashIconify(icon="tabler:chart-line", width=16),
                                                variant="light",
                                                size="compact-sm",
                                                disabled=True,
                                            ),
                                            dmc.Button(
                                                "Save Report",
                                                id="health-save-report-btn",
                                                leftSection=DashIconify(icon="tabler:device-floppy", width=16),
                                                variant="light",
                                                size="compact-sm",
                                            ),
                                            dmc.Button(
                                                "Delete Report",
                                                id="health-delete-report-btn",
                                                leftSection=DashIconify(icon="tabler:trash", width=16),
                                                variant="subtle",
                                                color="red",
                                                size="compact-sm",
                                                disabled=True,
                                            ),
                                            dmc.ActionIcon(
                                                DashIconify(icon="tabler:chevron-left", width=16),
                                                id="health-move-tab-left-btn",
                                                variant="subtle",
                                                color="gray",
                                                size="sm",
                                                disabled=True,
                                            ),
                                            dmc.ActionIcon(
                                                DashIconify(icon="tabler:chevron-right", width=16),
                                                id="health-move-tab-right-btn",
                                                variant="subtle",
                                                color="gray",
                                                size="sm",
                                                disabled=True,
                                            ),
                                            dmc.Button(
                                                "Clear Filters",
                                                id="health-clear-filters-btn",
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
                            dmc.Tabs(
                                _build_health_report_tabs(health_reports),
                                id="health-report-tabs",
                                value="all",
                            ),
                            customer_grid,
                        ], gap="xs"),
                        value="customer", pt="md",
                    ),
                    dmc.TabsPanel(product_grid, value="product", pt="md"),
                ],
                value="customer",
            ),
            dcc.Store(id="health-history-selection"),
            dcc.Store(id="health-history-selected-date"),
            dcc.Store(id="health-drilldown-customers-store"),
            dcc.Store(id="health-drilldown-ticket-id"),
            dcc.Store(id="health-customer-chat-history", data=[]),
            dcc.Interval(id="health-customer-chat-poll", interval=2000, disabled=True),
            # Drill-down modal for customer tickets
            dmc.Modal(
                id="health-drilldown-modal",
                title="Customer Tickets",
                size="90%",
                centered=True,
                children=[
                    # Tab strip only — content is in plain divs below so the
                    # AG Grid is never inside a hidden TabsPanel (which breaks
                    # its virtual rendering engine).
                    dmc.Tabs(
                        dmc.TabsList([
                            dmc.TabsTab("Tickets", value="tickets",
                                        leftSection=DashIconify(icon="tabler:tickets", width=16)),
                            dmc.TabsTab("Ask Matcha", value="chat",
                                        leftSection=DashIconify(icon="tabler:robot", width=16)),
                        ]),
                        id="health-drilldown-tabs",
                        value="tickets",
                        mb="sm",
                    ),
                    # ── Tickets content (always in DOM) ──────────────────
                    html.Div(
                        id="health-drilldown-tickets-panel",
                        children=[
                            html.Div(
                                id="health-drilldown-list-view",
                                children=[
                                    dmc.Text(id="health-drilldown-subtitle", size="sm", c="dimmed", mb="sm"),
                                    dmc.Group(
                                        [
                                            dmc.Text("Top Issue Clusters", size="sm", fw=600),
                                            dmc.Switch(
                                                id="health-cluster-scope-toggle",
                                                label="Open only",
                                                checked=True,
                                                size="xs",
                                            ),
                                        ],
                                        justify="space-between",
                                        mb="xs",
                                    ),
                                    html.Div(id="health-cluster-cards", style={"marginBottom": "12px"}),
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
                            html.Div(
                                id="health-drilldown-ticket-view",
                                children=[],
                                style={"display": "none"},
                            ),
                        ],
                    ),
                    # ── Ask Matcha chat (always in DOM, hidden when not active) ──
                    html.Div(
                        id="health-drilldown-chat-panel",
                        style={"display": "none"},
                        children=[
                            # Message history
                            html.Div(
                                id="health-customer-chat-messages",
                                style={
                                    "minHeight": 300,
                                    "maxHeight": 480,
                                    "overflowY": "auto",
                                    "display": "flex",
                                    "flexDirection": "column",
                                    "gap": "8px",
                                    "marginBottom": "12px",
                                    "padding": "4px 2px",
                                },
                            ),
                            # Input at the bottom
                            dmc.Textarea(
                                id="health-customer-chat-input",
                                placeholder="Ask anything about this customer's tickets… (Enter to send, Shift+Enter for newline)",
                                autosize=True,
                                minRows=2,
                                maxRows=8,
                                style={"width": "100%", "resize": "none"},
                                className="customer-chat-textarea",
                                mb="xs",
                            ),
                            dmc.Group(
                                [
                                    dmc.Button(
                                        "Send",
                                        id="health-customer-chat-send-btn",
                                        leftSection=DashIconify(icon="tabler:send", width=14),
                                        size="sm",
                                        className="customer-chat-send-btn",
                                    ),
                                    dmc.Button(
                                        "Clear chat",
                                        id="health-customer-chat-clear-btn",
                                        variant="subtle",
                                        color="gray",
                                        size="sm",
                                    ),
                                ],
                                gap="xs",
                            ),
                        ],
                    ),
                ],
            ),
            dmc.Modal(
                id="health-history-modal",
                title="Customer Health History",
                size="95%",
                centered=True,
                children=[
                    dmc.Text(id="health-history-subtitle", size="sm", c="dimmed", mb="sm"),
                    dmc.MultiSelect(
                        id="health-history-group-filter",
                        label="Groups",
                        placeholder="Select groups to include in this customer's health",
                        data=[],
                        value=[],
                        clearable=False,
                        searchable=True,
                        nothingFoundMessage="No groups found",
                        mb="sm",
                    ),
                    dmc.Group(
                        [
                            dmc.Button(
                                "Explain",
                                id="health-explain-btn",
                                leftSection=DashIconify(icon="tabler:bulb", width=16),
                                variant="light",
                                size="compact-sm",
                            ),
                            dmc.Text(
                                "Generates a Matcha explanation for the selected date and groups.",
                                size="sm",
                                c="dimmed",
                            ),
                        ],
                        mb="xs",
                    ),
                    dmc.Group(
                        [
                            dmc.Button(
                                "Generate Plan",
                                id="health-plan-btn",
                                leftSection=DashIconify(icon="tabler:checklist", width=16),
                                variant="light",
                                color="teal",
                                size="compact-sm",
                            ),
                            dmc.Select(
                                id="health-plan-target-band",
                                data=[
                                    {"value": "at_risk", "label": "At Risk  (score < 50)"},
                                    {"value": "watch",   "label": "Watch    (score < 30)"},
                                    {"value": "healthy", "label": "Healthy  (score < 15)"},
                                ],
                                value="watch",
                                w=200,
                                size="xs",
                            ),
                            dmc.Text(
                                "Simulate which tickets to resolve to reach the target band.",
                                size="sm",
                                c="dimmed",
                            ),
                        ],
                        mb="sm",
                    ),
                    html.Div(id="health-history-summary", style={"marginBottom": "1rem"}),
                    dcc.Graph(
                        id="health-history-graph",
                        figure=_empty_history_figure("No customer health history has been built yet."),
                        config={"displayModeBar": False},
                    ),
                    dmc.Text(id="health-contributors-subtitle", size="sm", c="dimmed", mb="sm"),
                    grid_with_export(
                        dag.AgGrid(
                            id="health-contributors-grid",
                            rowData=[],
                            columnDefs=CONTRIBUTOR_COL_DEFS,
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
                            },
                            style={"height": "42vh", "cursor": "pointer"},
                            className="ag-theme-quartz",
                        ),
                        "health-contributors-grid",
                    ),
                    dmc.Divider(my="md"),
                    dmc.Title("Past Explanations", order=4),
                    html.Div(id="health-explanations-list"),
                    dmc.Divider(my="md"),
                    dmc.Title("Improvement Plans", order=4),
                    html.Div(id="health-plans-list"),
                ],
            ),
            dmc.Modal(
                id="health-plan-modal",
                title="Improvement Plan",
                size="lg",
                centered=True,
                children=html.Div(id="health-plan-modal-body"),
            ),
            dmc.Modal(
                id="health-explain-modal",
                title="Health Explanation",
                size="lg",
                centered=True,
                children=html.Div(id="health-explain-modal-body"),
            ),
            # ── Saved reports for Customer Health ─────────────────────
            dcc.Store(
                id="health-saved-reports-store",
                data={str(r["id"]): r for r in health_reports},
            ),
            dmc.Modal(
                id="health-save-report-modal",
                title="Save Customer Health Filters as Report",
                centered=True,
                children=[
                    dmc.Stack(
                        [
                            dmc.TextInput(
                                id="health-report-name-input",
                                label="Report name",
                                placeholder="e.g. Key Accounts",
                            ),
                            dmc.Group(
                                [
                                    dmc.Button("Save", id="health-confirm-save-report-btn", color="blue"),
                                    dmc.Button("Cancel", id="health-cancel-save-report-btn", variant="subtle", color="gray"),
                                ],
                                justify="flex-end",
                            ),
                        ],
                        gap="md",
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
        Output("health-history-btn", "disabled"),
        Output("health-history-btn", "children"),
        Input("customer-health-grid", "selectedRows"),
        Input("customer-health-grid", "virtualRowData"),
    )
    def toggle_drilldown_btn(selected, virtual_rows):
        # Intersect selected rows with currently visible (filtered) rows.
        # This ensures select-all + filter only counts the visible subset.
        visible_customers = {
            r["customer"] for r in (virtual_rows or []) if r.get("customer")
        }
        effective = [
            r for r in (selected or [])
            if r.get("customer") in visible_customers
        ]
        if not effective:
            return True, "View Tickets", True, "View Health Trend"
        n = len(effective)
        history_disabled = n != 1
        selected_label = effective[0]["customer"]
        history_label = "View Health Trend" if history_disabled else f"View Health Trend ({selected_label})"
        return False, f"View Tickets ({n} row{'s' if n != 1 else ''})", history_disabled, history_label

    @app.callback(
        Output("health-drilldown-modal", "opened"),
        Output("health-drilldown-grid", "rowData"),
        Output("health-drilldown-subtitle", "children"),
        Output("health-drilldown-customers-store", "data"),
        Output("health-drilldown-tabs", "value"),
        Output("health-drilldown-tickets-panel", "style"),
        Output("health-drilldown-chat-panel", "style"),
        Input("health-drilldown-btn", "n_clicks"),
        State("customer-health-grid", "selectedRows"),
        State("customer-health-grid", "virtualRowData"),
        prevent_initial_call=True,
    )
    def open_drilldown(n_clicks, selected, virtual_rows):
        if not n_clicks or not selected:
            return no_update, no_update, no_update, no_update, no_update, no_update, no_update
        # Restrict to currently visible (filtered) rows only.
        # When virtualRowData is None the grid hasn't emitted its first render
        # event yet (e.g. immediately after navigating back), so fall back to
        # using the full selectedRows list to avoid an empty modal.
        if virtual_rows is not None:
            visible_customers = {
                r["customer"] for r in virtual_rows if r.get("customer")
            }
            effective = [
                r for r in selected if r.get("customer") in visible_customers
            ]
        else:
            effective = selected
        if not effective:
            return no_update, no_update, no_update, no_update, no_update, no_update, no_update
        names = [r["customer"] for r in effective if r.get("customer")]
        tickets = data.get_tickets_by_customers(names)
        label = ", ".join(names)
        subtitle = f"{len(tickets)} open ticket{'s' if len(tickets) != 1 else ''} for: {label}"
        return True, tickets, subtitle, names, "tickets", {}, {"display": "none"}

    @app.callback(
        Output("health-drilldown-tickets-panel", "style", allow_duplicate=True),
        Output("health-drilldown-chat-panel", "style", allow_duplicate=True),
        Input("health-drilldown-tabs", "value"),
        prevent_initial_call=True,
    )
    def switch_drilldown_panel(tab_value):
        if tab_value == "chat":
            return {"display": "none"}, {}
        return {}, {"display": "none"}

    @app.callback(
        Output("health-cluster-cards", "children"),
        Input("health-cluster-scope-toggle", "checked"),
        Input("health-drilldown-customers-store", "data"),
        prevent_initial_call=True,
    )
    def update_cluster_cards(open_only, customer_names):
        if not customer_names:
            return []
        clusters = data.get_top_clusters_for_customer(
            customer_names, top_n=3, open_only=bool(open_only),
        )
        if not clusters:
            return dmc.Text(
                "No cluster data available",
                size="sm", c="dimmed", fs="italic",
            )
        rank_colors = ["blue", "teal", "grape"]
        cards = []
        for i, c in enumerate(clusters):
            label = (c["cluster_key_l1"] or "").replace("_", " ").title()
            mech = (c["mechanism_class"] or "").replace("_", " ").title()
            cards.append(
                dmc.Paper(
                    [
                        dmc.Group(
                            [
                                dmc.Badge(
                                    f"#{i + 1}",
                                    color=rank_colors[i % len(rank_colors)],
                                    size="sm",
                                    variant="filled",
                                ),
                                dmc.Badge(
                                    mech,
                                    color="gray",
                                    size="xs",
                                    variant="light",
                                ),
                            ],
                            justify="space-between",
                            mb=4,
                        ),
                        dmc.Text(label, size="sm", fw=500, lineClamp=2),
                        dmc.Text(
                            f"{c['ticket_count']} ticket{'s' if c['ticket_count'] != 1 else ''}",
                            size="xs", c="dimmed",
                        ),
                    ],
                    p="sm",
                    radius="md",
                    withBorder=True,
                )
            )
        return dmc.SimpleGrid(cols=3, spacing="sm", children=cards)

    @app.callback(
        Output("health-history-modal", "opened"),
        Output("health-history-subtitle", "children"),
        Output("health-history-group-filter", "data"),
        Output("health-history-group-filter", "value"),
        Output("health-history-summary", "children"),
        Output("health-history-graph", "figure"),
        Output("health-contributors-grid", "rowData"),
        Output("health-contributors-subtitle", "children"),
        Output("health-explanations-list", "children"),
        Output("health-plans-list", "children"),
        Output("health-history-selection", "data"),
        Output("health-history-selected-date", "data"),
        Input("health-history-btn", "n_clicks"),
        State("customer-health-grid", "selectedRows"),
        State("customer-health-grid", "virtualRowData"),
        prevent_initial_call=True,
    )
    def open_health_history(n_clicks, selected, virtual_rows):
        if not n_clicks or not selected:
            return (no_update,) * 12
        if virtual_rows is not None:
            visible_customers = {
                r["customer"] for r in virtual_rows if r.get("customer")
            }
            effective = [r for r in selected if r.get("customer") in visible_customers]
        else:
            effective = selected
        if len(effective) != 1:
            return (no_update,) * 12

        customer = effective[0]["customer"]
        groups = data.get_customer_groups(customer)
        history = data.get_customer_health_history(customer, groups)
        explanations = data.get_customer_health_explanations(customer)
        plans = data.get_customer_health_plans(customer)
        selected_label = customer
        if not history:
            subtitle = f"No additive health history is available yet for {selected_label}."
            return (
                True,
                subtitle,
                [{"value": g, "label": g} for g in groups],
                groups,
                _history_summary(None),
                _empty_history_figure("Run the customer health history backfill to populate trend data."),
                [],
                "No ticket-level contributors available.",
                _explanation_history(explanations),
                _plan_history(plans),
                {"customer": customer},
                None,
            )

        latest = history[-1]
        contributors = data.get_customer_health_contributors(customer, latest["as_of_date"], groups)
        subtitle = f"{len(history)} daily snapshot(s) available for {selected_label}."
        contributor_label = (
            f"{len(contributors)} ticket driver(s) on {latest['as_of_date']}"
            if contributors
            else f"No ticket drivers recorded on {latest['as_of_date']}"
        )
        return (
            True,
            subtitle,
            [{"value": g, "label": g} for g in groups],
            groups,
            _history_summary(latest),
            _history_figure(history),
            contributors,
            contributor_label,
            _explanation_history(explanations),
            _plan_history(plans),
            {"customer": customer},
            latest["as_of_date"],
        )

    @app.callback(
        Output("health-history-summary", "children", allow_duplicate=True),
        Output("health-history-graph", "figure", allow_duplicate=True),
        Output("health-contributors-grid", "rowData", allow_duplicate=True),
        Output("health-contributors-subtitle", "children", allow_duplicate=True),
        Output("health-history-selected-date", "data", allow_duplicate=True),
        Input("health-history-group-filter", "value"),
        State("health-history-selection", "data"),
        prevent_initial_call=True,
    )
    def update_history_groups(group_names, selection):
        if not selection or not selection.get("customer"):
            return no_update, no_update, no_update, no_update, no_update
        customer = selection["customer"]
        history = data.get_customer_health_history(customer, group_names)
        if not history:
            return (
                _history_summary(None),
                _empty_history_figure("No customer health history matches the selected groups."),
                [],
                "No ticket-level contributors available.",
                None,
            )
        latest = history[-1]
        contributors = data.get_customer_health_contributors(customer, latest["as_of_date"], group_names)
        subtitle = (
            f"{len(contributors)} ticket driver(s) on {latest['as_of_date']}"
            if contributors
            else f"No ticket drivers recorded on {latest['as_of_date']}"
        )
        return _history_summary(latest), _history_figure(history), contributors, subtitle, latest["as_of_date"]

    @app.callback(
        Output("health-plan-modal", "opened"),
        Output("health-plan-modal-body", "children"),
        Output("health-plans-list", "children", allow_duplicate=True),
        Input("health-plan-btn", "n_clicks"),
        State("health-history-selection", "data"),
        State("health-history-selected-date", "data"),
        State("health-history-group-filter", "value"),
        State("health-plan-target-band", "value"),
        prevent_initial_call=True,
    )
    def generate_plan(n_clicks, selection, as_of_date, group_names, target_band):
        if not n_clicks or not selection or not selection.get("customer") or not as_of_date:
            return no_update, no_update, no_update
        customer = selection["customer"]
        available_groups = data.get_customer_groups(customer)
        record = generate_customer_health_plan(
            customer=customer,
            as_of_date=as_of_date,
            selected_groups=group_names or [],
            available_groups=available_groups,
            target_band=target_band or "watch",
        )
        all_records = data.get_customer_health_plans(customer)
        return True, _plan_record_card(record), _plan_history(all_records)

    @app.callback(
        Output("health-explain-modal", "opened"),
        Output("health-explain-modal-body", "children"),
        Output("health-explanations-list", "children", allow_duplicate=True),
        Input("health-explain-btn", "n_clicks"),
        State("health-history-selection", "data"),
        State("health-history-selected-date", "data"),
        State("health-history-group-filter", "value"),
        prevent_initial_call=True,
    )
    def explain_health(n_clicks, selection, as_of_date, group_names):
        if not n_clicks or not selection or not selection.get("customer") or not as_of_date:
            return no_update, no_update, no_update

        customer = selection["customer"]
        available_groups = data.get_customer_groups(customer)
        record = generate_customer_health_explanation(
            customer=customer,
            as_of_date=as_of_date,
            selected_groups=group_names or [],
            available_groups=available_groups,
        )
        all_records = data.get_customer_health_explanations(customer)
        return True, _explanation_record_card(record), _explanation_history(all_records)

    @app.callback(
        Output("health-contributors-grid", "rowData", allow_duplicate=True),
        Output("health-contributors-subtitle", "children", allow_duplicate=True),
        Input("health-history-graph", "clickData"),
        State("health-history-selection", "data"),
        State("health-history-group-filter", "value"),
        prevent_initial_call=True,
    )
    def update_history_contributors(click_data, selection, group_names):
        if not click_data or not selection:
            return no_update, no_update

        point = (click_data.get("points") or [{}])[0]
        as_of_date = point.get("x")
        if not as_of_date:
            return no_update, no_update

        contributors = data.get_customer_health_contributors(selection["customer"], as_of_date, group_names)
        subtitle = (
            f"{len(contributors)} ticket driver(s) on {as_of_date}"
            if contributors
            else f"No ticket drivers recorded on {as_of_date}"
        )
        return contributors, subtitle

    @app.callback(
        Output("health-drilldown-ticket-id", "data"),
        Input("health-drilldown-grid", "selectedRows"),
        prevent_initial_call=True,
    )
    def open_ticket_in_drilldown(selected_rows):
        if selected_rows and len(selected_rows) > 0:
            tid = selected_rows[0].get("ticket_id")
            if tid is not None:
                return tid
        return no_update

    @app.callback(
        Output("health-drilldown-ticket-id", "data", allow_duplicate=True),
        Input("health-drilldown-grid", "cellClicked"),
        prevent_initial_call=True,
    )
    def open_ticket_from_drilldown_cell(cell_event):
        if not isinstance(cell_event, dict):
            return no_update
        col_id = cell_event.get("colId") or ((cell_event.get("colDef") or {}).get("field"))
        if col_id != "ticket_number":
            return no_update
        row = cell_event.get("data") or {}
        tid = row.get("ticket_id")
        return tid if tid is not None else no_update

    @app.callback(
        Output("health-drilldown-list-view", "style"),
        Output("health-drilldown-ticket-view", "children"),
        Output("health-drilldown-ticket-view", "style"),
        Input("health-drilldown-ticket-id", "data"),
        prevent_initial_call=True,
    )
    def render_inline_drilldown_ticket(ticket_id):
        if not ticket_id:
            return {}, [], {"display": "none"}
        try:
            from .ticket_detail import build_ticket_shell
        except ImportError:
            from web.pages.ticket_detail import build_ticket_shell
        back_btn = dmc.Button(
            "\u2190 Customer Tickets",
            id="health-drilldown-back-btn",
            variant="subtle",
            size="compact-sm",
            mb="sm",
        )
        shell = build_ticket_shell(ticket_id, back_href=None, ctx="modal")
        return {"display": "none"}, [back_btn, shell], {}

    @app.callback(
        Output("health-drilldown-ticket-id", "data", allow_duplicate=True),
        Input("health-drilldown-back-btn", "n_clicks"),
        prevent_initial_call=True,
    )
    def back_to_drilldown_list(n_clicks):
        return None if n_clicks else no_update

    @app.callback(
        Output("health-drilldown-ticket-id", "data", allow_duplicate=True),
        Input("health-drilldown-modal", "opened"),
        prevent_initial_call=True,
    )
    def reset_drilldown_ticket_on_modal_close(opened):
        return None if not opened else no_update

    @app.callback(
        Output("url", "pathname", allow_duplicate=True),
        Input("health-contributors-grid", "selectedRows"),
        prevent_initial_call=True,
    )
    def navigate_from_health_contributors(selected_rows):
        if selected_rows and len(selected_rows) > 0:
            tid = selected_rows[0].get("ticket_id")
            if tid is not None:
                return f"/ticket/{tid}"
        return no_update

    @app.callback(
        Output("plans-accordion-container", "children"),
        Input("plans-customer-filter", "value"),
        Input("plans-band-filter", "value"),
        Input("plans-sort", "value"),
        State("plans-data-store", "data"),
        prevent_initial_call=True,
    )
    def filter_plans(customer, band, sort, store_data):
        import json as _json
        plans = _json.loads(store_data or "[]")
        if customer:
            plans = [p for p in plans if p.get("customer") == customer]
        if band:
            plans = [p for p in plans if p.get("target_band") == band]
        if sort == "oldest":
            plans = sorted(plans, key=lambda p: str(p.get("created_at") or ""))
        elif sort == "customer":
            plans = sorted(plans, key=lambda p: (p.get("customer") or "").lower())
        elif sort == "score":
            plans = sorted(plans, key=lambda p: float(p.get("projected_score") or 0))
        else:  # newest (default — already ordered DESC from DB)
            plans = sorted(plans, key=lambda p: str(p.get("created_at") or ""), reverse=True)
        return _build_plans_accordion(plans)

    # ── Saved reports for Customer Health ──────────────────────────────

    @app.callback(
        Output("health-save-report-modal", "opened"),
        Input("health-save-report-btn", "n_clicks"),
        Input("health-cancel-save-report-btn", "n_clicks"),
        Input("health-confirm-save-report-btn", "n_clicks"),
        prevent_initial_call=True,
    )
    def toggle_health_save_modal(open_clicks, cancel_clicks, confirm_clicks):
        from dash import ctx as _ctx
        if _ctx.triggered_id == "health-save-report-btn":
            return True
        return False

    @app.callback(
        Output("health-saved-reports-store", "data", allow_duplicate=True),
        Output("health-report-tabs", "value", allow_duplicate=True),
        Output("health-report-name-input", "value"),
        Input("health-confirm-save-report-btn", "n_clicks"),
        State("health-report-name-input", "value"),
        State("customer-health-grid", "filterModel"),
        prevent_initial_call=True,
    )
    def save_health_report(n_clicks, name, filter_model):
        if not n_clicks or not name or not name.strip():
            return no_update, no_update, no_update
        fm = filter_model or {}
        saved = data.save_report(name.strip(), fm, page="health")
        reports = {str(r["id"]): r for r in data.get_saved_reports("health")}
        selected_tab = f"report:{saved['id']}" if saved else no_update
        return reports, selected_tab, ""

    @app.callback(
        Output("health-report-tabs", "children"),
        Input("health-saved-reports-store", "data"),
    )
    def render_health_report_tabs(saved_reports):
        reports = _normalize_health_reports(saved_reports)
        return _build_health_report_tabs(reports)

    @app.callback(
        Output("customer-health-grid", "filterModel"),
        Input("health-report-tabs", "value"),
        State("health-saved-reports-store", "data"),
        prevent_initial_call=True,
    )
    def apply_health_report_filter(tab_value, saved_reports):
        if not tab_value or tab_value == "all":
            return {}
        if str(tab_value).startswith("report:"):
            report_id = str(tab_value).split(":", 1)[1]
            report = (saved_reports or {}).get(report_id) or (saved_reports or {}).get(int(report_id), {})
            return (report or {}).get("filter_model") or {}
        return {}

    @app.callback(
        Output("health-delete-report-btn", "disabled"),
        Output("health-delete-report-btn", "children"),
        Output("health-move-tab-left-btn", "disabled"),
        Output("health-move-tab-right-btn", "disabled"),
        Input("health-report-tabs", "value"),
        State("health-saved-reports-store", "data"),
    )
    def update_health_delete_btn(tab_value, saved_reports):
        if not tab_value or not str(tab_value).startswith("report:"):
            return True, "Delete Report", True, True
        report_id = str(tab_value).split(":", 1)[1]
        report = (saved_reports or {}).get(report_id) or (saved_reports or {}).get(int(report_id), {})
        report_name = (report or {}).get("name")
        if not report_name:
            return True, "Delete Report", True, True
        return False, f"Delete {report_name}", False, False

    @app.callback(
        Output("health-saved-reports-store", "data", allow_duplicate=True),
        Output("health-report-tabs", "value", allow_duplicate=True),
        Input("health-delete-report-btn", "n_clicks"),
        State("health-report-tabs", "value"),
        State("health-saved-reports-store", "data"),
        prevent_initial_call=True,
    )
    def delete_health_report(n_clicks, active_tab, saved_reports):
        if not n_clicks or not active_tab or not str(active_tab).startswith("report:"):
            return no_update, no_update
        report_id = str(active_tab).split(":", 1)[1]
        report = (saved_reports or {}).get(report_id) or (saved_reports or {}).get(int(report_id), {})
        if not report:
            return no_update, no_update
        data.delete_report(report_id)
        reports = {str(r["id"]): r for r in data.get_saved_reports("health")}
        return reports, "all"

    @app.callback(
        Output("health-saved-reports-store", "data", allow_duplicate=True),
        Input("health-move-tab-left-btn", "n_clicks"),
        Input("health-move-tab-right-btn", "n_clicks"),
        State("health-report-tabs", "value"),
        State("health-saved-reports-store", "data"),
        prevent_initial_call=True,
    )
    def reorder_health_tab(left_clicks, right_clicks, tab_value, saved_reports):
        from dash import ctx as _ctx
        from dash.exceptions import PreventUpdate
        if not tab_value or not str(tab_value).startswith("report:"):
            raise PreventUpdate
        triggered = _ctx.triggered_id
        if triggered == "health-move-tab-left-btn":
            direction = "left"
        elif triggered == "health-move-tab-right-btn":
            direction = "right"
        else:
            raise PreventUpdate
        report_id = int(str(tab_value).split(":", 1)[1])
        data.reorder_report(report_id, direction)
        reports = {str(r["id"]): r for r in data.get_saved_reports("health")}
        return reports

    @app.callback(
        Output("customer-health-grid", "filterModel", allow_duplicate=True),
        Input("health-clear-filters-btn", "n_clicks"),
        prevent_initial_call=True,
    )
    def clear_health_filters(n_clicks):
        if not n_clicks:
            return no_update
        return {}

    # ── Customer Ask Matcha chat ───────────────────────────────────────

    @app.callback(
        Output("health-customer-chat-messages", "children"),
        Output("health-customer-chat-history", "data"),
        Output("health-customer-chat-poll", "disabled"),
        Output("health-customer-chat-input", "value"),
        Input("health-customer-chat-send-btn", "n_clicks"),
        State("health-customer-chat-input", "value"),
        State("health-customer-chat-history", "data"),
        State("health-drilldown-customers-store", "data"),
        prevent_initial_call=True,
    )
    def customer_chat_send(n_clicks, user_text, chat_history, customer_names):
        if not (user_text or "").strip():
            return no_update, no_update, no_update, no_update
        user_text = user_text.strip()
        tickets = data.get_tickets_by_customers(customer_names or [])
        context_str = _build_customer_context(customer_names or [], tickets)
        prior = list(chat_history or [])
        new_history = prior + [{"role": "user", "content": user_text}]
        threading.Thread(
            target=_run_customer_chat,
            args=(context_str, [{"role": "user", "content": user_text}], prior),
            daemon=True,
        ).start()
        return _render_customer_chat_messages(new_history, pending=True), new_history, False, ""

    @app.callback(
        Output("health-customer-chat-messages", "children", allow_duplicate=True),
        Output("health-customer-chat-history", "data", allow_duplicate=True),
        Output("health-customer-chat-poll", "disabled", allow_duplicate=True),
        Input("health-customer-chat-poll", "n_intervals"),
        State("health-customer-chat-history", "data"),
        prevent_initial_call=True,
    )
    def customer_chat_poll(n_intervals, chat_history):
        with _customer_chat_lock:
            running = _customer_chat_state["running"]
            result = _customer_chat_state.get("result")
            error = _customer_chat_state.get("error")

        if running:
            return no_update, no_update, False

        if result is not None or error is not None:
            reply = result if result is not None else f"[Error: {error}]"
            new_history = list(chat_history or []) + [{"role": "assistant", "content": reply}]
            with _customer_chat_lock:
                _customer_chat_state["result"] = None
                _customer_chat_state["error"] = None
            return _render_customer_chat_messages(new_history), new_history, True

        return no_update, no_update, True

    @app.callback(
        Output("health-customer-chat-messages", "children", allow_duplicate=True),
        Output("health-customer-chat-history", "data", allow_duplicate=True),
        Input("health-customer-chat-clear-btn", "n_clicks"),
        prevent_initial_call=True,
    )
    def customer_chat_clear(n_clicks):
        if not n_clicks:
            return no_update, no_update
        return [], []

    # Reset chat history when the modal closes so the next customer starts fresh
    @app.callback(
        Output("health-customer-chat-history", "data", allow_duplicate=True),
        Output("health-customer-chat-messages", "children", allow_duplicate=True),
        Input("health-drilldown-modal", "opened"),
        prevent_initial_call=True,
    )
    def reset_customer_chat_on_modal_close(opened):
        if not opened:
            return [], []
        return no_update, no_update

