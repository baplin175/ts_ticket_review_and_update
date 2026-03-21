"""Ticket detail page — metadata, thread, scores, wait profile."""

import os
import subprocess
import sys

import dash_mantine_components as dmc
from dash import callback, dcc, html, Input, Output, State, no_update
from dash_iconify import DashIconify
import plotly.graph_objects as go

from .. import data

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_INGEST_SCRIPT = os.path.join(_PROJECT_ROOT, "run_ingest.py")


# ── Helpers ──────────────────────────────────────────────────────────

def _badge(label, color="gray", variant="light"):
    return dmc.Badge(str(label), color=color, variant=variant, size="sm")


def _severity_color(sev):
    if not sev:
        return "gray"
    s = str(sev).lower()
    if "1" in s or "critical" in s:
        return "red"
    if "2" in s or "high" in s:
        return "orange"
    if "3" in s or "medium" in s:
        return "yellow"
    return "blue"


def _priority_color(p):
    if p is None:
        return "gray"
    if p <= 2:
        return "red"
    if p <= 3:
        return "orange"
    if p <= 4:
        return "yellow"
    return "green"


def _complexity_color(c):
    if c is None:
        return "gray"
    if c >= 4:
        return "red"
    if c >= 3:
        return "orange"
    return "green"


def _format_dt(val):
    if not val:
        return "—"
    return str(val)[:16].replace("T", " ")


def _meta_item(label, value):
    return dmc.Stack(
        [
            dmc.Text(label, size="xs", c="dimmed", tt="uppercase", fw=700),
            dmc.Text(str(value) if value is not None else "—", size="sm"),
        ],
        gap=2,
    )


# ── Score cards ──────────────────────────────────────────────────────

def _score_card(title, value, subtitle, color, icon):
    return dmc.Paper(
        [
            dmc.Group(
                [
                    dmc.ThemeIcon(
                        DashIconify(icon=icon, width=24),
                        variant="light", color=color, size=44, radius="md",
                    ),
                    dmc.Stack(
                        [
                            dmc.Text(title, size="xs", c="dimmed", fw=600),
                            dmc.Title(str(value) if value is not None else "—", order=4),
                        ],
                        gap=0,
                    ),
                ],
                gap="sm",
            ),
            dmc.Text(subtitle or "", size="xs", c="dimmed", mt="xs",
                     style={"lineHeight": 1.4}) if subtitle else None,
        ],
        withBorder=True, p="md", radius="md",
    )


# ── Thread rendering ────────────────────────────────────────────────

def _action_card(action):
    party = action.get("party", "")
    border_color = "#1c7ed6" if party == "inh" else "#37b24d" if party == "cust" else "#868e96"
    party_color = "blue" if party == "inh" else "green" if party == "cust" else "gray"
    party_label = "inHANCE" if party == "inh" else "Customer" if party == "cust" else party or "?"

    desc = action.get("cleaned_description") or action.get("description") or ""
    is_empty = action.get("is_empty", False)
    if is_empty or not desc.strip():
        desc = "(empty)"

    action_class = action.get("action_class")
    class_badge = _badge(action_class, "gray", "outline") if action_class and action_class not in ("unknown",) else None

    return dmc.Paper(
        [
            dmc.Group(
                [
                    _badge(party_label, party_color, "filled"),
                    dmc.Text(action.get("creator_name") or "Unknown", fw=600, size="sm"),
                    dmc.Text(_format_dt(action.get("created_at")), size="xs", c="dimmed"),
                    _badge(action.get("action_type", ""), "gray", "outline") if action.get("action_type") else None,
                    class_badge,
                ],
                gap="xs",
            ),
            dmc.Text(
                desc,
                size="sm",
                style={"whiteSpace": "pre-wrap", "wordBreak": "break-word", "marginTop": "0.5rem"},
            ) if desc != "(empty)" else dmc.Text(desc, size="sm", c="dimmed", mt="xs"),
        ],
        p="sm",
        mb="xs",
        withBorder=True,
        radius="sm",
        style={"borderLeft": f"3px solid {border_color}"},
    )


# ── Wait profile chart ──────────────────────────────────────────────

def _wait_chart(profile):
    if not profile:
        return dmc.Text("No wait profile data available.", c="dimmed", ta="center", py="xl")

    labels = []
    values = []
    colors = []
    mapping = [
        ("waiting_on_customer_minutes", "Waiting on Customer", "#37b24d"),
        ("waiting_on_support_minutes", "Waiting on Support", "#1c7ed6"),
        ("waiting_on_dev_minutes", "Waiting on Dev", "#f59f00"),
        ("waiting_on_ps_minutes", "Waiting on PS", "#9775fa"),
        ("active_work_minutes", "Active Work", "#20c997"),
    ]
    for key, label, color in mapping:
        val = profile.get(key, 0) or 0
        if val > 0:
            labels.append(label)
            values.append(round(val / 60, 1))  # convert to hours
            colors.append(color)

    if not values:
        return dmc.Text("No wait data recorded.", c="dimmed", ta="center", py="xl")

    fig = go.Figure(go.Bar(
        x=values, y=labels, orientation="h",
        marker_color=colors,
        text=[f"{v:.1f}h" for v in values],
        textposition="auto",
    ))
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=140, r=20, t=10, b=40),
        height=250,
        xaxis_title="Hours",
        yaxis=dict(autorange="reversed"),
    )
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


# ── Layout ───────────────────────────────────────────────────────────

def _build_detail(ticket_id):
    """Build the full ticket detail content (header + tabs) from DB data."""
    ticket = data.get_ticket_detail(ticket_id)
    if not ticket:
        return dmc.Stack([
            dmc.Title("Ticket Not Found", order=3),
            dmc.Text(f"No ticket with ID {ticket_id} exists in the database.", c="dimmed"),
            dmc.Anchor("Back to Tickets", href="/tickets"),
        ], gap="md")

    actions = data.get_ticket_actions(ticket_id)
    wait = data.get_ticket_wait_profile(ticket_id)

    # Header
    header = dmc.Paper(
        [
            dmc.Group(
                [
                    dmc.Anchor(
                        dmc.Group([DashIconify(icon="tabler:arrow-left", width=16), "Tickets"], gap=4),
                        href="/tickets", size="sm",
                    ),
                    dmc.Group(
                        [
                            dmc.Button(
                                "Refresh",
                                id="ticket-refresh-btn",
                                leftSection=DashIconify(icon="tabler:refresh", width=16),
                                variant="light",
                                size="compact-sm",
                            ),
                        ],
                        ml="auto",
                    ),
                ],
                justify="space-between",
                mb="sm",
            ),
            dmc.Group(
                [
                    dmc.Title(f"#{ticket.get('ticket_number', '')} — {ticket.get('ticket_name', '')}", order=3),
                ],
                mb="sm",
            ),
            dmc.Group(
                [
                    _badge(ticket.get("status", ""), "blue", "filled"),
                    _badge(ticket.get("severity", ""), _severity_color(ticket.get("severity")), "light"),
                    dmc.Text(f"Product: {ticket.get('product_name', '—')}", size="sm"),
                    dmc.Text(f"Assignee: {ticket.get('assignee', '—')}", size="sm"),
                    dmc.Text(f"Customer: {ticket.get('customer', '—')}", size="sm"),
                ],
                gap="sm",
            ),
            dmc.Divider(my="sm"),
            dmc.SimpleGrid(
                cols={"base": 2, "sm": 4, "lg": 6},
                children=[
                    _meta_item("Created", _format_dt(ticket.get("date_created"))),
                    _meta_item("Modified", _format_dt(ticket.get("date_modified"))),
                    _meta_item("Age (days)", round(ticket["days_opened"]) if ticket.get("days_opened") else "—"),
                    _meta_item("Messages", ticket.get("action_count", "—")),
                    _meta_item("Customer Msgs", ticket.get("customer_message_count", "—")),
                    _meta_item("inHANCE Msgs", ticket.get("inhance_message_count", "—")),
                ],
            ),
        ],
        withBorder=True, p="md", radius="md", shadow="sm",
    )

    # Scores tab
    scores_content = dmc.SimpleGrid(
        cols={"base": 1, "sm": 2, "lg": 4},
        children=[
            _score_card("Priority", ticket.get("priority"),
                        ticket.get("priority_explanation"),
                        _priority_color(ticket.get("priority")),
                        "tabler:alert-triangle"),
            _score_card("Overall Complexity", ticket.get("overall_complexity"),
                        None,
                        _complexity_color(ticket.get("overall_complexity")),
                        "tabler:brain"),
            _score_card("Intrinsic", ticket.get("intrinsic_complexity"),
                        None, "blue", "tabler:code"),
            _score_card("Coordination", ticket.get("coordination_load"),
                        None, "violet", "tabler:users"),
        ],
    )
    scores_extra = dmc.SimpleGrid(
        cols={"base": 1, "sm": 2},
        mt="sm",
        children=[
            _score_card("Elapsed Drag", ticket.get("elapsed_drag"),
                        None, "yellow", "tabler:clock"),
            _score_card("Frustrated", ticket.get("frustrated", "—"),
                        ticket.get("frustrated_reason"),
                        "red" if ticket.get("frustrated") == "Yes" else "green",
                        "tabler:mood-sad" if ticket.get("frustrated") == "Yes" else "tabler:mood-happy"),
        ],
    )

    # Thread tab
    if actions:
        thread_content = dmc.Stack(
            [_action_card(a) for a in actions],
            gap="xs",
        )
    else:
        thread_content = dmc.Text("No actions found for this ticket.", c="dimmed", ta="center", py="xl")

    # Issue summary
    summary_items = []
    for key, label in [("issue_summary", "Issue"), ("cause_summary", "Cause"),
                       ("mechanism_summary", "Mechanism"), ("resolution_summary", "Resolution")]:
        val = ticket.get(key)
        if val:
            summary_items.append(
                dmc.Paper([
                    dmc.Text(label, size="xs", fw=700, c="dimmed", tt="uppercase"),
                    dmc.Text(val, size="sm", mt=4),
                ], withBorder=True, p="sm", radius="sm")
            )

    # Tabs
    tabs = dmc.Tabs(
        [
            dmc.TabsList([
                dmc.TabsTab("Thread", value="thread",
                            leftSection=DashIconify(icon="tabler:messages", width=16)),
                dmc.TabsTab("Scores", value="scores",
                            leftSection=DashIconify(icon="tabler:chart-bar", width=16)),
                dmc.TabsTab("Wait Profile", value="wait",
                            leftSection=DashIconify(icon="tabler:clock", width=16)),
                dmc.TabsTab("Summary", value="summary",
                            leftSection=DashIconify(icon="tabler:file-text", width=16)),
            ]),
            dmc.TabsPanel(thread_content, value="thread", pt="md"),
            dmc.TabsPanel(
                dmc.Stack([scores_content, scores_extra], gap="sm"),
                value="scores", pt="md",
            ),
            dmc.TabsPanel(_wait_chart(wait), value="wait", pt="md"),
            dmc.TabsPanel(
                dmc.Stack(summary_items, gap="sm") if summary_items
                else dmc.Text("No issue summary available.", c="dimmed", ta="center", py="xl"),
                value="summary", pt="md",
            ),
        ],
        value="thread",
    )

    return dmc.Stack([header, tabs], gap="md")


def ticket_detail_layout(ticket_id):
    """Shell layout: stores ticket_id and wraps _build_detail in a refreshable container."""
    return html.Div([
        dcc.Store(id="ticket-detail-id", data=ticket_id),
        dcc.Loading(
            id="ticket-detail-loading",
            type="dot",
            children=html.Div(id="ticket-detail-content", children=_build_detail(ticket_id)),
        ),
    ])


def register_callbacks(app):
    @app.callback(
        Output("ticket-detail-content", "children"),
        Input("ticket-refresh-btn", "n_clicks"),
        State("ticket-detail-id", "data"),
        prevent_initial_call=True,
    )
    def refresh_ticket(n_clicks, ticket_id):
        if not n_clicks or not ticket_id:
            return no_update

        # Re-sync this single ticket from TeamSupport
        try:
            subprocess.run(
                [sys.executable, _INGEST_SCRIPT, "sync", "--ticket-id", str(ticket_id), "--verbose"],
                cwd=_PROJECT_ROOT,
                env=os.environ.copy(),
                timeout=120,
                capture_output=True,
                text=True,
            )
        except Exception:
            pass  # still rebuild from whatever DB state we have

        return _build_detail(ticket_id)
