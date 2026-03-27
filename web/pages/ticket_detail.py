"""Ticket detail page — metadata, thread, scores, wait profile."""

import os
import subprocess
import sys
import threading
import urllib.parse

import dash_mantine_components as dmc
from dash import callback, ctx, dcc, html, Input, Output, State, no_update
from dash_iconify import DashIconify
import plotly.graph_objects as go

from .. import data

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_INGEST_SCRIPT = os.path.join(_PROJECT_ROOT, "run_ingest.py")
_WORK_ITEMS_SCRIPT = os.path.join(_PROJECT_ROOT, "run_import_work_items.py")

# ── Background auto-refresh state ───────────────────────────────────

_auto_refresh = {"running": False, "finished": False, "ticket_id": None}
_auto_lock = threading.Lock()


def _run_auto_refresh(ticket_id):
    """Background thread: re-sync ticket from TS + refresh its DO work item."""
    with _auto_lock:
        _auto_refresh["running"] = True
        _auto_refresh["finished"] = False
        _auto_refresh["ticket_id"] = ticket_id

    try:
        # 1. Refresh ticket from TeamSupport
        subprocess.run(
            [sys.executable, _INGEST_SCRIPT, "sync", "--ticket-id", str(ticket_id), "--verbose", "--enrich-new"],
            cwd=_PROJECT_ROOT, env=os.environ.copy(),
            timeout=180, capture_output=True, text=True,
        )
    except Exception:
        pass

    try:
        # 2. Refresh work items from Azure DevOps (quick — just upserts)
        subprocess.run(
            [sys.executable, _WORK_ITEMS_SCRIPT],
            cwd=_PROJECT_ROOT, env=os.environ.copy(),
            timeout=60, capture_output=True, text=True,
        )
    except Exception:
        pass

    with _auto_lock:
        _auto_refresh["running"] = False
        _auto_refresh["finished"] = True


def _rescore_and_rebuild(ticket_id: int, ticket_number: str) -> None:
    """Background thread: rescore all three stages for *ticket_number* (force=True)
    then rebuild customer- and product-level health rollups.

    The exclusion filters inside each run_* main will honour the newly saved
    ticket_exclusions row, so excluded stages are skipped and un-excluded
    stages are re-scored even when the thread hash hasn't changed.
    """
    try:
        from run_sentiment import main as sentiment_main
        sentiment_main(force=True, ticket_numbers=[ticket_number])
    except SystemExit:
        pass
    except Exception as exc:
        print(f"[exclusion] Sentiment rescore error for {ticket_number}: {exc}", flush=True)

    try:
        from run_priority import main as priority_main
        priority_main(write_back=False, force=True, ticket_numbers=[ticket_number])
    except SystemExit:
        pass
    except Exception as exc:
        print(f"[exclusion] Priority rescore error for {ticket_number}: {exc}", flush=True)

    try:
        from run_complexity import main as complexity_main
        complexity_main(write_back=False, force=True, ticket_numbers=[ticket_number])
    except SystemExit:
        pass
    except Exception as exc:
        print(f"[exclusion] Complexity rescore error for {ticket_number}: {exc}", flush=True)

    try:
        from run_rollups import rebuild_customer_ticket_health, rebuild_product_ticket_health
        rebuild_customer_ticket_health()
        rebuild_product_ticket_health()
        print(f"[exclusion] Health rollups rebuilt after exclusion change for {ticket_number}.", flush=True)
    except Exception as exc:
        print(f"[exclusion] Health rollup rebuild error: {exc}", flush=True)


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


def _build_teams_message(ticket):
    """Build a pre-filled Teams message from ticket metadata + complexity summary."""
    tid = ticket.get("ticket_number", "")
    name = ticket.get("ticket_name", "")
    status = ticket.get("status", "—")
    severity = ticket.get("severity", "—")
    customer = ticket.get("customer", "—")
    assignee = ticket.get("assignee", "—")
    product = ticket.get("product_name", "—")
    age = round(ticket["days_opened"]) if ticket.get("days_opened") else "—"
    do_number = ticket.get("do_number")
    do_status = ticket.get("do_status")

    lines = [
        f"📋 Ticket #{tid} — {name}",
        f"Status: {status} | Priority: {severity}",
        f"Customer: {customer}",
        f"Assignee: {assignee} | Age: {age} days",
        f"Product: {product}",
    ]
    if do_number:
        do_part = f"DO #{do_number}"
        if do_status:
            do_part += f" ({do_status})"
        lines.append(do_part)

    # Add complexity summary if available
    cx = data.get_ticket_complexity_detail(ticket.get("ticket_id"))
    if cx and cx.get("complexity_summary"):
        lines.append("")
        lines.append(cx["complexity_summary"])

    # Links
    lines.append("")
    if tid:
        lines.append(f"TeamSupport: https://app.na2.teamsupport.com/?TicketNumber={tid}")
    if do_number:
        lines.append(f"DevOps: https://dev.azure.com/inHanceUtilities/Impresa/_workitems/edit/{do_number}/")

    return "\n".join(lines)


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

def _build_detail(ticket_id, back_href="/tickets", *, inline=False):
    """Build the full ticket detail content (header + tabs) from DB data.

    When *inline* is True (e.g. embedded inside a modal), the nav row
    (back link, Share in Teams, Refresh) is omitted so the caller can
    provide its own navigation affordance.
    """
    ticket = data.get_ticket_detail(ticket_id)
    if not ticket:
        return dmc.Stack([
            dmc.Title("Ticket Not Found", order=3),
            dmc.Text(f"No ticket with ID {ticket_id} exists in the database.", c="dimmed"),
        ] + ([] if inline else [dmc.Anchor("Back to Tickets", href=back_href)]), gap="md")

    actions = data.get_ticket_actions(ticket_id)
    wait = data.get_ticket_wait_profile(ticket_id)
    events = data.get_ticket_events(ticket_id)
    ticket_number = ticket.get("ticket_number", "")
    ticket_name = ticket.get("ticket_name", "")
    teamsupport_url = (
        f"https://app.na2.teamsupport.com/?TicketNumber={ticket_number}"
        if ticket_number else None
    )

    # Teams deep link — msteams: protocol goes straight to desktop app
    teams_message = _build_teams_message(ticket)
    encoded_msg = urllib.parse.quote(teams_message, safe="")
    teams_url = f"msteams:/l/chat/0/0?users=&message={encoded_msg}"

    # Header
    nav_row = (
        None if inline else
        dmc.Group(
            [
                dmc.Anchor(
                    dmc.Group([DashIconify(icon="tabler:arrow-left", width=16), "Tickets"], gap=4),
                    href=back_href, size="sm",
                ),
                dmc.Group(
                    [
                        dmc.Button(
                            "Share in Teams",
                            id="ticket-teams-btn",
                            leftSection=DashIconify(icon="tabler:brand-teams", width=16),
                            variant="light",
                            color="violet",
                            size="compact-sm",
                        ),
                        dmc.Button(
                            "Refresh",
                            id="ticket-refresh-btn",
                            leftSection=DashIconify(icon="tabler:refresh", width=16),
                            variant="light",
                            size="compact-sm",
                        ),
                    ],
                    ml="auto",
                    gap="xs",
                ),
            ],
            justify="space-between",
            mb="sm",
        )
    )

    header = dmc.Paper(
        [c for c in [
            None if inline else dcc.Store(id="teams-share-url", data=teams_url),
            nav_row,
            dmc.Group(
                [
                    dmc.Title(
                        [
                            dmc.Anchor(
                                f"#{ticket_number}",
                                href=teamsupport_url,
                                target="_blank",
                                underline="never",
                                style={"color": "inherit"},
                            ),
                            html.Span(f" — {ticket_name}"),
                        ],
                        order=3,
                    ),
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
                cols={"base": 2, "sm": 4, "lg": 7},
                children=[
                    _meta_item("Created", _format_dt(ticket.get("date_created"))),
                    _meta_item("Modified", _format_dt(ticket.get("date_modified"))),
                    _meta_item("Age (days)", round(ticket["days_opened"]) if ticket.get("days_opened") else "—"),
                    _meta_item("Messages", ticket.get("action_count", "—")),
                    _meta_item("Customer Msgs", ticket.get("customer_message_count", "—")),
                    _meta_item("inHANCE Msgs", ticket.get("inhance_message_count", "—")),
                    _meta_item("DO #", ticket.get("do_number", "—")),
                    _meta_item("DO Status", ticket.get("do_status", "—")),
                ],
            ),
        ] if c is not None],
        withBorder=True, p="md", radius="md", shadow="sm",
    )

    # Scores tab
    excl = data.get_ticket_exclusions(ticket_id) or {}
    exclusion_panel = dmc.Paper(
        [
            dmc.Text("Scoring Exclusions", size="xs", fw=700, c="dimmed", tt="uppercase", mb="xs"),
            dmc.Text(
                "Prevent this ticket from being scored by the checked stages.",
                size="xs", c="dimmed", mb="sm",
            ),
            dmc.Stack(
                [
                    dmc.Checkbox(
                        id="excl-priority", label="Exclude from Priority scoring",
                        checked=bool(excl.get("exclude_priority")),
                    ),
                    dmc.Checkbox(
                        id="excl-sentiment", label="Exclude from Sentiment / Frustration scoring",
                        checked=bool(excl.get("exclude_sentiment")),
                    ),
                    dmc.Checkbox(
                        id="excl-complexity", label="Exclude from Complexity scoring",
                        checked=bool(excl.get("exclude_complexity")),
                    ),
                    dmc.TextInput(
                        id="excl-reason",
                        label="Reason (optional)",
                        placeholder="e.g. Training bundle — not a real support issue",
                        value=excl.get("reason") or "",
                        size="xs",
                    ),
                    dmc.Group(
                        [
                            dmc.Button(
                                "Save Exclusions",
                                id="excl-save-btn",
                                size="compact-sm",
                                variant="light",
                                color="orange",
                                leftSection=DashIconify(icon="tabler:device-floppy", width=14),
                            ),
                            html.Div(id="excl-save-status"),
                        ],
                        gap="sm",
                        mt="xs",
                    ),
                ],
                gap="xs",
            ),
        ],
        withBorder=True, p="md", radius="md", mt="md",
    )

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

    # Complexity analysis detail
    cx = data.get_ticket_complexity_detail(ticket_id)
    if cx:
        if cx.get("complexity_summary"):
            summary_items.append(
                dmc.Paper([
                    dmc.Text("Complexity Summary", size="xs", fw=700, c="dimmed", tt="uppercase"),
                    dmc.Text(cx["complexity_summary"], size="sm", mt=4),
                ], withBorder=True, p="sm", radius="sm")
            )
        if cx.get("evidence"):
            evidence = cx["evidence"] if isinstance(cx["evidence"], list) else [cx["evidence"]]
            summary_items.append(
                dmc.Paper([
                    dmc.Text("Evidence", size="xs", fw=700, c="dimmed", tt="uppercase"),
                    dmc.List(
                        [dmc.ListItem(dmc.Text(e, size="sm")) for e in evidence],
                        size="sm", mt=4,
                    ),
                ], withBorder=True, p="sm", radius="sm")
            )
        if cx.get("noise_factors"):
            noise = cx["noise_factors"] if isinstance(cx["noise_factors"], list) else [cx["noise_factors"]]
            summary_items.append(
                dmc.Paper([
                    dmc.Text("Noise Factors", size="xs", fw=700, c="dimmed", tt="uppercase"),
                    dmc.List(
                        [dmc.ListItem(dmc.Text(n, size="sm")) for n in noise],
                        size="sm", mt=4,
                    ),
                ], withBorder=True, p="sm", radius="sm")
            )
        if cx.get("duration_vs_complexity_note"):
            summary_items.append(
                dmc.Paper([
                    dmc.Text("Duration vs Complexity", size="xs", fw=700, c="dimmed", tt="uppercase"),
                    dmc.Text(cx["duration_vs_complexity_note"], size="sm", mt=4),
                ], withBorder=True, p="sm", radius="sm")
            )

    # DO work item tab (only when ticket has a DO #)
    do_number = ticket.get("do_number")
    do_tab = None
    do_panel = None
    if do_number:
        wi = data.get_work_item_detail(do_number)
        do_comments = data.get_do_comments(do_number)

        # Work item metadata
        do_url = f"https://dev.azure.com/inHanceUtilities/Impresa/_workitems/edit/{do_number}/"
        do_link = dmc.Stack([
            dmc.Text("DO #", size="xs", c="dimmed", tt="uppercase", fw=700),
            dmc.Anchor(str(do_number), href=do_url, target="_blank", size="sm"),
        ], gap=2)
        wi_meta_items = []
        if wi:
            wi_meta_items = [
                do_link,
                _meta_item("State", wi.get("state", "—")),
                _meta_item("Type", wi.get("work_item_type", "—")),
                _meta_item("Assigned To", wi.get("assigned_to", "—")),
                _meta_item("Iteration", wi.get("iteration_path", "—")),
                _meta_item("Changed", _format_dt(wi.get("changed_date"))),
            ]
        else:
            wi_meta_items = [
                do_link,
                _meta_item("State", ticket.get("do_status", "—")),
            ]

        wi_header = dmc.SimpleGrid(
            cols={"base": 2, "sm": 3, "lg": 6},
            children=wi_meta_items,
        )

        # Title
        wi_title = None
        if wi and wi.get("title"):
            wi_title = dmc.Text(wi["title"], size="sm", fw=500, mb="sm")

        # Comments
        if do_comments:
            import re
            comment_cards = []
            for c in do_comments:
                author = c.get("createdBy", {}).get("displayName", "Unknown")
                created = c.get("createdDate", "")[:16].replace("T", " ")
                text_html = c.get("text", "")
                # Strip HTML tags for clean display
                text_clean = re.sub(r"<[^>]+>", " ", text_html)
                text_clean = re.sub(r"\s+", " ", text_clean).strip()
                text_clean = text_clean.replace("&nbsp;", " ").replace("&amp;", "&").replace("&lt;", "<").replace("&gt;", ">").replace("&quot;", '"')

                comment_cards.append(
                    dmc.Paper(
                        [
                            dmc.Group(
                                [
                                    dmc.Text(author, size="sm", fw=600),
                                    dmc.Text(created, size="xs", c="dimmed"),
                                ],
                                gap="xs",
                                mb=4,
                            ),
                            dmc.Text(text_clean, size="sm", style={"lineHeight": 1.5}),
                        ],
                        withBorder=True, p="sm", radius="sm",
                        style={"borderLeft": "3px solid #7950f2"},
                    )
                )
            comments_section = dmc.Stack(comment_cards, gap="xs")
        else:
            comments_section = dmc.Text("No comments on this work item.", c="dimmed", ta="center", py="md")

        do_content = dmc.Stack(
            [wi_header, wi_title, dmc.Divider(my="sm"), comments_section],
            gap="sm",
        )

        do_tab = dmc.TabsTab(
            f"DO #{do_number}", value="devops",
            leftSection=DashIconify(icon="tabler:brand-azure", width=16),
        )
        do_panel = dmc.TabsPanel(do_content, value="devops", pt="md")

    # Tabs
    tab_list = [
        dmc.TabsTab("Thread", value="thread",
                    leftSection=DashIconify(icon="tabler:messages", width=16)),
        dmc.TabsTab("Scores", value="scores",
                    leftSection=DashIconify(icon="tabler:chart-bar", width=16)),
        dmc.TabsTab("Wait Profile", value="wait",
                    leftSection=DashIconify(icon="tabler:clock", width=16)),
        dmc.TabsTab("Summary", value="summary",
                    leftSection=DashIconify(icon="tabler:file-text", width=16)),
    ]
    if do_tab:
        tab_list.append(do_tab)

    # Activity tab (always present — shows user-initiated events like Teams shares)
    if events:
        event_cards = []
        for ev in events:
            icon_map = {"teams_share": "tabler:brand-teams"}
            label_map = {"teams_share": "Shared to Teams"}
            ev_type = ev.get("event_type", "")
            ev_icon = icon_map.get(ev_type, "tabler:activity")
            ev_label = label_map.get(ev_type, ev_type)
            ev_detail = ev.get("detail") or {}
            ev_by = ev.get("created_by") or ""
            ev_at = _format_dt(ev.get("created_at"))

            detail_text = None
            if isinstance(ev_detail, dict) and ev_detail.get("message_preview"):
                preview = ev_detail["message_preview"]
                if len(preview) > 200:
                    preview = preview[:200] + "…"
                detail_text = dmc.Text(preview, size="xs", c="dimmed",
                                       style={"whiteSpace": "pre-wrap", "marginTop": "0.25rem"})

            event_cards.append(
                dmc.Paper([
                    dmc.Group([
                        DashIconify(icon=ev_icon, width=16, color="#7950f2"),
                        dmc.Text(ev_label, size="sm", fw=600),
                        dmc.Text(ev_by, size="xs", c="dimmed") if ev_by else None,
                        dmc.Text(ev_at, size="xs", c="dimmed"),
                    ], gap="xs"),
                    detail_text,
                ], withBorder=True, p="sm", radius="sm",
                   style={"borderLeft": "3px solid #7950f2"})
            )
        activity_content = dmc.Stack(event_cards, gap="xs")
    else:
        activity_content = dmc.Text("No activity events recorded yet.", c="dimmed", ta="center", py="xl")

    tab_list.append(
        dmc.TabsTab("Activity", value="activity",
                    leftSection=DashIconify(icon="tabler:activity", width=16)),
    )

    panels = [
        dmc.TabsPanel(thread_content, value="thread", pt="md"),
        dmc.TabsPanel(
            dmc.Stack([scores_content, scores_extra, exclusion_panel], gap="sm"),
            value="scores", pt="md",
        ),
        dmc.TabsPanel(_wait_chart(wait), value="wait", pt="md"),
        dmc.TabsPanel(
            dmc.Stack(summary_items, gap="sm") if summary_items
            else dmc.Text("No issue summary available.", c="dimmed", ta="center", py="xl"),
            value="summary", pt="md",
        ),
    ]
    if do_panel:
        panels.append(do_panel)
    panels.append(dmc.TabsPanel(activity_content, value="activity", pt="md"))

    tabs = dmc.Tabs(
        [dmc.TabsList(tab_list)] + panels,
        value="thread",
    )

    return dmc.Stack([header, tabs], gap="md")


def ticket_detail_layout(ticket_id, back_href="/tickets"):
    """Shell layout: stores ticket_id and wraps _build_detail in a refreshable container."""
    # Kick off auto-refresh immediately
    with _auto_lock:
        already_running = _auto_refresh["running"]
    if not already_running:
        threading.Thread(target=_run_auto_refresh, args=(ticket_id,), daemon=True).start()

    return html.Div([
        dcc.Store(id="ticket-detail-id", data=ticket_id),
        dcc.Store(id="ticket-detail-back-href", data=back_href),
        dcc.Interval(id="auto-refresh-poll", interval=2000, disabled=False),
        html.Div(id="auto-refresh-indicator", children=dmc.Group([
            dmc.Loader(size="xs", type="dots"),
            dmc.Text("Syncing latest data…", size="xs", c="dimmed"),
        ], gap=4), style={"position": "fixed", "bottom": 16, "left": 16, "zIndex": 999,
                          "background": "white", "padding": "6px 12px", "borderRadius": 8,
                          "boxShadow": "0 1px 4px rgba(0,0,0,0.15)"}),
        html.Div(id="ticket-detail-content", children=_build_detail(ticket_id, back_href=back_href)),
    ])


def register_callbacks(app):
    @app.callback(
        Output("ticket-detail-content", "children"),
        Input("ticket-refresh-btn", "n_clicks"),
        State("ticket-detail-id", "data"),
        State("ticket-detail-back-href", "data"),
        prevent_initial_call=True,
    )
    def refresh_ticket(n_clicks, ticket_id, back_href):
        if not n_clicks or not ticket_id:
            return no_update

        # Manual refresh: re-sync this single ticket from TeamSupport
        with _auto_lock:
            already_running = _auto_refresh["running"]
        if not already_running:
            threading.Thread(target=_run_auto_refresh, args=(ticket_id,), daemon=True).start()
        return no_update

    @app.callback(
        Output("auto-refresh-indicator", "children"),
        Output("auto-refresh-indicator", "style"),
        Output("auto-refresh-poll", "disabled"),
        Output("ticket-detail-content", "children", allow_duplicate=True),
        Input("auto-refresh-poll", "n_intervals"),
        State("ticket-detail-id", "data"),
        State("ticket-detail-back-href", "data"),
        prevent_initial_call=True,
    )
    def poll_auto_refresh(_n, ticket_id, back_href):
        with _auto_lock:
            running = _auto_refresh["running"]
            finished = _auto_refresh["finished"]

        if running:
            indicator = dmc.Group([
                dmc.Loader(size="xs", type="dots"),
                dmc.Text("Syncing latest data…", size="xs", c="dimmed"),
            ], gap=4)
            style = {"position": "fixed", "bottom": 16, "left": 16, "zIndex": 999,
                     "background": "white", "padding": "6px 12px", "borderRadius": 8,
                     "boxShadow": "0 1px 4px rgba(0,0,0,0.15)"}
            return indicator, style, False, no_update

        if finished:
            with _auto_lock:
                _auto_refresh["finished"] = False
            return "", {"display": "none"}, True, _build_detail(ticket_id, back_href=back_href or "/tickets")

        return "", {"display": "none"}, True, no_update

    # Teams share: clientside opens the URL; server callback logs the event
    from dash import ClientsideFunction
    app.clientside_callback(
        ClientsideFunction(namespace="clientside", function_name="openTeamsLink"),
        Output("teams-share-url", "data"),  # dummy output (no-op)
        Input("ticket-teams-btn", "n_clicks"),
        State("teams-share-url", "data"),
        prevent_initial_call=True,
    )

    @app.callback(
        Output("ticket-detail-content", "children", allow_duplicate=True),
        Input("ticket-teams-btn", "n_clicks"),
        State("ticket-detail-id", "data"),
        State("ticket-detail-back-href", "data"),
        prevent_initial_call=True,
    )
    def log_teams_share(n_clicks, ticket_id, back_href):
        if not n_clicks or not ticket_id:
            return no_update
        ticket = data.get_ticket_detail(ticket_id)
        if ticket:
            msg = _build_teams_message(ticket)
            data.insert_ticket_event(
                ticket_id,
                "teams_share",
                detail={"message_preview": msg[:500]},
            )
        return _build_detail(ticket_id, back_href=back_href or "/tickets")

    @app.callback(
        Output("excl-save-status", "children"),
        Input("excl-save-btn", "n_clicks"),
        State("ticket-detail-id", "data"),
        State("excl-priority", "checked"),
        State("excl-sentiment", "checked"),
        State("excl-complexity", "checked"),
        State("excl-reason", "value"),
        prevent_initial_call=True,
    )
    def save_exclusions(n_clicks, ticket_id, excl_priority, excl_sentiment, excl_complexity, reason):
        if not n_clicks or not ticket_id:
            return no_update
        try:
            data.upsert_ticket_exclusions(
                ticket_id,
                exclude_priority=bool(excl_priority),
                exclude_sentiment=bool(excl_sentiment),
                exclude_complexity=bool(excl_complexity),
                reason=reason or None,
            )
            ticket_number = data.get_ticket_number(ticket_id)
            if ticket_number:
                threading.Thread(
                    target=_rescore_and_rebuild,
                    args=(ticket_id, ticket_number),
                    daemon=True,
                ).start()
            return dmc.Text("Saved — rescoring in background…", size="xs", c="green")
        except Exception as e:
            return dmc.Text(f"Error: {e}", size="xs", c="red")
