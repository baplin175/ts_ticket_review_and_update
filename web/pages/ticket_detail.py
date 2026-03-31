"""Ticket detail page — metadata, thread, scores, wait profile."""

import os
import subprocess
import sys
import threading
import urllib.parse

import dash_mantine_components as dmc
from dash import callback, ctx, dcc, html, Input, MATCH, Output, State, no_update
from dash_iconify import DashIconify
import plotly.graph_objects as go

from .. import data

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_INGEST_SCRIPT = os.path.join(_PROJECT_ROOT, "run_ingest.py")
_WORK_ITEMS_SCRIPT = os.path.join(_PROJECT_ROOT, "run_import_work_items.py")

# ── Background auto-refresh state ───────────────────────────────────

_auto_refresh = {"running": False, "finished": False, "ticket_id": None}
_auto_lock = threading.Lock()

# Per-instance chat state (keyed by ctx string, e.g. "page" or "modal")
_chat_state: dict[str, dict] = {}
_chat_lock = threading.Lock()


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


# ── Chat helpers ──────────────────────────────────────────────────────

def _build_ticket_context(ticket: dict, actions: list[dict], do_comments: list[dict] | None = None) -> str:
    """Serialize ticket metadata + activity thread into Matcha's 'context' field."""
    import re as _re
    from prompt_store import get_prompt

    do_number = ticket.get("do_number")
    do_status = ticket.get("do_status")

    try:
        system_prompt = get_prompt("ticket_chat_system", allow_fallback=True)["content"].strip()
    except Exception:
        system_prompt = None

    lines = []
    if system_prompt:
        lines += ["=== ANALYST INSTRUCTIONS ===", system_prompt, ""]

    lines += [
        "=== TICKET CONTEXT ===",
        f"Ticket #: {ticket.get('ticket_number', '—')}",
        f"Title: {ticket.get('ticket_name', '—')}",
        f"Status: {ticket.get('status', '—')}",
        f"Severity: {ticket.get('severity', '—')}",
        f"Customer: {ticket.get('customer', '—')}",
        f"Assignee: {ticket.get('assignee', '—')}",
        f"Product: {ticket.get('product_name', '—')}",
        f"Age (days): {round(ticket['days_opened']) if ticket.get('days_opened') else '—'}",
        f"Priority score: {ticket.get('priority', '—')}",
        f"Complexity: {ticket.get('overall_complexity', '—')}",
        f"Frustrated: {ticket.get('frustrated', '—')}",
    ]
    if do_number:
        lines.append(f"Linked DO #: {do_number} | DO Status: {do_status or '—'}")

    for key, label in [("issue_summary", "Issue"), ("cause_summary", "Cause"),
                       ("resolution_summary", "Resolution")]:
        val = ticket.get(key)
        if val:
            lines.append(f"{label}: {val}")

    if do_comments:
        lines.append("\n=== RECENT DO COMMENTS (most recent first) ===")
        for c in do_comments[:5]:
            author = c.get("createdBy", {}).get("displayName", "Unknown")
            created = c.get("createdDate", "")[:16].replace("T", " ")
            text_html = c.get("text", "")
            text_clean = _re.sub(r"<[^>]+>", " ", text_html)
            text_clean = _re.sub(r"\s+", " ", text_clean).strip()
            text_clean = (text_clean
                          .replace("&nbsp;", " ").replace("&amp;", "&")
                          .replace("&lt;", "<").replace("&gt;", ">")
                          .replace("&quot;", '"'))
            if len(text_clean) > 600:
                text_clean = text_clean[:600] + "…"
            lines.append(f"[{created}] {author}: {text_clean}")

    if actions:
        lines.append("\n=== TICKET THREAD (most recent messages) ===")
        for a in actions[-25:]:
            party = "inHANCE" if a.get("party") == "inh" else "Customer" if a.get("party") == "cust" else "System"
            creator = a.get("creator_name") or "Unknown"
            created = str(a.get("created_at") or "")[:16]
            desc = a.get("cleaned_description") or a.get("description") or "(empty)"
            if len(desc) > 600:
                desc = desc[:600] + "…"
            lines.append(f"[{created}] {party} ({creator}): {desc}")

    return "\n".join(lines)


def _render_chat_messages(history: list[dict], pending: bool = False) -> list:
    """Convert a list of {role, content} dicts into Dash Paper components, newest first."""
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
        role = msg.get("role", "")
        content = msg.get("content", "")
        is_user = role == "user"
        cards.append(
            dmc.Paper(
                [
                    dmc.Group(
                        [dmc.Badge("You" if is_user else "Matcha",
                                   color="blue" if is_user else "violet",
                                   variant="filled", size="sm")],
                        mb=4,
                    ),
                    dmc.Text(
                        str(content),
                        size="sm",
                        style={"whiteSpace": "pre-wrap", "lineHeight": 1.6},
                    ),
                ],
                withBorder=True,
                p="sm",
                radius="sm",
                style={"borderLeft": f"3px solid {'#1c7ed6' if is_user else '#7950f2'}"},
            )
        )
    return cards


def _run_chat(instance_ctx: str, context: str, messages: list[dict], chat_history: list[dict]) -> None:
    """Background thread: call Matcha chat API and store the result."""
    with _chat_lock:
        _chat_state[instance_ctx] = {"running": True, "result": None, "error": None}
    try:
        from matcha_client import call_matcha_chat
        reply = call_matcha_chat(context=context, messages=messages, chat_history=chat_history)
        with _chat_lock:
            _chat_state[instance_ctx]["result"] = reply
    except Exception as exc:
        with _chat_lock:
            _chat_state[instance_ctx]["error"] = str(exc)
    finally:
        with _chat_lock:
            _chat_state[instance_ctx]["running"] = False


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
        "─── Ticket Reference ───",
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

    # Links — bare URLs so Teams auto-linkifies them in the compose box
    lines.append("")
    if tid:
        lines.append(f"TeamSupport: https://app.na2.teamsupport.com/?TicketNumber={tid}")
    if do_number:
        lines.append(f"DevOps: https://dev.azure.com/inHanceUtilities/Impresa/_workitems/edit/{do_number}/")

    return "\n".join(lines)


def _build_email_body(ticket):
    """Build a pre-filled email body from ticket metadata (plain text; URLs auto-link in most clients)."""
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
        "",
        "",
        f"Ticket #{tid} — {name}",
        "",
        f"Status:   {status}",
        f"Priority: {severity}",
        f"Customer: {customer}",
        f"Assignee: {assignee}",
        f"Age:      {age} days",
        f"Product:  {product}",
    ]
    if do_number:
        do_part = f"DO #{do_number}"
        if do_status:
            do_part += f" ({do_status})"
        lines.append(f"DevOps:   {do_part}")

    cx = data.get_ticket_complexity_detail(ticket.get("ticket_id"))
    if cx and cx.get("complexity_summary"):
        lines.append("")
        lines.append(cx["complexity_summary"])

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


_ALIGNMENT_LABEL_TEXT = {
    "aligned": ("Aligned", "green"),
    "ticket_open_do_closed": ("Open / DO Closed", "red"),
    "ticket_closed_do_active": ("Closed / DO Active", "red"),
    "do_stalled_or_abandoned": ("DO Stalled", "yellow"),
    "do_scope_mismatch": ("Scope Mismatch", "red"),
    "unclear": ("Unclear", "gray"),
}


def _build_do_alignment_meta(ticket: dict) -> list:
    """Return a list containing one meta-style stack for DO Alignment (empty list if no data)."""
    label = ticket.get("do_mismatch_label")
    explanation = ticket.get("do_alignment_explanation")
    if not label:
        return []
    text, color = _ALIGNMENT_LABEL_TEXT.get(label, (label, "gray"))
    children = [
        dmc.Text("DO Align", size="xs", c="dimmed", tt="uppercase", fw=700),
        dmc.Badge(text, color=color, variant="light", size="sm"),
    ]
    if explanation and label != "aligned":
        children.append(dmc.Text(explanation, size="xs", c="dimmed", mt=2, style={"maxWidth": 260}))
    return [dmc.Stack(children, gap=2)]


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

def _build_detail(ticket_id, back_href=None, back_label="Tickets", *, ctx="page", active_tab="thread"):
    """Build the full ticket detail content (header + tabs) from DB data.

    *ctx* is used as the index in all pattern-matched component IDs so the
    same callbacks fire correctly whether the detail is on the full page
    (ctx="page") or embedded inside a modal (ctx="modal").
    *back_href* controls the back-link in the nav row; pass None to hide it.
    *back_label* is the visible text for the back link (default: "Tickets").
    """
    ticket = data.get_ticket_detail(ticket_id)
    if not ticket:
        return dmc.Stack([
            dmc.Title("Ticket Not Found", order=3),
            dmc.Text(f"No ticket with ID {ticket_id} exists in the database.", c="dimmed"),
        ] + ([dmc.Anchor(f"Back to {back_label}", href=back_href)] if back_href else []), gap="md")

    actions = data.get_ticket_actions(ticket_id)
    wait = data.get_ticket_wait_profile(ticket_id)
    events = data.get_ticket_events(ticket_id)
    ticket_number = ticket.get("ticket_number", "")
    ticket_name = ticket.get("ticket_name", "")
    teamsupport_url = (
        f"https://app.na2.teamsupport.com/?TicketNumber={ticket_number}"
        if ticket_number else None
    )

    # Teams deep link — https URL redirects to app and passes message reliably
    teams_message = _build_teams_message(ticket)
    encoded_msg = urllib.parse.quote(teams_message, safe="")
    teams_url = f"https://teams.microsoft.com/l/chat/0/0?users=&message={encoded_msg}"

    # Email deep link — mailto: protocol opens default email client
    email_subject = urllib.parse.quote(f"Ticket #{ticket.get('ticket_number', '')} — {ticket.get('ticket_name', '')}", safe="")
    email_body = urllib.parse.quote(_build_email_body(ticket), safe="")
    email_url = f"mailto:?subject={email_subject}&body={email_body}"

    flag_review = data.get_ticket_flag(ticket_id)

    # Header
    nav_row = dmc.Group(
        [c for c in [
            dmc.Anchor(
                dmc.Group([DashIconify(icon="tabler:arrow-left", width=16), back_label], gap=4),
                href=back_href, size="sm",
            ) if back_href else None,
            dmc.Group(
                [
                    dmc.Button(
                        "Flagged" if flag_review else "Flag",
                        id={"type": "ticket-flag-btn", "index": ctx},
                        leftSection=DashIconify(
                            icon="tabler:flag-filled" if flag_review else "tabler:flag",
                            width=16,
                        ),
                        variant="filled" if flag_review else "light",
                        color="orange",
                        size="compact-sm",
                    ),
                    dmc.Button(
                        "Share in Teams",
                        id={"type": "ticket-teams-btn", "index": ctx},
                        leftSection=DashIconify(icon="tabler:brand-teams", width=16),
                        variant="light",
                        color="violet",
                        size="compact-sm",
                    ),
                    dmc.Button(
                        "Email",
                        id={"type": "ticket-email-btn", "index": ctx},
                        leftSection=DashIconify(icon="tabler:mail", width=16),
                        variant="light",
                        color="blue",
                        size="compact-sm",
                    ),
                    dmc.Button(
                        "Refresh",
                        id={"type": "ticket-refresh-btn", "index": ctx},
                        leftSection=DashIconify(icon="tabler:refresh", width=16),
                        variant="light",
                        size="compact-sm",
                    ),
                ],
                ml="auto",
                gap="xs",
            ),
        ] if c is not None],
        justify="space-between",
        mb="sm",
    )

    header = dmc.Paper(
        [
            dcc.Store(id={"type": "teams-share-url", "index": ctx}, data=teams_url),
            dcc.Store(id={"type": "email-share-url", "index": ctx}, data=email_url),
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
                    *(_build_do_alignment_meta(ticket) if ticket.get("do_number") else []),
                ],
            ),
        ],
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
                        id={"type": "excl-priority", "index": ctx}, label="Exclude from Priority scoring",
                        checked=bool(excl.get("exclude_priority")),
                    ),
                    dmc.Checkbox(
                        id={"type": "excl-sentiment", "index": ctx}, label="Exclude from Sentiment / Frustration scoring",
                        checked=bool(excl.get("exclude_sentiment")),
                    ),
                    dmc.Checkbox(
                        id={"type": "excl-complexity", "index": ctx}, label="Exclude from Complexity scoring",
                        checked=bool(excl.get("exclude_complexity")),
                    ),
                    dmc.TextInput(
                        id={"type": "excl-reason", "index": ctx},
                        label="Reason (optional)",
                        placeholder="e.g. Training bundle — not a real support issue",
                        value=excl.get("reason") or "",
                        size="xs",
                    ),
                    dmc.Group(
                        [
                            dmc.Button(
                                "Save Exclusions",
                                id={"type": "excl-save-btn", "index": ctx},
                                size="compact-sm",
                                variant="light",
                                color="orange",
                                leftSection=DashIconify(icon="tabler:device-floppy", width=14),
                            ),
                            html.Div(id={"type": "excl-save-status", "index": ctx}),
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

        # DO alignment callout — shown when misaligned or partially aligned
        alignment_alert = None
        do_aligned = ticket.get("do_aligned")
        do_mismatch_label = ticket.get("do_mismatch_label")
        do_explanation = ticket.get("do_alignment_explanation")
        if do_aligned and do_aligned != "Yes":
            _atext, _acolor = _ALIGNMENT_LABEL_TEXT.get(
                do_mismatch_label, (do_mismatch_label or "Mismatch", "gray")
            )
            alignment_alert = dmc.Alert(
                children=[
                    dmc.Text(do_explanation or "Alignment issue detected.", size="sm"),
                ],
                title=f"DO Alignment: {_atext}",
                color=_acolor,
                icon=DashIconify(icon="tabler:alert-triangle", width=18),
                mb="sm",
            )

        do_content_children = [wi_header, wi_title]
        if alignment_alert:
            do_content_children.append(alignment_alert)
        do_content_children += [dmc.Divider(my="sm"), comments_section]

        do_content = dmc.Stack(do_content_children, gap="sm")

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
    tab_list.append(
        dmc.TabsTab("Ask Matcha", value="chat",
                    leftSection=DashIconify(icon="tabler:robot", width=16)),
    )

    # Ask Matcha chat panel
    chat_panel_content = html.Div(
        [
            # Input row — full width at the top
            dmc.Textarea(
                id={"type": "chat-input", "index": ctx},
                placeholder="Ask anything about this ticket… (Enter to send, Shift+Enter for newline)",
                autosize=True,
                minRows=2,
                maxRows=8,
                style={"width": "100%", "resize": "none"},
                className="chat-textarea",
                mb="xs",
            ),
            dmc.Group(
                [
                    dmc.Button(
                        "Send",
                        id={"type": "chat-send-btn", "index": ctx},
                        leftSection=DashIconify(icon="tabler:send", width=14),
                        size="sm",
                        className="chat-send-btn",
                    ),
                    dmc.Button(
                        "Clear chat",
                        id={"type": "chat-clear-btn", "index": ctx},
                        variant="subtle",
                        color="gray",
                        size="sm",
                    ),
                ],
                gap="xs",
                mb="sm",
            ),
            # Message history — scrollable, most recent first
            html.Div(
                id={"type": "chat-messages", "index": ctx},
                style={
                    "minHeight": 300,
                    "maxHeight": 480,
                    "overflowY": "auto",
                    "display": "flex",
                    "flexDirection": "column",
                    "gap": "8px",
                    "padding": "4px 2px",
                },
            ),
        ],
        className="chat-left-col",
        style={"display": "flex", "flexDirection": "column"},
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
    panels.append(dmc.TabsPanel(chat_panel_content, value="chat", pt="md"))

    tabs = dmc.Tabs(
        [dmc.TabsList(tab_list)] + panels,
        id={"type": "ticket-tabs", "index": ctx},
        value=active_tab,
        keepMounted=True,
    )

    return dmc.Stack([header, tabs], gap="md")


def build_ticket_shell(ticket_id, back_href=None, back_label="Tickets", *, ctx="modal"):
    """Return the full shell (stores, interval, indicator + detail content) for *ticket_id*.

    *ctx* is used as the index in all pattern-matched component IDs, so the same
    callbacks work whether the ticket is on its own page (ctx="page") or embedded
    inside a modal (ctx="modal").
    """
    with _auto_lock:
        already_running = _auto_refresh["running"]
    if not already_running:
        threading.Thread(target=_run_auto_refresh, args=(ticket_id,), daemon=True).start()

    return html.Div([
        dcc.Store(id={"type": "ticket-detail-id", "index": ctx}, data=ticket_id),
        dcc.Store(id={"type": "ticket-detail-back-href", "index": ctx}, data=back_href),
        dcc.Store(id={"type": "ticket-detail-back-label", "index": ctx}, data=back_label),
        dcc.Store(id={"type": "active-tab", "index": ctx}, data="thread"),
        # Chat stores live here (not inside _build_detail) so they survive auto-refresh rebuilds
        dcc.Store(id={"type": "chat-history", "index": ctx}, data=[]),
        dcc.Interval(id={"type": "chat-poll", "index": ctx}, interval=2000, disabled=True),
        dcc.Interval(id={"type": "auto-refresh-poll", "index": ctx}, interval=2000, disabled=False),
        html.Div(
            id={"type": "auto-refresh-indicator", "index": ctx},
            children=dmc.Group([
                dmc.Loader(size="xs", type="dots"),
                dmc.Text("Syncing latest data…", size="xs", c="dimmed"),
            ], gap=4),
            style={"position": "fixed", "bottom": 16, "left": 16, "zIndex": 999,
                   "background": "white", "padding": "6px 12px", "borderRadius": 8,
                   "boxShadow": "0 1px 4px rgba(0,0,0,0.15)"},
        ),
        html.Div(
            id={"type": "ticket-detail-content", "index": ctx},
            children=_build_detail(ticket_id, back_href=back_href, back_label=back_label, ctx=ctx),
        ),
    ])


def ticket_detail_layout(ticket_id, back_href="/tickets"):
    """Entry point for the /ticket/{id} page route."""
    return build_ticket_shell(ticket_id, back_href=back_href, ctx="page")


def register_callbacks(app):
    @app.callback(
        Output({"type": "ticket-detail-content", "index": MATCH}, "children"),
        Input({"type": "ticket-refresh-btn", "index": MATCH}, "n_clicks"),
        State({"type": "ticket-detail-id", "index": MATCH}, "data"),
        State({"type": "ticket-detail-back-href", "index": MATCH}, "data"),
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
        Output({"type": "auto-refresh-indicator", "index": MATCH}, "children"),
        Output({"type": "auto-refresh-indicator", "index": MATCH}, "style"),
        Output({"type": "auto-refresh-poll", "index": MATCH}, "disabled"),
        Output({"type": "ticket-detail-content", "index": MATCH}, "children", allow_duplicate=True),
        Input({"type": "auto-refresh-poll", "index": MATCH}, "n_intervals"),
        State({"type": "ticket-detail-id", "index": MATCH}, "data"),
        State({"type": "ticket-detail-back-href", "index": MATCH}, "data"),
        State({"type": "ticket-detail-back-label", "index": MATCH}, "data"),
        State({"type": "active-tab", "index": MATCH}, "data"),
        prevent_initial_call=True,
    )
    def poll_auto_refresh(_n, ticket_id, back_href, back_label, active_tab):
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
            instance_ctx = ctx.triggered_id["index"] if isinstance(ctx.triggered_id, dict) else "page"
            return "", {"display": "none"}, True, _build_detail(
                ticket_id, back_href=back_href, back_label=back_label or "Tickets",
                ctx=instance_ctx, active_tab=active_tab or "thread",
            )

        return "", {"display": "none"}, True, no_update

    # Teams share: clientside opens the URL; server callback logs the event
    from dash import ClientsideFunction
    app.clientside_callback(
        ClientsideFunction(namespace="clientside", function_name="openTeamsLink"),
        Output({"type": "teams-share-url", "index": MATCH}, "data"),  # dummy output (no-op)
        Input({"type": "ticket-teams-btn", "index": MATCH}, "n_clicks"),
        State({"type": "teams-share-url", "index": MATCH}, "data"),
        prevent_initial_call=True,
    )

    # Email share: clientside opens mailto: link
    app.clientside_callback(
        ClientsideFunction(namespace="clientside", function_name="openEmailLink"),
        Output({"type": "email-share-url", "index": MATCH}, "data"),  # dummy output (no-op)
        Input({"type": "ticket-email-btn", "index": MATCH}, "n_clicks"),
        State({"type": "email-share-url", "index": MATCH}, "data"),
        prevent_initial_call=True,
    )

    # Flag toggle: toggle review flag and rebuild to update button state
    @app.callback(
        Output({"type": "ticket-detail-content", "index": MATCH}, "children", allow_duplicate=True),
        Input({"type": "ticket-flag-btn", "index": MATCH}, "n_clicks"),
        State({"type": "ticket-detail-id", "index": MATCH}, "data"),
        State({"type": "ticket-detail-back-href", "index": MATCH}, "data"),
        State({"type": "ticket-detail-back-label", "index": MATCH}, "data"),
        State({"type": "active-tab", "index": MATCH}, "data"),
        prevent_initial_call=True,
    )
    def toggle_flag(n_clicks, ticket_id, back_href, back_label, active_tab):
        if not n_clicks or not ticket_id:
            return no_update
        data.toggle_ticket_flag(ticket_id)
        instance_ctx = ctx.triggered_id["index"] if isinstance(ctx.triggered_id, dict) else "page"
        return _build_detail(ticket_id, back_href=back_href, back_label=back_label or "Tickets",
                             ctx=instance_ctx, active_tab=active_tab or "thread")

    @app.callback(
        Output({"type": "ticket-detail-content", "index": MATCH}, "children", allow_duplicate=True),
        Input({"type": "ticket-teams-btn", "index": MATCH}, "n_clicks"),
        State({"type": "ticket-detail-id", "index": MATCH}, "data"),
        State({"type": "ticket-detail-back-href", "index": MATCH}, "data"),
        State({"type": "ticket-detail-back-label", "index": MATCH}, "data"),
        State({"type": "active-tab", "index": MATCH}, "data"),
        prevent_initial_call=True,
    )
    def log_teams_share(n_clicks, ticket_id, back_href, back_label, active_tab):
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
        instance_ctx = ctx.triggered_id["index"] if isinstance(ctx.triggered_id, dict) else "page"
        return _build_detail(ticket_id, back_href=back_href, back_label=back_label or "Tickets",
                             ctx=instance_ctx, active_tab=active_tab or "thread")

    # Persist active tab so rebuilds (auto-refresh, Teams share) don't reset it
    @app.callback(
        Output({"type": "active-tab", "index": MATCH}, "data"),
        Input({"type": "ticket-tabs", "index": MATCH}, "value"),
        prevent_initial_call=True,
    )
    def save_active_tab(value):
        return value or "thread"

    @app.callback(
        Output({"type": "excl-save-status", "index": MATCH}, "children"),
        Input({"type": "excl-save-btn", "index": MATCH}, "n_clicks"),
        State({"type": "ticket-detail-id", "index": MATCH}, "data"),
        State({"type": "excl-priority", "index": MATCH}, "checked"),
        State({"type": "excl-sentiment", "index": MATCH}, "checked"),
        State({"type": "excl-complexity", "index": MATCH}, "checked"),
        State({"type": "excl-reason", "index": MATCH}, "value"),
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

    # ── Ask Matcha chat callbacks ─────────────────────────────────────

    @app.callback(
        Output({"type": "chat-messages", "index": MATCH}, "children"),
        Output({"type": "chat-history", "index": MATCH}, "data"),
        Output({"type": "chat-poll", "index": MATCH}, "disabled"),
        Output({"type": "chat-input", "index": MATCH}, "value"),
        Input({"type": "chat-send-btn", "index": MATCH}, "n_clicks"),
        State({"type": "chat-input", "index": MATCH}, "value"),
        State({"type": "chat-history", "index": MATCH}, "data"),
        State({"type": "ticket-detail-id", "index": MATCH}, "data"),
        prevent_initial_call=True,
    )
    def chat_send(n_clicks, user_text, chat_history, ticket_id):
        if not (user_text or "").strip():
            return no_update, no_update, no_update, no_update

        user_text = user_text.strip()
        instance_ctx = ctx.triggered_id["index"] if isinstance(ctx.triggered_id, dict) else "page"

        ticket = data.get_ticket_detail(ticket_id)
        actions = data.get_ticket_actions(ticket_id) if ticket_id else []
        do_comments = []
        if ticket and ticket.get("do_number"):
            do_comments = data.get_do_comments(ticket["do_number"])
        ticket_ctx_str = _build_ticket_context(ticket, actions, do_comments) if ticket else ""

        prior_history = list(chat_history or [])
        new_history = prior_history + [{"role": "user", "content": user_text}]
        messages = [{"role": "user", "content": user_text}]

        threading.Thread(
            target=_run_chat,
            args=(instance_ctx, ticket_ctx_str, messages, prior_history),
            daemon=True,
        ).start()

        return _render_chat_messages(new_history, pending=True), new_history, False, ""

    @app.callback(
        Output({"type": "chat-messages", "index": MATCH}, "children", allow_duplicate=True),
        Output({"type": "chat-history", "index": MATCH}, "data", allow_duplicate=True),
        Output({"type": "chat-poll", "index": MATCH}, "disabled", allow_duplicate=True),
        Input({"type": "chat-poll", "index": MATCH}, "n_intervals"),
        State({"type": "chat-history", "index": MATCH}, "data"),
        prevent_initial_call=True,
    )
    def chat_poll(n_intervals, chat_history):
        instance_ctx = ctx.triggered_id["index"] if isinstance(ctx.triggered_id, dict) else "page"

        with _chat_lock:
            state = _chat_state.get(instance_ctx, {})

        if state.get("running"):
            return no_update, no_update, False

        result = state.get("result")
        error = state.get("error")

        if result is not None or error is not None:
            reply = result if result is not None else f"[Error communicating with Matcha: {error}]"
            new_history = list(chat_history or []) + [{"role": "assistant", "content": reply}]
            with _chat_lock:
                _chat_state.pop(instance_ctx, None)
            return _render_chat_messages(new_history), new_history, True

        return no_update, no_update, True

    @app.callback(
        Output({"type": "chat-messages", "index": MATCH}, "children", allow_duplicate=True),
        Output({"type": "chat-history", "index": MATCH}, "data", allow_duplicate=True),
        Input({"type": "chat-clear-btn", "index": MATCH}, "n_clicks"),
        prevent_initial_call=True,
    )
    def chat_clear(n_clicks):
        if not n_clicks:
            return no_update, no_update
        return [], []

    @app.callback(
        Output({"type": "chat-messages", "index": MATCH}, "children", allow_duplicate=True),
        Input({"type": "ticket-detail-content", "index": MATCH}, "children"),
        State({"type": "chat-history", "index": MATCH}, "data"),
        State({"type": "chat-poll", "index": MATCH}, "disabled"),
        prevent_initial_call=True,
    )
    def restore_chat_after_refresh(_, chat_history, chat_poll_disabled):
        if not chat_history:
            return no_update
        # If the poll is still active a response is in flight — restore with pending indicator
        pending = not chat_poll_disabled
        return _render_chat_messages(chat_history, pending=pending)
