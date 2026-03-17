"""Root Cause page — LLM pass results (pass1 phenomenon, pass2 grammar, future pass3)."""

import json

import dash_ag_grid as dag
import dash_mantine_components as dmc
from dash import dcc, html, callback, Input, Output, no_update
from dash_iconify import DashIconify

import data


# ── Helpers ──────────────────────────────────────────────────────────

def _status_badge(status):
    if not status:
        return dmc.Badge("—", color="gray", variant="light", size="sm")
    color = {"success": "green", "failed": "red", "pending": "yellow"}.get(status, "gray")
    return dmc.Badge(status, color=color, variant="light", size="sm")


def _pass_card(title, icon, status, fields, raw_json=None, error=None):
    """Render a card for one LLM pass."""
    items = []
    for label, value in fields:
        items.append(
            dmc.Group([
                dmc.Text(label, size="sm", fw=600, w=160),
                dmc.Text(str(value) if value else "—", size="sm",
                         style={"flex": 1, "wordBreak": "break-word"}),
            ], gap="sm", wrap="nowrap")
        )

    if error:
        items.append(
            dmc.Alert(str(error), title="Error", color="red", variant="light", mt="xs")
        )

    if raw_json and raw_json != "null":
        try:
            formatted = json.dumps(
                json.loads(raw_json) if isinstance(raw_json, str) else raw_json,
                indent=2, ensure_ascii=False,
            )
        except (json.JSONDecodeError, TypeError):
            formatted = str(raw_json)
        items.append(
            dmc.Spoiler(
                children=[dmc.Code(formatted, block=True,
                                   style={"maxHeight": "300px", "overflow": "auto",
                                          "whiteSpace": "pre-wrap", "fontSize": "12px"})],
                showLabel="Show parsed JSON",
                hideLabel="Hide",
                maxHeight=0,
                mt="xs",
            )
        )

    return dmc.Paper([
        dmc.Group([
            dmc.ThemeIcon(
                DashIconify(icon=icon, width=20),
                variant="light", color="indigo", size=36, radius="md",
            ),
            dmc.Text(title, fw=700, size="md"),
            _status_badge(status),
        ], gap="sm", mb="sm"),
        dmc.Stack(items, gap="xs"),
    ], withBorder=True, p="md", radius="md", shadow="sm")


def _placeholder_card(title, icon):
    """Placeholder for a future pass."""
    return dmc.Paper([
        dmc.Group([
            dmc.ThemeIcon(
                DashIconify(icon=icon, width=20),
                variant="light", color="gray", size=36, radius="md",
            ),
            dmc.Text(title, fw=700, size="md", c="dimmed"),
            dmc.Badge("coming soon", color="gray", variant="outline", size="sm"),
        ], gap="sm", mb="sm"),
        dmc.Text("This pass has not been implemented yet.", size="sm", c="dimmed"),
    ], withBorder=True, p="md", radius="md",
       style={"opacity": 0.6, "borderStyle": "dashed"})


# ── List grid columns ───────────────────────────────────────────────

_GRID_COLS = [
    {"field": "ticket_number", "headerName": "Ticket #", "width": 100},
    {"field": "ticket_name", "headerName": "Name", "minWidth": 200, "flex": 1},
    {"field": "product_name", "headerName": "Product", "width": 130},
    {"field": "customer", "headerName": "Customer", "width": 150},
    {"field": "pass1_status", "headerName": "Pass 1", "width": 90},
    {"field": "phenomenon", "headerName": "Phenomenon", "minWidth": 200, "flex": 1},
    {"field": "pass2_status", "headerName": "Pass 2", "width": 90},
    {"field": "component", "headerName": "Component", "width": 160},
    {"field": "operation", "headerName": "Operation", "width": 120},
    {"field": "unexpected_state", "headerName": "Unexpected State", "minWidth": 180, "flex": 1},
]


# ── Layout ───────────────────────────────────────────────────────────

def root_cause_layout():
    rows = data.get_root_cause_tickets()

    grid = dag.AgGrid(
        id="root-cause-grid",
        rowData=rows,
        columnDefs=_GRID_COLS,
        defaultColDef={"sortable": True, "filter": True, "resizable": True},
        dashGridOptions={
            "rowSelection": "single",
            "animateRows": True,
            "pagination": True,
            "paginationPageSize": 50,
        },
        style={"height": "calc(100vh - 400px)"},
        className="ag-theme-alpine",
    )

    return dmc.Stack([
        dmc.Title("Root Cause Analysis", order=2),
        dmc.Text(
            f"{len(rows)} ticket(s) processed through LLM passes. "
            "Click a row to view full details.",
            size="sm", c="dimmed",
        ),
        grid,
        html.Div(id="root-cause-detail"),
    ], gap="md")


# ── Detail callback ─────────────────────────────────────────────────

def register_callbacks(app):
    @app.callback(
        Output("root-cause-detail", "children"),
        Input("root-cause-grid", "selectedRows"),
        prevent_initial_call=True,
    )
    def show_detail(selected):
        if not selected:
            return no_update
        row = selected[0]
        ticket_id = row.get("ticket_id")
        if not ticket_id:
            return no_update

        detail = data.get_root_cause_detail(ticket_id)
        passes = detail.get("passes", [])
        thread = detail.get("thread") or {}

        # Group passes by name (latest first per query order)
        by_name = {}
        for p in passes:
            name = p["pass_name"]
            if name not in by_name:
                by_name[name] = p

        # Pass 1 — Phenomenon
        p1 = by_name.get("pass1_phenomenon")
        if p1:
            pass1_card = _pass_card(
                "Pass 1 — Phenomenon", "tabler:search",
                p1.get("status"),
                [
                    ("Phenomenon", p1.get("phenomenon")),
                    ("Model", p1.get("model_name")),
                    ("Prompt Version", p1.get("prompt_version")),
                    ("Completed", str(p1.get("completed_at", ""))[:19]),
                ],
                raw_json=p1.get("parsed_json"),
                error=p1.get("error_message"),
            )
        else:
            pass1_card = _placeholder_card("Pass 1 — Phenomenon", "tabler:search")

        # Pass 2 — Grammar Decomposition
        p2 = by_name.get("pass2_grammar")
        if p2:
            pj = p2.get("parsed_json") or {}
            pass2_card = _pass_card(
                "Pass 2 — Grammar Decomposition", "tabler:puzzle",
                p2.get("status"),
                [
                    ("Component", p2.get("component") or pj.get("component")),
                    ("Operation", p2.get("operation") or pj.get("operation")),
                    ("Unexpected State", p2.get("unexpected_state") or pj.get("unexpected_state")),
                    ("Canonical Failure", p2.get("canonical_failure") or pj.get("canonical_failure")),
                    ("Model", p2.get("model_name")),
                    ("Prompt Version", p2.get("prompt_version")),
                    ("Completed", str(p2.get("completed_at", ""))[:19]),
                ],
                raw_json=p2.get("parsed_json"),
                error=p2.get("error_message"),
            )
        else:
            pass2_card = _placeholder_card("Pass 2 — Grammar Decomposition", "tabler:puzzle")

        # Pass 3 — placeholder
        pass3_card = _placeholder_card("Pass 3 — (Future)", "tabler:bulb")

        # Cleaned thread text
        thread_text = thread.get("technical_core_text") or thread.get("full_thread_text") or ""
        thread_section = dmc.Paper([
            dmc.Group([
                dmc.ThemeIcon(
                    DashIconify(icon="tabler:message-2", width=20),
                    variant="light", color="teal", size=36, radius="md",
                ),
                dmc.Text("Cleaned Thread Input", fw=700, size="md"),
            ], gap="sm", mb="sm"),
            dmc.Spoiler(
                children=[dmc.Code(thread_text, block=True,
                                   style={"maxHeight": "400px", "overflow": "auto",
                                          "whiteSpace": "pre-wrap", "fontSize": "12px"})],
                showLabel="Show thread text",
                hideLabel="Hide",
                maxHeight=0,
            ) if thread_text else dmc.Text("No thread text available.", size="sm", c="dimmed"),
        ], withBorder=True, p="md", radius="md", shadow="sm")

        return dmc.Stack([
            dmc.Divider(
                label=f"Ticket {row.get('ticket_number', '')} — {row.get('ticket_name', '')}",
                labelPosition="center", mt="lg",
            ),
            dmc.SimpleGrid(
                cols={"base": 1, "lg": 3},
                children=[pass1_card, pass2_card, pass3_card],
            ),
            thread_section,
        ], gap="md")
