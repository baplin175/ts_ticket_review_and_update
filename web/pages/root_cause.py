"""Root Cause page — LLM pass results (pass1 phenomenon+grammar, pass3 mechanism, pass4 intervention)."""

import json

import dash_ag_grid as dag
import dash_mantine_components as dmc
from dash import dcc, html, callback, Input, Output, no_update
from dash_iconify import DashIconify

import data
from renderer import grid_with_export


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
    {"field": "confidence", "headerName": "Confidence", "width": 110},
    {"field": "phenomenon", "headerName": "Phenomenon", "minWidth": 200, "flex": 1},
    {"field": "component", "headerName": "Component", "width": 160},
    {"field": "operation", "headerName": "Operation", "width": 120},
    {"field": "pass3_status", "headerName": "Pass 3", "width": 90},
    {"field": "mechanism", "headerName": "Mechanism", "minWidth": 200, "flex": 1},
    {"field": "evidence", "headerName": "Evidence", "width": 120},
    {"field": "pass4_status", "headerName": "Pass 4", "width": 90},
    {"field": "mechanism_class", "headerName": "Mechanism Class", "width": 180},
    {"field": "intervention_type", "headerName": "Intervention Type", "width": 160},
    {"field": "intervention_action", "headerName": "Intervention Action", "minWidth": 200, "flex": 1},
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
        grid_with_export(grid, "root-cause-grid"),
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

        # Pass 1 — Phenomenon + Grammar
        p1 = by_name.get("pass1_phenomenon")
        if p1:
            pj1 = p1.get("parsed_json") or {}
            confidence = pj1.get("confidence", "")
            pass1_card = _pass_card(
                "Pass 1 — Phenomenon + Grammar", "tabler:search",
                p1.get("status"),
                [
                    ("Phenomenon", p1.get("phenomenon")),
                    ("Confidence", confidence),
                    ("Component", p1.get("component") or pj1.get("component")),
                    ("Operation", p1.get("operation") or pj1.get("operation")),
                    ("Unexpected State", p1.get("unexpected_state") or pj1.get("unexpected_state")),
                    ("Canonical Failure", p1.get("canonical_failure") or pj1.get("canonical_failure")),
                    ("Model", p1.get("model_name")),
                    ("Prompt Version", p1.get("prompt_version")),
                    ("Completed", str(p1.get("completed_at", ""))[:19]),
                ],
                raw_json=p1.get("parsed_json"),
                error=p1.get("error_message"),
            )
        else:
            pass1_card = _placeholder_card("Pass 1 — Phenomenon + Grammar", "tabler:search")

        # Pass 3 — Mechanism Inference
        p3 = by_name.get("pass3_mechanism")
        if p3:
            pj3 = p3.get("parsed_json") or {}
            pass3_card = _pass_card(
                "Pass 3 — Mechanism Inference", "tabler:bulb",
                p3.get("status"),
                [
                    ("Mechanism", p3.get("mechanism")),
                    ("Evidence", pj3.get("evidence", "")),
                    ("Category", pj3.get("category", "")),
                    ("Model", p3.get("model_name")),
                    ("Prompt Version", p3.get("prompt_version")),
                    ("Completed", str(p3.get("completed_at", ""))[:19]),
                ],
                raw_json=p3.get("parsed_json"),
                error=p3.get("error_message"),
            )
        else:
            pass3_card = _placeholder_card("Pass 3 — Mechanism Inference", "tabler:bulb")

        # Pass 4 — Intervention Mapping
        p4 = by_name.get("pass4_intervention")
        if p4:
            pj4 = p4.get("parsed_json") or {}
            p4_fields = [
                ("Mechanism Class", p4.get("mechanism_class")),
                ("Intervention Type", p4.get("intervention_type")),
                ("Intervention Action", p4.get("intervention_action")),
            ]
            if pj4.get("proposed_class"):
                p4_fields.append(("Proposed Class", pj4["proposed_class"]))
            if pj4.get("proposed_type"):
                p4_fields.append(("Proposed Type", pj4["proposed_type"]))
            p4_fields.extend([
                ("Model", p4.get("model_name")),
                ("Prompt Version", p4.get("prompt_version")),
                ("Completed", str(p4.get("completed_at", ""))[:19]),
            ])
            pass4_card = _pass_card(
                "Pass 4 — Intervention Mapping", "tabler:tools",
                p4.get("status"),
                p4_fields,
                raw_json=p4.get("parsed_json"),
                error=p4.get("error_message"),
            )
        else:
            pass4_card = _placeholder_card("Pass 4 — Intervention Mapping", "tabler:tools")

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
                cols={"base": 1, "lg": 2},
                children=[pass1_card, pass3_card, pass4_card],
            ),
            thread_section,
        ], gap="md")
