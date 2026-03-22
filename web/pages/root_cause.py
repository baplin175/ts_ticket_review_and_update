"""Root Cause Analytics — dashboard visualizations + detail drill-down."""

import json
from collections import OrderedDict

import dash_ag_grid as dag
import dash_mantine_components as dmc
from dash import dcc, html, callback, Input, Output, State, no_update
from dash_iconify import DashIconify
import plotly.graph_objects as go

from .. import data
from ..renderer import grid_with_export


# ══════════════════════════════════════════════════════════════════════
# ── Shared helpers ───────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════

_LABEL_MAP = {
    "calculation_logic_error": "Calculation Logic",
    "schema_mismatch": "Schema Mismatch",
    "data_validation_failure": "Data Validation",
    "configuration_mismatch": "Config Mismatch",
    "authentication_failure": "Authentication",
    "integration_mapping_error": "Integration Mapping",
    "integration_communication_failure": "Integration Comms",
    "state_inconsistency": "State Inconsistency",
    "synchronization_failure": "Sync Failure",
    "dependency_missing": "Missing Dependency",
    "field_mapping_error": "Field Mapping",
    "cache_inconsistency": "Cache Inconsistency",
    "permission_error": "Permission Error",
    "other": "Other",
    # intervention types
    "software_fix": "Software Fix",
    "configuration_change": "Config Change",
    "validation_guardrail": "Validation Guardrail",
    "integration_fix": "Integration Fix",
    "data_repair": "Data Repair",
    "documentation": "Documentation",
    "customer_training": "Customer Training",
}


def _pretty(raw):
    """Human-readable label for a mechanism class or intervention type."""
    if not raw:
        return "Unknown"
    return _LABEL_MAP.get(raw, raw.replace("_", " ").title())


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


# ── Color palette ────────────────────────────────────────────────────

_MECH_COLORS = [
    "#4263eb", "#ae3ec9", "#e8590c", "#2b8a3e", "#1098ad",
    "#d6336c", "#f59f00", "#5c940d", "#845ef7", "#c92a2a",
    "#1c7ed6", "#0b7285", "#e67700", "#364fc7",
]

_INTERVENTION_COLORS = {
    "software_fix": "#4263eb",
    "configuration_change": "#ae3ec9",
    "validation_guardrail": "#e8590c",
    "integration_fix": "#1098ad",
    "data_repair": "#2b8a3e",
    "documentation": "#f59f00",
    "customer_training": "#d6336c",
    "other": "#868e96",
}


# ══════════════════════════════════════════════════════════════════════
# ── Fixes drill-down column defs ─────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════

_FIXES_DRILLDOWN_COLS = [
    {"field": "ticket_number", "headerName": "Ticket #", "width": 110, "pinned": "left"},
    {"field": "ticket_name", "headerName": "Name", "minWidth": 200, "flex": 1,
     "tooltipField": "ticket_name"},
    {"field": "status", "headerName": "Status", "width": 120},
    {"field": "severity", "headerName": "Severity", "width": 140},
    {"field": "product_name", "headerName": "Product", "width": 140},
    {"field": "customer", "headerName": "Customer", "width": 140},
    {"field": "mechanism_class", "headerName": "Mechanism", "width": 160},
    {"field": "intervention_type", "headerName": "Intervention", "width": 150},
    {"field": "intervention_action", "headerName": "Action", "minWidth": 200, "flex": 1,
     "tooltipField": "intervention_action"},
    {"field": "days_opened", "headerName": "Age (d)", "width": 90, "type": "numericColumn",
     "valueFormatter": {"function": "params.value != null ? Math.round(params.value) : ''"}},
    {"field": "priority", "headerName": "Priority", "width": 95, "type": "numericColumn"},
    {"field": "overall_complexity", "headerName": "Complexity", "width": 110, "type": "numericColumn"},
    {"field": "date_modified", "headerName": "Last Modified", "width": 130,
     "valueFormatter": {"function": "params.value ? new Date(params.value).toLocaleDateString() : ''"},
     "sort": "desc"},
]


# ══════════════════════════════════════════════════════════════════════
# ── Dashboard tab ────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════

def _build_dashboard_tab():
    stats = data.get_root_cause_stats()
    mech_dist = data.get_mechanism_class_distribution()
    intv_dist = data.get_intervention_type_distribution()
    comp_dist = data.get_component_distribution()
    op_dist = data.get_operation_distribution()
    cluster_catalog = data.get_root_cause_cluster_catalog()
    top_fixes = data.get_top_engineering_fixes()
    by_product = data.get_root_cause_by_product()
    sankey_data = data.get_root_cause_sankey()
    funnel = data.get_pipeline_completion_funnel()

    children = []

    # ── KPI stat row ─────────────────────────────────────────────────
    p1 = stats.get("pass1_success", 0) or 0
    p3 = stats.get("pass3_success", 0) or 0
    p4 = stats.get("pass4_success", 0) or 0
    completion_pct = f"{round(p4 / p1 * 100)}%" if p1 else "—"
    top_mech = mech_dist[0]["mechanism_class"] if mech_dist else "—"

    children.append(
        dmc.SimpleGrid(
            cols={"base": 1, "sm": 2, "lg": 5},
            children=[
                _stat_card("Tickets Analyzed", p1,
                           "tabler:search", "blue"),
                _stat_card("Mechanisms Found", p3,
                           "tabler:bulb", "violet"),
                _stat_card("Interventions Mapped", p4,
                           "tabler:tools", "teal"),
                _stat_card("Pipeline Completion", completion_pct,
                           "tabler:chart-arrows-vertical", "green"),
                _stat_card("Top Mechanism", _pretty(top_mech),
                           "tabler:alert-circle", "orange"),
            ],
        )
    )

    # ── Pipeline funnel ──────────────────────────────────────────────
    children.append(_pipeline_funnel_chart(funnel))

    # ── Two-column: Mechanism Class distrib + Intervention Type donut ─
    children.append(
        dmc.SimpleGrid(
            cols={"base": 1, "md": 2},
            children=[
                dmc.Paper([
                    dmc.Text("Mechanism Class Distribution", fw=600, mb="xs"),
                    _mechanism_class_chart(mech_dist),
                ], withBorder=True, p="md", radius="md", shadow="sm"),
                dmc.Paper([
                    dmc.Text("Intervention Type Breakdown", fw=600, mb="xs"),
                    _intervention_donut(intv_dist),
                ], withBorder=True, p="md", radius="md", shadow="sm"),
            ],
        )
    )

    # ── Two-column: Component treemap + Operation distribution ───────
    children.append(
        dmc.SimpleGrid(
            cols={"base": 1, "md": 2},
            children=[
                dmc.Paper([
                    dmc.Text("Top Components (by ticket count)", fw=600, mb="xs"),
                    _component_treemap(comp_dist),
                ], withBorder=True, p="md", radius="md", shadow="sm"),
                dmc.Paper([
                    dmc.Text("Operation Verb Frequency", fw=600, mb="xs"),
                    _operation_chart(op_dist),
                ], withBorder=True, p="md", radius="md", shadow="sm"),
            ],
        )
    )

    children.append(
        dmc.SimpleGrid(
            cols={"base": 1, "xl": 2},
            children=[
                dmc.Paper([
                    dmc.Text("Cluster Catalog", fw=600, mb="xs"),
                    _cluster_catalog_grid(cluster_catalog),
                ], withBorder=True, p="md", radius="md", shadow="sm"),
                dmc.Paper([
                    dmc.Group(
                        [
                            dmc.Text("Subcluster Breakdown", fw=600),
                            dmc.Text(
                                "Select a cluster to see its dominant component/operation slices.",
                                size="sm",
                                c="dimmed",
                            ),
                        ],
                        justify="space-between",
                        align="flex-end",
                        mb="xs",
                    ),
                    html.Div(
                        id="rc-cluster-subcluster-chart",
                        children=_subcluster_chart(cluster_catalog[0] if cluster_catalog else None),
                    ),
                ], withBorder=True, p="md", radius="md", shadow="sm"),
            ],
        )
    )

    # ── Sankey diagram ───────────────────────────────────────────────
    children.append(
        dmc.Paper([
            dmc.Text("Root Cause Flow: Component → Mechanism → Intervention", fw=600, mb="xs"),
            _sankey_chart(sankey_data),
        ], withBorder=True, p="md", radius="md", shadow="sm")
    )

    # ── Root cause by product ────────────────────────────────────────
    children.append(
        dmc.Paper([
            dmc.Text("Root Cause by Product", fw=600, mb="xs"),
            _product_mechanism_chart(by_product),
        ], withBorder=True, p="md", radius="md", shadow="sm")
    )

    # ── Top engineering fixes table ──────────────────────────────────
    if top_fixes:
        fix_grid = dag.AgGrid(
            id="rc-fixes-grid",
            rowData=top_fixes,
            columnDefs=[
                {"field": "mechanism_class", "headerName": "Mechanism Class", "width": 200,
                 "checkboxSelection": True, "headerCheckboxSelection": True,
                 "valueFormatter": {"function": "params.value ? params.value.replace(/_/g, ' ').replace(/\\b\\w/g, c => c.toUpperCase()) : ''"}},
                {"field": "intervention_type", "headerName": "Intervention Type", "width": 180,
                 "valueFormatter": {"function": "params.value ? params.value.replace(/_/g, ' ').replace(/\\b\\w/g, c => c.toUpperCase()) : ''"}},
                {"field": "ticket_count", "headerName": "Tickets", "width": 100,
                 "type": "numericColumn", "sort": "desc"},
                {"field": "representative_action", "headerName": "Representative Fix",
                 "minWidth": 300, "flex": 1, "tooltipField": "representative_action"},
            ],
            defaultColDef={"sortable": True, "filter": True, "resizable": True},
            dashGridOptions={"pagination": True, "paginationPageSize": 10,
                             "animateRows": True,
                             "rowSelection": "multiple",
                             "suppressRowClickSelection": True},
            style={"height": "400px"},
            className="ag-theme-alpine",
        )
        children.append(
            dmc.Paper([
                dmc.Group(
                    [
                        dmc.Text("Top Engineering Fixes (ROI-ranked)", fw=600),
                        dmc.Button(
                            "View Tickets",
                            id="rc-fixes-drilldown-btn",
                            leftSection=DashIconify(icon="tabler:list-search", width=16),
                            variant="light",
                            size="compact-sm",
                            disabled=True,
                        ),
                    ],
                    justify="space-between",
                    mb="xs",
                ),
                grid_with_export(fix_grid, "rc-fixes-grid"),
            ], withBorder=True, p="md", radius="md", shadow="sm")
        )
    else:
        # Placeholder components so callbacks don't error
        children.append(html.Div([
            html.Div(id="rc-fixes-drilldown-btn"),
        ], style={"display": "none"}))

    # Drill-down modal for engineering fixes
    children.append(
        dmc.Modal(
            id="rc-fixes-drilldown-modal",
            title="Engineering Fix — Tickets",
            size="90%",
            centered=True,
            children=[
                dmc.Text(id="rc-fixes-drilldown-subtitle", size="sm", c="dimmed", mb="sm"),
                grid_with_export(
                    dag.AgGrid(
                        id="rc-fixes-drilldown-grid",
                        rowData=[],
                        columnDefs=_FIXES_DRILLDOWN_COLS,
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
                    "rc-fixes-drilldown-grid",
                ),
            ],
        )
    )

    return dmc.Stack(children, gap="md")


# ── Chart builders ───────────────────────────────────────────────────

def _pipeline_funnel_chart(funnel):
    """Horizontal bar funnel: Pass 1 → Pass 2 → Pass 3 completion."""
    stages = ["Pass 1 — Phenomenon", "Pass 2 — Mechanism", "Pass 3 — Intervention"]
    values = [funnel.get("pass1", 0), funnel.get("pass3", 0), funnel.get("pass4", 0)]
    colors = ["#4263eb", "#ae3ec9", "#099268"]

    if not any(values):
        return dmc.Text("No pipeline data yet.", c="dimmed", ta="center", py="xl")

    fig = go.Figure(go.Funnel(
        y=stages, x=values,
        textinfo="value+percent initial",
        marker=dict(color=colors),
        connector=dict(line=dict(color="#dee2e6", width=2)),
    ))
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=10, r=10, t=10, b=10),
        height=200,
        funnelmode="stack",
    )
    return dmc.Paper([
        dmc.Text("Pipeline Completion Funnel", fw=600, mb="xs"),
        dcc.Graph(figure=fig, config={"displayModeBar": False}),
    ], withBorder=True, p="md", radius="md", shadow="sm")


def _mechanism_class_chart(rows):
    """Horizontal bar chart of mechanism class distribution."""
    if not rows:
        return dmc.Text("No mechanism data yet.", c="dimmed", ta="center", py="xl")

    labels = [_pretty(r["mechanism_class"]) for r in reversed(rows)]
    values = [r["ticket_count"] for r in reversed(rows)]
    colors = (_MECH_COLORS * 3)[:len(rows)]
    colors = list(reversed(colors[:len(labels)]))

    fig = go.Figure(go.Bar(
        x=values, y=labels, orientation="h",
        marker_color=colors,
        text=values, textposition="auto",
        hovertemplate="%{y}: %{x} tickets<extra></extra>",
    ))
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=10, r=20, t=10, b=10),
        height=max(260, len(rows) * 32),
        yaxis=dict(automargin=True),
        xaxis_title="Tickets",
    )
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _intervention_donut(rows):
    """Donut chart of intervention type distribution."""
    if not rows:
        return dmc.Text("No intervention data yet.", c="dimmed", ta="center", py="xl")

    labels = [_pretty(r["intervention_type"]) for r in rows]
    values = [r["ticket_count"] for r in rows]
    colors = [_INTERVENTION_COLORS.get(r["intervention_type"], "#868e96") for r in rows]

    fig = go.Figure(go.Pie(
        labels=labels, values=values,
        hole=0.50,
        marker=dict(colors=colors),
        textinfo="label+percent",
        textposition="outside",
        hovertemplate="%{label}: %{value} tickets (%{percent})<extra></extra>",
    ))
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=10, r=10, t=10, b=10),
        height=340,
        showlegend=False,
        annotations=[dict(text="Interventions", x=0.5, y=0.5,
                          font_size=14, showarrow=False, font_color="#495057")],
    )
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _component_treemap(rows):
    """Treemap of top components by ticket count."""
    if not rows:
        return dmc.Text("No component data yet.", c="dimmed", ta="center", py="xl")

    labels = [r["component"] for r in rows]
    values = [r["ticket_count"] for r in rows]
    parents = [""] * len(labels)

    fig = go.Figure(go.Treemap(
        labels=labels, values=values, parents=parents,
        textinfo="label+value",
        hovertemplate="<b>%{label}</b><br>Tickets: %{value}<extra></extra>",
        marker=dict(
            colors=values,
            colorscale="Blues",
            showscale=False,
        ),
        pathbar=dict(visible=False),
    ))
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=5, r=5, t=5, b=5),
        height=360,
    )
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _operation_chart(rows):
    """Horizontal bar chart of operation verb frequency."""
    if not rows:
        return dmc.Text("No operation data yet.", c="dimmed", ta="center", py="xl")

    labels = [r["operation"].title() for r in reversed(rows)]
    values = [r["ticket_count"] for r in reversed(rows)]

    fig = go.Figure(go.Bar(
        x=values, y=labels, orientation="h",
        marker_color="#4263eb",
        text=values, textposition="auto",
        hovertemplate="%{y}: %{x} tickets<extra></extra>",
    ))
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=10, r=20, t=10, b=10),
        height=max(260, len(rows) * 32),
        yaxis=dict(automargin=True),
        xaxis_title="Tickets",
    )
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _cluster_catalog_grid(rows):
    """Single-select grid of persisted cluster catalog rows."""
    grid = dag.AgGrid(
        id="rc-cluster-catalog-grid",
        rowData=rows,
        columnDefs=[
            {
                "field": "cluster_id",
                "headerName": "Cluster",
                "minWidth": 180,
                "checkboxSelection": True,
                "headerCheckboxSelection": False,
                "valueFormatter": {
                    "function": "params.value ? params.value.replace(/_/g, ' ').replace(/\\b\\w/g, c => c.toUpperCase()) : ''"
                },
            },
            {"field": "ticket_count", "headerName": "Tickets", "width": 95, "type": "numericColumn", "sort": "desc"},
            {
                "field": "percent_of_total",
                "headerName": "% Total",
                "width": 95,
                "type": "numericColumn",
                "valueFormatter": {"function": "params.value != null ? (params.value * 100).toFixed(1) + '%' : ''"},
            },
            {"field": "customer_count", "headerName": "Customers", "width": 105, "type": "numericColumn"},
            {"field": "product_count", "headerName": "Products", "width": 95, "type": "numericColumn"},
            {"field": "dominant_component", "headerName": "Dominant Component", "minWidth": 170, "flex": 1},
            {"field": "dominant_operation", "headerName": "Dominant Operation", "width": 150},
            {
                "field": "dominant_intervention_type",
                "headerName": "Dominant Intervention",
                "width": 170,
                "valueFormatter": {
                    "function": "params.value ? params.value.replace(/_/g, ' ').replace(/\\b\\w/g, c => c.toUpperCase()) : ''"
                },
            },
        ],
        defaultColDef={"sortable": True, "filter": True, "resizable": True},
        dashGridOptions={
            "rowSelection": "single",
            "pagination": True,
            "paginationPageSize": 10,
            "animateRows": True,
        },
        style={"height": "420px"},
        className="ag-theme-alpine",
    )
    return grid_with_export(grid, "rc-cluster-catalog-grid")


def _subcluster_chart(cluster_row):
    """Horizontal bar chart of component/operation subclusters for one cluster."""
    if not cluster_row:
        return dmc.Text("No cluster catalog data yet.", c="dimmed", ta="center", py="xl")

    subclusters = cluster_row.get("subclusters") or []
    if not subclusters:
        return dmc.Text("No subcluster data for this cluster.", c="dimmed", ta="center", py="xl")

    labels = []
    values = []
    percents = []
    for item in reversed(subclusters[:12]):
        component = item.get("component") or "Unknown"
        operation = item.get("operation") or "Unknown"
        labels.append(f"{component} -> {operation}")
        values.append(item.get("ticket_count", 0))
        percents.append(item.get("percent_within_cluster", 0))

    fig = go.Figure(go.Bar(
        x=values,
        y=labels,
        orientation="h",
        marker_color="#0b7285",
        text=[f"{v} ({p * 100:.0f}%)" for v, p in zip(values, percents)],
        textposition="auto",
        hovertemplate="%{y}<br>Tickets: %{x}<extra></extra>",
    ))
    fig.update_layout(
        template="plotly_white",
        title={
            "text": f"{_pretty(cluster_row.get('cluster_id'))}: top component/operation slices",
            "x": 0.01,
            "xanchor": "left",
            "font": {"size": 14},
        },
        margin=dict(l=10, r=20, t=50, b=10),
        height=max(320, len(labels) * 34),
        yaxis=dict(automargin=True),
        xaxis_title="Tickets",
    )
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _sankey_chart(rows):
    """Sankey diagram: Component → Mechanism Class → Intervention Type."""
    if not rows:
        return dmc.Text("No flow data yet — run Pass 1 → 3 → 4 on tickets first.",
                        c="dimmed", ta="center", py="xl")

    # Build unique node list: components | mechanism_classes | intervention_types
    components = list(OrderedDict.fromkeys(r["component"] for r in rows))
    mechs = list(OrderedDict.fromkeys(r["mechanism_class"] for r in rows))
    intvs = list(OrderedDict.fromkeys(r["intervention_type"] for r in rows))

    node_labels = components + mechs + intvs
    node_idx = {label: i for i, label in enumerate(node_labels)}

    # Build links
    # Phase 1: component → mechanism_class
    link_src, link_tgt, link_val, link_color = [], [], [], []
    comp_to_mech = {}
    for r in rows:
        key = (r["component"], r["mechanism_class"])
        comp_to_mech[key] = comp_to_mech.get(key, 0) + r["ticket_count"]
    for (comp, mech), count in comp_to_mech.items():
        link_src.append(node_idx[comp])
        link_tgt.append(node_idx[mech])
        link_val.append(count)
        link_color.append("rgba(66, 99, 235, 0.25)")

    # Phase 2: mechanism_class → intervention_type
    mech_to_intv = {}
    for r in rows:
        key = (r["mechanism_class"], r["intervention_type"])
        mech_to_intv[key] = mech_to_intv.get(key, 0) + r["ticket_count"]
    for (mech, intv), count in mech_to_intv.items():
        link_src.append(node_idx[mech])
        link_tgt.append(node_idx[intv])
        link_val.append(count)
        link_color.append("rgba(174, 62, 201, 0.25)")

    # Node colors
    node_colors = (
        ["#4263eb"] * len(components)
        + ["#ae3ec9"] * len(mechs)
        + ["#099268"] * len(intvs)
    )

    fig = go.Figure(go.Sankey(
        arrangement="snap",
        node=dict(
            pad=20, thickness=20,
            label=[_pretty(n) for n in node_labels],
            color=node_colors,
            hovertemplate="%{label}: %{value} tickets<extra></extra>",
        ),
        link=dict(
            source=link_src, target=link_tgt, value=link_val,
            color=link_color,
            hovertemplate="%{source.label} → %{target.label}: %{value} tickets<extra></extra>",
        ),
    ))
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=10, r=10, t=30, b=10),
        height=max(420, (len(mechs) + len(intvs)) * 28),
        font=dict(size=11),
    )
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


def _product_mechanism_chart(rows):
    """Stacked horizontal bar chart: mechanism class breakdown per product."""
    if not rows:
        return dmc.Text("No per-product root cause data yet.",
                        c="dimmed", ta="center", py="xl")

    # Pivot: {product: {mechanism_class: count}}
    product_data = {}
    product_totals = {}
    all_mechs = OrderedDict()
    for r in rows:
        p = r["product_name"]
        mc = r["mechanism_class"]
        c = r["ticket_count"]
        product_data.setdefault(p, {})[mc] = c
        product_totals[p] = product_totals.get(p, 0) + c
        all_mechs[mc] = True

    sorted_products = sorted(product_totals, key=lambda p: product_totals[p], reverse=True)
    sorted_products = list(reversed(sorted_products))  # largest at top for horizontal bar
    mech_list = list(all_mechs.keys())
    color_map = dict(zip(mech_list, (_MECH_COLORS * 3)[:len(mech_list)]))

    fig = go.Figure()
    for mc in mech_list:
        vals = [product_data.get(p, {}).get(mc, 0) for p in sorted_products]
        fig.add_trace(go.Bar(
            y=sorted_products, x=vals, orientation="h",
            name=_pretty(mc),
            marker_color=color_map[mc],
            hovertemplate="%{y}: %{x} tickets (" + _pretty(mc) + ")<extra></extra>",
        ))

    fig.update_layout(
        barmode="stack",
        template="plotly_white",
        margin=dict(l=10, r=20, t=10, b=40),
        height=max(280, len(sorted_products) * 36),
        xaxis_title="Tickets",
        yaxis=dict(automargin=True),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )
    return dcc.Graph(figure=fig, config={"displayModeBar": False})


# ══════════════════════════════════════════════════════════════════════
# ── Detail tab helpers ────────────────────────────────────────────────

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


# ── Detail grid columns ─────────────────────────────────────────────

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
    {"field": "pass3_status", "headerName": "Pass 2", "width": 90},
    {"field": "mechanism", "headerName": "Mechanism", "minWidth": 200, "flex": 1},
    {"field": "evidence", "headerName": "Evidence", "width": 120},
    {"field": "pass4_status", "headerName": "Pass 3", "width": 90},
    {"field": "mechanism_class", "headerName": "Mechanism Class", "width": 180},
    {"field": "intervention_type", "headerName": "Intervention Type", "width": 160},
    {"field": "intervention_action", "headerName": "Intervention Action", "minWidth": 200, "flex": 1},
]


def _build_detail_tab():
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
        dmc.Text(
            f"{len(rows)} ticket(s) processed through LLM passes. "
            "Click a row to view full details.",
            size="sm", c="dimmed",
        ),
        grid_with_export(grid, "root-cause-grid"),
        html.Div(id="root-cause-detail"),
    ], gap="md")


# ══════════════════════════════════════════════════════════════════════
# ── Glossary tab ─────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════

def _glossary_section(title, icon, color, items):
    """Render a glossary section with a title and list of term→definition pairs."""
    rows = []
    for term, defn in items:
        rows.append(
            dmc.Group([
                dmc.Badge(term, color=color, variant="light", size="lg",
                          style={"minWidth": "200px", "textAlign": "left"}),
                dmc.Text(defn, size="sm", style={"flex": 1}),
            ], gap="sm", wrap="nowrap", align="flex-start")
        )
    return dmc.Paper([
        dmc.Group([
            dmc.ThemeIcon(DashIconify(icon=icon, width=20),
                          variant="light", color=color, size=36, radius="md"),
            dmc.Text(title, fw=700, size="md"),
        ], gap="sm", mb="md"),
        dmc.Stack(rows, gap="xs"),
    ], withBorder=True, p="md", radius="md", shadow="sm")


def _build_glossary_tab():
    from glossary import GLOSSARY

    return dmc.Stack([
        dmc.Text(
            "Reference guide for the terms, categories, and pipeline stages "
            "used throughout the Root Cause Analysis dashboard.",
            size="sm", c="dimmed",
        ),
        *[_glossary_section(s["title"], s["icon"], s["color"], s["items"])
          for s in GLOSSARY],
    ], gap="md")


# ══════════════════════════════════════════════════════════════════════
# ── Main layout (tabbed: Dashboard + Detail + Glossary) ──────────────
# ══════════════════════════════════════════════════════════════════════

def root_cause_layout():
    return dmc.Stack([
        dmc.Title("Root Cause Analysis", order=2),
        dmc.Tabs(
            [
                dmc.TabsList([
                    dmc.TabsTab("Dashboard", value="dashboard",
                                leftSection=DashIconify(icon="tabler:chart-treemap", width=16)),
                    dmc.TabsTab("Detail", value="detail",
                                leftSection=DashIconify(icon="tabler:list-details", width=16)),
                    dmc.TabsTab("Glossary", value="glossary",
                                leftSection=DashIconify(icon="tabler:book", width=16)),
                ]),
                dmc.TabsPanel(_build_dashboard_tab(), value="dashboard", pt="md"),
                dmc.TabsPanel(_build_detail_tab(), value="detail", pt="md"),
                dmc.TabsPanel(_build_glossary_tab(), value="glossary", pt="md"),
            ],
            value="dashboard",
        ),
    ], gap="md")


# ══════════════════════════════════════════════════════════════════════
# ── Callbacks ────────────────────────────────────────────────────────
# ══════════════════════════════════════════════════════════════════════

def register_callbacks(app):
    @app.callback(
        Output("rc-cluster-subcluster-chart", "children"),
        Input("rc-cluster-catalog-grid", "selectedRows"),
        State("rc-cluster-catalog-grid", "rowData"),
    )
    def show_cluster_subclusters(selected, rows):
        if selected:
            return _subcluster_chart(selected[0])
        return _subcluster_chart(rows[0] if rows else None)

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

        # Pass 2 — Mechanism Inference
        p3 = by_name.get("pass3_mechanism")
        if p3:
            pj3 = p3.get("parsed_json") or {}
            pass3_card = _pass_card(
                "Pass 2 — Mechanism Inference", "tabler:bulb",
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
            pass3_card = _placeholder_card("Pass 2 — Mechanism Inference", "tabler:bulb")

        # Pass 3 — Intervention Mapping
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
                "Pass 3 — Intervention Mapping", "tabler:tools",
                p4.get("status"),
                p4_fields,
                raw_json=p4.get("parsed_json"),
                error=p4.get("error_message"),
            )
        else:
            pass4_card = _placeholder_card("Pass 3 — Intervention Mapping", "tabler:tools")

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

    # ── Fixes drill-down callbacks ───────────────────────────────────

    @app.callback(
        Output("rc-fixes-drilldown-btn", "disabled"),
        Output("rc-fixes-drilldown-btn", "children"),
        Input("rc-fixes-grid", "selectedRows"),
    )
    def toggle_fixes_btn(selected):
        if not selected:
            return True, "View Tickets"
        n = len(selected)
        total = sum(r.get("ticket_count", 0) for r in selected)
        return False, f"View Tickets ({n} fix{'es' if n != 1 else ''}, ~{total} tickets)"

    @app.callback(
        Output("rc-fixes-drilldown-modal", "opened"),
        Output("rc-fixes-drilldown-grid", "rowData"),
        Output("rc-fixes-drilldown-subtitle", "children"),
        Input("rc-fixes-drilldown-btn", "n_clicks"),
        State("rc-fixes-grid", "selectedRows"),
        prevent_initial_call=True,
    )
    def open_fixes_drilldown(n_clicks, selected):
        if not n_clicks or not selected:
            return no_update, no_update, no_update
        fix_keys = [
            (r["mechanism_class"], r["intervention_type"])
            for r in selected
        ]
        tickets = data.get_tickets_by_fixes(fix_keys)
        labels = [f"{r['mechanism_class']} / {r['intervention_type']}" for r in selected]
        subtitle = f"{len(tickets)} ticket{'s' if len(tickets) != 1 else ''} matching: {', '.join(labels)}"
        return True, tickets, subtitle
