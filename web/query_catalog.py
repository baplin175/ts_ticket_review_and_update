"""Curated query catalogue for runtime-configured dashboards.

V1 intentionally exposes only server-owned query handlers. Dashboard widgets may
select a query key plus validated parameters, but may not run arbitrary SQL.
"""

from dataclasses import dataclass, field
from typing import Any, Callable

from . import data


ALLOWED_WIDGET_TYPES = {
    "alert",
    "stat_row",
    "grid",
    "chart",
}


@dataclass(frozen=True)
class QueryParam:
    """Allowed parameter for a curated dashboard query."""

    name: str
    param_type: str
    required: bool = False
    default: Any = None
    minimum: int | float | None = None
    maximum: int | float | None = None
    choices: tuple[Any, ...] | None = None


@dataclass(frozen=True)
class QueryDefinition:
    """Metadata and execution contract for a dashboard-safe query."""

    key: str
    label: str
    description: str
    handler: Callable[..., Any]
    result_kind: str
    allowed_widget_types: tuple[str, ...]
    params: tuple[QueryParam, ...] = field(default_factory=tuple)


def _coerce_param(param: QueryParam, value: Any) -> Any:
    if param.param_type == "int":
        if isinstance(value, bool):
            raise ValueError(f"Parameter '{param.name}' must be an integer.")
        value = int(value)
    elif param.param_type == "str":
        value = str(value)
    elif param.param_type == "bool":
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            lowered = value.strip().lower()
            if lowered in {"true", "1", "yes"}:
                return True
            if lowered in {"false", "0", "no"}:
                return False
        raise ValueError(f"Parameter '{param.name}' must be a boolean.")
    else:
        raise ValueError(f"Unsupported parameter type '{param.param_type}'.")

    if param.minimum is not None and value < param.minimum:
        raise ValueError(f"Parameter '{param.name}' must be >= {param.minimum}.")
    if param.maximum is not None and value > param.maximum:
        raise ValueError(f"Parameter '{param.name}' must be <= {param.maximum}.")
    if param.choices is not None and value not in param.choices:
        raise ValueError(
            f"Parameter '{param.name}' must be one of: {', '.join(map(str, param.choices))}."
        )
    return value


QUERY_CATALOG: dict[str, QueryDefinition] = {
    "open_ticket_stats": QueryDefinition(
        key="open_ticket_stats",
        label="Open Ticket KPI Summary",
        description="Single-row KPI summary of current open ticket volume and top risk indicators.",
        handler=data.get_open_ticket_stats,
        result_kind="row",
        allowed_widget_types=("stat_row",),
    ),
    "backlog_daily": QueryDefinition(
        key="backlog_daily",
        label="Backlog Daily Trend",
        description="Daily total open backlog over time.",
        handler=data.get_backlog_daily,
        result_kind="rows",
        allowed_widget_types=("chart", "grid"),
    ),
    "backlog_daily_by_severity": QueryDefinition(
        key="backlog_daily_by_severity",
        label="Backlog Daily By Severity",
        description="Daily open backlog broken out by severity tier.",
        handler=data.get_backlog_daily_by_severity,
        result_kind="rows",
        allowed_widget_types=("chart", "grid"),
    ),
    "backlog_aging": QueryDefinition(
        key="backlog_aging",
        label="Backlog Aging",
        description="Current open-ticket aging buckets.",
        handler=data.get_backlog_aging,
        result_kind="rows",
        allowed_widget_types=("chart", "grid"),
    ),
    "open_by_product": QueryDefinition(
        key="open_by_product",
        label="Open Tickets By Product",
        description="Open backlog by product and severity tier.",
        handler=data.get_open_by_product,
        result_kind="rows",
        allowed_widget_types=("chart", "grid"),
    ),
    "open_by_status": QueryDefinition(
        key="open_by_status",
        label="Open Tickets By Status",
        description="Current open ticket counts grouped by status.",
        handler=data.get_open_by_status,
        result_kind="rows",
        allowed_widget_types=("chart", "grid"),
    ),
    "ticket_list": QueryDefinition(
        key="ticket_list",
        label="Ticket List",
        description="Current operational ticket list.",
        handler=data.get_ticket_list,
        result_kind="rows",
        allowed_widget_types=("grid",),
    ),
    "customer_health": QueryDefinition(
        key="customer_health",
        label="Customer Health",
        description="Current customer health table.",
        handler=data.get_customer_health,
        result_kind="rows",
        allowed_widget_types=("grid",),
    ),
    "product_health": QueryDefinition(
        key="product_health",
        label="Product Health",
        description="Current product health table.",
        handler=data.get_product_health,
        result_kind="rows",
        allowed_widget_types=("grid",),
    ),
    "root_cause_stats": QueryDefinition(
        key="root_cause_stats",
        label="Root Cause KPI Summary",
        description="Single-row KPI summary for root cause analytics coverage.",
        handler=data.get_root_cause_stats,
        result_kind="row",
        allowed_widget_types=("stat_row",),
    ),
    "mechanism_class_distribution": QueryDefinition(
        key="mechanism_class_distribution",
        label="Mechanism Class Distribution",
        description="Counts by mechanism class from pass 4 results.",
        handler=data.get_mechanism_class_distribution,
        result_kind="rows",
        allowed_widget_types=("chart", "grid"),
    ),
    "intervention_type_distribution": QueryDefinition(
        key="intervention_type_distribution",
        label="Intervention Type Distribution",
        description="Counts by intervention type from pass 4 results.",
        handler=data.get_intervention_type_distribution,
        result_kind="rows",
        allowed_widget_types=("chart", "grid"),
    ),
    "component_distribution": QueryDefinition(
        key="component_distribution",
        label="Top Components",
        description="Top components from pass 1 results.",
        handler=data.get_component_distribution,
        result_kind="rows",
        allowed_widget_types=("chart", "grid"),
        params=(
            QueryParam("limit", "int", required=False, default=20, minimum=1, maximum=100),
        ),
    ),
    "operation_distribution": QueryDefinition(
        key="operation_distribution",
        label="Operation Distribution",
        description="Operation verb counts from pass 1 results.",
        handler=data.get_operation_distribution,
        result_kind="rows",
        allowed_widget_types=("chart", "grid"),
    ),
    "top_engineering_fixes": QueryDefinition(
        key="top_engineering_fixes",
        label="Top Engineering Fixes",
        description="Top engineering fixes ranked by ticket count.",
        handler=data.get_top_engineering_fixes,
        result_kind="rows",
        allowed_widget_types=("chart", "grid"),
        params=(
            QueryParam("limit", "int", required=False, default=25, minimum=1, maximum=100),
        ),
    ),
    "root_cause_by_product": QueryDefinition(
        key="root_cause_by_product",
        label="Root Cause By Product",
        description="Mechanism class counts by product.",
        handler=data.get_root_cause_by_product,
        result_kind="rows",
        allowed_widget_types=("chart", "grid"),
        params=(
            QueryParam("limit", "int", required=False, default=10, minimum=1, maximum=50),
        ),
    ),
    "pipeline_completion_funnel": QueryDefinition(
        key="pipeline_completion_funnel",
        label="Pipeline Completion Funnel",
        description="Single-row counts for pass completion stages.",
        handler=data.get_pipeline_completion_funnel,
        result_kind="row",
        allowed_widget_types=("stat_row", "grid"),
    ),
    "ops_overview_kpis": QueryDefinition(
        key="ops_overview_kpis",
        label="CS Overview KPIs",
        description="Current-month close time, trailing six-month close time, and backlog snapshot KPIs.",
        handler=data.get_ops_overview_kpis,
        result_kind="row",
        allowed_widget_types=("stat_row",),
        params=(
            QueryParam("months", "int", required=False, default=6, minimum=1, maximum=24),
            QueryParam("group_name", "str", required=False, default="Customer Support (CS)"),
        ),
    ),
    "ops_most_improved_customers": QueryDefinition(
        key="ops_most_improved_customers",
        label="Most Improved Customers",
        description="Customers with the largest open-backlog reduction over the configured lookback.",
        handler=data.get_ops_most_improved_customers,
        result_kind="rows",
        allowed_widget_types=("grid", "chart"),
        params=(
            QueryParam("months", "int", required=False, default=3, minimum=1, maximum=24),
            QueryParam("top_n", "int", required=False, default=5, minimum=1, maximum=50),
            QueryParam("group_name", "str", required=False, default="Customer Support (CS)"),
        ),
    ),
    "ops_analyst_scorecard": QueryDefinition(
        key="ops_analyst_scorecard",
        label="Analyst Scorecard",
        description="Per-analyst closure, severity, and action-mix metrics for CS Overview.",
        handler=data.get_ops_analyst_scorecard,
        result_kind="rows",
        allowed_widget_types=("grid",),
        params=(
            QueryParam("months", "int", required=False, default=6, minimum=1, maximum=24),
        ),
    ),
    "analyst_action_profile": QueryDefinition(
        key="analyst_action_profile",
        label="Analyst Action Profile",
        description="Technical and scheduling action percentages per analyst.",
        handler=data.get_analyst_action_profile,
        result_kind="rows",
        allowed_widget_types=("chart", "grid"),
        params=(
            QueryParam("months", "int", required=False, default=6, minimum=1, maximum=24),
        ),
    ),
    "analyst_severity_profile": QueryDefinition(
        key="analyst_severity_profile",
        label="Analyst Severity Profile",
        description="High-severity closure share per analyst.",
        handler=data.get_analyst_severity_profile,
        result_kind="rows",
        allowed_widget_types=("chart", "grid"),
        params=(
            QueryParam("months", "int", required=False, default=6, minimum=1, maximum=24),
        ),
    ),
    "analyst_reassignment_profile": QueryDefinition(
        key="analyst_reassignment_profile",
        label="Analyst Reassignment Profile",
        description="Average handoffs per ticket by analyst and severity.",
        handler=data.get_analyst_reassignment_profile,
        result_kind="rows",
        allowed_widget_types=("chart", "grid"),
        params=(
            QueryParam("months", "int", required=False, default=6, minimum=1, maximum=24),
        ),
    ),
    "ops_analyst_monthly_closures": QueryDefinition(
        key="ops_analyst_monthly_closures",
        label="Analyst Monthly Closures",
        description="Monthly closure counts by analyst.",
        handler=data.get_ops_analyst_monthly_closures,
        result_kind="rows",
        allowed_widget_types=("chart", "grid"),
        params=(
            QueryParam("months", "int", required=False, default=12, minimum=1, maximum=36),
            QueryParam("top_n", "int", required=False, default=10, minimum=1, maximum=25),
        ),
    ),
    "monthly_created_vs_closed": QueryDefinition(
        key="monthly_created_vs_closed",
        label="Monthly Created vs Closed",
        description="Monthly created and closed ticket counts.",
        handler=data.get_monthly_created_vs_closed,
        result_kind="rows",
        allowed_widget_types=("chart", "grid"),
        params=(
            QueryParam("months", "int", required=False, default=12, minimum=1, maximum=36),
        ),
    ),
}


def list_queries() -> list[dict[str, Any]]:
    """Return query metadata suitable for populating editor pickers."""
    result = []
    for query in QUERY_CATALOG.values():
        result.append(
            {
                "key": query.key,
                "label": query.label,
                "description": query.description,
                "result_kind": query.result_kind,
                "allowed_widget_types": list(query.allowed_widget_types),
                "params": [
                    {
                        "name": param.name,
                        "type": param.param_type,
                        "required": param.required,
                        "default": param.default,
                        "minimum": param.minimum,
                        "maximum": param.maximum,
                        "choices": list(param.choices) if param.choices is not None else None,
                    }
                    for param in query.params
                ],
            }
        )
    return sorted(result, key=lambda item: item["label"])


def get_query_definition(query_key: str) -> QueryDefinition:
    """Return one query definition or raise a validation-friendly error."""
    query = QUERY_CATALOG.get(query_key)
    if not query:
        raise ValueError(f"Unknown dashboard query '{query_key}'.")
    return query


def validate_widget_query(query_key: str, widget_type: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
    """Validate a widget/query pairing and normalize query parameters."""
    if widget_type not in ALLOWED_WIDGET_TYPES:
        raise ValueError(f"Unsupported widget type '{widget_type}'.")

    query = get_query_definition(query_key)
    if widget_type not in query.allowed_widget_types:
        raise ValueError(
            f"Query '{query_key}' cannot be used with widget type '{widget_type}'."
        )

    params = params or {}
    allowed_names = {param.name for param in query.params}
    unknown_names = sorted(set(params) - allowed_names)
    if unknown_names:
        raise ValueError(
            f"Unknown parameters for query '{query_key}': {', '.join(unknown_names)}."
        )

    normalized: dict[str, Any] = {}
    for param in query.params:
        if param.name in params:
            normalized[param.name] = _coerce_param(param, params[param.name])
        elif param.required:
            raise ValueError(f"Missing required parameter '{param.name}' for query '{query_key}'.")
        elif param.default is not None:
            normalized[param.name] = param.default

    return normalized


def run_query(query_key: str, widget_type: str, params: dict[str, Any] | None = None) -> Any:
    """Execute a curated query after validation."""
    query = get_query_definition(query_key)
    normalized = validate_widget_query(query_key, widget_type, params)
    return query.handler(**normalized)
