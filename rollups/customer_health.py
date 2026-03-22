"""Customer health scoring helpers."""

from __future__ import annotations

from collections import defaultdict
from typing import Any

SCORE_FORMULA_VERSION = "v1"


def build_customer_health_model(rows: list[dict[str, Any]], as_of_date) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Build customer snapshot rows and per-ticket contributor rows for one date."""
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        customer = row.get("customer")
        group_name = row.get("group_name") or ""
        if customer:
            grouped[(customer, group_name)].append(row)

    snapshots: list[dict[str, Any]] = []
    contributors: list[dict[str, Any]] = []

    for (customer, group_name), customer_rows in grouped.items():
        cluster_counts: dict[str, int] = defaultdict(int)
        products: set[str] = set()
        components: set[str] = set()
        for row in customer_rows:
            cluster_id = row.get("cluster_id")
            if cluster_id:
                cluster_counts[cluster_id] += 1
            product_name = row.get("product_name")
            if product_name:
                products.add(product_name)
            component = row.get("component")
            if component:
                components.add(component)

        breadth_total = round(max(len(products) - 1, 0) * 0.75 + max(len(components) - 1, 0) * 0.25, 2)
        breadth_per_ticket = round(breadth_total / len(customer_rows), 2) if customer_rows else 0.0

        customer_contributors: list[dict[str, Any]] = []
        for row in customer_rows:
            contributor = _build_contributor_row(
                row,
                as_of_date=as_of_date,
                customer=customer,
                group_name=group_name,
                cluster_count=cluster_counts.get(row.get("cluster_id") or "", 0),
                breadth_contribution=breadth_per_ticket,
            )
            customer_contributors.append(contributor)
            contributors.append(contributor)

        factor_totals = {
            "pressure_score": round(sum(r["pressure_contribution"] for r in customer_contributors), 2),
            "aging_score": round(sum(r["aging_contribution"] for r in customer_contributors), 2),
            "friction_score": round(sum(r["friction_contribution"] for r in customer_contributors), 2),
            "concentration_score": round(sum(r["concentration_contribution"] for r in customer_contributors), 2),
            "breadth_score": round(sum(r["breadth_contribution"] for r in customer_contributors), 2),
        }
        health_score = round(sum(factor_totals.values()), 2)

        top_tickets = sorted(
            customer_contributors,
            key=lambda item: (-item["total_contribution"], -(item.get("days_opened") or 0), item["ticket_id"]),
        )[:5]
        top_clusters = [cluster for cluster, _ in sorted(cluster_counts.items(), key=lambda item: (-item[1], item[0]))[:5]]
        top_products = sorted(products)[:5]

        snapshots.append(
            {
                "as_of_date": as_of_date,
                "customer": customer,
                "group_name": group_name,
                "pressure_score": factor_totals["pressure_score"],
                "aging_score": factor_totals["aging_score"],
                "friction_score": factor_totals["friction_score"],
                "concentration_score": factor_totals["concentration_score"],
                "breadth_score": factor_totals["breadth_score"],
                "customer_health_score": health_score,
                "customer_health_band": health_band(health_score),
                "top_cluster_ids": top_clusters,
                "top_products": top_products,
                "factor_summary_json": {
                    "formula_version": SCORE_FORMULA_VERSION,
                    "top_tickets": [
                        {
                            "ticket_id": row["ticket_id"],
                            "ticket_number": row.get("ticket_number"),
                            "total_contribution": row["total_contribution"],
                            "cluster_id": row.get("cluster_id"),
                            "product_name": row.get("product_name"),
                        }
                        for row in top_tickets
                    ],
                    "factor_totals": factor_totals,
                },
                "score_formula_version": SCORE_FORMULA_VERSION,
            }
        )

    return snapshots, contributors


def health_band(score: float) -> str:
    """Bucket a customer into a readable health band."""
    if score < 15:
        return "healthy"
    if score < 30:
        return "watch"
    if score < 50:
        return "at_risk"
    return "critical"


def _build_contributor_row(
    row: dict[str, Any],
    *,
    as_of_date,
    customer: str,
    group_name: str,
    cluster_count: int,
    breadth_contribution: float,
) -> dict[str, Any]:
    open_flag = bool(row.get("open_flag"))
    priority = row.get("priority")
    complexity = row.get("overall_complexity")
    frustrated = row.get("frustrated")
    days_opened = _number(row.get("days_opened"))
    days_since_modified = _number(row.get("days_since_modified"))
    customer_messages = int(_number(row.get("customer_message_count")))
    handoff_count = int(_number(row.get("handoff_count")))

    pressure = 0.0
    if open_flag:
        pressure += 1.0
    if open_flag and priority is not None and priority <= 3:
        pressure += 2.0
    if open_flag and complexity is not None and complexity >= 4:
        pressure += 1.5
    if frustrated == "Yes":
        pressure += 3.0

    aging = 0.0
    if open_flag:
        if days_opened >= 90:
            aging += 3.0
        elif days_opened >= 60:
            aging += 2.0
        elif days_opened >= 30:
            aging += 1.0

        if days_since_modified >= 30:
            aging += 1.5
        elif days_since_modified >= 14:
            aging += 1.0
        elif days_since_modified >= 7:
            aging += 0.5

    friction = 0.0
    if frustrated == "Yes":
        friction += 1.5
    if customer_messages >= 10:
        friction += 1.0
    elif customer_messages >= 5:
        friction += 0.5
    if handoff_count >= 6:
        friction += 1.0
    elif handoff_count >= 3:
        friction += 0.5

    concentration = 0.0
    if row.get("cluster_id") and cluster_count > 1:
        concentration = min(2.0, round((cluster_count - 1) * 0.5, 2))

    breadth = round(breadth_contribution, 2)
    total = round(pressure + aging + friction + concentration + breadth, 2)

    return {
        "as_of_date": as_of_date,
        "customer": customer,
        "group_name": group_name,
        "ticket_id": row["ticket_id"],
        "ticket_number": row.get("ticket_number"),
        "ticket_name": row.get("ticket_name"),
        "product_name": row.get("product_name"),
        "status": row.get("status"),
        "severity": row.get("severity"),
        "assignee": row.get("assignee"),
        "days_opened": row.get("days_opened"),
        "date_modified": row.get("date_modified"),
        "priority": priority,
        "overall_complexity": complexity,
        "frustrated": frustrated,
        "cluster_id": row.get("cluster_id"),
        "mechanism_class": row.get("mechanism_class"),
        "intervention_type": row.get("intervention_type"),
        "pressure_contribution": round(pressure, 2),
        "aging_contribution": round(aging, 2),
        "friction_contribution": round(friction, 2),
        "concentration_contribution": round(concentration, 2),
        "breadth_contribution": breadth,
        "total_contribution": total,
        "score_formula_version": SCORE_FORMULA_VERSION,
    }


def _number(value: Any) -> float:
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0
