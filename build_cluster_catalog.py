"""
build_cluster_catalog.py - Deterministic support-ticket clustering exports.

Reads LLM pass data from Postgres, normalizes the source columns to snake_case,
builds a mechanism-class cluster catalog, and writes catalog + ticket mapping
outputs to disk as CSV and JSON.

The code supports two source shapes:
1. A wide analytics export that already uses human-readable columns like
   "Ticket #", "Pass 4", and "Mechanism Class".
2. This repo's native row-per-pass table (`ticket_llm_pass_results`), which is
   reshaped into the same wide analytical form before downstream processing.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import uuid
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

import db
from config import DATABASE_SCHEMA, DATABASE_URL, OUTPUT_DIR

SPACE_COLUMN_MAPPING = {
    "Ticket #": "ticket_id",
    "Name": "name",
    "Product": "product",
    "Customer": "customer",
    "Pass 1": "pass_1",
    "Phenomenon": "phenomenon",
    "Pass 2": "pass_2",
    "Component": "component",
    "Operation": "operation",
    "Unexpected State": "unexpected_state",
    "Pass 3": "pass_3",
    "Mechanism": "mechanism",
    "Pass 4": "pass_4",
    "Mechanism Class": "mechanism_class",
    "Intervention Type": "intervention_type",
    "Intervention Action": "intervention_action",
}

LONG_FORM_PASSES = {
    "pass1_phenomenon",
    "pass2_grammar",
    "pass3_mechanism",
    "pass4_intervention",
}

CLUSTER_METHOD = "mechanism_class_catalog"
CLUSTER_SCOPE = "ticket_llm_pass_results.pass4_success"


def get_db_engine() -> Engine:
    """Create the SQLAlchemy engine for the configured Postgres database."""
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is not set.")
    return create_engine(DATABASE_URL, future=True)


def load_data(engine: Engine) -> pd.DataFrame:
    """Load the raw pass-results table from Postgres."""
    query = text(f"SELECT * FROM {DATABASE_SCHEMA}.ticket_llm_pass_results")
    return pd.read_sql_query(query, engine)


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize user-facing columns to snake_case and strip string values."""
    cleaned = df.rename(columns=SPACE_COLUMN_MAPPING).copy()
    for column in cleaned.columns:
        if pd.api.types.is_object_dtype(cleaned[column]):
            cleaned[column] = cleaned[column].map(_normalize_text)
    return cleaned


def filter_success(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only rows with a successful Pass 4 result and a cluster id."""
    if "pass_4" not in df.columns:
        raise ValueError("Expected normalized 'pass_4' column.")
    filtered = df[df["pass_4"] == "success"].copy()
    filtered = filtered[filtered["mechanism_class"].notna()]
    filtered = filtered[filtered["mechanism_class"] != ""]
    return filtered


def compute_dominant(df: pd.DataFrame, group_col: str, target_col: str) -> pd.Series:
    """Return the most frequent non-empty target value per group.

    Ties are broken lexicographically for deterministic output.
    """
    required = {group_col, target_col}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns for dominant calculation: {sorted(missing)}")

    values = df[[group_col, target_col]].copy()
    values[target_col] = values[target_col].map(_normalize_text)
    values = values[values[target_col].notna()]
    if values.empty:
        return pd.Series(dtype="object")

    counts = (
        values.groupby([group_col, target_col], dropna=False)
        .size()
        .reset_index(name="count")
        .sort_values([group_col, "count", target_col], ascending=[True, False, True])
    )
    dominant = counts.drop_duplicates(subset=[group_col], keep="first")
    return dominant.set_index(group_col)[target_col]


def build_subclusters(df: pd.DataFrame) -> dict[str, list[dict[str, Any]]]:
    """Build ordered component/operation subclusters within each mechanism class."""
    working = df[["cluster_id", "component", "operation"]].copy()
    working["component"] = working["component"].fillna("")
    working["operation"] = working["operation"].fillna("")

    counts = (
        working.groupby(["cluster_id", "component", "operation"], dropna=False)
        .size()
        .reset_index(name="ticket_count")
    )
    totals = counts.groupby("cluster_id")["ticket_count"].transform("sum")
    counts["percent_within_cluster"] = (counts["ticket_count"] / totals).round(6)
    counts = counts.sort_values(
        ["cluster_id", "ticket_count", "component", "operation"],
        ascending=[True, False, True, True],
    )

    subclusters: dict[str, list[dict[str, Any]]] = {}
    for cluster_id, group in counts.groupby("cluster_id", sort=False):
        subclusters[str(cluster_id)] = [
            {
                "component": row.component or None,
                "operation": row.operation or None,
                "ticket_count": int(row.ticket_count),
                "percent_within_cluster": float(row.percent_within_cluster),
            }
            for row in group.itertuples(index=False)
        ]
    return subclusters


def build_cluster_catalog(df: pd.DataFrame) -> pd.DataFrame:
    """Build the mechanism-class cluster catalog."""
    if df.empty:
        return pd.DataFrame(
            columns=[
                "cluster_id",
                "ticket_count",
                "percent_of_total",
                "customer_count",
                "product_count",
                "dominant_component",
                "dominant_operation",
                "dominant_intervention_type",
                "example_ticket_ids",
                "example_mechanisms",
                "subclusters",
            ]
        )

    working = df.copy()
    working["cluster_id"] = working["mechanism_class"]
    total_rows = len(working)

    base = (
        working.groupby("cluster_id", dropna=False)
        .agg(ticket_count=("ticket_id", "count"))
        .reset_index()
    )
    base["percent_of_total"] = (base["ticket_count"] / total_rows).round(6)

    customer_counts = (
        working.assign(customer=working["customer"].map(_normalize_text))
        .loc[lambda frame: frame["customer"].notna()]
        .groupby("cluster_id")["customer"]
        .nunique()
    )
    product_counts = (
        working.assign(product=working["product"].map(_normalize_text))
        .loc[lambda frame: frame["product"].notna()]
        .groupby("cluster_id")["product"]
        .nunique()
    )

    dominant_component = compute_dominant(working, "cluster_id", "component")
    dominant_operation = compute_dominant(working, "cluster_id", "operation")
    dominant_intervention_type = compute_dominant(working, "cluster_id", "intervention_type")

    examples = (
        working.sort_values(["cluster_id", "ticket_id", "created_at"], kind="stable")
        if "created_at" in working.columns
        else working.sort_values(["cluster_id", "ticket_id"], kind="stable")
    )
    example_ticket_ids = (
        examples.groupby("cluster_id")["ticket_id"]
        .apply(lambda series: [int(value) for value in series.head(5).tolist()])
    )
    example_mechanisms = (
        examples.assign(mechanism=examples["mechanism"].map(_normalize_text))
        .loc[lambda frame: frame["mechanism"].notna()]
        .drop_duplicates(subset=["cluster_id", "mechanism"])
        .groupby("cluster_id")["mechanism"]
        .apply(lambda series: series.head(3).tolist())
    )
    subclusters = build_subclusters(working)

    catalog = base.copy()
    catalog["customer_count"] = catalog["cluster_id"].map(customer_counts).fillna(0).astype(int)
    catalog["product_count"] = catalog["cluster_id"].map(product_counts).fillna(0).astype(int)
    catalog["dominant_component"] = catalog["cluster_id"].map(dominant_component)
    catalog["dominant_operation"] = catalog["cluster_id"].map(dominant_operation)
    catalog["dominant_intervention_type"] = catalog["cluster_id"].map(dominant_intervention_type)
    catalog["example_ticket_ids"] = catalog["cluster_id"].map(example_ticket_ids).apply(
        lambda value: value if isinstance(value, list) else []
    )
    catalog["example_mechanisms"] = catalog["cluster_id"].map(example_mechanisms).apply(
        lambda value: value if isinstance(value, list) else []
    )
    catalog["subclusters"] = catalog["cluster_id"].map(
        lambda cluster_id: subclusters.get(str(cluster_id), [])
    )
    catalog = catalog.sort_values(["ticket_count", "cluster_id"], ascending=[False, True]).reset_index(
        drop=True
    )
    return catalog


def build_ticket_mapping(df: pd.DataFrame) -> pd.DataFrame:
    """Build the per-ticket cluster mapping table."""
    mapping = df[
        ["ticket_id", "mechanism_class", "component", "operation", "intervention_type"]
    ].copy()
    mapping = mapping.rename(columns={"mechanism_class": "cluster_id"})
    mapping = mapping.sort_values(["cluster_id", "ticket_id"], kind="stable").reset_index(drop=True)
    return mapping


def save_outputs(
    cluster_catalog: pd.DataFrame,
    ticket_mapping: pd.DataFrame,
    output_dir: str | os.PathLike[str],
) -> None:
    """Write the catalog and mapping outputs to CSV and JSON."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    catalog_csv = cluster_catalog.copy()
    for column in ("example_ticket_ids", "example_mechanisms", "subclusters"):
        if column in catalog_csv.columns:
            catalog_csv[column] = catalog_csv[column].apply(_json_dumps)
    catalog_csv.to_csv(output_path / "cluster_catalog.csv", index=False)

    with (output_path / "cluster_catalog.json").open("w", encoding="utf-8") as handle:
        json.dump(cluster_catalog.to_dict(orient="records"), handle, indent=2, ensure_ascii=False)

    ticket_mapping.to_csv(output_path / "ticket_cluster_mapping.csv", index=False)


def persist_outputs(
    engine: Engine,
    cluster_catalog: pd.DataFrame,
    ticket_mapping: pd.DataFrame,
) -> str:
    """Persist the current deterministic cluster run and replace prior rows."""
    cluster_run_id = str(uuid.uuid4())
    params_json = {
        "source_table": f"{DATABASE_SCHEMA}.ticket_llm_pass_results",
        "filter": {"pass_4": "success"},
        "cluster_id_source": "mechanism_class",
        "subcluster_keys": ["component", "operation"],
        "deterministic": True,
    }
    notes = (
        "Deterministic mechanism-class clustering derived from pass 4 success rows. "
        "Each run replaces prior rows for the same cluster_method."
    )

    with engine.begin() as conn:
        conn.execute(
            text(
                f"""
                DELETE FROM {DATABASE_SCHEMA}.cluster_runs
                WHERE cluster_method = :cluster_method
                """
            ),
            {"cluster_method": CLUSTER_METHOD},
        )
        conn.execute(
            text(
                f"""
                INSERT INTO {DATABASE_SCHEMA}.cluster_runs (
                    cluster_run_id,
                    cluster_method,
                    cluster_scope,
                    embedding_model,
                    clustering_params,
                    run_status,
                    notes
                ) VALUES (
                    :cluster_run_id,
                    :cluster_method,
                    :cluster_scope,
                    NULL,
                    CAST(:clustering_params AS JSONB),
                    'completed',
                    :notes
                )
                """
            ),
            {
                "cluster_run_id": cluster_run_id,
                "cluster_method": CLUSTER_METHOD,
                "cluster_scope": CLUSTER_SCOPE,
                "clustering_params": json.dumps(params_json),
                "notes": notes,
            },
        )

        if not ticket_mapping.empty:
            conn.execute(
                text(
                    f"""
                    INSERT INTO {DATABASE_SCHEMA}.ticket_clusters (
                        ticket_id,
                        ticket_number,
                        cluster_run_id,
                        cluster_id,
                        cluster_label,
                        cluster_confidence,
                        cluster_method,
                        assigned_at
                    ) VALUES (
                        :ticket_id,
                        (
                            SELECT ticket_number
                            FROM {DATABASE_SCHEMA}.tickets
                            WHERE ticket_id = :ticket_id
                        ),
                        :cluster_run_id,
                        :cluster_id,
                        :cluster_label,
                        :cluster_confidence,
                        :cluster_method,
                        now()
                    )
                    """
                ),
                ticket_cluster_records_for_db(ticket_mapping, cluster_run_id),
            )

        if not cluster_catalog.empty:
            conn.execute(
                text(
                    f"""
                    INSERT INTO {DATABASE_SCHEMA}.cluster_catalog (
                        cluster_run_id,
                        cluster_id,
                        cluster_label,
                        cluster_description,
                        representative_tickets,
                        common_issue_pattern,
                        common_mechanism_pattern,
                        suggested_intervention_type,
                        ticket_count,
                        percent_of_total,
                        customer_count,
                        product_count,
                        dominant_component,
                        dominant_operation,
                        dominant_intervention_type,
                        example_ticket_ids,
                        example_mechanisms,
                        subclusters,
                        updated_at
                    ) VALUES (
                        :cluster_run_id,
                        :cluster_id,
                        :cluster_label,
                        :cluster_description,
                        CAST(:representative_tickets AS JSONB),
                        :common_issue_pattern,
                        :common_mechanism_pattern,
                        :suggested_intervention_type,
                        :ticket_count,
                        :percent_of_total,
                        :customer_count,
                        :product_count,
                        :dominant_component,
                        :dominant_operation,
                        :dominant_intervention_type,
                        CAST(:example_ticket_ids AS JSONB),
                        CAST(:example_mechanisms AS JSONB),
                        CAST(:subclusters AS JSONB),
                        now()
                    )
                    """
                ),
                cluster_catalog_records_for_db(cluster_catalog, cluster_run_id),
            )
    return cluster_run_id


def generate_llm_label_input(cluster: dict[str, Any]) -> dict[str, Any]:
    """Prepare a compact deterministic label prompt payload for a cluster."""
    subclusters = cluster.get("subclusters") or []
    return {
        "mechanism_class": cluster.get("cluster_id") or cluster.get("mechanism_class"),
        "dominant_component": cluster.get("dominant_component"),
        "dominant_operation": cluster.get("dominant_operation"),
        "example_mechanisms": list(cluster.get("example_mechanisms") or []),
        "top_3_subclusters": list(subclusters[:3]),
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build deterministic support-ticket mechanism clusters from Postgres.",
    )
    parser.add_argument(
        "--output-dir",
        default=OUTPUT_DIR,
        help="Directory for cluster_catalog.csv/json and ticket_cluster_mapping.csv.",
    )
    args = parser.parse_args()

    engine = get_db_engine()
    try:
        db.migrate()
        raw = load_data(engine)
        normalized = clean_columns(raw)
        analytical = _ensure_analytical_shape(normalized, engine)
        successful = filter_success(analytical)
        cluster_catalog = build_cluster_catalog(successful)
        ticket_mapping = build_ticket_mapping(successful)
        persist_outputs(engine, cluster_catalog, ticket_mapping)
        save_outputs(cluster_catalog, ticket_mapping, args.output_dir)
    finally:
        engine.dispose()


def _ensure_analytical_shape(df: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    """Convert the repo's long-form pass table into the analytical wide form."""
    expected = {
        "ticket_id",
        "name",
        "product",
        "customer",
        "pass_1",
        "phenomenon",
        "pass_2",
        "component",
        "operation",
        "unexpected_state",
        "pass_3",
        "mechanism",
        "pass_4",
        "mechanism_class",
        "intervention_type",
        "intervention_action",
    }
    if expected.issubset(df.columns):
        return df

    if not {"ticket_id", "pass_name", "status"}.issubset(df.columns):
        missing = sorted(expected - set(df.columns))
        raise ValueError(f"Unsupported source shape; missing analytical columns: {missing}")

    return _reshape_long_form_results(df, engine)


def _reshape_long_form_results(results: pd.DataFrame, engine: Engine) -> pd.DataFrame:
    """Reshape row-per-pass results into the expected wide analytical rows."""
    results = results[results["pass_name"].isin(LONG_FORM_PASSES)].copy()
    if results.empty:
        return pd.DataFrame(columns=list(SPACE_COLUMN_MAPPING.values()))

    ordering_columns = [column for column in ("completed_at", "updated_at", "created_at") if column in results]
    results["__status_rank"] = results["status"].eq("success").map({True: 0, False: 1})
    results = results.sort_values(
        ["ticket_id", "pass_name", "__status_rank", *ordering_columns],
        ascending=[True, True, True, False, False, False][: 3 + len(ordering_columns)],
        kind="stable",
    )
    latest = results.drop_duplicates(subset=["ticket_id", "pass_name"], keep="first").drop(
        columns="__status_rank"
    )

    pass1 = _select_pass_columns(
        latest,
        "pass1_phenomenon",
        {
            "status": "pass_1",
            "phenomenon": "phenomenon",
            "component": "component",
            "operation": "operation",
            "unexpected_state": "unexpected_state",
        },
    )
    legacy_pass2 = _select_pass_columns(
        latest,
        "pass2_grammar",
        {
            "status": "pass_2_legacy",
            "component": "component_legacy",
            "operation": "operation_legacy",
            "unexpected_state": "unexpected_state_legacy",
        },
    )
    pass3 = _select_pass_columns(
        latest,
        "pass3_mechanism",
        {
            "status": "pass_3",
            "mechanism": "mechanism",
        },
    )
    pass4 = _select_pass_columns(
        latest,
        "pass4_intervention",
        {
            "status": "pass_4",
            "mechanism_class": "mechanism_class",
            "intervention_type": "intervention_type",
            "intervention_action": "intervention_action",
            "created_at": "created_at",
        },
    )
    tickets = pd.read_sql_query(
        text(
            f"""
            SELECT
                ticket_id,
                ticket_name AS name,
                product_name AS product,
                customer
            FROM {DATABASE_SCHEMA}.tickets
            """
        ),
        engine,
    )

    wide = (
        tickets.merge(pass1, on="ticket_id", how="left")
        .merge(legacy_pass2, on="ticket_id", how="left")
        .merge(pass3, on="ticket_id", how="left")
        .merge(pass4, on="ticket_id", how="left")
    )

    for current, legacy in (
        ("component", "component_legacy"),
        ("operation", "operation_legacy"),
        ("unexpected_state", "unexpected_state_legacy"),
    ):
        wide[current] = wide[current].where(wide[current].notna(), wide[legacy])
    wide["pass_2"] = wide["pass_1"].where(
        wide[["component", "operation", "unexpected_state"]].notna().any(axis=1),
        wide["pass_2_legacy"],
    )
    return wide[
        [
            "ticket_id",
            "name",
            "product",
            "customer",
            "pass_1",
            "phenomenon",
            "pass_2",
            "component",
            "operation",
            "unexpected_state",
            "pass_3",
            "mechanism",
            "pass_4",
            "mechanism_class",
            "intervention_type",
            "intervention_action",
            "created_at",
        ]
    ]


def _select_pass_columns(
    latest: pd.DataFrame,
    pass_name: str,
    column_mapping: dict[str, str],
) -> pd.DataFrame:
    subset = latest[latest["pass_name"] == pass_name][["ticket_id", *column_mapping.keys()]].copy()
    return subset.rename(columns=column_mapping)


def _normalize_text(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return value


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def cluster_catalog_records_for_db(
    cluster_catalog: pd.DataFrame,
    cluster_run_id: str,
) -> list[dict[str, Any]]:
    """Serialize catalog rows into DB-ready records."""
    records: list[dict[str, Any]] = []
    for row in cluster_catalog.to_dict(orient="records"):
        dominant_component = row.get("dominant_component")
        dominant_operation = row.get("dominant_operation")
        issue_pattern_parts = [part for part in (dominant_component, dominant_operation) if part]
        example_mechanisms = list(row.get("example_mechanisms") or [])
        records.append(
            {
                "cluster_run_id": cluster_run_id,
                "cluster_id": row["cluster_id"],
                "cluster_label": _prettify_cluster_id(row["cluster_id"]),
                "cluster_description": (
                    f"Deterministic mechanism-class cluster for {row['cluster_id']} derived "
                    "from successful pass 4 results."
                ),
                "representative_tickets": _json_dumps(row.get("example_ticket_ids") or []),
                "common_issue_pattern": " / ".join(issue_pattern_parts) or None,
                "common_mechanism_pattern": example_mechanisms[0] if example_mechanisms else None,
                "suggested_intervention_type": row.get("dominant_intervention_type"),
                "ticket_count": int(row["ticket_count"]),
                "percent_of_total": float(row["percent_of_total"]),
                "customer_count": int(row["customer_count"]),
                "product_count": int(row["product_count"]),
                "dominant_component": dominant_component,
                "dominant_operation": dominant_operation,
                "dominant_intervention_type": row.get("dominant_intervention_type"),
                "example_ticket_ids": _json_dumps(row.get("example_ticket_ids") or []),
                "example_mechanisms": _json_dumps(example_mechanisms),
                "subclusters": _json_dumps(row.get("subclusters") or []),
            }
        )
    return records


def ticket_cluster_records_for_db(
    ticket_mapping: pd.DataFrame,
    cluster_run_id: str,
) -> list[dict[str, Any]]:
    """Serialize ticket mapping rows into DB-ready records."""
    records: list[dict[str, Any]] = []
    for row in ticket_mapping.to_dict(orient="records"):
        records.append(
            {
                "ticket_id": int(row["ticket_id"]),
                "cluster_run_id": cluster_run_id,
                "cluster_id": row["cluster_id"],
                "cluster_label": _prettify_cluster_id(row["cluster_id"]),
                "cluster_confidence": 1.0,
                "cluster_method": CLUSTER_METHOD,
            }
        )
    return records


def _prettify_cluster_id(value: str | None) -> str | None:
    if not value:
        return None
    return value.replace("_", " ").title()


if __name__ == "__main__":
    main()
