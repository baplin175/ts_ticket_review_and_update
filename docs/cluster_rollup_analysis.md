# Cluster Rollup Analysis

Root-cause clustering workflow built on top of `tickets_ai.v_root_cause_cluster`.

## Concepts: L1 vs L2

| Level | Meaning | Source |
|-------|---------|--------|
| **L2** | Fine-grained `cluster_key` produced by Pass 4 (cluster key normalization) | `v_root_cause_cluster.cluster_key` |
| **L1** | Broader reusable bucket that groups related L2 keys | `cluster_key_rollup_map.cluster_key_l1` (falls back to L2 when unmapped) |

Example: L2 keys `incorrect_charge_calculation_logic`, `incorrect_meter_readings_used`, and `incorrect_usage_reading_used` all roll up to L1 bucket `incorrect_calculation_logic`.

## Database Objects

### Tables

| Table | Purpose |
|-------|---------|
| `cluster_key_rollup_map` | Maps L2 cluster_key → L1 broader bucket. Has `notes` and `is_active` columns. |
| `cluster_recommendations` | Persisted per-cluster engineering recommendations with confidence, source model. |

### Views

| View | Purpose |
|------|---------|
| `v_ticket_failure_flat` | One-row-per-ticket flat view (passthrough from `v_root_cause_cluster`) |
| `v_cluster_summary_l2` | Ticket counts grouped by product, mechanism_class, cluster_key |
| `v_cluster_summary_l1` | Ticket counts rolled up to L1 via `cluster_key_rollup_map` |
| `v_cluster_examples` | Per-ticket detail with both L1 and L2 keys for drill-down |

## Querying Clusters

All functions are in `web/data.py`.

### L2 summary (current cluster_key granularity)

```python
from web.data import get_cluster_summary_l2
rows = get_cluster_summary_l2()
# [{'product_name': ..., 'mechanism_class': ..., 'cluster_key': ..., 'ticket_count': ...}, ...]
```

### L1 summary (rolled-up broader buckets)

```python
from web.data import get_cluster_summary_l1
rows = get_cluster_summary_l1()
# [{'product_name': ..., 'mechanism_class': ..., 'cluster_key_l1': ..., 'ticket_count': ...}, ...]
```

### Top N clusters per product

Uses `ROW_NUMBER() OVER (PARTITION BY product_name ORDER BY ticket_count DESC)` to rank clusters within each product, then filters to the top N.

```python
from web.data import get_top_clusters

# Top 5 per product (default)
rows = get_top_clusters()

# Top 3 per product
rows = get_top_clusters(top_n=3)

# Filtered to specific products
rows = get_top_clusters(product_names=["PM - Utility Billing", "PM - Payroll"], top_n=5)
```

Each row includes `rn` (rank within its product, 1-based).

## Fetching Examples

```python
from web.data import get_cluster_examples

tickets = get_cluster_examples(
    product_name="PM - Utility Billing",
    mechanism_class="configuration_mismatch",
    cluster_key_l1="invalid_configuration_state"
)
# Returns ticket_id, product_name, mechanism_class, cluster_key_l1, cluster_key_l2, mechanism, intervention_action
```

## Saving Recommendations

```python
from web.data import save_cluster_recommendation, get_cluster_recommendations

# Save
rec = save_cluster_recommendation(
    product_name="PM - Utility Billing",
    mechanism_class="configuration_mismatch",
    cluster_key_l1="invalid_configuration_state",
    ticket_count=13,
    recommended_change="Add config validation on save",
    where_to_implement="Config service save endpoint",
    why_it_prevents_recurrence="Catches invalid state before it propagates",
    confidence="high",
    source_model="gpt-4o"
)

# Fetch
recs = get_cluster_recommendations(product_name="PM - Utility Billing")
recs = get_cluster_recommendations(cluster_key_l1="invalid_configuration_state")
```

## Migration

Applied via `python3 db.py migrate` — file `036_cluster_rollup_analysis.sql`.
