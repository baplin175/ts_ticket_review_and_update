# Dashboard Platform Plan

## Objective

Build a database-backed dashboard platform for the Dash web app that allows runtime creation of shared dashboards, sections, and widgets without code changes per dashboard. The v1 implementation must preserve a clean path to future per-user dashboards without introducing an auth model yet.

## V1 Scope

- Shared dashboards only
- Curated server-owned query catalogue only
- Form-based dashboard editor
- Widget types:
  - `alert`
  - `stat_row`
  - `grid`
  - `chart` with `bar`, `line`, `stacked_bar`, and optional `donut`
- DB-backed dashboards rendered at runtime
- Dynamic sidebar/navigation for active dashboards
- Validation and safe error states for invalid widget configuration

## Explicit Non-Goals For V1

- No raw SQL authored by end users
- No per-user dashboard ownership UI
- No dashboard permissions model beyond global visibility
- No generic click-to-drill-down framework for arbitrary widgets
- No drag-and-drop editor
- No dashboard version history or draft/publish workflow
- No forced migration of current custom interactive pages into the new platform

## Architectural Decisions

### Ownership Model

Although v1 dashboards are shared, ownership is modeled now so the persistence layer does not assume global-only records.

Recommended ownership fields:

- `owner_type`: string enum-like value
  - v1 value: `global`
  - future values: `user`, `team`
- `owner_id`: nullable string/integer reference
  - `NULL` for global dashboards in v1

Dashboard slug uniqueness should be scoped by ownership semantics, not assumed to be globally unique forever. If the migration implementation keeps v1 uniqueness simple, the service layer must still avoid hard-coding that assumption.

### Route Strategy

Use a dedicated route prefix for dynamic dashboards:

- Static pages keep their current routes
- DB-backed dashboards live under `/dashboards/<slug>`

This avoids route collisions with existing pages like `/tickets`, `/health`, and `/ticket/<id>`, and leaves room for future prefixes such as `/u/<user>/dashboards/<slug>` if user scoping is introduced later.

### Canonical Dashboard Definition

Both YAML-defined and DB-defined pages must be transformed into the same in-memory structure before rendering.

Canonical shape:

```python
{
    "title": "Operations Dashboard",
    "description": "Shared operational view",
    "route": "/dashboards/operations",
    "sections": [
        {
            "id": "section-summary",
            "title": "Summary",
            "description": None,
            "layout": {"columns": 2},
            "widgets": [
                {
                    "id": "widget-kpis",
                    "type": "stat_row",
                    "title": None,
                    "query": {
                        "key": "open_ticket_stats",
                        "params": {}
                    },
                    "config": {
                        "items": [
                            {"field": "total_open", "title": "Open Tickets", "icon": "tabler:ticket", "color": "blue"}
                        ]
                    }
                }
            ]
        }
    ]
}
```

Rules:

- Renderer input must not depend on whether the source was YAML or DB.
- Layout metadata belongs at the section level.
- Widget presentation config belongs in widget config, not in the query definition.
- Query keys always reference server-owned definitions.

## Data Model

### `dashboards`

Purpose: stores page-level metadata.

Recommended fields:

- `id`
- `name`
- `slug`
- `description`
- `icon`
- `sort_order`
- `is_default`
- `is_active`
- `owner_type`
- `owner_id`
- `created_at`
- `updated_at`

### `dashboard_sections`

Purpose: stores ordered sections within a dashboard.

Recommended fields:

- `id`
- `dashboard_id`
- `title`
- `description`
- `layout_columns`
- `sort_order`
- `is_active`
- `created_at`
- `updated_at`

### `dashboard_widgets`

Purpose: stores ordered widgets within a section.

Recommended fields:

- `id`
- `section_id`
- `widget_type`
- `title`
- `query_key`
- `query_params_json`
- `display_config_json`
- `sort_order`
- `is_active`
- `created_at`
- `updated_at`

Notes:

- `query_params_json` stores validated parameter values only.
- `display_config_json` stores widget-specific presentation settings such as chart axes, colors, column configs, empty-state text, and stat-row items.
- Widget type should be validated in the application layer against an allowed set.

## Query Catalogue Contract

The query catalogue is server-owned code, not user-authored data.

Each query definition must include:

- `key`
- `label`
- `description`
- `handler`
- `params_schema`
- `result_kind`
- `allowed_widget_types`
- `default_limit` or other guardrails as appropriate

Supported result kinds:

- `rows`
- `row`

Parameter schema requirements:

- each parameter has an explicit type
- optional default values
- optional enum choices
- optional min/max constraints
- unknown parameters are rejected

## Initial V1 Query Catalogue

Candidate queries already present in `web/data.py`:

- `open_ticket_stats`
- `backlog_daily`
- `backlog_daily_by_severity`
- `backlog_aging`
- `open_by_product`
- `open_by_status`
- `ticket_list`
- `customer_health`
- `product_health`
- `root_cause_stats`
- `mechanism_class_distribution`
- `intervention_type_distribution`
- `component_distribution`
- `operation_distribution`
- `top_engineering_fixes`
- `root_cause_by_product`
- `pipeline_completion_funnel`

Queries that should not be self-service in v1:

- ticket-detail lookups
- drill-down-specific fetchers
- sync actions
- multi-record interactive actions tied to current custom pages

## Guardrails

### Query Safety

- No raw SQL input from users
- All widget data must come from registered catalogue handlers
- Parameter values are validated server-side before execution

### Result Limits

- Grid widgets must enforce a maximum row count
- High-cardinality chart queries should expose a server-enforced limit parameter
- Renderer should fail gracefully when a query returns empty data

### Save Validation

Dashboard save operations must validate:

- dashboard metadata
- slug format
- allowed widget types
- query key existence
- query/widget compatibility
- parameter schema compatibility
- chart axis fields for chart widgets
- column definitions for grid widgets
- stat-row item definitions for stat widgets

### Error Handling

- Invalid widget config renders as an inline alert rather than crashing the page
- Query execution failures are isolated to the widget that failed
- Missing dashboard slug returns a dashboard-specific not-found state

## Routing And Navigation Design

### Static Pages

Existing static pages remain code-driven:

- `/`
- `/tickets`
- `/health`
- `/root-cause`
- `/config`
- `/ticket/<id>`

### Dynamic Dashboards

Dynamic dashboards are loaded from DB and exposed under:

- `/dashboards/<slug>`

### Unified Page Registry

The app should resolve pages through one registry abstraction:

- static pages from code/YAML
- dynamic dashboards from DB

The sidebar should be data-driven rather than generated from a fixed list of output IDs at import time.

## Renderer Refactor Target

The renderer should expose public functions:

- `render_dashboard(definition)`
- `render_section(section_def)`
- `render_widget(widget_def)`

The renderer should also expose validation helpers or rely on a dedicated validation module.

YAML support can remain during migration, but YAML should be treated as another definition source transformed into the canonical shape.

## Editor Design

The v1 editor should support:

- create dashboard
- edit metadata
- add/edit/delete sections
- reorder sections
- add/edit/delete widgets
- reorder widgets
- configure query and parameters from a curated list
- preview rendered output using the shared renderer

The editor should be form-based. Reordering can be implemented with explicit move up/down controls or sort-order inputs.

## Test Matrix

### Migration Tests

- dashboard tables exist
- ownership fields exist
- ordering and active-state columns exist

### Data Layer Tests

- CRUD functions create/update/delete correctly
- fetch functions return a full ordered dashboard tree
- ownership filters work for global dashboards

### Query Catalogue Tests

- unknown query keys are rejected
- unsupported parameters are rejected
- invalid parameter types are rejected
- allowed widget compatibility is enforced

### Renderer Tests

- canonical dashboard definitions render successfully
- invalid widget definitions render safe alerts
- empty data states are handled per widget type

### Route Tests

- static pages still resolve correctly
- `/dashboards/<slug>` resolves active dashboards
- missing or inactive slugs return the expected state

### Editor Tests

- valid dashboards save successfully
- invalid widget/query combinations are blocked
- reordering persists correctly
- preview uses the shared render path

## Delivery Sequence

1. finalize query catalogue contract and v1 query inventory
2. refactor renderer to accept canonical definitions
3. add migration and data-layer CRUD
4. refactor routing/nav for dynamic dashboards
5. build editor
6. seed example dashboards
7. add hardening tests and docs
