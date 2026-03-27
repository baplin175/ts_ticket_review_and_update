-- Migration 048 — Seed a configurable runtime dashboard mirroring the
-- current "CS Overview" page under Operations.
--
-- This does not replace the existing static page. It creates a separate
-- runtime dashboard entry so the original page remains the success model.

WITH existing_dashboard AS (
    SELECT id
    FROM dashboards
    WHERE owner_type = 'global'
      AND owner_id IS NULL
      AND slug = 'cs-overview-configurable'
),
inserted_dashboard AS (
    INSERT INTO dashboards (
        name, slug, description, icon, sort_order, is_default, is_active,
        owner_type, owner_id, created_at, updated_at
    )
    SELECT
        'CS Overview (Configurable)',
        'cs-overview-configurable',
        'Attempted extraction of the current CS Overview page into the runtime dashboard framework.',
        'tabler:layout-dashboard',
        100,
        FALSE,
        TRUE,
        'global',
        NULL,
        now(),
        now()
    WHERE NOT EXISTS (SELECT 1 FROM existing_dashboard)
    RETURNING id
),
dashboard_row AS (
    SELECT id FROM inserted_dashboard
    UNION ALL
    SELECT id FROM existing_dashboard
),
summary_section AS (
    INSERT INTO dashboard_sections (
        dashboard_id, title, description, layout_columns, sort_order, is_active, created_at, updated_at
    )
    SELECT
        dashboard_row.id,
        'Summary',
        NULL,
        1,
        0,
        TRUE,
        now(),
        now()
    FROM dashboard_row
    WHERE NOT EXISTS (
        SELECT 1 FROM dashboard_sections s
        WHERE s.dashboard_id = dashboard_row.id
          AND COALESCE(s.title, '') = 'Summary'
    )
    RETURNING id, dashboard_id
),
summary_section_row AS (
    SELECT id, dashboard_id FROM summary_section
    UNION ALL
    SELECT s.id, s.dashboard_id
    FROM dashboard_sections s
    JOIN dashboard_row d ON d.id = s.dashboard_id
    WHERE COALESCE(s.title, '') = 'Summary'
),
most_improved_section AS (
    INSERT INTO dashboard_sections (
        dashboard_id, title, description, layout_columns, sort_order, is_active, created_at, updated_at
    )
    SELECT
        dashboard_row.id,
        'Most Improved Customers (last 3 months)',
        'CS-group customers whose open backlog decreased the most.',
        1,
        1,
        TRUE,
        now(),
        now()
    FROM dashboard_row
    WHERE NOT EXISTS (
        SELECT 1 FROM dashboard_sections s
        WHERE s.dashboard_id = dashboard_row.id
          AND COALESCE(s.title, '') = 'Most Improved Customers (last 3 months)'
    )
    RETURNING id, dashboard_id
),
most_improved_section_row AS (
    SELECT id, dashboard_id FROM most_improved_section
    UNION ALL
    SELECT s.id, s.dashboard_id
    FROM dashboard_sections s
    JOIN dashboard_row d ON d.id = s.dashboard_id
    WHERE COALESCE(s.title, '') = 'Most Improved Customers (last 3 months)'
),
scorecard_section AS (
    INSERT INTO dashboard_sections (
        dashboard_id, title, description, layout_columns, sort_order, is_active, created_at, updated_at
    )
    SELECT
        dashboard_row.id,
        'Analyst Scorecard (last 6 months)',
        'Highlighted rows indicate metrics that differ notably from team averages.',
        1,
        2,
        TRUE,
        now(),
        now()
    FROM dashboard_row
    WHERE NOT EXISTS (
        SELECT 1 FROM dashboard_sections s
        WHERE s.dashboard_id = dashboard_row.id
          AND COALESCE(s.title, '') = 'Analyst Scorecard (last 6 months)'
    )
    RETURNING id, dashboard_id
),
scorecard_section_row AS (
    SELECT id, dashboard_id FROM scorecard_section
    UNION ALL
    SELECT s.id, s.dashboard_id
    FROM dashboard_sections s
    JOIN dashboard_row d ON d.id = s.dashboard_id
    WHERE COALESCE(s.title, '') = 'Analyst Scorecard (last 6 months)'
),
mix_section AS (
    INSERT INTO dashboard_sections (
        dashboard_id, title, description, layout_columns, sort_order, is_active, created_at, updated_at
    )
    SELECT
        dashboard_row.id,
        'Analyst Mix',
        NULL,
        2,
        3,
        TRUE,
        now(),
        now()
    FROM dashboard_row
    WHERE NOT EXISTS (
        SELECT 1 FROM dashboard_sections s
        WHERE s.dashboard_id = dashboard_row.id
          AND COALESCE(s.title, '') = 'Analyst Mix'
    )
    RETURNING id, dashboard_id
),
mix_section_row AS (
    SELECT id, dashboard_id FROM mix_section
    UNION ALL
    SELECT s.id, s.dashboard_id
    FROM dashboard_sections s
    JOIN dashboard_row d ON d.id = s.dashboard_id
    WHERE COALESCE(s.title, '') = 'Analyst Mix'
),
handoff_section AS (
    INSERT INTO dashboard_sections (
        dashboard_id, title, description, layout_columns, sort_order, is_active, created_at, updated_at
    )
    SELECT
        dashboard_row.id,
        'Avg Handoffs per Ticket (High Severity)',
        'Average number of times a high-severity ticket was passed between inHANCE analysts before closure.',
        1,
        4,
        TRUE,
        now(),
        now()
    FROM dashboard_row
    WHERE NOT EXISTS (
        SELECT 1 FROM dashboard_sections s
        WHERE s.dashboard_id = dashboard_row.id
          AND COALESCE(s.title, '') = 'Avg Handoffs per Ticket (High Severity)'
    )
    RETURNING id, dashboard_id
),
handoff_section_row AS (
    SELECT id, dashboard_id FROM handoff_section
    UNION ALL
    SELECT s.id, s.dashboard_id
    FROM dashboard_sections s
    JOIN dashboard_row d ON d.id = s.dashboard_id
    WHERE COALESCE(s.title, '') = 'Avg Handoffs per Ticket (High Severity)'
),
closures_section AS (
    INSERT INTO dashboard_sections (
        dashboard_id, title, description, layout_columns, sort_order, is_active, created_at, updated_at
    )
    SELECT
        dashboard_row.id,
        'Monthly Closures by Analyst',
        NULL,
        1,
        5,
        TRUE,
        now(),
        now()
    FROM dashboard_row
    WHERE NOT EXISTS (
        SELECT 1 FROM dashboard_sections s
        WHERE s.dashboard_id = dashboard_row.id
          AND COALESCE(s.title, '') = 'Monthly Closures by Analyst'
    )
    RETURNING id, dashboard_id
),
closures_section_row AS (
    SELECT id, dashboard_id FROM closures_section
    UNION ALL
    SELECT s.id, s.dashboard_id
    FROM dashboard_sections s
    JOIN dashboard_row d ON d.id = s.dashboard_id
    WHERE COALESCE(s.title, '') = 'Monthly Closures by Analyst'
),
created_section AS (
    INSERT INTO dashboard_sections (
        dashboard_id, title, description, layout_columns, sort_order, is_active, created_at, updated_at
    )
    SELECT
        dashboard_row.id,
        'Tickets Created by Month',
        'Created and closed ticket counts by month for the CS group.',
        1,
        6,
        TRUE,
        now(),
        now()
    FROM dashboard_row
    WHERE NOT EXISTS (
        SELECT 1 FROM dashboard_sections s
        WHERE s.dashboard_id = dashboard_row.id
          AND COALESCE(s.title, '') = 'Tickets Created by Month'
    )
    RETURNING id, dashboard_id
),
created_section_row AS (
    SELECT id, dashboard_id FROM created_section
    UNION ALL
    SELECT s.id, s.dashboard_id
    FROM dashboard_sections s
    JOIN dashboard_row d ON d.id = s.dashboard_id
    WHERE COALESCE(s.title, '') = 'Tickets Created by Month'
)
INSERT INTO dashboard_widgets (
    section_id, widget_type, title, query_key, query_params_json,
    display_config_json, sort_order, is_active, created_at, updated_at
)
SELECT
    sr.id,
    'stat_row',
    'CS Overview KPIs',
    'ops_overview_kpis',
    '{"months": 6, "group_name": "Customer Support (CS)"}'::jsonb,
    '{
      "items": [
        {
          "field": "current_month_avg_days_to_close",
          "title": "Avg Days to Close",
          "subtitle": "Current Month",
          "icon": "tabler:clock-hour-4",
          "color": "blue",
          "format": "fixed1"
        },
        {
          "field": "six_month_avg_days_to_close",
          "title": "Avg Days to Close",
          "subtitle": "Past 6 Months",
          "icon": "tabler:clock-hour-4",
          "color": "blue",
          "format": "fixed1"
        },
        {
          "field": "backlog_jan1",
          "title": "CS Backlog",
          "subtitle": "Jan 1",
          "icon": "tabler:inbox",
          "color": "orange",
          "format": "int"
        },
        {
          "field": "backlog_now",
          "title": "CS Backlog",
          "subtitle": "Now",
          "icon": "tabler:inbox",
          "color": "orange",
          "format": "int",
          "badge_field": "backlog_delta",
          "badge_direction_field": "backlog_delta_direction",
          "badge_color_down": "green",
          "badge_color_up": "red"
        }
      ]
    }'::jsonb,
    0,
    TRUE,
    now(),
    now()
FROM summary_section_row sr
WHERE NOT EXISTS (
    SELECT 1 FROM dashboard_widgets w
    WHERE w.section_id = sr.id
      AND w.query_key = 'ops_overview_kpis'
)
UNION ALL
SELECT
    mir.id,
    'grid',
    'Most Improved Customers',
    'ops_most_improved_customers',
    '{"months": 3, "top_n": 5, "group_name": "Customer Support (CS)"}'::jsonb,
    '{
      "height": "320px",
      "columns": [
        {"field": "customer", "headerName": "Customer", "minWidth": 220, "flex": 1.8},
        {"field": "open_then", "headerName": "3 Months Ago", "minWidth": 120, "type": "numericColumn"},
        {"field": "open_now", "headerName": "Now", "minWidth": 100, "type": "numericColumn"},
        {"field": "reduction", "headerName": "Reduction", "minWidth": 110, "type": "numericColumn"}
      ],
      "grid_options": {
        "pagination": false,
        "animateRows": true,
        "domLayout": "normal"
      }
    }'::jsonb,
    0,
    TRUE,
    now(),
    now()
FROM most_improved_section_row mir
WHERE NOT EXISTS (
    SELECT 1 FROM dashboard_widgets w
    WHERE w.section_id = mir.id
      AND w.query_key = 'ops_most_improved_customers'
)
UNION ALL
SELECT
    scr.id,
    'grid',
    'Analyst Scorecard',
    'ops_analyst_scorecard',
    '{"months": 6}'::jsonb,
    '{
      "height": "520px",
      "columns": [
        {"field": "assignee", "headerName": "Analyst", "minWidth": 150, "flex": 1.5, "pinned": "left"},
        {"field": "tickets_closed", "headerName": "Closed", "minWidth": 80, "flex": 1, "type": "numericColumn"},
        {"field": "avg_days_open", "headerName": "Avg Days Open", "minWidth": 100, "flex": 1, "type": "numericColumn"},
        {"field": "pct_high_severity", "headerName": "High Sev %", "minWidth": 90, "flex": 1, "type": "numericColumn"},
        {"field": "pct_technical", "headerName": "Technical %", "minWidth": 90, "flex": 1, "type": "numericColumn"},
        {"field": "pct_scheduling", "headerName": "Scheduling %", "minWidth": 90, "flex": 1, "type": "numericColumn"},
        {"field": "high_priority_count", "headerName": "High Pri", "minWidth": 80, "flex": 1, "type": "numericColumn"},
        {"field": "frustrated_count", "headerName": "Frustrated", "minWidth": 80, "flex": 1, "type": "numericColumn"}
      ],
      "grid_options": {
        "animateRows": true,
        "pagination": false,
        "domLayout": "normal"
      }
    }'::jsonb,
    0,
    TRUE,
    now(),
    now()
FROM scorecard_section_row scr
WHERE NOT EXISTS (
    SELECT 1 FROM dashboard_widgets w
    WHERE w.section_id = scr.id
      AND w.query_key = 'ops_analyst_scorecard'
)
UNION ALL
SELECT
    msr.id,
    'chart',
    'Technical vs Scheduling Work',
    'analyst_action_profile',
    '{"months": 6}'::jsonb,
    '{
      "chart_type": "horizontal_bar",
      "x": "assignee",
      "y": ["pct_technical", "pct_scheduling"],
      "series_labels": {
        "pct_technical": "Technical Work %",
        "pct_scheduling": "Scheduling %"
      },
      "barmode": "group",
      "sort_by": "pct_scheduling",
      "sort_dir": "desc",
      "height": 420,
      "xaxis_title": "% of Own Actions"
    }'::jsonb,
    0,
    TRUE,
    now(),
    now()
FROM mix_section_row msr
WHERE NOT EXISTS (
    SELECT 1 FROM dashboard_widgets w
    WHERE w.section_id = msr.id
      AND w.query_key = 'analyst_action_profile'
)
UNION ALL
SELECT
    msr.id,
    'chart',
    'High-Severity Closure Share',
    'analyst_severity_profile',
    '{"months": 6}'::jsonb,
    '{
      "chart_type": "horizontal_bar",
      "x": "assignee",
      "y": "pct_high_severity",
      "sort_by": "pct_high_severity",
      "sort_dir": "asc",
      "height": 420,
      "xaxis_title": "High-Severity % of Closures"
    }'::jsonb,
    1,
    TRUE,
    now(),
    now()
FROM mix_section_row msr
WHERE NOT EXISTS (
    SELECT 1 FROM dashboard_widgets w
    WHERE w.section_id = msr.id
      AND w.query_key = 'analyst_severity_profile'
)
UNION ALL
SELECT
    hsr.id,
    'chart',
    'Avg Handoffs per Ticket (High Severity)',
    'analyst_reassignment_profile',
    '{"months": 6}'::jsonb,
    '{
      "chart_type": "horizontal_bar",
      "x": "assignee",
      "y": "avg_handoffs",
      "color": "severity",
      "barmode": "group",
      "sort_by": "avg_handoffs",
      "sort_dir": "desc",
      "height": 420,
      "xaxis_title": "Avg Handoffs per Ticket"
    }'::jsonb,
    0,
    TRUE,
    now(),
    now()
FROM handoff_section_row hsr
WHERE NOT EXISTS (
    SELECT 1 FROM dashboard_widgets w
    WHERE w.section_id = hsr.id
      AND w.query_key = 'analyst_reassignment_profile'
)
UNION ALL
SELECT
    csr.id,
    'chart',
    'Monthly Closures by Analyst',
    'ops_analyst_monthly_closures',
    '{"months": 12, "top_n": 10}'::jsonb,
    '{
      "chart_type": "line",
      "x": "month",
      "y": "closed_count",
      "color": "assignee",
      "height": 420,
      "yaxis_title": "Tickets Closed"
    }'::jsonb,
    0,
    TRUE,
    now(),
    now()
FROM closures_section_row csr
WHERE NOT EXISTS (
    SELECT 1 FROM dashboard_widgets w
    WHERE w.section_id = csr.id
      AND w.query_key = 'ops_analyst_monthly_closures'
)
UNION ALL
SELECT
    cds.id,
    'chart',
    'Tickets Created by Month',
    'monthly_created_vs_closed',
    '{"months": 12}'::jsonb,
    '{
      "chart_type": "bar",
      "x": "month",
      "y": "ticket_count",
      "color": "series",
      "barmode": "group",
      "height": 360,
      "yaxis_title": "Count"
    }'::jsonb,
    0,
    TRUE,
    now(),
    now()
FROM created_section_row cds
WHERE NOT EXISTS (
    SELECT 1 FROM dashboard_widgets w
    WHERE w.section_id = cds.id
      AND w.query_key = 'monthly_created_vs_closed'
);
