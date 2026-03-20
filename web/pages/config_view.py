"""Config editor page — view and edit pipeline configuration."""

import os
import re

import dash_mantine_components as dmc
from dash import callback, Input, Output, State, no_update, ctx
from dash_iconify import DashIconify

import requests

import config
import data

# Path to the config.py file on disk
_CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "config.py")

# Matcha models endpoint
_LLMS_URL = "https://matcha.harriscomputer.com/rest/api/v1/llms"


def _fetch_matcha_models():
    """Fetch available LLM models from Matcha. Returns list of {id, name}."""
    try:
        headers = {
            "Content-Type": "application/json",
            "MATCHA-API-KEY": config.MATCHA_API_KEY,
        }
        resp = requests.get(
            _LLMS_URL, headers=headers,
            params={"select": "id,name"}, timeout=10,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return []


# ── Editable field helpers ───────────────────────────────────────────

def _edit_item(field_id, label, value, masked=False, placeholder=""):
    """Return a label + TextInput row."""
    display = "" if value is None else str(value)
    if masked:
        input_el = dmc.PasswordInput(
            id=field_id,
            value=display,
            placeholder=placeholder,
            style={"flex": 1},
        )
    else:
        input_el = dmc.TextInput(
            id=field_id,
            value=display,
            placeholder=placeholder,
            style={"flex": 1},
        )
    return dmc.Group(
        [
            dmc.Text(label, size="sm", fw=600, w=220),
            input_el,
        ],
        gap="sm",
        wrap="nowrap",
    )


def _toggle_item(field_id, label, value):
    """Return a label + Switch row for boolean fields."""
    return dmc.Group(
        [
            dmc.Text(label, size="sm", fw=600, w=220),
            dmc.Switch(id=field_id, checked=bool(value), size="md"),
        ],
        gap="sm",
        wrap="nowrap",
    )


def _readonly_item(label, value, masked=False):
    display = "••••••••" if masked else str(value)
    return dmc.Group(
        [
            dmc.Text(label, size="sm", fw=600, w=220),
            dmc.Code(display, style={"flex": 1}),
        ],
        gap="sm",
        wrap="nowrap",
    )


def _cfg_card(title, icon, items):
    return dmc.Paper(
        [
            dmc.Group(
                [
                    dmc.ThemeIcon(
                        DashIconify(icon=icon, width=20),
                        variant="light", color="blue", size=36, radius="md",
                    ),
                    dmc.Text(title, fw=700, size="md"),
                ],
                gap="sm", mb="sm",
            ),
            dmc.Stack(items, gap="xs"),
        ],
        withBorder=True, p="md", radius="md", shadow="sm",
    )


def config_layout():
    # Sync status
    sync_rows = []
    try:
        sync_rows = data.get_sync_status()
    except Exception:
        pass

    sync_items = []
    for s in sync_rows:
        sync_items.append(
            dmc.Group([
                dmc.Badge(s["source_name"], variant="light"),
                dmc.Text(f"Last sync: {s.get('last_successful_sync_at', '—')}", size="xs"),
                dmc.Badge(s.get("last_status", "—"),
                          color="green" if s.get("last_status") == "success" else "red",
                          variant="light", size="sm"),
            ], gap="sm")
        )

    response_llm_val = "" if config.MATCHA_RESPONSE_LLM is None else str(config.MATCHA_RESPONSE_LLM)

    # Fetch available models for the dropdown
    models = _fetch_matcha_models()
    model_options = [{"value": "", "label": "(Mission default)"}] + [
        {"value": str(m["id"]), "label": f"{m['name']}  (id: {m['id']})"}
        for m in sorted(models, key=lambda m: m.get("name", ""))
    ]

    return dmc.Stack(
        [
            dmc.Title("Pipeline Configuration", order=2),
            dmc.Text("Edit settings below and click Save. Changes are written to config.py.", size="sm", c="dimmed"),

            # Save button + feedback
            dmc.Group([
                dmc.Button(
                    "Save Configuration",
                    id="config-save-btn",
                    leftSection=DashIconify(icon="tabler:device-floppy", width=18),
                    color="blue",
                ),
                dmc.Text(id="config-save-feedback", size="sm", c="green"),
            ], gap="md"),

            dmc.SimpleGrid(
                cols={"base": 1, "lg": 2},
                children=[
                    _cfg_card("TeamSupport API", "tabler:api", [
                        _edit_item("cfg-ts-base", "TS_BASE", config.TS_BASE),
                        _edit_item("cfg-ts-key", "TS_KEY", config.TS_KEY, masked=True),
                        _edit_item("cfg-ts-user-id", "TS_USER_ID", config.TS_USER_ID),
                    ]),

                    _cfg_card("Matcha LLM", "tabler:brain", [
                        _edit_item("cfg-matcha-url", "MATCHA_URL", config.MATCHA_URL),
                        _edit_item("cfg-matcha-api-key", "MATCHA_API_KEY", config.MATCHA_API_KEY, masked=True),
                        _edit_item("cfg-matcha-mission-id", "MATCHA_MISSION_ID", config.MATCHA_MISSION_ID),
                        dmc.Group(
                            [
                                dmc.Text("MATCHA_RESPONSE_LLM", size="sm", fw=600, w=220),
                                dmc.Select(
                                    id="cfg-matcha-response-llm",
                                    data=model_options,
                                    value=response_llm_val,
                                    placeholder="Select model or leave blank for mission default",
                                    clearable=True,
                                    searchable=True,
                                    style={"flex": 1},
                                ),
                            ],
                            gap="sm",
                            wrap="nowrap",
                        ),
                    ]),

                    _cfg_card("Pull Limits", "tabler:adjustments", [
                        _edit_item("cfg-max-tickets", "MAX_TICKETS", config.MAX_TICKETS),
                        _edit_item("cfg-target-tickets", "TARGET_TICKETS",
                                   ", ".join(config.TARGET_TICKETS) if config.TARGET_TICKETS else "",
                                   placeholder="Comma-separated ticket numbers"),
                    ]),

                    _cfg_card("Stage Toggles", "tabler:toggle-right", [
                        _toggle_item("cfg-run-sentiment", "RUN_SENTIMENT", config.RUN_SENTIMENT),
                        _toggle_item("cfg-run-priority", "RUN_PRIORITY", config.RUN_PRIORITY),
                        _toggle_item("cfg-run-complexity", "RUN_COMPLEXITY", config.RUN_COMPLEXITY),
                        _toggle_item("cfg-force-enrichment", "FORCE_ENRICHMENT", config.FORCE_ENRICHMENT),
                    ]),

                    _cfg_card("Write-Back & Output", "tabler:upload", [
                        _toggle_item("cfg-ts-writeback", "TS_WRITEBACK", config.TS_WRITEBACK),
                        _toggle_item("cfg-skip-output-files", "SKIP_OUTPUT_FILES", config.SKIP_OUTPUT_FILES),
                        _edit_item("cfg-output-dir", "OUTPUT_DIR", config.OUTPUT_DIR),
                    ]),

                    _cfg_card("Database", "tabler:database", [
                        _edit_item("cfg-database-url", "DATABASE_URL", config.DATABASE_URL, masked=True),
                        _edit_item("cfg-database-schema", "DATABASE_SCHEMA", config.DATABASE_SCHEMA),
                    ]),

                    _cfg_card("Incremental Sync", "tabler:refresh", [
                        _edit_item("cfg-safety-buffer", "SAFETY_BUFFER_MINUTES", config.SAFETY_BUFFER_MINUTES),
                        _edit_item("cfg-initial-backfill", "INITIAL_BACKFILL_DAYS", config.INITIAL_BACKFILL_DAYS),
                    ]),

                    _cfg_card("Logging", "tabler:file-text", [
                        _toggle_item("cfg-log-to-file", "LOG_TO_FILE", config.LOG_TO_FILE),
                        _toggle_item("cfg-log-api-calls", "LOG_API_CALLS", config.LOG_API_CALLS),
                    ]),
                ],
            ),

            # Sync status section
            dmc.Paper(
                [
                    dmc.Group(
                        [
                            dmc.ThemeIcon(
                                DashIconify(icon="tabler:cloud-download", width=20),
                                variant="light", color="teal", size=36, radius="md",
                            ),
                            dmc.Text("Sync Status", fw=700, size="md"),
                        ],
                        gap="sm", mb="sm",
                    ),
                    dmc.Stack(sync_items, gap="xs") if sync_items
                    else dmc.Text("No sync state recorded yet.", c="dimmed", size="sm"),
                ],
                withBorder=True, p="md", radius="md", shadow="sm",
            ),
        ],
        gap="md",
    )


# ── Save callback ────────────────────────────────────────────────────

def _update_config_line(content, var_name, new_value_expr):
    """Replace a variable assignment line in config.py content."""
    pattern = re.compile(
        r'^(' + re.escape(var_name) + r'\s*=\s*).*$',
        re.MULTILINE,
    )
    return pattern.sub(r'\g<1>' + new_value_expr, content, count=1)


@callback(
    Output("config-save-feedback", "children"),
    Input("config-save-btn", "n_clicks"),
    # Text inputs
    State("cfg-ts-base", "value"),
    State("cfg-ts-key", "value"),
    State("cfg-ts-user-id", "value"),
    State("cfg-matcha-url", "value"),
    State("cfg-matcha-api-key", "value"),
    State("cfg-matcha-mission-id", "value"),
    State("cfg-matcha-response-llm", "value"),
    State("cfg-max-tickets", "value"),
    State("cfg-target-tickets", "value"),
    State("cfg-output-dir", "value"),
    State("cfg-database-url", "value"),
    State("cfg-database-schema", "value"),
    State("cfg-safety-buffer", "value"),
    State("cfg-initial-backfill", "value"),
    # Toggle inputs
    State("cfg-run-sentiment", "checked"),
    State("cfg-run-priority", "checked"),
    State("cfg-run-complexity", "checked"),
    State("cfg-force-enrichment", "checked"),
    State("cfg-ts-writeback", "checked"),
    State("cfg-skip-output-files", "checked"),
    State("cfg-log-to-file", "checked"),
    State("cfg-log-api-calls", "checked"),
    prevent_initial_call=True,
)
def save_config(
    n_clicks,
    ts_base, ts_key, ts_user_id,
    matcha_url, matcha_api_key, matcha_mission_id, matcha_response_llm,
    max_tickets, target_tickets,
    output_dir,
    database_url, database_schema,
    safety_buffer, initial_backfill,
    run_sentiment, run_priority, run_complexity, force_enrichment,
    ts_writeback, skip_output_files,
    log_to_file, log_api_calls,
):
    if not n_clicks:
        return no_update

    try:
        with open(_CONFIG_PATH, "r") as f:
            content = f.read()

        # String defaults (written into os.getenv default)
        str_fields = {
            "TS_BASE": ts_base,
            "TS_KEY": ts_key,
            "TS_USER_ID": ts_user_id,
            "MATCHA_URL": matcha_url,
            "MATCHA_API_KEY": matcha_api_key,
            "MATCHA_MISSION_ID": matcha_mission_id,
            "DATABASE_URL": database_url,
            "DATABASE_SCHEMA": database_schema,
            "OUTPUT_DIR": output_dir,
        }
        for var, val in str_fields.items():
            if val is not None:
                escaped = val.replace("\\", "\\\\").replace('"', '\\"')
                if var == "OUTPUT_DIR":
                    # OUTPUT_DIR uses a different pattern — just update the default string
                    pattern = re.compile(
                        r'^(OUTPUT_DIR\s*=\s*os\.getenv\("OUTPUT_DIR",\s*).*(\))$',
                        re.MULTILINE,
                    )
                    content = pattern.sub(
                        rf'\g<1>"{escaped}")',
                        content, count=1,
                    )
                else:
                    # Pattern: VAR = os.getenv("VAR", "default")
                    pattern = re.compile(
                        r'^(' + re.escape(var) + r'\s*=\s*os\.getenv\("' + re.escape(var) + r'",\s*)"[^"]*"(\))',
                        re.MULTILINE,
                    )
                    content = pattern.sub(
                        rf'\g<1>"{escaped}")',
                        content, count=1,
                    )

        # Integer defaults
        int_fields = {
            "MAX_TICKETS": max_tickets,
            "SAFETY_BUFFER_MINUTES": safety_buffer,
            "INITIAL_BACKFILL_DAYS": initial_backfill,
        }
        for var, val in int_fields.items():
            if val is not None and str(val).strip():
                safe_val = str(int(val))
                pattern = re.compile(
                    r'^(' + re.escape(var) + r'\s*=\s*int\(os\.getenv\("' + re.escape(var) + r'",\s*)"[^"]*"(\)\))',
                    re.MULTILINE,
                )
                content = pattern.sub(
                    rf'\g<1>"{safe_val}"))',
                    content, count=1,
                )

        # Boolean toggles (written as env-var defaults "1"/"0")
        bool_fields = {
            "RUN_SENTIMENT": run_sentiment,
            "RUN_PRIORITY": run_priority,
            "RUN_COMPLEXITY": run_complexity,
            "FORCE_ENRICHMENT": force_enrichment,
            "TS_WRITEBACK": ts_writeback,
            "SKIP_OUTPUT_FILES": skip_output_files,
            "LOG_TO_FILE": log_to_file,
            "LOG_API_CALLS": log_api_calls,
        }
        for var, val in bool_fields.items():
            new_default = "1" if val else "0"
            pattern = re.compile(
                r'^(' + re.escape(var) + r'\s*=\s*os\.getenv\("' + re.escape(var) + r'",\s*)"[^"]*"',
                re.MULTILINE,
            )
            content = pattern.sub(
                rf'\g<1>"{new_default}"',
                content, count=1,
            )

        # Target tickets (env-var default)
        if target_tickets is not None:
            escaped_tt = target_tickets.strip().replace("\\", "\\\\").replace('"', '\\"')
            pattern = re.compile(
                r'^(_TARGET_RAW\s*=\s*os\.getenv\("TARGET_TICKET",\s*)"[^"]*"',
                re.MULTILINE,
            )
            content = pattern.sub(
                rf'\g<1>"{escaped_tt}"',
                content, count=1,
            )

        # MATCHA_RESPONSE_LLM — direct assignment (not env-var based)
        llm_str = (matcha_response_llm or "").strip()
        if llm_str:
            new_expr = str(int(llm_str))
        else:
            new_expr = "None"
        content = _update_config_line(content, "MATCHA_RESPONSE_LLM", new_expr)

        with open(_CONFIG_PATH, "w") as f:
            f.write(content)

        # Reload config module so the page reflects new values
        import importlib
        importlib.reload(config)

        return "✓ Configuration saved."

    except Exception as exc:
        return f"Error saving: {exc}"
