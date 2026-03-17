"""Config viewer page — read-only display of pipeline configuration."""

import dash_mantine_components as dmc
from dash_iconify import DashIconify

import config
import data


def _cfg_item(label, value, masked=False):
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

    return dmc.Stack(
        [
            dmc.Title("Pipeline Configuration", order=2),
            dmc.Text("Read-only view of current settings from config.py and environment.", size="sm", c="dimmed"),

            dmc.SimpleGrid(
                cols={"base": 1, "lg": 2},
                children=[
                    _cfg_card("TeamSupport API", "tabler:api", [
                        _cfg_item("TS_BASE", config.TS_BASE),
                        _cfg_item("TS_KEY", config.TS_KEY, masked=True),
                        _cfg_item("TS_USER_ID", config.TS_USER_ID),
                    ]),

                    _cfg_card("Matcha LLM", "tabler:brain", [
                        _cfg_item("MATCHA_URL", config.MATCHA_URL),
                        _cfg_item("MATCHA_API_KEY", config.MATCHA_API_KEY, masked=True),
                        _cfg_item("MATCHA_MISSION_ID", config.MATCHA_MISSION_ID),
                    ]),

                    _cfg_card("Pull Limits", "tabler:adjustments", [
                        _cfg_item("MAX_TICKETS", config.MAX_TICKETS),
                        _cfg_item("TARGET_TICKETS", ", ".join(config.TARGET_TICKETS) or "(none)"),
                    ]),

                    _cfg_card("Stage Toggles", "tabler:toggle-right", [
                        _cfg_item("RUN_SENTIMENT", config.RUN_SENTIMENT),
                        _cfg_item("RUN_PRIORITY", config.RUN_PRIORITY),
                        _cfg_item("RUN_COMPLEXITY", config.RUN_COMPLEXITY),
                    ]),

                    _cfg_card("Write-Back & Output", "tabler:upload", [
                        _cfg_item("TS_WRITEBACK", config.TS_WRITEBACK),
                        _cfg_item("SKIP_OUTPUT_FILES", config.SKIP_OUTPUT_FILES),
                        _cfg_item("OUTPUT_DIR", config.OUTPUT_DIR),
                    ]),

                    _cfg_card("Database", "tabler:database", [
                        _cfg_item("DATABASE_URL", config.DATABASE_URL, masked=True),
                        _cfg_item("DATABASE_SCHEMA", config.DATABASE_SCHEMA),
                    ]),

                    _cfg_card("Incremental Sync", "tabler:refresh", [
                        _cfg_item("SAFETY_BUFFER_MINUTES", config.SAFETY_BUFFER_MINUTES),
                        _cfg_item("INITIAL_BACKFILL_DAYS", config.INITIAL_BACKFILL_DAYS),
                    ]),

                    _cfg_card("Logging", "tabler:file-text", [
                        _cfg_item("LOG_TO_FILE", config.LOG_TO_FILE),
                        _cfg_item("LOG_API_CALLS", config.LOG_API_CALLS),
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
