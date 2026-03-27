"""
Pull Azure DevOps update history for non-PBI work items into the database.

Usage:
    python run_import_work_item_updates.py                           # default projects
    python run_import_work_item_updates.py Impresa                   # single project
    python run_import_work_item_updates.py Impresa PowerManager      # specific projects
"""

import sys
import time
from datetime import datetime, timezone

from azdevops_client import get_updates
from db import get_conn, put_conn, migrate, upsert_work_item_updates

DEFAULT_PROJECTS = ["Impresa", "PowerManager", "Datawest"]
EXCLUDED_TYPE = "Product Backlog Item"
MAX_UPDATES_PER_ITEM = 200


def _load_target_work_items(projects: list) -> list:
    """Return (work_item_id, project, work_item_type) for non-PBI items."""
    placeholders = ",".join(["%s"] * len(projects))
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT work_item_id, project, work_item_type
                FROM work_items
                WHERE project IN ({placeholders})
                  AND work_item_type <> %s
                ORDER BY project, work_item_id
            """, (*projects, EXCLUDED_TYPE))
            return cur.fetchall()
    finally:
        put_conn(conn)


def main() -> None:
    projects = sys.argv[1:] if len(sys.argv) > 1 else DEFAULT_PROJECTS
    print(f"[work-item-updates] Starting for projects: {', '.join(projects)}", flush=True)

    migrate()

    items = _load_target_work_items(projects)
    print(f"[work-item-updates] {len(items)} non-PBI work items to process", flush=True)

    now = datetime.now(timezone.utc)
    total_updates = 0
    errors = 0

    for i, (wid, proj, wtype) in enumerate(items, 1):
        try:
            updates = get_updates(wid, top=MAX_UPDATES_PER_ITEM)
            if not isinstance(updates, list):
                updates = updates.get("value", [])
            n = upsert_work_item_updates(wid, updates, now=now)
            total_updates += n
            if i % 25 == 0 or i == len(items):
                print(f"[work-item-updates] Progress: {i}/{len(items)} items, "
                      f"{total_updates} updates so far", flush=True)
        except Exception as exc:
            errors += 1
            print(f"[work-item-updates] ERROR on {wid} ({proj}/{wtype}): {exc}", flush=True)

    print(f"[work-item-updates] Done — {total_updates} updates upserted "
          f"across {len(items)} work items ({errors} errors).", flush=True)


if __name__ == "__main__":
    main()
