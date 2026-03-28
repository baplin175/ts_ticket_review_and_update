"""
Pull Azure DevOps comments for non-PBI work items into the database.

Usage:
    python run_import_work_item_comments.py                          # default projects
    python run_import_work_item_comments.py Impresa                  # single project
    python run_import_work_item_comments.py Impresa PowerManager     # specific projects
"""

import sys
from datetime import datetime, timezone

from azdevops_client import get_comments
from db import get_conn, put_conn, migrate, upsert_work_item_comments

DEFAULT_PROJECTS = ["Impresa", "PowerManager", "Datawest"]
EXCLUDED_TYPE = "Product Backlog Item"
MAX_COMMENTS_PER_ITEM = 50


def _load_target_work_items(projects: list) -> list:
    """Return (work_item_id, project, work_item_type) for non-PBI items that have comments."""
    placeholders = ",".join(["%s"] * len(projects))
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT work_item_id, project, work_item_type
                FROM work_items
                WHERE project IN ({placeholders})
                  AND work_item_type <> %s
                  AND COALESCE(comment_count, 0) > 0
                ORDER BY project, work_item_id
            """, (*projects, EXCLUDED_TYPE))
            return cur.fetchall()
    finally:
        put_conn(conn)


def main() -> None:
    projects = sys.argv[1:] if len(sys.argv) > 1 else DEFAULT_PROJECTS
    print(f"[work-item-comments] Starting for projects: {', '.join(projects)}", flush=True)

    migrate()

    items = _load_target_work_items(projects)
    print(f"[work-item-comments] {len(items)} non-PBI work items with comments to process", flush=True)

    now = datetime.now(timezone.utc)
    total_comments = 0
    errors = 0

    for i, (wid, proj, wtype) in enumerate(items, 1):
        try:
            raw = get_comments(int(wid), top=MAX_COMMENTS_PER_ITEM)
            if not isinstance(raw, list):
                raw = raw.get("value", []) if isinstance(raw, dict) else []
            n = upsert_work_item_comments(wid, raw, now=now)
            total_comments += n
            if i % 25 == 0 or i == len(items):
                print(
                    f"[work-item-comments] Progress: {i}/{len(items)} items, "
                    f"{total_comments} comments so far",
                    flush=True,
                )
        except Exception as exc:
            errors += 1
            print(f"[work-item-comments] ERROR on {wid} ({proj}/{wtype}): {exc}", flush=True)

    print(
        f"[work-item-comments] Done — {total_comments} comments upserted "
        f"across {len(items)} work items ({errors} errors).",
        flush=True,
    )


if __name__ == "__main__":
    main()
