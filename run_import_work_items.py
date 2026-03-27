"""
Pull open Azure DevOps work items into the database.

Usage:
    python run_import_work_items.py                  # all configured projects
    python run_import_work_items.py Impresa           # single project
    python run_import_work_items.py Impresa Datawest  # specific projects
"""

import sys
from datetime import datetime, timezone
from typing import Any, Dict, List

from azdevops_client import query_work_items
from db import migrate, upsert_work_item

DEFAULT_PROJECTS = ["Impresa", "PowerManager", "Datawest"]

WIQL_OPEN = (
    "SELECT [System.Id], [System.Title], [System.State], "
    "[System.AssignedTo], [System.WorkItemType], "
    "[System.CreatedDate], [System.ChangedDate] "
    "FROM WorkItems "
    "WHERE [System.State] <> 'Closed' "
    "AND [System.State] <> 'Removed' "
    "ORDER BY [System.ChangedDate] DESC"
)


def _extract_identity(field: Any) -> tuple:
    """Return (displayName, uniqueName) from an identity dict, or (None, None)."""
    if isinstance(field, dict):
        return field.get("displayName"), field.get("uniqueName")
    return None, None


def _parse_item(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten an Azure DevOps work item dict into a DB-ready dict."""
    f = raw.get("fields", {})
    assigned_name, assigned_email = _extract_identity(f.get("System.AssignedTo"))
    return {
        "work_item_id": raw["id"],
        "project": f.get("System.TeamProject"),
        "work_item_type": f.get("System.WorkItemType"),
        "title": f.get("System.Title"),
        "state": f.get("System.State"),
        "reason": f.get("System.Reason"),
        "assigned_to": assigned_name,
        "assigned_to_email": assigned_email,
        "area_path": f.get("System.AreaPath"),
        "iteration_path": f.get("System.IterationPath"),
        "priority": f.get("Microsoft.VSTS.Common.Priority"),
        "severity": f.get("Microsoft.VSTS.Common.Severity"),
        "created_date": f.get("System.CreatedDate"),
        "changed_date": f.get("System.ChangedDate"),
        "state_change_date": f.get("Microsoft.VSTS.Common.StateChangeDate"),
        "activated_date": f.get("Microsoft.VSTS.Common.ActivatedDate"),
        "board_column": f.get("System.BoardColumn"),
        "tags": f.get("System.Tags"),
        "description": f.get("System.Description"),
        "completed_work": f.get("Microsoft.VSTS.Scheduling.CompletedWork"),
        "remaining_work": f.get("Microsoft.VSTS.Scheduling.RemainingWork"),
        "original_estimate": f.get("Microsoft.VSTS.Scheduling.OriginalEstimate"),
        "value_area": f.get("Microsoft.VSTS.Common.ValueArea"),
        "billable": f.get("Custom.Billable"),
        "work_type": f.get("Custom.WorkType"),
        "comment_count": f.get("System.CommentCount"),
        "rev": raw.get("rev"),
        "source_payload": raw,
    }


def pull_work_items(projects: List[str]) -> int:
    """Query open work items for the given projects and upsert into DB.

    Returns the total number of items upserted.
    """
    # Run a single WIQL query (org-wide) and filter by project client-side.
    project_set = set(projects)
    print(f"[work-items] Querying open work items for projects: {', '.join(projects)}", flush=True)

    result = query_work_items(WIQL_OPEN, project=projects[0], top=20000)
    all_items = result.get("value", [])
    print(f"[work-items] Gateway returned {len(all_items)} total items", flush=True)

    now = datetime.now(timezone.utc)
    count = 0
    for raw in all_items:
        proj = raw.get("fields", {}).get("System.TeamProject", "")
        if proj not in project_set:
            continue
        parsed = _parse_item(raw)
        upsert_work_item(parsed, now=now)
        count += 1

    return count


def main() -> None:
    projects = sys.argv[1:] if len(sys.argv) > 1 else DEFAULT_PROJECTS
    print(f"[work-items] Starting import for: {', '.join(projects)}", flush=True)

    migrate()

    total = pull_work_items(projects)

    # Print per-project breakdown
    from collections import Counter
    result = query_work_items(WIQL_OPEN, project=projects[0], top=20000)
    project_set = set(projects)
    counts = Counter()
    for item in result.get("value", []):
        p = item.get("fields", {}).get("System.TeamProject", "")
        if p in project_set:
            counts[p] += 1
    for p in projects:
        print(f"  {p}: {counts.get(p, 0)} items", flush=True)

    print(f"[work-items] Done — {total} work items upserted.", flush=True)


if __name__ == "__main__":
    main()
