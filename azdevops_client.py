"""
Azure DevOps Gateway client — query and manage work items via the REST gateway.

The gateway is a FastAPI proxy in front of Azure DevOps, exposing endpoints for
work items, iterations, teams, repos, pipelines, tests, and wiki.  Auth is via
a static API key sent in the ``X-API-Key`` header.
"""

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

from config import AZDEVOPS_BASE, AZDEVOPS_API_KEY, AZDEVOPS_DEFAULT_PROJECT, LOG_API_CALLS, OUTPUT_DIR, SKIP_OUTPUT_FILES


# ── API call logging ────────────────────────────────────────────────

def _log_api_call(method: str, url: str, params: Any = None,
                  payload: Any = None, status: int | None = None,
                  error: str | None = None,
                  response_body: Any = None) -> None:
    if not LOG_API_CALLS or SKIP_OUTPUT_FILES:
        return
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    log_path = os.path.join(OUTPUT_DIR, "api_calls.json")
    entry: Dict[str, Any] = {
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "method": method,
        "url": url,
        "source": "azdevops",
    }
    if params:
        entry["params"] = params if isinstance(params, (dict, list)) else str(params)
    if payload:
        entry["payload"] = payload
    if status is not None:
        entry["status"] = status
    if response_body is not None:
        entry["response"] = response_body
    if error:
        entry["error"] = error
    existing: list = []
    if os.path.exists(log_path):
        try:
            with open(log_path, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except (json.JSONDecodeError, OSError):
            existing = []
    existing.append(entry)
    tmp_path = log_path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(existing, f, ensure_ascii=False, indent=2)
    os.replace(tmp_path, log_path)


# ── HTTP helpers ────────────────────────────────────────────────────


def _resolve_project(project: Optional[str]) -> Optional[str]:
    """Return *project* if given, otherwise fall back to the config default."""
    return project if project is not None else AZDEVOPS_DEFAULT_PROJECT


def _headers() -> Dict[str, str]:
    return {
        "X-API-Key": AZDEVOPS_API_KEY,
        "Accept": "application/json",
    }


def _get(path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    url = f"{AZDEVOPS_BASE}{path}"
    try:
        print(f"[azdevops] GET {url}" + (f" params={params}" if params else ""), flush=True)
        r = requests.get(url, headers=_headers(), params=params or {}, timeout=60)
        _log_api_call("GET", url, params=params, status=r.status_code)
        print(f"[azdevops] GET {url} → {r.status_code}", flush=True)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.HTTPError:
        raise
    except Exception as exc:
        print(f"[azdevops] GET {url} → ERROR: {exc}", flush=True)
        _log_api_call("GET", url, params=params, error=str(exc))
        raise


def _post(path: str, payload: Dict[str, Any]) -> Any:
    url = f"{AZDEVOPS_BASE}{path}"
    try:
        print(f"[azdevops] POST {url}", flush=True)
        hdrs = {**_headers(), "Content-Type": "application/json"}
        r = requests.post(url, headers=hdrs, json=payload, timeout=60)
        _log_api_call("POST", url, payload=payload, status=r.status_code)
        print(f"[azdevops] POST {url} → {r.status_code}", flush=True)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.HTTPError:
        raise
    except Exception as exc:
        print(f"[azdevops] POST {url} → ERROR: {exc}", flush=True)
        _log_api_call("POST", url, payload=payload, error=str(exc))
        raise


def _patch(path: str, payload: Dict[str, Any]) -> Any:
    url = f"{AZDEVOPS_BASE}{path}"
    try:
        print(f"[azdevops] PATCH {url}", flush=True)
        hdrs = {**_headers(), "Content-Type": "application/json"}
        r = requests.patch(url, headers=hdrs, json=payload, timeout=60)
        _log_api_call("PATCH", url, payload=payload, status=r.status_code)
        print(f"[azdevops] PATCH {url} → {r.status_code}", flush=True)
        r.raise_for_status()
        return r.json()
    except requests.exceptions.HTTPError:
        raise
    except Exception as exc:
        print(f"[azdevops] PATCH {url} → ERROR: {exc}", flush=True)
        _log_api_call("PATCH", url, payload=payload, error=str(exc))
        raise


# ── Work Items ──────────────────────────────────────────────────────

def get_work_item(work_item_id: int, project: Optional[str] = None) -> Dict[str, Any]:
    """Fetch a single work item by ID."""
    params = {}
    p = _resolve_project(project)
    if p:
        params["project"] = p
    return _get(f"/work-items/{work_item_id}", params=params)


def query_work_items(wiql: str, project: Optional[str] = None,
                     top: int = 200) -> Any:
    """Run a WIQL query and return matching work items."""
    payload: Dict[str, Any] = {"query": wiql, "top": top}
    p = _resolve_project(project)
    if p:
        payload["project"] = p
    return _post("/work-items/query", payload)


def create_work_item(work_item_type: str, fields: Dict[str, str],
                     project: Optional[str] = None) -> Any:
    """Create a new work item."""
    payload: Dict[str, Any] = {
        "work_item_type": work_item_type,
        "fields": fields,
    }
    p = _resolve_project(project)
    if p:
        payload["project"] = p
    return _post("/work-items/", payload)


def update_work_item(work_item_id: int, fields: Dict[str, str],
                     project: Optional[str] = None) -> Any:
    """Update fields on an existing work item."""
    payload: Dict[str, Any] = {"fields": fields}
    p = _resolve_project(project)
    if p:
        payload["project"] = p
    return _patch(f"/work-items/{work_item_id}", payload)


def add_comment(work_item_id: int, text: str,
                project: Optional[str] = None) -> Any:
    """Add a comment to a work item."""
    payload: Dict[str, Any] = {"text": text}
    p = _resolve_project(project)
    if p:
        payload["project"] = p
    return _post(f"/work-items/{work_item_id}/comments", payload)


def get_comments(work_item_id: int, project: Optional[str] = None,
                 top: int = 1) -> Any:
    """Get comments on a work item."""
    params: Dict[str, Any] = {"top": top}
    p = _resolve_project(project)
    if p:
        params["project"] = p
    return _get(f"/work-items/{work_item_id}/comments", params=params)


def get_revisions(work_item_id: int, project: Optional[str] = None,
                  top: int = 50) -> Any:
    """Get revision history of a work item."""
    params: Dict[str, Any] = {"top": top}
    p = _resolve_project(project)
    if p:
        params["project"] = p
    return _get(f"/work-items/{work_item_id}/revisions", params=params)


def get_updates(work_item_id: int, project: Optional[str] = None,
                top: int = 50) -> Any:
    """Get field-level update history of a work item."""
    params: Dict[str, Any] = {"top": top}
    p = _resolve_project(project)
    if p:
        params["project"] = p
    return _get(f"/work-items/{work_item_id}/updates", params=params)


def get_attachments(work_item_id: int,
                    project: Optional[str] = None) -> Any:
    """Get attachments on a work item."""
    params = {}
    p = _resolve_project(project)
    if p:
        params["project"] = p
    return _get(f"/work-items/{work_item_id}/attachments", params=params)


def get_relations(work_item_id: int,
                  project: Optional[str] = None) -> Any:
    """Get relations (links) on a work item."""
    params = {}
    p = _resolve_project(project)
    if p:
        params["project"] = p
    return _get(f"/work-items/{work_item_id}/relations", params=params)


# ── Iterations ──────────────────────────────────────────────────────

def list_iterations(project: Optional[str] = None, team: Optional[str] = None) -> Any:
    """List iterations (sprints) for a project."""
    params: Dict[str, Any] = {"project": _resolve_project(project)}
    if team:
        params["team"] = team
    return _get("/iterations/", params=params)


def get_current_iteration(project: Optional[str] = None,
                          team: Optional[str] = None) -> Any:
    """Get the current iteration for a project/team."""
    params: Dict[str, Any] = {"project": _resolve_project(project)}
    if team:
        params["team"] = team
    return _get("/iterations/current", params=params)


# ── Teams ───────────────────────────────────────────────────────────

def list_teams(project: Optional[str] = None) -> Any:
    """List teams in a project."""
    return _get("/teams/", params={"project": _resolve_project(project)})


def get_team_members(team: str, project: Optional[str] = None) -> Any:
    """Get members of a team."""
    return _get(f"/teams/{team}/members", params={"project": _resolve_project(project)})


def get_area_paths(project: Optional[str] = None, depth: int = 5) -> Any:
    """Get area paths for a project."""
    return _get("/teams/area-paths", params={"project": _resolve_project(project), "depth": depth})


# ── Repos ───────────────────────────────────────────────────────────

def list_repos(project: Optional[str] = None) -> Any:
    """List Git repositories in a project."""
    return _get("/repos/", params={"project": _resolve_project(project)})


def get_branches(repository_id: str, project: Optional[str] = None) -> Any:
    """Get branches for a repository."""
    return _get(f"/repos/{repository_id}/branches",
                params={"project": _resolve_project(project)})


def get_commits(repository_id: str, project: Optional[str] = None,
                branch: Optional[str] = None, top: int = 50) -> Any:
    """Get recent commits for a repository."""
    params: Dict[str, Any] = {"project": _resolve_project(project), "top": top}
    if branch:
        params["branch"] = branch
    return _get(f"/repos/{repository_id}/commits", params=params)


def list_pull_requests(repository_id: str, project: Optional[str] = None,
                       status: str = "active", top: int = 50) -> Any:
    """List pull requests for a repository."""
    return _get(f"/repos/{repository_id}/pullrequests",
                params={"project": _resolve_project(project), "status": status, "top": top})


def get_pull_request(repository_id: str, pull_request_id: int,
                     project: Optional[str] = None) -> Any:
    """Get a single pull request."""
    return _get(f"/repos/{repository_id}/pullrequests/{pull_request_id}",
                params={"project": _resolve_project(project)})


def get_pull_request_threads(repository_id: str, pull_request_id: int,
                             project: Optional[str] = None) -> Any:
    """Get review threads on a pull request."""
    return _get(f"/repos/{repository_id}/pullrequests/{pull_request_id}/threads",
                params={"project": _resolve_project(project)})


# ── Pipelines ───────────────────────────────────────────────────────

def list_pipelines(project: Optional[str] = None, top: int = 50) -> Any:
    """List pipelines in a project."""
    return _get("/pipelines/", params={"project": _resolve_project(project), "top": top})


def list_pipeline_runs(pipeline_id: int, project: Optional[str] = None,
                       top: int = 20) -> Any:
    """List runs for a pipeline."""
    return _get(f"/pipelines/{pipeline_id}/runs",
                params={"project": _resolve_project(project), "top": top})


def get_pipeline_run(pipeline_id: int, run_id: int,
                     project: Optional[str] = None) -> Any:
    """Get details of a specific pipeline run."""
    return _get(f"/pipelines/{pipeline_id}/runs/{run_id}",
                params={"project": _resolve_project(project)})


# ── Tests ───────────────────────────────────────────────────────────

def list_test_runs(project: Optional[str] = None, top: int = 20) -> Any:
    """List test runs in a project."""
    return _get("/tests/runs", params={"project": _resolve_project(project), "top": top})


def get_test_results(run_id: int, project: Optional[str] = None, top: int = 200) -> Any:
    """Get test results for a test run."""
    return _get(f"/tests/runs/{run_id}/results",
                params={"project": _resolve_project(project), "top": top})


# ── Wiki ────────────────────────────────────────────────────────────

def list_wikis(project: Optional[str] = None) -> Any:
    """List wikis in a project."""
    return _get("/wiki/", params={"project": _resolve_project(project)})


def get_wiki_page(wiki_id: str, project: Optional[str] = None,
                  path: str = "/") -> Any:
    """Get a wiki page by path."""
    return _get(f"/wiki/{wiki_id}/pages",
                params={"project": _resolve_project(project), "path": path})


# ── Convenience helpers ─────────────────────────────────────────────

def fetch_open_work_items(project: Optional[str] = None,
                          top: int = 200) -> List[Dict[str, Any]]:
    """Fetch all non-closed work items via WIQL query."""
    wiql = (
        "SELECT [System.Id], [System.Title], [System.State], "
        "[System.AssignedTo], [System.WorkItemType], "
        "[System.CreatedDate], [System.ChangedDate] "
        "FROM WorkItems "
        "WHERE [System.State] <> 'Closed' "
        "AND [System.State] <> 'Removed' "
        "ORDER BY [System.ChangedDate] DESC"
    )
    return query_work_items(wiql, project=project, top=top)


def fetch_work_items_by_state(state: str, project: Optional[str] = None,
                              top: int = 200) -> Any:
    """Fetch work items in a specific state via WIQL query."""
    wiql = (
        "SELECT [System.Id], [System.Title], [System.State], "
        "[System.AssignedTo], [System.WorkItemType] "
        "FROM WorkItems "
        f"WHERE [System.State] = '{state}' "
        "ORDER BY [System.ChangedDate] DESC"
    )
    return query_work_items(wiql, project=project, top=top)
