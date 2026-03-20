"""
Azure Blob Storage helper for persisting CSV pipeline results.

Uploads output CSVs and job state JSON to blob storage so they survive
container restarts.  Falls back gracefully when no connection string is
configured (local-dev mode keeps everything on local disk only).

Requires env var:
    AZURE_STORAGE_CONNECTION_STRING  — full connection string for the
                                       storage account.

Container: csv-pipeline-results  (created automatically if missing).
Blob layout:
    {job_id}/pass1_results.csv
    {job_id}/pass3_results.csv
    {job_id}/pass4_results.csv
    {job_id}/job_state.json
"""

import json
import logging
import os
from typing import Optional

log = logging.getLogger(__name__)

_CONTAINER = "csv-pipeline-results"
_conn_str = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "")


def _get_client():
    """Return a ContainerClient, or None if blob storage is not configured."""
    if not _conn_str:
        return None
    try:
        from azure.storage.blob import ContainerClient
        client = ContainerClient.from_connection_string(_conn_str, _CONTAINER)
        # Ensure the container exists (no-op if already present)
        try:
            client.create_container()
        except Exception:
            pass  # container already exists
        return client
    except Exception as exc:
        log.warning("Blob storage unavailable: %s", exc)
        return None


def upload_file(job_id: str, filename: str, local_path: str) -> bool:
    """Upload a local file to blob storage.  Returns True on success."""
    client = _get_client()
    if client is None:
        return False
    blob_name = f"{job_id}/{filename}"
    try:
        with open(local_path, "rb") as f:
            client.upload_blob(blob_name, f, overwrite=True)
        log.info("Uploaded %s → %s", local_path, blob_name)
        return True
    except Exception as exc:
        log.warning("Blob upload failed for %s: %s", blob_name, exc)
        return False


def upload_json(job_id: str, filename: str, data: dict) -> bool:
    """Upload a dict as JSON to blob storage.  Returns True on success."""
    client = _get_client()
    if client is None:
        return False
    blob_name = f"{job_id}/{filename}"
    try:
        payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
        client.upload_blob(blob_name, payload, overwrite=True)
        return True
    except Exception as exc:
        log.warning("Blob upload failed for %s: %s", blob_name, exc)
        return False


def download_file(job_id: str, filename: str, local_path: str) -> bool:
    """Download a blob to a local path.  Returns True on success."""
    client = _get_client()
    if client is None:
        return False
    blob_name = f"{job_id}/{filename}"
    try:
        blob_data = client.download_blob(blob_name)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "wb") as f:
            blob_data.readinto(f)
        log.info("Downloaded %s → %s", blob_name, local_path)
        return True
    except Exception as exc:
        log.debug("Blob download failed for %s: %s", blob_name, exc)
        return False


def download_json(job_id: str, filename: str) -> Optional[dict]:
    """Download and parse a JSON blob.  Returns None on failure."""
    client = _get_client()
    if client is None:
        return None
    blob_name = f"{job_id}/{filename}"
    try:
        blob_data = client.download_blob(blob_name)
        return json.loads(blob_data.readall())
    except Exception:
        return None


def list_job_ids() -> list[str]:
    """Return all job IDs that have a persisted job_state.json in blob."""
    client = _get_client()
    if client is None:
        return []
    try:
        ids = set()
        for blob in client.list_blobs(name_starts_with=""):
            parts = blob.name.split("/")
            if len(parts) == 2 and parts[1] == "job_state.json":
                ids.add(parts[0])
        return sorted(ids)
    except Exception as exc:
        log.warning("Blob list failed: %s", exc)
        return []


def list_blobs(prefix: str = "") -> list[dict]:
    """Return metadata for all blobs matching *prefix*.

    Each dict has keys: name, size, last_modified.
    """
    client = _get_client()
    if client is None:
        return []
    try:
        result = []
        for blob in client.list_blobs(name_starts_with=prefix):
            result.append({
                "name": blob.name,
                "size": blob.size,
                "last_modified": blob.last_modified.isoformat() if blob.last_modified else None,
            })
        return result
    except Exception as exc:
        log.warning("Blob list failed: %s", exc)
        return []


def upload_text(job_id: str, filename: str, text: str) -> bool:
    """Upload a plain-text string to blob storage.  Returns True on success."""
    client = _get_client()
    if client is None:
        return False
    blob_name = f"{job_id}/{filename}"
    try:
        payload = text.encode("utf-8")
        client.upload_blob(blob_name, payload, overwrite=True)
        return True
    except Exception as exc:
        log.warning("Blob upload failed for %s: %s", blob_name, exc)
        return False


def download_blob_bytes(blob_name: str) -> Optional[bytes]:
    """Download raw bytes for an arbitrary blob path.  Returns None on failure."""
    client = _get_client()
    if client is None:
        return None
    try:
        return client.download_blob(blob_name).readall()
    except Exception:
        return None
