"""
CSV-only pipeline runner — Pass 1 → Pass 3 → Pass 4.

Pure CSV orchestration with no database dependency.
Reuses matcha_client.call_matcha and existing parsers/validators.
"""

import csv
import json
import os
import re
import sys
import threading
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Callable, Optional

csv.field_size_limit(sys.maxsize)

from matcha_client import call_matcha
from pass1_parser import parse_pass1_response, Pass1ParseError
from pass3_parser import parse_pass3_response, validate_mechanism, Pass3ParseError
from pass4.mechanism_classifier import (
    parse_pass4_response,
    validate_intervention_action,
    Pass4ParseError,
)
from prompt_store import get_prompt
from pipeline.blob_store import (
    upload_file as blob_upload_file,
    upload_json as blob_upload_json,
    upload_text as blob_upload_text,
    download_file as blob_download_file,
    download_json as blob_download_json,
    list_job_ids as blob_list_job_ids,
)

# ── Prompt helpers ───────────────────────────────────────────────────

_VIOLATION_RE = re.compile(
    r"^.*(?:Ticket\s+\d+\s+is\s+in\s+violation|"
    r"Warning:\s*Ticket\s+\d+|"
    r"SLA\s+violation\s+notice).*$",
    re.MULTILINE | re.IGNORECASE,
)


def _load_prompt(prompt_key: str) -> str:
    return get_prompt(prompt_key, allow_fallback=True)["content"]


def _strip_violation_warnings(text: str) -> str:
    return _VIOLATION_RE.sub("", text).strip()


# ── Pass 1 ───────────────────────────────────────────────────────────

PASS1_COLUMNS = [
    "ticket_id", "ticket_name", "phenomenon", "component", "operation",
    "unexpected_state", "canonical_failure", "confidence", "status", "error",
]


def run_pass1_csv(
    input_csv: str,
    output_csv: str,
    progress_cb: Optional[Callable] = None,
    inference_server: Optional[int] = None,
    log_cb: Optional[Callable] = None,
) -> int:
    """Run Pass 1 on every row of *input_csv*, write results to *output_csv*.

    Returns the number of rows processed.
    """
    template = _load_prompt("pass1_phenomenon")
    rows = _read_csv(input_csv)
    total = len(rows)

    with open(output_csv, "w", newline="", encoding="utf-8") as fout:
        writer = csv.DictWriter(fout, fieldnames=PASS1_COLUMNS)
        writer.writeheader()

        for idx, row in enumerate(rows, 1):
            ticket_id = row["ticket_id"]
            ticket_name = row.get("ticket_name", "")
            thread = row.get("full_thread_text", "")

            result = {
                "ticket_id": ticket_id,
                "ticket_name": ticket_name,
                "phenomenon": None,
                "component": None,
                "operation": None,
                "unexpected_state": None,
                "canonical_failure": None,
                "confidence": None,
                "status": "pending",
                "error": None,
            }

            try:
                cleaned = _strip_violation_warnings(thread)
                prompt = template.replace("{{ticket_name}}", ticket_name or "(no title)")
                prompt = prompt.replace("{{input_text}}", cleaned)

                raw = call_matcha(prompt, inference_server=inference_server)
                parsed, phenomenon = parse_pass1_response(raw)

                result["phenomenon"] = phenomenon
                result["component"] = parsed.get("component")
                result["operation"] = parsed.get("operation")
                result["unexpected_state"] = parsed.get("unexpected_state")
                result["canonical_failure"] = parsed.get("canonical_failure")
                result["confidence"] = parsed.get("confidence")
                result["status"] = "success"
            except (Pass1ParseError, Exception) as exc:
                result["status"] = "failed"
                result["error"] = str(exc)
                if log_cb:
                    log_cb(f"Pass1 ticket {ticket_id} FAILED: {exc}")

            writer.writerow(result)
            if progress_cb:
                progress_cb(1, idx, total)

    return total


# ── Pass 3 ───────────────────────────────────────────────────────────

PASS3_COLUMNS = [
    "ticket_id", "mechanism", "evidence", "category", "status", "error",
]


def run_pass3_csv(
    pass1_csv: str,
    input_csv: str,
    output_csv: str,
    progress_cb: Optional[Callable] = None,
    inference_server: Optional[int] = None,
    log_cb: Optional[Callable] = None,
) -> int:
    """Run Pass 3 on rows from *pass1_csv*, joining thread text from *input_csv*.

    Skips rows with LOW confidence or missing canonical_failure.
    Returns the number of rows processed.
    """
    template = _load_prompt("pass3_mechanism")

    pass1_rows = _read_csv(pass1_csv)
    thread_map = {r["ticket_id"]: r.get("full_thread_text", "") for r in _read_csv(input_csv)}
    total = len(pass1_rows)

    with open(output_csv, "w", newline="", encoding="utf-8") as fout:
        writer = csv.DictWriter(fout, fieldnames=PASS3_COLUMNS)
        writer.writeheader()

        for idx, p1 in enumerate(pass1_rows, 1):
            ticket_id = p1["ticket_id"]
            confidence = (p1.get("confidence") or "").upper()
            canonical_failure = p1.get("canonical_failure") or ""

            result = {
                "ticket_id": ticket_id,
                "mechanism": None,
                "evidence": None,
                "category": None,
                "status": "pending",
                "error": None,
            }

            # Skip if LOW confidence or no canonical failure
            if confidence == "LOW" or not canonical_failure.strip():
                result["status"] = "skipped"
                writer.writerow(result)
                if progress_cb:
                    progress_cb(3, idx, total)
                continue

            try:
                thread_context = thread_map.get(ticket_id, "")
                trimmed = thread_context[:3000] if thread_context else "(no thread context available)"

                prompt = template.replace("{{input_text}}", canonical_failure)
                prompt = prompt.replace("{{thread_context}}", trimmed)

                raw = call_matcha(prompt, inference_server=inference_server)
                parsed, mechanism = parse_pass3_response(raw)
                validate_mechanism(mechanism, canonical_failure)

                result["mechanism"] = mechanism
                result["evidence"] = parsed.get("evidence")
                result["category"] = parsed.get("category")
                result["status"] = "success"
            except (Pass3ParseError, Exception) as exc:
                result["status"] = "failed"
                result["error"] = str(exc)
                if log_cb:
                    log_cb(f"Pass3 ticket {ticket_id} FAILED: {exc}")

            writer.writerow(result)
            if progress_cb:
                progress_cb(3, idx, total)

    return total


# ── Pass 4 ───────────────────────────────────────────────────────────

PASS4_COLUMNS = [
    "ticket_id", "mechanism_class", "intervention_type", "intervention_action",
    "proposed_class", "proposed_type", "status", "error",
]


def run_pass4_csv(
    pass3_csv: str,
    output_csv: str,
    progress_cb: Optional[Callable] = None,
    inference_server: Optional[int] = None,
    log_cb: Optional[Callable] = None,
) -> int:
    """Run Pass 4 on successful rows from *pass3_csv*.

    Skips rows that did not succeed in Pass 3.
    Returns the number of rows processed.
    """
    template = _load_prompt("pass4_intervention")

    pass3_rows = _read_csv(pass3_csv)
    total = len(pass3_rows)

    with open(output_csv, "w", newline="", encoding="utf-8") as fout:
        writer = csv.DictWriter(fout, fieldnames=PASS4_COLUMNS)
        writer.writeheader()

        for idx, p3 in enumerate(pass3_rows, 1):
            ticket_id = p3["ticket_id"]
            mechanism = p3.get("mechanism") or ""
            p3_status = p3.get("status", "")

            result = {
                "ticket_id": ticket_id,
                "mechanism_class": None,
                "intervention_type": None,
                "intervention_action": None,
                "proposed_class": None,
                "proposed_type": None,
                "status": "pending",
                "error": None,
            }

            if p3_status != "success" or not mechanism.strip():
                result["status"] = "skipped"
                writer.writerow(result)
                if progress_cb:
                    progress_cb(4, idx, total)
                continue

            try:
                prompt = template.replace("{{mechanism}}", mechanism)

                raw = call_matcha(prompt, inference_server=inference_server)
                parsed, mclass, itype, iaction = parse_pass4_response(raw)
                validate_intervention_action(iaction, mechanism)

                result["mechanism_class"] = mclass
                result["intervention_type"] = itype
                result["intervention_action"] = iaction
                result["proposed_class"] = parsed.get("proposed_class")
                result["proposed_type"] = parsed.get("proposed_type")
                result["status"] = "success"
            except (Pass4ParseError, Exception) as exc:
                result["status"] = "failed"
                result["error"] = str(exc)
                if log_cb:
                    log_cb(f"Pass4 ticket {ticket_id} FAILED: {exc}")

            writer.writerow(result)
            if progress_cb:
                progress_cb(4, idx, total)

    return total


# ── Full pipeline ────────────────────────────────────────────────────

def run_full_pipeline(
    input_csv: str,
    output_dir: str,
    progress_cb: Optional[Callable] = None,
    inference_server: Optional[int] = None,
    job_id: Optional[str] = None,
    log_cb: Optional[Callable] = None,
) -> dict:
    """Orchestrate Pass 1 → 3 → 4, writing CSVs to *output_dir*.

    *progress_cb(pass_num, processed, total)* is called after each row.
    If *job_id* is provided, output CSVs are uploaded to blob storage
    after each pass completes.

    Returns dict with paths to the three output CSVs.
    """
    os.makedirs(output_dir, exist_ok=True)

    p1_csv = os.path.join(output_dir, "pass1_results.csv")
    p3_csv = os.path.join(output_dir, "pass3_results.csv")
    p4_csv = os.path.join(output_dir, "pass4_results.csv")

    run_pass1_csv(input_csv, p1_csv, progress_cb, inference_server, log_cb=log_cb)
    if job_id:
        blob_upload_file(job_id, "pass1_results.csv", p1_csv)

    run_pass3_csv(p1_csv, input_csv, p3_csv, progress_cb, inference_server, log_cb=log_cb)
    if job_id:
        blob_upload_file(job_id, "pass3_results.csv", p3_csv)

    run_pass4_csv(p3_csv, p4_csv, progress_cb, inference_server, log_cb=log_cb)
    if job_id:
        blob_upload_file(job_id, "pass4_results.csv", p4_csv)

    return {"pass1": p1_csv, "pass3": p3_csv, "pass4": p4_csv}


# ── CSV helper ───────────────────────────────────────────────────────

def _read_csv(path: str) -> list[dict]:
    with open(path, "r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


# ── Job manager ──────────────────────────────────────────────────────

import tempfile

_JOBS_DIR = os.path.join(tempfile.gettempdir(), "csv_pipeline_jobs")
os.makedirs(_JOBS_DIR, exist_ok=True)


@dataclass
class JobState:
    id: str
    status: str = "queued"          # queued | running | complete | failed
    current_pass: int = 0           # 1, 3, or 4
    processed: int = 0
    total: int = 0
    error: Optional[str] = None
    output_dir: str = ""
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> dict:
        return asdict(self)


_jobs: dict[str, JobState] = {}
_jobs_lock = threading.Lock()


def _state_file(job_id: str) -> str:
    """Path to the JSON state file for a job."""
    # State file lives next to the output dir (inside the job work dir)
    job = _jobs.get(job_id)
    if job and job.output_dir:
        return os.path.join(os.path.dirname(job.output_dir), "job_state.json")
    return os.path.join(_JOBS_DIR, f"{job_id}_state.json")


def _save_job(job: JobState) -> None:
    """Write job state to disk and to blob storage."""
    try:
        path = _state_file(job.id)
        with open(path, "w") as f:
            json.dump(job.to_dict(), f)
    except Exception:
        pass  # best-effort
    # Persist to blob (best-effort)
    blob_upload_json(job.id, "job_state.json", job.to_dict())


def _load_jobs_from_disk() -> None:
    """Scan local work directory and blob storage for persisted job states."""
    # 1. Local disk
    for entry in os.scandir(_JOBS_DIR):
        if not entry.is_dir():
            continue
        state_path = os.path.join(entry.path, "job_state.json")
        if os.path.isfile(state_path):
            try:
                with open(state_path) as f:
                    data = json.load(f)
                job = JobState(**{k: v for k, v in data.items() if k in JobState.__dataclass_fields__})
                if job.status in ("running", "queued"):
                    job.status = "failed"
                    job.error = "Server restarted during processing"
                _jobs[job.id] = job
            except Exception:
                pass

    # 2. Blob storage — recover jobs not on local disk
    try:
        for job_id in blob_list_job_ids():
            if job_id in _jobs:
                continue
            data = blob_download_json(job_id, "job_state.json")
            if data is None:
                continue
            job = JobState(**{k: v for k, v in data.items() if k in JobState.__dataclass_fields__})
            if job.status in ("running", "queued"):
                job.status = "failed"
                job.error = "Server restarted during processing"
            _jobs[job.id] = job
    except Exception:
        pass


_load_jobs_from_disk()


# ── Per-job logger ───────────────────────────────────────────────────

import io
import traceback as _tb


class JobLogger:
    """Captures log lines for a pipeline run, uploads to blob on close."""

    def __init__(self, job_id: str, output_dir: str):
        self.job_id = job_id
        self._buf = io.StringIO()
        self._local_path = os.path.join(output_dir, "run.log")

    def log(self, msg: str) -> None:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        line = f"[{ts}] {msg}\n"
        self._buf.write(line)

    def flush_to_disk(self) -> None:
        os.makedirs(os.path.dirname(self._local_path), exist_ok=True)
        with open(self._local_path, "w", encoding="utf-8") as f:
            f.write(self._buf.getvalue())

    def flush_to_blob(self) -> None:
        blob_upload_text(self.job_id, "run.log", self._buf.getvalue())

    def close(self) -> None:
        self.flush_to_disk()
        self.flush_to_blob()


def submit_job(input_csv: str, output_dir: str, inference_server: Optional[int] = None) -> str:
    """Start the pipeline in a background thread. Returns job_id."""
    job_id = uuid.uuid4().hex[:12]
    job = JobState(id=job_id, output_dir=output_dir)

    with _jobs_lock:
        _jobs[job_id] = job
    _save_job(job)

    def _run():
        jlog = JobLogger(job_id, output_dir)
        try:
            job.status = "running"
            _save_job(job)
            jlog.log(f"Job {job_id} started. Input: {os.path.basename(input_csv)}")

            # Count total rows once for progress tracking
            rows = _read_csv(input_csv)
            row_count = len(rows)
            # Total work = rows * 3 passes
            job.total = row_count * 3
            jlog.log(f"Loaded {row_count} row(s). Total work units: {job.total}")

            def _on_progress(pass_num: int, processed: int, total: int):
                job.current_pass = pass_num
                if pass_num == 1:
                    job.processed = processed
                elif pass_num == 3:
                    job.processed = row_count + processed
                elif pass_num == 4:
                    job.processed = row_count * 2 + processed
                if processed == 1 or processed == total:
                    jlog.log(f"Pass {pass_num}: {processed}/{total}")

            run_full_pipeline(input_csv, output_dir, _on_progress, inference_server, job_id=job_id, log_cb=jlog.log)
            job.status = "complete"
            job.processed = job.total
            jlog.log("Pipeline complete.")
            _save_job(job)
        except Exception as exc:
            job.status = "failed"
            job.error = str(exc)
            jlog.log(f"FAILED: {exc}\n{_tb.format_exc()}")
            _save_job(job)
        finally:
            jlog.close()

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    return job_id


def get_job_status(job_id: str) -> Optional[dict]:
    with _jobs_lock:
        job = _jobs.get(job_id)
    if job is None:
        return None
    return job.to_dict()


def list_jobs() -> list[dict]:
    """Return all known jobs, most recent first."""
    with _jobs_lock:
        jobs = list(_jobs.values())
    jobs.sort(key=lambda j: j.created_at, reverse=True)
    return [j.to_dict() for j in jobs]
