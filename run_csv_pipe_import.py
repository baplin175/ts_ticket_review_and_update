"""
Import CSV pipeline results (pass1 / pass2 / pass3) into the database.

Reads CSVs produced by the standalone CSV pipeline and inserts them into
ticket_llm_pass_results, matching the same schema used by the DB-backed
pass runners (run_ticket_pass1.py, run_ticket_pass2.py, run_ticket_pass3.py).

Only rows with status='success' are imported.  Rows whose ticket_id does
not exist in the tickets table are skipped with a warning.

Usage:
    python run_csv_pipe_import.py --dir csv_pipe_runs
    python run_csv_pipe_import.py --dir csv_pipe_runs --pass1 pass1_results.csv
    python run_csv_pipe_import.py --dir csv_pipe_runs --pass1 p1.csv --pass2 p2.csv --pass3 p3.csv
    python run_csv_pipe_import.py --dir csv_pipe_runs --force   # overwrite existing success rows
"""

import argparse
import csv
import os
import sys
from datetime import datetime, timezone

csv.field_size_limit(sys.maxsize)

# Pass metadata must match the DB-backed runners exactly
PASS1_PASS_NAME = "pass1_phenomenon"
PASS1_PROMPT_VERSION = "2"

PASS2_PASS_NAME = "pass2_mechanism"
PASS2_PROMPT_VERSION = "3"

PASS3_PASS_NAME = "pass3_intervention"
PASS3_PROMPT_VERSION = "2"

MODEL_NAME = "csv-pipeline-import"


def _log(msg: str) -> None:
    print(msg, flush=True)


def _read_csv(path: str) -> list[dict]:
    with open(path, "r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _get_known_ticket_ids() -> set[int]:
    """Return the set of ticket_ids that exist in the tickets table."""
    import db
    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT ticket_id FROM tickets WHERE closed_at IS NOT NULL;")
            return {row[0] for row in cur.fetchall()}
    finally:
        db.put_conn(conn)


def import_pass1(csv_path: str, known_ids: set[int], force: bool = False) -> tuple[int, int, int]:
    """Import Pass 1 results.  Returns (imported, skipped_no_ticket, skipped_not_success)."""
    import db

    rows = _read_csv(csv_path)
    imported = 0
    skipped_no_ticket = 0
    skipped_status = 0
    now = datetime.now(timezone.utc)

    for row in rows:
        ticket_id = int(row["ticket_id"])
        status = row.get("status", "")

        if status != "success":
            skipped_status += 1
            continue

        if ticket_id not in known_ids:
            _log(f"  [pass1] SKIP ticket_id={ticket_id} — not in tickets table")
            skipped_no_ticket += 1
            continue

        phenomenon = row.get("phenomenon") or None
        component = row.get("component") or None
        operation = row.get("operation") or None
        unexpected_state = row.get("unexpected_state") or None
        canonical_failure = row.get("canonical_failure") or None
        confidence = row.get("confidence") or None

        parsed_json = {
            "phenomenon": phenomenon,
            "component": component,
            "operation": operation,
            "unexpected_state": unexpected_state,
            "canonical_failure": canonical_failure,
            "confidence": confidence,
        }

        if force:
            db.delete_prior_failed_pass(ticket_id, PASS1_PASS_NAME, PASS1_PROMPT_VERSION)
            conn = db.get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        DELETE FROM ticket_llm_pass_results
                         WHERE ticket_id = %s AND pass_name = %s
                           AND prompt_version = %s AND status = 'success';
                    """, (ticket_id, PASS1_PASS_NAME, PASS1_PROMPT_VERSION))
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                db.put_conn(conn)
        else:
            existing = db.get_latest_pass_result(ticket_id, PASS1_PASS_NAME, PASS1_PROMPT_VERSION)
            if existing and existing.get("status") == "success":
                continue

        row_id = db.insert_pass_result(
            ticket_id,
            pass_name=PASS1_PASS_NAME,
            prompt_version=PASS1_PROMPT_VERSION,
            model_name=MODEL_NAME,
            status="pending",
            started_at=now,
        )
        db.update_pass_result(
            row_id,
            status="success",
            parsed_json=parsed_json,
            phenomenon=phenomenon,
            component=component,
            operation=operation,
            unexpected_state=unexpected_state,
            canonical_failure=canonical_failure,
            completed_at=now,
        )
        imported += 1

    return imported, skipped_no_ticket, skipped_status


def import_pass2(csv_path: str, known_ids: set[int], force: bool = False) -> tuple[int, int, int]:
    """Import Pass 2 results.  Returns (imported, skipped_no_ticket, skipped_not_success)."""
    import db

    rows = _read_csv(csv_path)
    imported = 0
    skipped_no_ticket = 0
    skipped_status = 0
    now = datetime.now(timezone.utc)

    for row in rows:
        ticket_id = int(row["ticket_id"])
        status = row.get("status", "")

        if status != "success":
            skipped_status += 1
            continue

        if ticket_id not in known_ids:
            _log(f"  [pass2] SKIP ticket_id={ticket_id} — not in tickets table")
            skipped_no_ticket += 1
            continue

        mechanism = row.get("mechanism") or None
        evidence = row.get("evidence") or None
        category = row.get("category") or None

        parsed_json = {
            "mechanism": mechanism,
            "evidence": evidence,
            "category": category,
        }

        if force:
            db.delete_prior_failed_pass(ticket_id, PASS2_PASS_NAME, PASS2_PROMPT_VERSION)
            conn = db.get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        DELETE FROM ticket_llm_pass_results
                         WHERE ticket_id = %s AND pass_name = %s
                           AND prompt_version = %s AND status = 'success';
                    """, (ticket_id, PASS2_PASS_NAME, PASS2_PROMPT_VERSION))
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                db.put_conn(conn)
        else:
            existing = db.get_latest_pass_result(ticket_id, PASS2_PASS_NAME, PASS2_PROMPT_VERSION)
            if existing and existing.get("status") == "success":
                continue

        row_id = db.insert_pass_result(
            ticket_id,
            pass_name=PASS2_PASS_NAME,
            prompt_version=PASS2_PROMPT_VERSION,
            model_name=MODEL_NAME,
            status="pending",
            started_at=now,
        )
        db.update_pass_result(
            row_id,
            status="success",
            parsed_json=parsed_json,
            mechanism=mechanism,
            completed_at=now,
        )
        imported += 1

    return imported, skipped_no_ticket, skipped_status


def import_pass3(csv_path: str, known_ids: set[int], force: bool = False) -> tuple[int, int, int]:
    """Import Pass 3 results.  Returns (imported, skipped_no_ticket, skipped_not_success)."""
    import db

    rows = _read_csv(csv_path)
    imported = 0
    skipped_no_ticket = 0
    skipped_status = 0
    now = datetime.now(timezone.utc)

    for row in rows:
        ticket_id = int(row["ticket_id"])
        status = row.get("status", "")

        if status != "success":
            skipped_status += 1
            continue

        if ticket_id not in known_ids:
            _log(f"  [pass3] SKIP ticket_id={ticket_id} — not in tickets table")
            skipped_no_ticket += 1
            continue

        mechanism_class = (row.get("mechanism_class") or "").strip().lower() or None
        intervention_type = (row.get("intervention_type") or "").strip().lower() or None
        intervention_action = (row.get("intervention_action") or "").strip() or None
        proposed_class = (row.get("proposed_class") or "").strip().lower() or None
        proposed_type = (row.get("proposed_type") or "").strip().lower() or None

        parsed_json = {
            "mechanism_class": mechanism_class,
            "intervention_type": intervention_type,
            "intervention_action": intervention_action,
        }
        if proposed_class:
            parsed_json["proposed_class"] = proposed_class
        if proposed_type:
            parsed_json["proposed_type"] = proposed_type

        if force:
            db.delete_prior_failed_pass(ticket_id, PASS3_PASS_NAME, PASS3_PROMPT_VERSION)
            conn = db.get_conn()
            try:
                with conn.cursor() as cur:
                    cur.execute("""
                        DELETE FROM ticket_llm_pass_results
                         WHERE ticket_id = %s AND pass_name = %s
                           AND prompt_version = %s AND status = 'success';
                    """, (ticket_id, PASS3_PASS_NAME, PASS3_PROMPT_VERSION))
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            finally:
                db.put_conn(conn)
        else:
            existing = db.get_latest_pass_result(ticket_id, PASS3_PASS_NAME, PASS3_PROMPT_VERSION)
            if existing and existing.get("status") == "success":
                continue

        row_id = db.insert_pass_result(
            ticket_id,
            pass_name=PASS3_PASS_NAME,
            prompt_version=PASS3_PROMPT_VERSION,
            model_name=MODEL_NAME,
            status="pending",
            started_at=now,
        )
        db.update_pass_result(
            row_id,
            status="success",
            parsed_json=parsed_json,
            mechanism_class=mechanism_class,
            intervention_type=intervention_type,
            intervention_action=intervention_action,
            completed_at=now,
        )
        imported += 1

    return imported, skipped_no_ticket, skipped_status


def main(
    csv_dir: str,
    *,
    pass1_file: str = "pass1_results.csv",
    pass2_file: str = "pass2_results.csv",
    pass3_file: str = "pass3_results.csv",
    force: bool = False,
) -> None:
    import db

    if not db._is_enabled():
        _log("[csv-import] DATABASE_URL is not set.")
        sys.exit(1)

    applied = db.migrate()
    if applied:
        from run_rollups import run_full_rollups
        run_full_rollups()

    known_ids = _get_known_ticket_ids()
    _log(f"[csv-import] {len(known_ids)} ticket(s) in DB.")
    _log(f"[csv-import] Source directory: {csv_dir}")
    _log(f"[csv-import] Force overwrite: {force}")
    _log("=" * 60)

    total_imported = 0

    # Pass 1
    p1_path = os.path.join(csv_dir, pass1_file)
    if os.path.isfile(p1_path):
        _log(f"[csv-import] Importing Pass 1 from {pass1_file} …")
        imp, skip_t, skip_s = import_pass1(p1_path, known_ids, force)
        _log(f"[csv-import]   Pass 1: {imp} imported, {skip_s} skipped (non-success), {skip_t} skipped (no ticket)")
        total_imported += imp
    else:
        _log(f"[csv-import] Pass 1 file not found: {p1_path}")

    # Pass 2
    p2_path = os.path.join(csv_dir, pass2_file)
    if os.path.isfile(p2_path):
        _log(f"[csv-import] Importing Pass 2 from {pass2_file} …")
        imp, skip_t, skip_s = import_pass2(p2_path, known_ids, force)
        _log(f"[csv-import]   Pass 2: {imp} imported, {skip_s} skipped (non-success), {skip_t} skipped (no ticket)")
        total_imported += imp
    else:
        _log(f"[csv-import] Pass 2 file not found: {p2_path}")

    # Pass 3
    p3_path = os.path.join(csv_dir, pass3_file)
    if os.path.isfile(p3_path):
        _log(f"[csv-import] Importing Pass 3 from {pass3_file} …")
        imp, skip_t, skip_s = import_pass3(p3_path, known_ids, force)
        _log(f"[csv-import]   Pass 3: {imp} imported, {skip_s} skipped (non-success), {skip_t} skipped (no ticket)")
        total_imported += imp
    else:
        _log(f"[csv-import] Pass 3 file not found: {p3_path}")

    _log("=" * 60)
    _log(f"[csv-import] Done. Total rows imported: {total_imported}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Import CSV pipeline results into the database."
    )
    parser.add_argument(
        "--dir",
        type=str,
        default="csv_pipe_runs",
        help="Directory containing the CSV result files (default: csv_pipe_runs).",
    )
    parser.add_argument("--pass1", type=str, default="pass1_results.csv", help="Pass 1 CSV filename.")
    parser.add_argument("--pass2", type=str, default="pass2_results.csv", help="Pass 2 CSV filename.")
    parser.add_argument("--pass3", type=str, default="pass3_results.csv", help="Pass 3 CSV filename.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing success rows for the same ticket/pass/version.",
    )

    args = parser.parse_args()
    main(
        args.dir,
        pass1_file=args.pass1,
        pass2_file=args.pass2,
        pass3_file=args.pass3,
        force=args.force,
    )
