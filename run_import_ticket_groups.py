"""
Backfill tickets.group_name from All Tickets_for_group.csv.

Matches CSV "Ticket Number" to tickets.ticket_number and writes CSV "Group Name"
into tickets.group_name.
"""

import argparse
import csv
import os
import sys
from pathlib import Path

import psycopg2.extras

import db

CSV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "All Tickets_for_group.csv")


def _log(msg: str) -> None:
    print(msg, flush=True)


def _load_group_rows(csv_path: str) -> tuple[list[tuple[str, str]], int]:
    rows_by_ticket: dict[str, str] = {}
    conflicts = 0

    with Path(csv_path).open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ticket_number = (row.get("Ticket Number") or "").strip()
            group_name = (row.get("Group Name") or "").strip()
            if not ticket_number or not group_name:
                continue

            existing = rows_by_ticket.get(ticket_number)
            if existing is None:
                rows_by_ticket[ticket_number] = group_name
                continue
            if existing != group_name:
                conflicts += 1

    data = sorted(rows_by_ticket.items(), key=lambda item: item[0])
    return data, conflicts


def run(csv_path: str, *, dry_run: bool = False) -> dict:
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    if not db._is_enabled():
        raise RuntimeError("DATABASE_URL not set; cannot update ticket groups.")

    applied = db.migrate()
    if applied:
        _log(f"[group-import] Applied migrations: {', '.join(applied)}")

    csv_rows, conflicts = _load_group_rows(csv_path)
    if not csv_rows:
        return {
            "csv_rows": 0,
            "conflicts": conflicts,
            "matched_tickets": 0,
            "updated_tickets": 0,
            "missing_in_db": 0,
        }

    conn = db.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TEMP TABLE tmp_ticket_groups (
                    ticket_number TEXT PRIMARY KEY,
                    group_name TEXT NOT NULL
                ) ON COMMIT DROP;
            """)
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO tmp_ticket_groups (ticket_number, group_name)
                VALUES %s
                """,
                csv_rows,
                page_size=1000,
            )

            cur.execute("""
                SELECT COUNT(*)
                FROM tmp_ticket_groups g
                JOIN tickets t ON t.ticket_number = g.ticket_number;
            """)
            matched_tickets = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*)
                FROM tmp_ticket_groups g
                JOIN tickets t ON t.ticket_number = g.ticket_number
                WHERE t.group_name IS DISTINCT FROM g.group_name;
            """)
            updated_tickets = cur.fetchone()[0]

            cur.execute("""
                SELECT COUNT(*)
                FROM tmp_ticket_groups g
                LEFT JOIN tickets t ON t.ticket_number = g.ticket_number
                WHERE t.ticket_id IS NULL;
            """)
            missing_in_db = cur.fetchone()[0]

            if not dry_run:
                cur.execute("""
                    UPDATE tickets t
                    SET group_name = g.group_name
                    FROM tmp_ticket_groups g
                    WHERE t.ticket_number = g.ticket_number
                      AND t.group_name IS DISTINCT FROM g.group_name;
                """)
                conn.commit()
            else:
                conn.rollback()

        return {
            "csv_rows": len(csv_rows),
            "conflicts": conflicts,
            "matched_tickets": matched_tickets,
            "updated_tickets": updated_tickets,
            "missing_in_db": missing_in_db,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        db.put_conn(conn)


def main() -> None:
    parser = argparse.ArgumentParser(description="Backfill tickets.group_name from CSV.")
    parser.add_argument("--csv", default=CSV_PATH, help="Path to CSV file.")
    parser.add_argument("--dry-run", action="store_true", help="Preview counts without writing.")
    args = parser.parse_args()

    try:
        stats = run(args.csv, dry_run=args.dry_run)
    except Exception as exc:
        _log(f"[group-import] Failed: {exc}")
        sys.exit(1)

    _log(
        "[group-import] "
        f"csv_rows={stats['csv_rows']:,} "
        f"matched={stats['matched_tickets']:,} "
        f"updated={stats['updated_tickets']:,} "
        f"missing_in_db={stats['missing_in_db']:,} "
        f"conflicts={stats['conflicts']:,} "
        f"dry_run={args.dry_run}"
    )


if __name__ == "__main__":
    main()
