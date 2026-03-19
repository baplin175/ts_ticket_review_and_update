"""
Evaluate Pass 3 v3 results against actual ticket thread text.
Pulls v3 results + thread text + Pass 1 v2 results from DB.
"""
import json
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import db

TICKET_IDS = [
    18835794, 18897699, 17706465, 18119965, 17937213, 18552060, 17288538,
    15285693, 18385639, 14324709, 18385384, 18201096, 18678076, 18822492,
    18935221, 15303912, 18463472, 18775025, 17445735, 18974950, 17777854,
    17903003, 6245039, 18929571, 18676212, 17843420, 17319634, 18652869,
    16942554, 18884039,
]

conn = db.get_conn()
try:
    with conn.cursor() as cur:
        for tid in sorted(TICKET_IDS):
            # Get ticket name
            cur.execute("SELECT ticket_name FROM tickets WHERE ticket_id = %s", (tid,))
            name_row = cur.fetchone()
            ticket_name = name_row[0] if name_row else "(unknown)"

            # Get thread text (first 600 chars for review)
            cur.execute(
                "SELECT full_thread_text FROM ticket_thread_rollups WHERE ticket_id = %s",
                (tid,),
            )
            thread_row = cur.fetchone()
            thread_text = thread_row[0] if thread_row else "(no thread)"

            # Get v2 pass1 result
            cur.execute("""
                SELECT phenomenon, canonical_failure, parsed_json
                FROM ticket_llm_pass_results
                WHERE ticket_id = %s
                  AND pass_name = 'pass1_phenomenon'
                  AND prompt_version = '2'
                  AND status = 'success'
                ORDER BY completed_at DESC NULLS LAST
                LIMIT 1
            """, (tid,))
            p1_row = cur.fetchone()

            # Get v3 pass3 result
            cur.execute("""
                SELECT mechanism, parsed_json, status, error_message
                FROM ticket_llm_pass_results
                WHERE ticket_id = %s
                  AND pass_name = 'pass3_mechanism'
                  AND prompt_version = '3'
                ORDER BY completed_at DESC NULLS LAST
                LIMIT 1
            """, (tid,))
            p3_row = cur.fetchone()

            # Get old v2 pass3 result for comparison
            cur.execute("""
                SELECT mechanism, parsed_json
                FROM ticket_llm_pass_results
                WHERE ticket_id = %s
                  AND pass_name = 'pass3_mechanism'
                  AND prompt_version = '2'
                  AND status = 'success'
                ORDER BY completed_at DESC NULLS LAST
                LIMIT 1
            """, (tid,))
            old_p3 = cur.fetchone()

            print(f"\n{'='*80}")
            print(f"TICKET {tid} — {ticket_name}")
            print(f"{'='*80}")

            # Thread preview
            thread_preview = thread_text[:600] if thread_text else "(empty)"
            print(f"\nTHREAD (first 600 chars):\n{thread_preview}")
            if len(thread_text or "") > 600:
                print(f"... [{len(thread_text)} total chars]")

            # Pass 1 v2 result
            if p1_row:
                phenomenon, canonical_failure, pj = p1_row
                confidence = pj.get("confidence", "N/A") if isinstance(pj, dict) else "N/A"
                print(f"\nPASS1 v2:")
                print(f"  confidence:  {confidence}")
                print(f"  phenomenon:  {phenomenon}")
                print(f"  grammar:     {canonical_failure}")
            else:
                print(f"\nPASS1 v2: (no result)")
                phenomenon = None
                canonical_failure = None

            # Pass 3 v3 result
            if p3_row:
                mechanism, pj3, status, error = p3_row
                if status == "success":
                    pj3_dict = pj3 if isinstance(pj3, dict) else {}
                    evidence = pj3_dict.get("evidence", "N/A")
                    print(f"\nPASS3 v3:")
                    print(f"  mechanism:   {mechanism}")
                    print(f"  evidence:    {evidence}")
                else:
                    print(f"\nPASS3 v3: FAILED — {error}")
            else:
                if phenomenon is None:
                    print(f"\nPASS3 v3: (skipped — no phenomenon)")
                else:
                    print(f"\nPASS3 v3: (no result)")

            # Old pass3 v2 result
            if old_p3:
                old_mech, old_pj3 = old_p3
                old_ev = old_pj3.get("evidence", "N/A") if isinstance(old_pj3, dict) else "N/A"
                print(f"\nPASS3 v2 (old):")
                print(f"  mechanism:   {old_mech}")
                print(f"  evidence:    {old_ev}")
            else:
                print(f"\nPASS3 v2 (old): (no result)")

finally:
    db.put_conn(conn)
