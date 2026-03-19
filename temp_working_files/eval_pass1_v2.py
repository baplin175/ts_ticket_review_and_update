"""
Evaluate Pass 1 v2 results against actual ticket thread text.
Pulls v2 results + thread text from DB, prints for manual assessment.
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

            # Get thread text (first 500 chars for review)
            cur.execute(
                "SELECT full_thread_text FROM ticket_thread_rollups WHERE ticket_id = %s",
                (tid,),
            )
            thread_row = cur.fetchone()
            thread_text = thread_row[0] if thread_row else "(no thread)"

            # Get v2 pass1 result
            cur.execute("""
                SELECT phenomenon, component, operation, unexpected_state,
                       canonical_failure, parsed_json, status, error_message
                FROM ticket_llm_pass_results
                WHERE ticket_id = %s
                  AND pass_name = 'pass1_phenomenon'
                  AND prompt_version = '2'
                ORDER BY completed_at DESC NULLS LAST
                LIMIT 1
            """, (tid,))
            result = cur.fetchone()

            # Get v1 pass1 result for comparison
            cur.execute("""
                SELECT phenomenon
                FROM ticket_llm_pass_results
                WHERE ticket_id = %s
                  AND pass_name = 'pass1_phenomenon'
                  AND prompt_version = '1'
                  AND status = 'success'
                ORDER BY completed_at DESC NULLS LAST
                LIMIT 1
            """, (tid,))
            v1_row = cur.fetchone()
            v1_phenomenon = v1_row[0] if v1_row else None

            # Get v1 pass2 result for comparison
            cur.execute("""
                SELECT canonical_failure
                FROM ticket_llm_pass_results
                WHERE ticket_id = %s
                  AND pass_name = 'pass2_grammar'
                  AND prompt_version = '1'
                  AND status = 'success'
                ORDER BY completed_at DESC NULLS LAST
                LIMIT 1
            """, (tid,))
            v1p2_row = cur.fetchone()
            v1_grammar = v1p2_row[0] if v1p2_row else None

            print(f"\n{'='*80}")
            print(f"TICKET {tid} — {ticket_name}")
            print(f"{'='*80}")

            # Print thread summary (first 400 chars)
            thread_preview = thread_text[:400] if thread_text else "(empty)"
            print(f"\nTHREAD (first 400 chars):\n{thread_preview}")
            if len(thread_text or "") > 400:
                print(f"... [{len(thread_text)} total chars]")

            if result:
                phenomenon, component, operation, unexpected_state, canonical_failure, parsed_json, status, error_message = result
                pj = parsed_json if isinstance(parsed_json, dict) else {}
                confidence = pj.get("confidence", "N/A") if pj else "N/A"

                print(f"\nV2 STATUS:      {status}")
                if status == "failed":
                    print(f"V2 ERROR:       {error_message}")
                else:
                    print(f"V2 CONFIDENCE:  {confidence}")
                    print(f"V2 PHENOMENON:  {phenomenon}")
                    print(f"V2 GRAMMAR:     {canonical_failure}")
                    print(f"   component:   {component}")
                    print(f"   operation:   {operation}")
                    print(f"   unexp_state: {unexpected_state}")
            else:
                print("\nV2: (no result)")

            print(f"\nV1 PHENOMENON:  {v1_phenomenon}")
            print(f"V1 GRAMMAR:     {v1_grammar}")

finally:
    db.put_conn(conn)
