"""
Evaluate Pass 4 results — intervention mapping from Pass 3 mechanisms.
Pulls full pipeline results (Pass1v2 → Pass3v3 → Pass4) for all 30 tickets.
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
            cur.execute("SELECT ticket_name FROM tickets WHERE ticket_id = %s", (tid,))
            name_row = cur.fetchone()
            ticket_name = name_row[0] if name_row else "(unknown)"

            # Pass 1 v2
            cur.execute("""
                SELECT phenomenon, canonical_failure, parsed_json
                FROM ticket_llm_pass_results
                WHERE ticket_id = %s AND pass_name = 'pass1_phenomenon'
                  AND prompt_version = '2' AND status = 'success'
                ORDER BY completed_at DESC NULLS LAST LIMIT 1
            """, (tid,))
            p1 = cur.fetchone()

            # Pass 3 v3
            cur.execute("""
                SELECT mechanism, parsed_json
                FROM ticket_llm_pass_results
                WHERE ticket_id = %s AND pass_name = 'pass3_mechanism'
                  AND prompt_version = '3' AND status = 'success'
                ORDER BY completed_at DESC NULLS LAST LIMIT 1
            """, (tid,))
            p3 = cur.fetchone()

            # Pass 4 (current version)
            cur.execute("""
                SELECT parsed_json, status, error_message
                FROM ticket_llm_pass_results
                WHERE ticket_id = %s AND pass_name = 'pass4_intervention'
                ORDER BY completed_at DESC NULLS LAST LIMIT 1
            """, (tid,))
            p4 = cur.fetchone()

            print(f"\n{'='*80}")
            print(f"TICKET {tid} — {ticket_name}")
            print(f"{'='*80}")

            if p1:
                phenomenon, canonical_failure, pj1 = p1
                confidence = pj1.get("confidence", "N/A") if isinstance(pj1, dict) else "N/A"
                print(f"  P1 phenomenon:  {phenomenon}")
                print(f"  P1 grammar:     {canonical_failure}")
                print(f"  P1 confidence:  {confidence}")
            else:
                print(f"  P1: (no result)")

            if p3:
                mechanism, pj3 = p3
                evidence = pj3.get("evidence", "N/A") if isinstance(pj3, dict) else "N/A"
                print(f"  P3 mechanism:   {mechanism}")
                print(f"  P3 evidence:    {evidence}")
            else:
                print(f"  P3: (skipped)")

            if p4:
                pj4, status, error = p4
                if status == "success" and isinstance(pj4, dict):
                    mc = pj4.get("mechanism_class", "N/A")
                    it = pj4.get("intervention_type", "N/A")
                    ia = pj4.get("intervention_action", "N/A")
                    print(f"  P4 mech_class:  {mc}")
                    print(f"  P4 interv_type: {it}")
                    print(f"  P4 interv_act:  {ia}")
                elif status == "skipped":
                    print(f"  P4: SKIPPED — {error or 'upstream P3 missing'}")
                elif status == "failed":
                    print(f"  P4: FAILED — {error}")
                else:
                    print(f"  P4: status={status}")
            else:
                if not p1 or (p1 and p1[0] is None):
                    print(f"  P4: (skipped — no phenomenon)")
                else:
                    print(f"  P4: (no result)")

finally:
    db.put_conn(conn)
