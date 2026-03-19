"""Evaluate pass 1-4 output quality against actual ticket content."""
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import db
import json
import textwrap

TICKET_NUMS = ['85200', '84954', '84929', '84732', '84660', '84372', '84356', '84073', '37153', '37131',
               '36999', '36558', '36173', '35782', '35778', '34990', '34617', '33861', '33667', '33384',
               '33166', '32904', '31752', '31230', '31082', '29696', '24434', '24395', '21457', '2587']

def main():
    conn = db.get_conn()
    cur = conn.cursor()

    # Resolve ticket_number -> ticket_id
    cur.execute("""
        SELECT ticket_id, ticket_number FROM tickets
        WHERE ticket_number = ANY(%s)
    """, (TICKET_NUMS,))
    id_map = {row[1]: row[0] for row in cur.fetchall()}
    ticket_ids = list(id_map.values())
    print(f"Resolved {len(ticket_ids)} ticket_ids from {len(TICKET_NUMS)} ticket_numbers")

    # 1) Get rollup content for each ticket
    cur.execute("""
        SELECT r.ticket_id, r.ticket_number,
               tk.ticket_name,
               LEFT(r.technical_core_text, 2000) AS tech_core,
               LEFT(r.customer_visible_text, 2000) AS cust_vis,
               LEFT(r.latest_customer_text, 1000) AS latest_cust,
               LEFT(r.latest_inhance_text, 1000) AS latest_inh
        FROM ticket_thread_rollups r
        JOIN tickets tk ON tk.ticket_id = r.ticket_id
        WHERE r.ticket_id = ANY(%s)
        ORDER BY r.ticket_id DESC
    """, (ticket_ids,))
    rollups = {row[0]: row for row in cur.fetchall()}

    # 2) Get all pass results for these tickets
    cur.execute("""
        SELECT ticket_id, pass_name, status, phenomenon,
               component, operation, unexpected_state, canonical_failure,
               mechanism, mechanism_class, intervention_type, intervention_action,
               LEFT(input_text, 2000) AS input_text
        FROM ticket_llm_pass_results
        WHERE ticket_id = ANY(%s)
          AND status = 'success'
        ORDER BY ticket_id DESC, pass_name
    """, (ticket_ids,))
    
    pass_results = {}
    for row in cur.fetchall():
        tid = row[0]
        pname = row[1]
        if tid not in pass_results:
            pass_results[tid] = {}
        pass_results[tid][pname] = {
            "status": row[2],
            "phenomenon": row[3],
            "component": row[4],
            "operation": row[5],
            "unexpected_state": row[6],
            "canonical_failure": row[7],
            "mechanism": row[8],
            "mechanism_class": row[9],
            "intervention_type": row[10],
            "intervention_action": row[11],
            "input_text": row[12],
        }

    db.put_conn(conn)

    # 3) Print comparison for each ticket
    missing = []
    for tid in ticket_ids:
        if tid not in rollups:
            missing.append(tid)
            continue
        
        _, tnum, tname, tech_core, cust_vis, latest_cust, latest_inh = rollups[tid]
        passes = pass_results.get(tid, {})

        print("=" * 80)
        print(f"TICKET {tid} (#{tnum}): {tname}")
        print("=" * 80)

        print("\n--- ACTUAL TICKET CONTENT (technical_core_text) ---")
        if tech_core:
            print(textwrap.fill(tech_core[:800], width=100))
        else:
            print("  [EMPTY]")

        print("\n--- LATEST CUSTOMER TEXT ---")
        if latest_cust:
            print(textwrap.fill(latest_cust[:500], width=100))
        else:
            print("  [EMPTY]")

        print("\n--- LATEST INHANCE TEXT ---")
        if latest_inh:
            print(textwrap.fill(latest_inh[:500], width=100))
        else:
            print("  [EMPTY]")

        # Pass 1
        p1 = passes.get("pass1_phenomenon", {})
        print(f"\n--- PASS 1 (Phenomenon) ---")
        print(f"  phenomenon: {p1.get('phenomenon', 'N/A')}")

        # Pass 2
        p2 = passes.get("pass2_grammar", {})
        print(f"\n--- PASS 2 (Grammar) ---")
        print(f"  component:        {p2.get('component', 'N/A')}")
        print(f"  operation:        {p2.get('operation', 'N/A')}")
        print(f"  unexpected_state: {p2.get('unexpected_state', 'N/A')}")
        print(f"  canonical_failure:{p2.get('canonical_failure', 'N/A')}")

        # Pass 3
        p3 = passes.get("pass3_mechanism", {})
        print(f"\n--- PASS 3 (Mechanism) ---")
        print(f"  mechanism: {p3.get('mechanism', 'N/A')}")

        # Pass 4
        p4 = passes.get("pass4_intervention", {})
        print(f"\n--- PASS 4 (Intervention) ---")
        print(f"  mechanism_class:    {p4.get('mechanism_class', 'N/A')}")
        print(f"  intervention_type:  {p4.get('intervention_type', 'N/A')}")
        print(f"  intervention_action:{p4.get('intervention_action', 'N/A')}")

        print("\n")

    if missing:
        print(f"\n[WARNING] Tickets not found in rollups: {missing}")

    print(f"\nTotal tickets reviewed: {len(TICKET_NUMS)}")
    print(f"Found in rollups: {len(rollups)}")
    print(f"With pass results: {len(pass_results)}")


if __name__ == "__main__":
    main()
