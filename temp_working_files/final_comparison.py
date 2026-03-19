#!/usr/bin/env python3
"""Final comparison of all 4 passes (using latest results) vs CSV baseline."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import db, csv

conn = db.get_conn()
cur = conn.cursor()

nums = ['85200','84954','84929','84732','84660','84372','84356','84073',
        '37153','37131','36999','36558','36173','35782','35778','34990',
        '34617','33861','33667','33384','33166','32904','31752','31230',
        '31082','29696','24434','24395','21457','2587']

cur.execute('SELECT ticket_id, ticket_number FROM tickets WHERE ticket_number = ANY(%s)', (nums,))
id_map = {r[1]: r[0] for r in cur.fetchall()}
rev_map = {v: k for k, v in id_map.items()}

csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'root_c_extraction.csv')
with open(csv_path, encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    csv_rows = {row['Ticket #']: row for row in reader}

ids = list(id_map.values())

def latest_results(query, params):
    cur.execute(query, params)
    results = {}
    for r in cur.fetchall():
        if r[0] not in results:
            results[r[0]] = r[1:]
    return results

# ---- PASS 4: using the latest results from v2 pass3 input ----
print("=" * 70)
print("PASS 4 COMPARISON (using v2 Pass 3 mechanisms)")
print("=" * 70)

cur.execute("""
    SELECT ticket_id, mechanism_class, intervention_type, intervention_action, status
    FROM ticket_llm_pass_results
    WHERE pass_name = 'pass4_intervention' AND ticket_id = ANY(%s)
    ORDER BY updated_at DESC
""", (ids,))
p4 = {}
for r in cur.fetchall():
    if r[0] not in p4:
        p4[r[0]] = {'mechanism_class': r[1], 'intervention_type': r[2],
                     'intervention_action': r[3], 'status': r[4]}

mc_match = 0
it_match = 0
both_match = 0
failed = 0
total = 0
for num in sorted(csv_rows.keys(), key=lambda x: int(x)):
    tid = id_map.get(num)
    csv_mc = csv_rows[num].get('Mechanism Class', '').strip()
    csv_it = csv_rows[num].get('Intervention Type', '').strip()
    rec = p4.get(tid, {})
    if not rec:
        print(f"  [MISS] #{num}: no pass4 result")
        continue
    total += 1
    mc = rec.get('mechanism_class', '') or ''
    it = rec.get('intervention_type', '') or ''
    ia = rec.get('intervention_action', '') or ''
    st = rec.get('status', '')
    
    if st == 'failed':
        failed += 1
        print(f"  [FAIL] #{num}: {ia[:60]}")
        continue
    
    mc_ok = mc == csv_mc
    it_ok = it == csv_it
    if mc_ok:
        mc_match += 1
    if it_ok:
        it_match += 1
    if mc_ok and it_ok:
        both_match += 1
    
    label = "MATCH" if mc_ok and it_ok else ("mc" if not mc_ok else "it") + " DIFF"
    print(f"  [{label:>7}] #{num}: mc={mc:<30} it={it:<20} | csv: mc={csv_mc:<30} it={csv_it}")

print()
print(f"  Total: {total}  |  Failed: {failed}")
print(f"  MC match: {mc_match}/{total-failed}  |  IT match: {it_match}/{total-failed}  |  Both match: {both_match}/{total-failed}")

print()
print("=" * 70)
print("OVERALL SUMMARY")
print("=" * 70)

# Pass 1 stats
cur.execute("""
    SELECT ticket_id, phenomenon FROM ticket_llm_pass_results
    WHERE pass_name = 'pass1_phenomenon' AND ticket_id = ANY(%s)
    ORDER BY updated_at DESC
""", (ids,))
p1 = {}
for r in cur.fetchall():
    if r[0] not in p1:
        p1[r[0]] = r[1]
p1_extracted = sum(1 for v in p1.values() if v and v != 'null')
p1_null = len(ids) - p1_extracted

# Pass 2 stats
cur.execute("""
    SELECT ticket_id, operation FROM ticket_llm_pass_results
    WHERE pass_name = 'pass2_grammar' AND ticket_id = ANY(%s)
    ORDER BY updated_at DESC
""", (ids,))
p2 = {}
for r in cur.fetchall():
    if r[0] not in p2:
        p2[r[0]] = r[1]
p2_load = sum(1 for v in p2.values() if v and v.lower() == 'load')

# CSV baseline load count
csv_load = sum(1 for r in csv_rows.values() if r.get('Operation', '').strip().lower() == 'load')

# Pass 3 stats
cur.execute("""
    SELECT ticket_id, parsed_json FROM ticket_llm_pass_results
    WHERE pass_name = 'pass3_mechanism' AND ticket_id = ANY(%s)
    AND prompt_version = '2'
    ORDER BY updated_at DESC
""", (ids,))
p3 = {}
for r in cur.fetchall():
    if r[0] not in p3:
        pj = r[1] if isinstance(r[1], dict) else {}
        p3[r[0]] = pj
p3_from_thread = sum(1 for pj in p3.values() if pj.get('evidence') == 'from_thread')
p3_inferred = sum(1 for pj in p3.values() if pj.get('evidence') == 'inferred')

print(f"""
  Pass 1: {p1_extracted}/30 phenomena extracted, {p1_null} null returns
    - 8 NULL: tickets with sparse/ambiguous threads correctly flagged (or over-suppressed)
    
  Pass 2: {len(p2)}/30 grammars produced
    - 'load' operations: {p2_load} new vs {csv_load} CSV baseline
    
  Pass 3 (v2): {len(p3)}/30 mechanisms with category/evidence
    - from_thread: {p3_from_thread}  |  inferred: {p3_inferred}
    - Categories: software_defect, configuration, data_issue, user_training
    
  Pass 4: {total}/30 interventions
    - Failed: {failed} (user_training ticket - expected)
    - MC match vs CSV: {mc_match}/{total-failed}
    - IT match vs CSV: {it_match}/{total-failed}
""")

db.put_conn(conn)
