#!/usr/bin/env python3
"""Check thread text for NULL-returning tickets and run all pass comparisons."""
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

# ---- PASS 1 ----
print("=" * 70)
print("PASS 1 - PHENOMENON")
print("=" * 70)
cur.execute("""
    SELECT ticket_id, phenomenon, status
    FROM ticket_llm_pass_results
    WHERE pass_name = 'pass1_phenomenon' AND ticket_id = ANY(%s)
    ORDER BY updated_at DESC
""", (ids,))
p1 = {}
for r in cur.fetchall():
    if r[0] not in p1:
        p1[r[0]] = {'phenomenon': r[1], 'status': r[2]}

null_ids = []
for num in sorted(csv_rows.keys(), key=lambda x: int(x)):
    tid = id_map.get(num)
    csv_phenom = csv_rows[num].get('Phenomenon', '').strip()
    rec = p1.get(tid, {})
    phen = rec.get('phenomenon')
    is_null = not phen or phen == 'null'
    if is_null:
        print(f"  [NULL] #{num}: CSV=\"{csv_phenom[:65]}\"")
        null_ids.append(tid)
    else:
        print(f"  [OK]   #{num}: \"{phen[:75]}\"")

p1_ok = sum(1 for tid in p1 if p1[tid].get('phenomenon') and p1[tid]['phenomenon'] != 'null')
p1_null = len(ids) - p1_ok
print(f"\n  Extracted: {p1_ok}/30  |  Null: {p1_null}/30")

# Check thread text for null tickets
if null_ids:
    print("\n  Thread text for NULL tickets:")
    for tid in null_ids:
        cur.execute('SELECT length(full_thread_text), length(technical_core_text) FROM ticket_thread_rollups WHERE ticket_id = %s', (tid,))
        row = cur.fetchone()
        num = rev_map.get(tid, '?')
        if row:
            print(f"    #{num}: full_thread={row[0]} chars, core={row[1]} chars")
        else:
            print(f"    #{num}: NO ROLLUP ROW")

# ---- PASS 2 ----
print("\n" + "=" * 70)
print("PASS 2 - GRAMMAR (Component / Operation / Unexpected State)")
print("=" * 70)
cur.execute("""
    SELECT ticket_id, component, operation, unexpected_state, status
    FROM ticket_llm_pass_results
    WHERE pass_name = 'pass2_grammar' AND ticket_id = ANY(%s)
    ORDER BY updated_at DESC
""", (ids,))
p2 = {}
for r in cur.fetchall():
    if r[0] not in p2:
        p2[r[0]] = {'component': r[1], 'operation': r[2], 'unexpected_state': r[3], 'status': r[4]}

load_count = 0
for num in sorted(csv_rows.keys(), key=lambda x: int(x)):
    tid = id_map.get(num)
    csv_comp = csv_rows[num].get('Component', '').strip()
    csv_op = csv_rows[num].get('Operation', '').strip()
    csv_us = csv_rows[num].get('Unexpected State', '').strip()
    rec = p2.get(tid, {})
    if not rec:
        print(f"  [MISS] #{num}: no pass2 result")
        continue
    comp = rec.get('component', '') or ''
    op = rec.get('operation', '') or ''
    us = rec.get('unexpected_state', '') or ''
    if op.lower() == 'load':
        load_count += 1
    flag = " [load]" if op.lower() == 'load' else ""
    print(f"  #{num}: C=\"{comp[:40]}\" O=\"{op}\" US=\"{us[:50]}\"{flag}")

p2_total = len(p2)
print(f"\n  Total with results: {p2_total}/30  |  'load' operations: {load_count}")

# ---- PASS 3 ----
print("\n" + "=" * 70)
print("PASS 3 - MECHANISM")
print("=" * 70)
cur.execute("""
    SELECT ticket_id, mechanism, status, parsed_json
    FROM ticket_llm_pass_results
    WHERE pass_name = 'pass3_mechanism' AND ticket_id = ANY(%s)
    ORDER BY updated_at DESC
""", (ids,))
p3 = {}
for r in cur.fetchall():
    if r[0] not in p3:
        p3[r[0]] = {'mechanism': r[1], 'status': r[2], 'parsed_json': r[3]}

for num in sorted(csv_rows.keys(), key=lambda x: int(x)):
    tid = id_map.get(num)
    csv_mech = csv_rows[num].get('Mechanism', '').strip()
    rec = p3.get(tid, {})
    if not rec:
        print(f"  [MISS] #{num}: no pass3 result")
        continue
    mech = rec.get('mechanism', '') or ''
    status = rec.get('status', '')
    pj = rec.get('parsed_json') or {}
    cat = pj.get('category', '?') if isinstance(pj, dict) else '?'
    ev = pj.get('evidence', '?') if isinstance(pj, dict) else '?'
    print(f"  #{num} [{status}]: \"{mech[:70]}\" cat={cat} ev={ev}")

p3_success = sum(1 for r in p3.values() if r['status'] == 'success')
print(f"\n  Total: {len(p3)}/30  |  Success: {p3_success}")

# ---- PASS 4 ----
print("\n" + "=" * 70)
print("PASS 4 - INTERVENTION")
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

for num in sorted(csv_rows.keys(), key=lambda x: int(x)):
    tid = id_map.get(num)
    csv_mc = csv_rows[num].get('Mechanism Class', '').strip()
    csv_it = csv_rows[num].get('Intervention Type', '').strip()
    rec = p4.get(tid, {})
    if not rec:
        print(f"  [MISS] #{num}: no pass4 result")
        continue
    mc = rec.get('mechanism_class', '') or ''
    it = rec.get('intervention_type', '') or ''
    ia = rec.get('intervention_action', '') or ''
    csv_match = "MATCH" if mc == csv_mc and it == csv_it else "DIFF"
    print(f"  #{num} [{csv_match}]: mc={mc} it={it} | csv: mc={csv_mc} it={csv_it}")

p4_success = sum(1 for r in p4.values() if r['status'] == 'success')
mc_match = sum(1 for num in csv_rows
               if id_map.get(num) in p4
               and (p4[id_map[num]].get('mechanism_class') or '') == csv_rows[num].get('Mechanism Class', '').strip())
it_match = sum(1 for num in csv_rows
               if id_map.get(num) in p4
               and (p4[id_map[num]].get('intervention_type') or '') == csv_rows[num].get('Intervention Type', '').strip())
print(f"\n  Total: {len(p4)}/30  |  Success: {p4_success}  |  MC match: {mc_match}  |  IT match: {it_match}")

db.put_conn(conn)
