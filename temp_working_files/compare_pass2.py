#!/usr/bin/env python3
"""Compare Pass 2 results in detail - focus on 'load' operation usage."""
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

print("=== PASS 2 DETAILED COMPARISON ===")
print(f"{'Tkt#':<8} {'Op Match':<10} {'New Op':<12} {'CSV Op':<12} {'New Component':<35} {'CSV Component':<35}")
print("-" * 112)

load_new = 0
load_csv = 0
op_match = 0
for num in sorted(csv_rows.keys(), key=lambda x: int(x)):
    tid = id_map.get(num)
    rec = p2.get(tid, {})
    if not rec:
        print(f"{num:<8} {'SKIP':<10}")
        continue
    new_op = (rec.get('operation') or '').lower()
    csv_op = csv_rows[num].get('Operation', '').strip().lower()
    new_comp = rec.get('component', '') or ''
    csv_comp = csv_rows[num].get('Component', '').strip()
    
    if new_op == 'load':
        load_new += 1
    if csv_op == 'load':
        load_csv += 1
    match = "YES" if new_op == csv_op else "NO"
    if new_op == csv_op:
        op_match += 1
    print(f"{num:<8} {match:<10} {new_op:<12} {csv_op:<12} {new_comp[:34]:<35} {csv_comp[:34]:<35}")

print()
print(f"Operation matches: {op_match}/30")
print(f"'load' in new results: {load_new}")
print(f"'load' in CSV baseline: {load_csv}")

db.put_conn(conn)
