#!/usr/bin/env python3
"""Compare Pass 3 v2 results in detail - check category, evidence, mechanism quality."""
import sys, os, json
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

# Get Pass 3 v2 results
cur.execute("""
    SELECT ticket_id, mechanism, status, parsed_json
    FROM ticket_llm_pass_results
    WHERE pass_name = 'pass3_mechanism' AND ticket_id = ANY(%s)
    AND prompt_version = '2'
    ORDER BY updated_at DESC
""", (ids,))
p3 = {}
for r in cur.fetchall():
    if r[0] not in p3:
        pj = r[3] if isinstance(r[3], dict) else {}
        p3[r[0]] = {'mechanism': r[1], 'status': r[2], 'category': pj.get('category', '?'), 'evidence': pj.get('evidence', '?')}

print("=== PASS 3 v2 DETAILED COMPARISON ===")
print(f"Found {len(p3)} v2 results")
print()

cat_counts = {}
ev_counts = {}
from_thread = 0
inferred = 0

for num in sorted(csv_rows.keys(), key=lambda x: int(x)):
    tid = id_map.get(num)
    csv_mech = csv_rows[num].get('Mechanism', '').strip()
    rec = p3.get(tid, {})
    if not rec:
        print(f"  [MISS] #{num}: no pass3 v2 result")
        continue
    mech = rec.get('mechanism', '') or ''
    cat = rec.get('category', '?')
    ev = rec.get('evidence', '?')
    cat_counts[cat] = cat_counts.get(cat, 0) + 1
    ev_counts[ev] = ev_counts.get(ev, 0) + 1
    if ev == 'from_thread':
        from_thread += 1
    elif ev == 'inferred':
        inferred += 1
    
    print(f"  #{num} [{cat}|{ev}]:")
    print(f"    NEW: \"{mech[:90]}\"")
    print(f"    CSV: \"{csv_mech[:90]}\"")
    print()

print("=" * 70)
print("SUMMARY")
print(f"  Total v2 results: {len(p3)}/30")
print(f"  Category distribution: {dict(sorted(cat_counts.items()))}")
print(f"  Evidence distribution: {dict(sorted(ev_counts.items()))}")
print(f"  from_thread: {from_thread}  |  inferred: {inferred}")

db.put_conn(conn)
