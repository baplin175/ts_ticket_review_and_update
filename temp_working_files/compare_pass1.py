#!/usr/bin/env python3
"""Compare Pass 1 results against CSV baseline."""
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

with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'root_c_extraction.csv'), encoding='utf-8-sig') as f:
    reader = csv.DictReader(f)
    csv_rows = {row['Ticket #']: row for row in reader}

ids = list(id_map.values())
cur.execute("""
    SELECT ticket_id, phenomenon
    FROM ticket_llm_pass_results
    WHERE pass_name = 'pass1_phenomenon' AND ticket_id = ANY(%s)
    AND prompt_version = '1'
    ORDER BY updated_at DESC
""", (ids,))

results = {}
for r in cur.fetchall():
    if r[0] not in results:
        results[r[0]] = r[1]

print('=== PASS 1 COMPARISON: New results vs CSV baseline ===')
print()
null_but_should_have = []
matched = 0
nulls = 0
for num in sorted(csv_rows.keys(), key=lambda x: int(x)):
    tid = id_map.get(num)
    csv_phenom = csv_rows[num].get('Phenomenon', '').strip()
    new_phenom = results.get(tid, 'N/A')

    is_null = new_phenom is None or new_phenom == '' or new_phenom == 'null'
    if is_null:
        nulls += 1
        null_but_should_have.append((num, csv_phenom))
        print(f'[NULL] #{num}: CSV="{csv_phenom[:70]}" -> New=NULL')
    else:
        matched += 1
        print(f'[OK]   #{num}: "{new_phenom[:80]}"')

print()
print(f'Total: {len(csv_rows)} | Phenomena extracted: {matched} | Null returns: {nulls}')
if null_but_should_have:
    print()
    print('--- Tickets where Pass 1 returned NULL but CSV had a phenomenon ---')
    for num, p in null_but_should_have:
        print(f'  #{num}: "{p}"')

db.put_conn(conn)
