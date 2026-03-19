#!/usr/bin/env python3
"""Analyze the specific failure patterns in the evaluation."""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import db

conn = db.get_conn()
cur = conn.cursor()

# Check what Pass 1 is seeing vs what it outputs for the fabrication cases
# #33384 - "Unable to upload Key Bank Lockbox file" but thread is "how does lockbox work"
# #34990 - "Meter exchanges generate errors" but thread is OOO reply
# #84929 - "GP fails due to antivirus" but thread only says "customer is still down"
# #32904 - "deposits do not populate in user-friendly manner" but thread is about form changes

problem_nums = ['33384', '34990', '84929', '32904', '84732', '84954']
cur.execute('''
    SELECT t.ticket_number, t.ticket_name, r.full_thread_text, r.technical_core_text
    FROM tickets t
    LEFT JOIN ticket_thread_rollups r ON r.ticket_id = t.ticket_id
    WHERE t.ticket_number = ANY(%s)
    ORDER BY t.ticket_number::int
''', (problem_nums,))

for r in cur.fetchall():
    print("=" * 70)
    print(f"TICKET #{r[0]} — {r[1]}")
    print(f"full_thread ({len(r[2] or '')} chars):")
    print((r[2] or '(empty)')[:2000])
    print()
    print(f"technical_core ({len(r[3] or '')} chars):")
    print((r[3] or '(empty)')[:2000])
    print()

db.put_conn(conn)
