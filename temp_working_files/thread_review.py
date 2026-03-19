#!/usr/bin/env python3
"""Pull actual ticket thread text + all current pass results for quality review."""
import sys, os, textwrap
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import db

conn = db.get_conn()
cur = conn.cursor()

nums = ['85200','84954','84929','84732','84660','84372','84356','84073',
        '37153','37131','36999','36558','36173','35782','35778','34990',
        '34617','33861','33667','33384','33166','32904','31752','31230',
        '31082','29696','24434','24395','21457','2587']

cur.execute('SELECT ticket_id, ticket_number, ticket_name FROM tickets WHERE ticket_number = ANY(%s)', (nums,))
tickets = {r[0]: {'number': r[1], 'name': r[2]} for r in cur.fetchall()}
ids = list(tickets.keys())

# Get thread text
cur.execute('SELECT ticket_id, technical_core_text, full_thread_text FROM ticket_thread_rollups WHERE ticket_id = ANY(%s)', (ids,))
threads = {r[0]: {'core': r[1], 'full': r[2]} for r in cur.fetchall()}

# Get latest pass results
for pass_name in ['pass1_phenomenon', 'pass2_grammar', 'pass3_mechanism', 'pass4_intervention']:
    if pass_name == 'pass1_phenomenon':
        cur.execute("""
            SELECT DISTINCT ON (ticket_id) ticket_id, phenomenon, status
            FROM ticket_llm_pass_results
            WHERE pass_name = %s AND ticket_id = ANY(%s)
            ORDER BY ticket_id, updated_at DESC
        """, (pass_name, ids))
        for r in cur.fetchall():
            tickets[r[0]].setdefault('passes', {})[pass_name] = {'phenomenon': r[1], 'status': r[2]}
    elif pass_name == 'pass2_grammar':
        cur.execute("""
            SELECT DISTINCT ON (ticket_id) ticket_id, component, operation, unexpected_state, canonical_failure, status
            FROM ticket_llm_pass_results
            WHERE pass_name = %s AND ticket_id = ANY(%s)
            ORDER BY ticket_id, updated_at DESC
        """, (pass_name, ids))
        for r in cur.fetchall():
            tickets[r[0]].setdefault('passes', {})[pass_name] = {
                'component': r[1], 'operation': r[2], 'unexpected_state': r[3],
                'canonical_failure': r[4], 'status': r[5]
            }
    elif pass_name == 'pass3_mechanism':
        cur.execute("""
            SELECT DISTINCT ON (ticket_id) ticket_id, mechanism, parsed_json, status, prompt_version
            FROM ticket_llm_pass_results
            WHERE pass_name = %s AND ticket_id = ANY(%s)
            ORDER BY ticket_id, updated_at DESC
        """, (pass_name, ids))
        for r in cur.fetchall():
            pj = r[2] if isinstance(r[2], dict) else {}
            tickets[r[0]].setdefault('passes', {})[pass_name] = {
                'mechanism': r[1], 'category': pj.get('category', '?'),
                'evidence': pj.get('evidence', '?'), 'status': r[3], 'version': r[4]
            }
    elif pass_name == 'pass4_intervention':
        cur.execute("""
            SELECT DISTINCT ON (ticket_id) ticket_id, mechanism_class, intervention_type, intervention_action, status
            FROM ticket_llm_pass_results
            WHERE pass_name = %s AND ticket_id = ANY(%s)
            ORDER BY ticket_id, updated_at DESC
        """, (pass_name, ids))
        for r in cur.fetchall():
            tickets[r[0]].setdefault('passes', {})[pass_name] = {
                'mechanism_class': r[1], 'intervention_type': r[2],
                'intervention_action': r[3], 'status': r[4]
            }

# Output for review
for tid in sorted(tickets.keys(), key=lambda t: int(tickets[t]['number'])):
    t = tickets[tid]
    th = threads.get(tid, {})
    core = th.get('core', '') or ''
    full_len = len(th.get('full', '') or '')
    
    print("=" * 80)
    print(f"TICKET #{t['number']} — {t['name']}")
    print(f"Thread: {full_len} chars full, {len(core)} chars core")
    print("-" * 80)
    
    # Show first 1500 chars of core thread text
    if core:
        print("THREAD EXCERPT (first 1500 chars of technical_core_text):")
        print(core[:1500])
        if len(core) > 1500:
            print(f"  ... [{len(core) - 1500} more chars]")
    else:
        print("THREAD: (no core text)")
    
    print("-" * 80)
    passes = t.get('passes', {})
    
    p1 = passes.get('pass1_phenomenon', {})
    print(f"PASS 1 [{p1.get('status', 'missing')}]: {p1.get('phenomenon', 'N/A')}")
    
    p2 = passes.get('pass2_grammar', {})
    print(f"PASS 2 [{p2.get('status', 'missing')}]: {p2.get('component', '')} + {p2.get('operation', '')} + {p2.get('unexpected_state', '')}")
    
    p3 = passes.get('pass3_mechanism', {})
    print(f"PASS 3 [{p3.get('status', 'missing')}] v{p3.get('version', '?')} cat={p3.get('category', '?')} ev={p3.get('evidence', '?')}:")
    print(f"  {p3.get('mechanism', 'N/A')}")
    
    p4 = passes.get('pass4_intervention', {})
    print(f"PASS 4 [{p4.get('status', 'missing')}]: mc={p4.get('mechanism_class', '')} it={p4.get('intervention_type', '')}")
    print(f"  action: {p4.get('intervention_action', 'N/A')}")
    print()

db.put_conn(conn)
