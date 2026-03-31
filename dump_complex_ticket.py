"""Dump a complex ticket with all activities/details as JSON."""

import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "web"))

from web.data import query, query_one

def find_complex_ticket():
    """Find the most complex open ticket with the most actions."""
    return query_one("""
        SELECT ticket_id, ticket_number, status, customer,
               action_count, overall_complexity
        FROM vw_ticket_analytics_core
        WHERE status NOT ILIKE '%%Closed%%'
          AND overall_complexity IS NOT NULL
        ORDER BY overall_complexity DESC, action_count DESC
        LIMIT 1
    """)

def dump_ticket(ticket_id):
    """Gather all data for a ticket and return as a dict."""
    detail = query_one("SELECT * FROM vw_ticket_analytics_core WHERE ticket_id = %s", (ticket_id,))
    actions = query(
        "SELECT * FROM ticket_actions WHERE ticket_id = %s ORDER BY created_at",
        (ticket_id,),
    )
    wait = query_one(
        "SELECT * FROM vw_ticket_wait_profile WHERE ticket_id = %s", (ticket_id,),
    )
    complexity = query_one(
        "SELECT * FROM vw_latest_ticket_complexity WHERE ticket_id = %s", (ticket_id,),
    )
    events = query(
        "SELECT * FROM ticket_events WHERE ticket_id = %s ORDER BY created_at", (ticket_id,),
    )
    return {
        "ticket": detail,
        "actions": actions,
        "wait_profile": wait,
        "complexity_detail": complexity,
        "events": events,
    }

if __name__ == "__main__":
    if len(sys.argv) > 1:
        tid = int(sys.argv[1])
    else:
        pick = find_complex_ticket()
        if not pick:
            print("No complex open ticket found.", file=sys.stderr)
            sys.exit(1)
        tid = pick["ticket_id"]
        print(f"Selected ticket #{pick['ticket_number']} "
              f"(complexity={pick['overall_complexity']}, "
              f"actions={pick['action_count']})", file=sys.stderr)

    result = dump_ticket(tid)
    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output",
                            f"ticket_{result['ticket']['ticket_number']}.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w") as f:
        json.dump(result, f, indent=2, default=str)
    print(f"Wrote {out_path}", file=sys.stderr)
