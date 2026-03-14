"""
run_analytics.py — Rebuild derived analytics tables from canonical DB state.

Populates:
  - ticket_participants   (from ticket_actions)
  - ticket_handoffs       (from ticket_actions)
  - ticket_wait_states    (from ticket_actions + lifecycle)
  - ticket_snapshots_daily (daily snapshot of all tickets)
  - customer_ticket_health (rolled up per customer)
  - product_ticket_health  (rolled up per product)

No TeamSupport API calls required.  Reads entirely from DB.

Usage:
    python run_analytics.py participants                     # rebuild all
    python run_analytics.py participants --ticket 29696      # rebuild one
    python run_analytics.py handoffs                         # rebuild all
    python run_analytics.py wait-states                      # rebuild all
    python run_analytics.py snapshot                         # today's snapshot
    python run_analytics.py snapshot --date 2026-03-14       # specific date
    python run_analytics.py customer-health                  # today
    python run_analytics.py product-health                   # today
    python run_analytics.py all                              # all of the above
    python run_analytics.py all --ticket 29696               # participants/handoffs/wait-states for one ticket + snapshot + health

Requires DATABASE_URL to be set and data to be ingested via run_ingest.py.
"""

import argparse
import json
import sys
from datetime import date, datetime, timezone
from typing import Optional

import db


# ── Helpers ──────────────────────────────────────────────────────────

def _ticket_ids(ticket_number: str | None = None) -> list[int]:
    """Return ticket_id(s) to process.  If ticket_number is given, look it up."""
    if ticket_number:
        rows = db.fetch_all(
            "SELECT ticket_id FROM tickets WHERE ticket_number = %s;",
            (ticket_number,),
        )
        if not rows:
            print(f"[analytics] No ticket found with number {ticket_number}.", flush=True)
            return []
        return [r[0] for r in rows]
    else:
        rows = db.fetch_all("SELECT ticket_id FROM tickets ORDER BY ticket_id;")
        return [r[0] for r in rows]


def _all_ticket_ids() -> list[int]:
    """Return all ticket_ids."""
    rows = db.fetch_all("SELECT ticket_id FROM tickets ORDER BY ticket_id;")
    return [r[0] for r in rows]


# ── A. Rebuild ticket_participants ───────────────────────────────────

def rebuild_ticket_participants(ticket_ids: list[int] | None = None) -> int:
    """Populate ticket_participants from ticket_actions.

    For each ticket, tracks:
    - participant_id (creator_id)
    - participant_name (creator_name)
    - participant_type (party: 'inh', 'cust', or 'unknown')
    - first_seen_at / last_seen_at
    - action_count
    - first_response_flag (first inh action after first cust action)

    Uses DELETE + INSERT (full refresh per ticket) for simplicity.
    Returns count of tickets processed.
    """
    if ticket_ids is None:
        ticket_ids = _all_ticket_ids()

    now = datetime.now(timezone.utc)
    count = 0

    for tid in ticket_ids:
        rows = db.fetch_all(
            "SELECT action_id, creator_id, creator_name, party, created_at, is_empty "
            "FROM ticket_actions WHERE ticket_id = %s ORDER BY created_at;",
            (tid,),
        )

        # Build participant map
        participants: dict[str, dict] = {}
        first_cust_at = None
        first_response_action_creator = None

        for aid, cid, cname, party, cat, is_empty in rows:
            if not cid:
                continue

            pid = str(cid)
            if pid not in participants:
                participants[pid] = {
                    "participant_id": pid,
                    "participant_name": cname,
                    "participant_type": party or "unknown",
                    "first_seen_at": cat,
                    "last_seen_at": cat,
                    "action_count": 0,
                    "first_response_flag": False,
                }

            p = participants[pid]
            p["action_count"] += 1
            if cname and not p["participant_name"]:
                p["participant_name"] = cname
            if cat:
                if p["first_seen_at"] is None or cat < p["first_seen_at"]:
                    p["first_seen_at"] = cat
                if p["last_seen_at"] is None or cat > p["last_seen_at"]:
                    p["last_seen_at"] = cat

            # Track first customer action and first response
            if party == "cust" and cat and first_cust_at is None:
                first_cust_at = cat
            if (first_response_action_creator is None
                    and first_cust_at is not None
                    and party == "inh" and cat):
                first_response_action_creator = pid

        # Mark first response flag
        if first_response_action_creator and first_response_action_creator in participants:
            participants[first_response_action_creator]["first_response_flag"] = True

        # Delete existing and insert fresh
        db.execute("DELETE FROM ticket_participants WHERE ticket_id = %s;", (tid,))
        for p in participants.values():
            db.execute("""
                INSERT INTO ticket_participants (
                    ticket_id, participant_id, participant_name, participant_type,
                    first_seen_at, last_seen_at, action_count, first_response_flag,
                    created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                tid, p["participant_id"], p["participant_name"], p["participant_type"],
                p["first_seen_at"], p["last_seen_at"], p["action_count"],
                p["first_response_flag"], now, now,
            ))
        count += 1

    print(f"[analytics] Rebuilt participants for {count} ticket(s).", flush=True)
    return count


# ── B. Rebuild ticket_handoffs ───────────────────────────────────────

def rebuild_ticket_handoffs(ticket_ids: list[int] | None = None) -> int:
    """Infer handoffs from transitions between parties/participants over time.

    A handoff occurs when the party changes between consecutive non-empty
    human actions (inh→cust or cust→inh), or when the creator_id changes
    within the same party.

    Uses DELETE + INSERT (full refresh per ticket).
    Returns count of tickets processed.
    """
    if ticket_ids is None:
        ticket_ids = _all_ticket_ids()

    now = datetime.now(timezone.utc)
    count = 0

    for tid in ticket_ids:
        rows = db.fetch_all(
            "SELECT action_id, creator_id, creator_name, party, created_at, is_empty, action_class "
            "FROM ticket_actions WHERE ticket_id = %s ORDER BY created_at;",
            (tid,),
        )

        # Filter to human, non-empty actions
        human_actions = [
            (aid, cid, cname, party, cat, ac)
            for aid, cid, cname, party, cat, is_empty, ac in rows
            if party in ("inh", "cust") and not is_empty and cat
        ]

        db.execute("DELETE FROM ticket_handoffs WHERE ticket_id = %s;", (tid,))

        prev = None
        for aid, cid, cname, party, cat, ac in human_actions:
            if prev is not None:
                prev_aid, prev_cid, prev_cname, prev_party, prev_cat, prev_ac = prev
                # Handoff if party changes
                if party != prev_party:
                    reason = f"{prev_party}→{party}"
                    db.execute("""
                        INSERT INTO ticket_handoffs (
                            ticket_id, from_party, to_party,
                            from_participant_id, to_participant_id,
                            handoff_at, handoff_reason,
                            inferred_from_action_id, confidence, created_at
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """, (
                        tid, prev_party, party,
                        str(prev_cid) if prev_cid else None,
                        str(cid) if cid else None,
                        cat, reason, aid, 0.9, now,
                    ))
            prev = (aid, cid, cname, party, cat, ac)

        count += 1

    print(f"[analytics] Rebuilt handoffs for {count} ticket(s).", flush=True)
    return count


# ── C. Rebuild ticket_wait_states ────────────────────────────────────

# State inference rules:
#   - After a cust action: waiting_on_support (customer sent something, support should respond)
#   - After an inh action with class "waiting_on_customer": waiting_on_customer
#   - After an inh action with class "scheduling": scheduled
#   - After an inh action with class "delivery_confirmation": resolved
#   - After an inh action (general): active_work (support is working)
#   - If action class contains "dev" or ticket mentions dev: waiting_on_dev (heuristic)
#   - Default after inh: active_work

_STATE_MAP = {
    "waiting_on_customer": "waiting_on_customer",
    "scheduling": "scheduled",
    "delivery_confirmation": "resolved",
}


def _infer_state(party: str, action_class: str | None, desc: str | None) -> str:
    """Infer the wait state after a given action."""
    ac = (action_class or "").lower()

    if party == "cust":
        return "waiting_on_support"

    # inh action
    if ac in _STATE_MAP:
        return _STATE_MAP[ac]

    # Heuristic: if description mentions dev/engineering handoff
    # "escalat" is a prefix match for both "escalated" and "escalation"
    text = (desc or "").lower()
    if any(kw in text for kw in ("dev team", "engineering", "r&d", "development team", "escalat")):
        return "waiting_on_dev"
    if any(kw in text for kw in ("professional services", " ps ", "ps team")):
        return "waiting_on_ps"

    return "active_work"


def rebuild_ticket_wait_states(ticket_ids: list[int] | None = None) -> int:
    """Infer deterministic wait segments from ticket action stream.

    States: waiting_on_customer, waiting_on_support, waiting_on_dev,
    waiting_on_ps, active_work, scheduled, resolved.

    Uses DELETE + INSERT (full refresh per ticket).
    Returns count of tickets processed.
    """
    if ticket_ids is None:
        ticket_ids = _all_ticket_ids()

    now = datetime.now(timezone.utc)
    count = 0

    for tid in ticket_ids:
        rows = db.fetch_all(
            "SELECT action_id, party, created_at, is_empty, action_class, cleaned_description "
            "FROM ticket_actions WHERE ticket_id = %s ORDER BY created_at;",
            (tid,),
        )

        # Filter to meaningful actions
        meaningful = [
            (aid, party, cat, ac, desc)
            for aid, party, cat, is_empty, ac, desc in rows
            if party in ("inh", "cust") and not is_empty and cat
        ]

        db.execute("DELETE FROM ticket_wait_states WHERE ticket_id = %s;", (tid,))

        if not meaningful:
            count += 1
            continue

        segments = []
        prev_state = None
        prev_start = None
        prev_action_ids = []

        for aid, party, cat, ac, desc in meaningful:
            state = _infer_state(party, ac, desc)

            if prev_state is None:
                # First action — start a segment
                prev_state = state
                prev_start = cat
                prev_action_ids = [aid]
            elif state != prev_state:
                # State change — close previous segment, start new one
                duration = None
                if prev_start and cat:
                    duration = round((cat - prev_start).total_seconds() / 60.0, 2)
                segments.append({
                    "state_name": prev_state,
                    "start_at": prev_start,
                    "end_at": cat,
                    "duration_minutes": duration,
                    "action_ids": prev_action_ids,
                })
                prev_state = state
                prev_start = cat
                prev_action_ids = [aid]
            else:
                prev_action_ids.append(aid)

        # Close the last segment (open-ended — no end_at)
        if prev_state is not None:
            segments.append({
                "state_name": prev_state,
                "start_at": prev_start,
                "end_at": None,
                "duration_minutes": None,
                "action_ids": prev_action_ids,
            })

        for seg in segments:
            db.execute("""
                INSERT INTO ticket_wait_states (
                    ticket_id, state_name, start_at, end_at, duration_minutes,
                    inferred_from_action_ids, confidence, inference_method,
                    created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
            """, (
                tid, seg["state_name"], seg["start_at"], seg["end_at"],
                seg["duration_minutes"],
                json.dumps(seg["action_ids"]) if seg["action_ids"] else None,
                0.8, "rule_based_v1", now, now,
            ))

        count += 1

    print(f"[analytics] Rebuilt wait states for {count} ticket(s).", flush=True)
    return count


# ── D. Snapshot tickets daily ────────────────────────────────────────

def snapshot_tickets_daily(
    snapshot_date: date | None = None,
    ticket_ids: list[int] | None = None,
) -> int:
    """Create or upsert snapshot rows for the given date (default: today).

    Populates open_flag, status, owner, product_name, customer, age_days,
    days_since_modified, priority, overall_complexity, waiting_state,
    high_priority_flag, high_complexity_flag.

    Returns count of snapshots written.
    """
    if snapshot_date is None:
        snapshot_date = date.today()

    # Build ticket filter
    if ticket_ids is not None:
        placeholders = ",".join(["%s"] * len(ticket_ids))
        where_clause = f"WHERE t.ticket_id IN ({placeholders})"
        params: tuple = tuple(ticket_ids)
    else:
        where_clause = ""
        params = ()

    # Fetch all tickets with latest analytics
    sql = f"""
        SELECT
            t.ticket_id,
            t.ticket_number,
            t.ticket_name,
            t.status,
            t.assignee,
            t.product_name,
            t.customer,
            t.date_created,
            t.date_modified,
            t.closed_at,
            t.days_opened,
            t.days_since_modified,
            t.source_updated_at
        FROM tickets t
        {where_clause}
        ORDER BY t.ticket_id;
    """
    ticket_rows = db.fetch_all(sql, params)

    if not ticket_rows:
        print(f"[analytics] No tickets found for snapshot.", flush=True)
        return 0

    # Fetch latest priority scores
    priority_map: dict[int, int] = {}
    prows = db.fetch_all(
        "SELECT DISTINCT ON (ticket_id) ticket_id, priority "
        "FROM ticket_priority_scores ORDER BY ticket_id, scored_at DESC, id DESC;"
    )
    for r in prows:
        if r[1] is not None:
            priority_map[r[0]] = r[1]

    # Fetch latest complexity scores
    complexity_map: dict[int, int] = {}
    crows = db.fetch_all(
        "SELECT DISTINCT ON (ticket_id) ticket_id, overall_complexity "
        "FROM ticket_complexity_scores ORDER BY ticket_id, scored_at DESC, id DESC;"
    )
    for r in crows:
        if r[1] is not None:
            complexity_map[r[0]] = r[1]

    # Fetch latest wait state per ticket (most recent non-closed segment)
    wait_map: dict[int, str] = {}
    wrows = db.fetch_all(
        "SELECT DISTINCT ON (ticket_id) ticket_id, state_name "
        "FROM ticket_wait_states ORDER BY ticket_id, start_at DESC;"
    )
    for r in wrows:
        wait_map[r[0]] = r[1]

    count = 0
    for row in ticket_rows:
        tid = row[0]
        ticket_number = row[1]
        ticket_name = row[2]
        status = row[3]
        owner = row[4]
        product_name = row[5]
        customer = row[6]
        date_created = row[7]
        date_modified = row[8]
        closed_at = row[9]
        days_opened_val = row[10]
        days_since_modified_val = row[11]
        source_updated_at = row[12]

        # Derive open_flag
        open_flag = closed_at is None and (status or "").lower() not in ("closed", "completed")

        # Derive age_days
        age_days = None
        if date_created:
            age_days = (datetime.now(timezone.utc) - date_created).days
        elif days_opened_val is not None:
            age_days = float(days_opened_val)

        # Derive days_since_modified
        dsm = None
        if date_modified:
            dsm = round((datetime.now(timezone.utc) - date_modified).total_seconds() / 86400.0, 2)
        elif days_since_modified_val is not None:
            dsm = float(days_since_modified_val)

        priority = priority_map.get(tid)
        overall_complexity = complexity_map.get(tid)
        waiting_state = wait_map.get(tid)

        # Threshold flags
        high_priority_flag = priority is not None and priority <= 3
        high_complexity_flag = overall_complexity is not None and overall_complexity >= 4

        db.execute("""
            INSERT INTO ticket_snapshots_daily (
                snapshot_date, ticket_id, ticket_number, ticket_name,
                status, owner, product_name, customer,
                open_flag, age_days, days_since_modified,
                priority, overall_complexity, waiting_state,
                high_priority_flag, high_complexity_flag, source_updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (snapshot_date, ticket_id) DO UPDATE SET
                ticket_number       = EXCLUDED.ticket_number,
                ticket_name         = EXCLUDED.ticket_name,
                status              = EXCLUDED.status,
                owner               = EXCLUDED.owner,
                product_name        = EXCLUDED.product_name,
                customer            = EXCLUDED.customer,
                open_flag           = EXCLUDED.open_flag,
                age_days            = EXCLUDED.age_days,
                days_since_modified = EXCLUDED.days_since_modified,
                priority            = EXCLUDED.priority,
                overall_complexity  = EXCLUDED.overall_complexity,
                waiting_state       = EXCLUDED.waiting_state,
                high_priority_flag  = EXCLUDED.high_priority_flag,
                high_complexity_flag= EXCLUDED.high_complexity_flag,
                source_updated_at   = EXCLUDED.source_updated_at;
        """, (
            snapshot_date, tid, ticket_number, ticket_name,
            status, owner, product_name, customer,
            open_flag, age_days, dsm,
            priority, overall_complexity, waiting_state,
            high_priority_flag, high_complexity_flag, source_updated_at,
        ))
        count += 1

    print(f"[analytics] Snapshot for {snapshot_date}: {count} ticket(s).", flush=True)
    return count


# ── E. Rebuild customer_ticket_health ────────────────────────────────

def rebuild_customer_ticket_health(as_of_date: date | None = None) -> int:
    """Refresh customer_ticket_health for the given date (default: today).

    Derives from latest snapshots + latest analytics.

    ticket_load_pressure_score formula (first-pass heuristic):
        = open_ticket_count
          + 2 * high_priority_count
          + 1.5 * high_complexity_count
          + 3 * frustration_count_90d

    Returns count of customer rows upserted.
    """
    if as_of_date is None:
        as_of_date = date.today()

    now = datetime.now(timezone.utc)

    # Get latest snapshot date <= as_of_date for each customer
    # Use the snapshot closest to as_of_date
    rows = db.fetch_all("""
        WITH latest AS (
            SELECT
                customer,
                COUNT(*) FILTER (WHERE open_flag)                         AS open_ticket_count,
                COUNT(*) FILTER (WHERE open_flag AND high_priority_flag)   AS high_priority_count,
                COUNT(*) FILTER (WHERE open_flag AND high_complexity_flag) AS high_complexity_count,
                AVG(overall_complexity) FILTER (WHERE open_flag)           AS avg_complexity,
                ARRAY_AGG(DISTINCT ticket_id) FILTER (WHERE open_flag)    AS open_ticket_ids
            FROM ticket_snapshots_daily
            WHERE snapshot_date = (
                SELECT MAX(snapshot_date) FROM ticket_snapshots_daily
                WHERE snapshot_date <= %s
            )
            AND customer IS NOT NULL
            GROUP BY customer
        )
        SELECT
            l.customer,
            COALESCE(l.open_ticket_count, 0),
            COALESCE(l.high_priority_count, 0),
            COALESCE(l.high_complexity_count, 0),
            l.avg_complexity,
            l.open_ticket_ids
        FROM latest l;
    """, (as_of_date,))

    if not rows:
        print(f"[analytics] No snapshot data for customer health.", flush=True)
        return 0

    # Fetch avg elapsed_drag and frustration from latest enrichments
    drag_map: dict[int, float] = {}
    drows = db.fetch_all(
        "SELECT DISTINCT ON (ticket_id) ticket_id, elapsed_drag "
        "FROM ticket_complexity_scores ORDER BY ticket_id, scored_at DESC, id DESC;"
    )
    for r in drows:
        if r[1] is not None:
            drag_map[r[0]] = float(r[1])

    frust_map: dict[int, bool] = {}
    frows = db.fetch_all(
        "SELECT DISTINCT ON (ticket_id) ticket_id, frustrated "
        "FROM ticket_sentiment ORDER BY ticket_id, scored_at DESC, id DESC;"
    )
    for r in frows:
        frust_map[r[0]] = (r[1] or "").lower() == "yes"

    # Fetch top cluster ids per ticket
    cluster_map: dict[int, str] = {}
    clrows = db.fetch_all(
        "SELECT DISTINCT ON (ticket_id) ticket_id, cluster_id "
        "FROM ticket_clusters ORDER BY ticket_id, assigned_at DESC;"
    )
    for r in clrows:
        cluster_map[r[0]] = r[1]

    count = 0
    for customer, open_count, hp_count, hc_count, avg_cx, open_tids in rows:
        open_tids = open_tids or []

        # Compute avg_elapsed_drag for open tickets
        drags = [drag_map[t] for t in open_tids if t in drag_map]
        avg_drag = round(sum(drags) / len(drags), 2) if drags else None

        # Count frustrations (among recent tickets — 90d window from as_of_date)
        frust_count = sum(1 for t in open_tids if frust_map.get(t, False))

        # Top clusters
        clusters = [cluster_map[t] for t in open_tids if t in cluster_map]
        top_clusters = json.dumps(list(set(clusters))[:10]) if clusters else None

        # Top products
        prod_rows = db.fetch_all(
            "SELECT DISTINCT product_name FROM tickets WHERE ticket_id = ANY(%s) AND product_name IS NOT NULL;",
            (open_tids,),
        )
        top_products = json.dumps([r[0] for r in prod_rows][:10]) if prod_rows else None

        # Pressure score
        pressure = (
            open_count
            + 2 * hp_count
            + 1.5 * hc_count
            + 3 * frust_count
        )

        db.execute("""
            INSERT INTO customer_ticket_health (
                as_of_date, customer, open_ticket_count, high_priority_count,
                high_complexity_count, avg_complexity, avg_elapsed_drag,
                reopen_count_90d, frustration_count_90d,
                top_cluster_ids, top_products, ticket_load_pressure_score,
                created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (as_of_date, customer) DO UPDATE SET
                open_ticket_count          = EXCLUDED.open_ticket_count,
                high_priority_count        = EXCLUDED.high_priority_count,
                high_complexity_count      = EXCLUDED.high_complexity_count,
                avg_complexity             = EXCLUDED.avg_complexity,
                avg_elapsed_drag           = EXCLUDED.avg_elapsed_drag,
                reopen_count_90d           = EXCLUDED.reopen_count_90d,
                frustration_count_90d      = EXCLUDED.frustration_count_90d,
                top_cluster_ids            = EXCLUDED.top_cluster_ids,
                top_products               = EXCLUDED.top_products,
                ticket_load_pressure_score = EXCLUDED.ticket_load_pressure_score;
        """, (
            as_of_date, customer, open_count, hp_count,
            hc_count,
            round(float(avg_cx), 2) if avg_cx is not None else None,
            avg_drag,
            0,  # reopen_count_90d — not yet implemented
            frust_count,
            top_clusters, top_products, round(pressure, 2),
            now,
        ))
        count += 1

    print(f"[analytics] Customer health for {as_of_date}: {count} customer(s).", flush=True)
    return count


# ── F. Rebuild product_ticket_health ─────────────────────────────────

def rebuild_product_ticket_health(as_of_date: date | None = None) -> int:
    """Refresh product_ticket_health for the given date (default: today).

    Derives from latest snapshots + latest analytics.
    Returns count of product rows upserted.
    """
    if as_of_date is None:
        as_of_date = date.today()

    now = datetime.now(timezone.utc)

    rows = db.fetch_all("""
        WITH latest AS (
            SELECT
                product_name,
                COUNT(*)                                                     AS ticket_volume,
                ARRAY_AGG(DISTINCT ticket_id)                                AS all_ticket_ids
            FROM ticket_snapshots_daily
            WHERE snapshot_date = (
                SELECT MAX(snapshot_date) FROM ticket_snapshots_daily
                WHERE snapshot_date <= %s
            )
            AND product_name IS NOT NULL
            GROUP BY product_name
        )
        SELECT product_name, ticket_volume, all_ticket_ids FROM latest;
    """, (as_of_date,))

    if not rows:
        print(f"[analytics] No snapshot data for product health.", flush=True)
        return 0

    # Fetch latest complexity/drag/coordination per ticket
    cx_map: dict[int, dict] = {}
    cxrows = db.fetch_all(
        "SELECT DISTINCT ON (ticket_id) ticket_id, overall_complexity, coordination_load, elapsed_drag "
        "FROM ticket_complexity_scores ORDER BY ticket_id, scored_at DESC, id DESC;"
    )
    for r in cxrows:
        cx_map[r[0]] = {
            "overall": r[1],
            "coordination": r[2],
            "drag": r[3],
        }

    # Fetch latest wait state per ticket
    wait_map: dict[int, str] = {}
    wrows = db.fetch_all(
        "SELECT DISTINCT ON (ticket_id) ticket_id, state_name "
        "FROM ticket_wait_states ORDER BY ticket_id, start_at DESC;"
    )
    for r in wrows:
        wait_map[r[0]] = r[1]

    # Fetch cluster and mechanism info per ticket
    cluster_map: dict[int, str] = {}
    clrows = db.fetch_all(
        "SELECT DISTINCT ON (ticket_id) ticket_id, cluster_id "
        "FROM ticket_clusters ORDER BY ticket_id, assigned_at DESC;"
    )
    for r in clrows:
        cluster_map[r[0]] = r[1]

    mechanism_map: dict[int, str] = {}
    mrows = db.fetch_all(
        "SELECT DISTINCT ON (ticket_id) ticket_id, mechanism_summary "
        "FROM ticket_issue_summaries WHERE mechanism_summary IS NOT NULL "
        "ORDER BY ticket_id, scored_at DESC, id DESC;"
    )
    for r in mrows:
        mechanism_map[r[0]] = r[1]

    # Check action classes for dev involvement
    dev_action_map: dict[int, bool] = {}
    darows = db.fetch_all(
        "SELECT DISTINCT ticket_id FROM ticket_actions WHERE action_class = 'technical_work';"
    )
    for r in darows:
        dev_action_map[r[0]] = True

    count = 0
    for product_name, volume, tids in rows:
        tids = tids or []

        # Avg complexity, coordination, drag
        cxs = [cx_map[t]["overall"] for t in tids if t in cx_map and cx_map[t]["overall"] is not None]
        coords = [cx_map[t]["coordination"] for t in tids if t in cx_map and cx_map[t]["coordination"] is not None]
        drags = [cx_map[t]["drag"] for t in tids if t in cx_map and cx_map[t]["drag"] is not None]

        avg_cx = round(sum(cxs) / len(cxs), 2) if cxs else None
        avg_coord = round(sum(coords) / len(coords), 2) if coords else None
        avg_drag = round(sum(drags) / len(drags), 2) if drags else None

        # Top clusters
        clusters = [cluster_map[t] for t in tids if t in cluster_map]
        top_clusters = json.dumps(list(set(clusters))[:10]) if clusters else None

        # Top mechanisms
        mechanisms = [mechanism_map[t] for t in tids if t in mechanism_map]
        top_mechanisms = json.dumps(list(set(mechanisms))[:10]) if mechanisms else None

        # dev_touched_rate: fraction of tickets with technical_work actions
        dev_count = sum(1 for t in tids if dev_action_map.get(t, False))
        dev_touched_rate = round(dev_count / len(tids), 4) if tids else None

        # customer_wait_rate: fraction of tickets currently waiting_on_customer
        cust_wait_count = sum(1 for t in tids if wait_map.get(t) == "waiting_on_customer")
        customer_wait_rate = round(cust_wait_count / len(tids), 4) if tids else None

        db.execute("""
            INSERT INTO product_ticket_health (
                as_of_date, product_name, ticket_volume,
                avg_complexity, avg_coordination_load, avg_elapsed_drag,
                top_clusters, top_mechanisms,
                dev_touched_rate, customer_wait_rate, created_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (as_of_date, product_name) DO UPDATE SET
                ticket_volume          = EXCLUDED.ticket_volume,
                avg_complexity         = EXCLUDED.avg_complexity,
                avg_coordination_load  = EXCLUDED.avg_coordination_load,
                avg_elapsed_drag       = EXCLUDED.avg_elapsed_drag,
                top_clusters           = EXCLUDED.top_clusters,
                top_mechanisms         = EXCLUDED.top_mechanisms,
                dev_touched_rate       = EXCLUDED.dev_touched_rate,
                customer_wait_rate     = EXCLUDED.customer_wait_rate;
        """, (
            as_of_date, product_name, volume,
            avg_cx, avg_coord, avg_drag,
            top_clusters, top_mechanisms,
            dev_touched_rate, customer_wait_rate, now,
        ))
        count += 1

    print(f"[analytics] Product health for {as_of_date}: {count} product(s).", flush=True)
    return count


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Rebuild derived analytics tables from canonical DB state.",
    )
    sub = parser.add_subparsers(dest="command")

    for cmd in ("participants", "handoffs", "wait-states", "snapshot",
                "customer-health", "product-health", "all"):
        p = sub.add_parser(cmd, help={
            "participants": "Rebuild ticket_participants.",
            "handoffs": "Rebuild ticket_handoffs.",
            "wait-states": "Rebuild ticket_wait_states.",
            "snapshot": "Write today's ticket_snapshots_daily rows.",
            "customer-health": "Refresh customer_ticket_health.",
            "product-health": "Refresh product_ticket_health.",
            "all": "Run all analytics rebuilds.",
        }[cmd])
        if cmd in ("participants", "handoffs", "wait-states", "all"):
            p.add_argument("--ticket", "-t", help="Process only this ticket number.")
        if cmd in ("snapshot",):
            p.add_argument("--date", "-d", help="Snapshot date (YYYY-MM-DD). Default: today.")
            p.add_argument("--ticket", "-t", help="Process only this ticket number.")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    if not db._is_enabled():
        print("ERROR: DATABASE_URL is not set.", file=sys.stderr)
        sys.exit(1)

    # Ensure schema and tables exist
    db.migrate()

    ticket_number = getattr(args, "ticket", None)
    tids = _ticket_ids(ticket_number) if ticket_number else None

    snapshot_date = None
    if hasattr(args, "date") and args.date:
        snapshot_date = date.fromisoformat(args.date)

    if tids is not None:
        print(f"[analytics] Processing {len(tids)} ticket(s) …", flush=True)
    else:
        print(f"[analytics] Processing all tickets …", flush=True)

    if args.command in ("participants", "all"):
        rebuild_ticket_participants(tids)

    if args.command in ("handoffs", "all"):
        rebuild_ticket_handoffs(tids)

    if args.command in ("wait-states", "all"):
        rebuild_ticket_wait_states(tids)

    if args.command in ("snapshot", "all"):
        snapshot_tickets_daily(snapshot_date=snapshot_date, ticket_ids=tids)

    if args.command in ("customer-health", "all"):
        rebuild_customer_ticket_health(as_of_date=snapshot_date)

    if args.command in ("product-health", "all"):
        rebuild_product_ticket_health(as_of_date=snapshot_date)

    print("[analytics] Done.", flush=True)


if __name__ == "__main__":
    main()
