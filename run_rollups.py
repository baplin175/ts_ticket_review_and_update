"""
run_rollups.py — Rebuild action classification, thread rollups, and metrics
from canonical DB state.  No TeamSupport API calls required.

Usage:
    python run_rollups.py classify                  # classify all actions
    python run_rollups.py classify --ticket 29696   # classify actions for one ticket
    python run_rollups.py rollups                   # rebuild rollups for all tickets
    python run_rollups.py rollups --ticket 29696    # rebuild rollups for one ticket
    python run_rollups.py metrics                   # rebuild metrics for all tickets
    python run_rollups.py metrics --ticket 29696    # rebuild metrics for one ticket
    python run_rollups.py all                       # classify + rollups + metrics (all)
    python run_rollups.py all --ticket 29696        # classify + rollups + metrics (one)
    python run_rollups.py participants              # rebuild ticket_participants
    python run_rollups.py handoffs                  # rebuild ticket_handoffs
    python run_rollups.py wait_states               # rebuild ticket_wait_states
    python run_rollups.py snapshot                  # write today's ticket_snapshots_daily
    python run_rollups.py health                    # refresh customer + product health
    python run_rollups.py analytics                 # participants + handoffs + wait_states + snapshot + health
    python run_rollups.py full                      # all + analytics (everything)

Requires DATABASE_URL to be set and data to be ingested via run_ingest.py.
"""

import argparse
import hashlib
import json
import sys
from datetime import datetime, date, timezone
from typing import Optional

import psycopg2.extras

import db
from action_classifier import classify_action, is_noise, is_technical_substance


# ── Helpers ──────────────────────────────────────────────────────────

def _ticket_ids(ticket_number: str | None = None) -> list[int]:
    """Return ticket_id(s) to process.  If ticket_number is given, look it up."""
    if ticket_number:
        rows = db.fetch_all(
            "SELECT ticket_id FROM tickets WHERE ticket_number = %s;",
            (ticket_number,),
        )
        if not rows:
            print(f"[rollups] No ticket found with number {ticket_number}.", flush=True)
            return []
        return [r[0] for r in rows]
    else:
        rows = db.fetch_all("SELECT ticket_id FROM tickets ORDER BY ticket_id;")
        return [r[0] for r in rows]


def _sha256(text: str) -> str:
    """Return hex SHA-256 of the given text."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


# ── A. Classify actions ──────────────────────────────────────────────

def classify_actions(ticket_ids: list[int]) -> dict:
    """Apply deterministic classification to actions and UPDATE action_class in DB.

    Returns counts: {classified, skipped, total}.
    """
    classified = 0
    skipped = 0

    for tid in ticket_ids:
        rows = db.fetch_all(
            "SELECT action_id, cleaned_description, party, action_type, is_empty "
            "FROM ticket_actions WHERE ticket_id = %s ORDER BY created_at;",
            (tid,),
        )
        for action_id, desc, party, action_type, is_empty in rows:
            action_class = classify_action(desc, party=party, action_type=action_type, is_empty=is_empty)
            db.execute(
                "UPDATE ticket_actions SET action_class = %s WHERE action_id = %s;",
                (action_class, action_id),
            )
            classified += 1

    total = classified + skipped
    print(f"[rollups] Classified {classified} action(s) across {len(ticket_ids)} ticket(s).", flush=True)
    return {"classified": classified, "skipped": skipped, "total": total}


# ── B. Thread rollups ────────────────────────────────────────────────

def rebuild_rollups(ticket_ids: list[int]) -> int:
    """Rebuild ticket_thread_rollups from ticket_actions for each ticket.

    Returns count of tickets processed.
    """
    now = datetime.now(timezone.utc)
    count = 0
    tnum_map = db.ticket_numbers_for_ids(ticket_ids)

    for tid in ticket_ids:
        # Fetch all actions (sorted chronologically)
        rows = db.fetch_all(
            "SELECT action_id, cleaned_description, party, action_class, is_empty, "
            "       is_visible, creator_name, created_at "
            "FROM ticket_actions WHERE ticket_id = %s ORDER BY created_at;",
            (tid,),
        )

        # Build thread segments
        full_parts = []
        customer_visible_parts = []
        technical_core_parts = []
        latest_customer_text = None
        latest_inhance_text = None

        for aid, desc, party, ac, empty, vis, cname, cat in rows:
            text = (desc or "").strip()
            if not text or empty:
                continue

            # Prefix for thread context (includes timestamp for LLM sequence interpretation)
            ts = cat.strftime("%Y-%m-%d %H:%M") if cat else "unknown-date"
            prefix = f"[{ts} | {cname or party or '?'}]"
            line = f"{prefix} {text}"

            # full_thread_text: all non-empty actions, even noise
            full_parts.append(line)

            # customer_visible_text: non-noise actions
            if not is_noise(ac or "unknown"):
                customer_visible_parts.append(line)

            # technical_core_text: only technical substance
            if is_technical_substance(ac or "unknown"):
                technical_core_parts.append(line)

            # Track latest per party
            if party == "cust":
                latest_customer_text = text
            elif party == "inh":
                latest_inhance_text = text

        full_thread = "\n\n".join(full_parts) if full_parts else None
        customer_visible = "\n\n".join(customer_visible_parts) if customer_visible_parts else None
        technical_core = "\n\n".join(technical_core_parts) if technical_core_parts else None

        # summary_for_embedding: customer_visible (non-noise) capped at ~4000 chars
        summary_for_embedding = None
        if customer_visible:
            summary_for_embedding = customer_visible[:4000]

        thread_hash = _sha256(full_thread) if full_thread else None
        technical_core_hash = _sha256(technical_core) if technical_core else None

        # Upsert into ticket_thread_rollups
        db.execute("""
            INSERT INTO ticket_thread_rollups (
                ticket_id, ticket_number, full_thread_text, customer_visible_text,
                latest_customer_text, latest_inhance_text,
                technical_core_text, summary_for_embedding,
                thread_hash, technical_core_hash, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticket_id) DO UPDATE SET
                ticket_number         = EXCLUDED.ticket_number,
                full_thread_text      = EXCLUDED.full_thread_text,
                customer_visible_text = EXCLUDED.customer_visible_text,
                latest_customer_text  = EXCLUDED.latest_customer_text,
                latest_inhance_text   = EXCLUDED.latest_inhance_text,
                technical_core_text   = EXCLUDED.technical_core_text,
                summary_for_embedding = EXCLUDED.summary_for_embedding,
                thread_hash           = EXCLUDED.thread_hash,
                technical_core_hash   = EXCLUDED.technical_core_hash,
                updated_at            = EXCLUDED.updated_at;
        """, (
            tid, tnum_map.get(tid), full_thread, customer_visible,
            latest_customer_text, latest_inhance_text,
            technical_core, summary_for_embedding,
            thread_hash, technical_core_hash, now,
        ))
        count += 1

    print(f"[rollups] Rebuilt rollups for {count} ticket(s).", flush=True)
    return count


# ── C. Metrics ───────────────────────────────────────────────────────

def rebuild_metrics(ticket_ids: list[int]) -> int:
    """Rebuild ticket_metrics from ticket_actions for each ticket.

    Returns count of tickets processed.

    Heuristics / approximations:
    - first_response_at: earliest inh action after the first cust action
    - last_human_activity_at: latest inh or cust action (ignores system actions)
    - handoff_count: number of times the party switches between consecutive
      human actions (inh→cust or cust→inh). This is a rough proxy — it counts
      back-and-forth turns, not literal reassignments.
    """
    now = datetime.now(timezone.utc)
    count = 0
    tnum_map = db.ticket_numbers_for_ids(ticket_ids)

    for tid in ticket_ids:
        rows = db.fetch_all(
            "SELECT action_id, party, is_empty, created_at, creator_id "
            "FROM ticket_actions WHERE ticket_id = %s ORDER BY created_at;",
            (tid,),
        )

        # Fetch ticket-level fields for date_created and days_opened
        trow = db.fetch_one(
            "SELECT date_created, days_opened FROM tickets WHERE ticket_id = %s;",
            (tid,),
        )
        date_created = trow[0] if trow else None
        days_open = float(trow[1]) if trow and trow[1] is not None else None

        action_count = len(rows)
        nonempty_count = 0
        cust_count = 0
        inh_count = 0
        participants = set()
        first_cust_at = None
        first_response_at = None
        last_human_at = None
        handoffs = 0
        prev_party = None

        for aid, party, is_empty, cat, cid in rows:
            if not is_empty:
                nonempty_count += 1

            if party == "cust":
                cust_count += 1
                if first_cust_at is None and cat:
                    first_cust_at = cat
            elif party == "inh":
                inh_count += 1

            # Track participants
            if cid:
                participants.add(cid)

            # Last human activity
            if party in ("inh", "cust") and cat:
                last_human_at = cat

            # First response: first inh action after first cust action
            if (first_response_at is None
                    and first_cust_at is not None
                    and party == "inh" and cat):
                first_response_at = cat

            # Handoff counting
            if party in ("inh", "cust"):
                if prev_party and prev_party != party:
                    handoffs += 1
                prev_party = party

        empty_ratio = None
        if action_count > 0:
            empty_ratio = round((action_count - nonempty_count) / action_count, 4)

        # Compute hours to first response
        hours_to_first_response = None
        if first_response_at and date_created:
            delta = (first_response_at - date_created).total_seconds()
            hours_to_first_response = round(delta / 3600, 2)

        # Upsert into ticket_metrics
        db.execute("""
            INSERT INTO ticket_metrics (
                ticket_id, ticket_number, action_count, nonempty_action_count,
                customer_message_count, inhance_message_count,
                distinct_participant_count, first_response_at,
                last_human_activity_at, empty_action_ratio,
                handoff_count, date_created, hours_to_first_response,
                days_open, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (ticket_id) DO UPDATE SET
                ticket_number              = EXCLUDED.ticket_number,
                action_count               = EXCLUDED.action_count,
                nonempty_action_count      = EXCLUDED.nonempty_action_count,
                customer_message_count     = EXCLUDED.customer_message_count,
                inhance_message_count      = EXCLUDED.inhance_message_count,
                distinct_participant_count = EXCLUDED.distinct_participant_count,
                first_response_at          = EXCLUDED.first_response_at,
                last_human_activity_at     = EXCLUDED.last_human_activity_at,
                empty_action_ratio         = EXCLUDED.empty_action_ratio,
                handoff_count              = EXCLUDED.handoff_count,
                date_created               = EXCLUDED.date_created,
                hours_to_first_response    = EXCLUDED.hours_to_first_response,
                days_open                  = EXCLUDED.days_open,
                updated_at                 = EXCLUDED.updated_at;
        """, (
            tid, tnum_map.get(tid), action_count, nonempty_count,
            cust_count, inh_count,
            len(participants), first_response_at,
            last_human_at, empty_ratio,
            handoffs, date_created, hours_to_first_response,
            days_open, now,
        ))
        count += 1

    print(f"[rollups] Rebuilt metrics for {count} ticket(s).", flush=True)
    return count


# ── D. Participants ──────────────────────────────────────────────────

# Maps action_class to participant_type derivation.
_PARTY_TO_TYPE = {"inh": "inhance", "cust": "customer"}


def rebuild_ticket_participants(ticket_ids: list[int]) -> int:
    """Rebuild ticket_participants from ticket_actions.  Full-refresh per ticket.

    Participant_id is creator_id when available, else a synthetic
    ``party:creator_name`` key.  participant_type is derived from party.
    """
    if not ticket_ids:
        return 0
    db.delete_for_tickets("ticket_participants", ticket_ids)

    now = datetime.now(timezone.utc)
    count = 0
    tnum_map = db.ticket_numbers_for_ids(ticket_ids)

    for tid in ticket_ids:
        rows = db.fetch_all(
            "SELECT action_id, creator_id, creator_name, party, created_at, is_empty "
            "FROM ticket_actions WHERE ticket_id = %s ORDER BY created_at;",
            (tid,),
        )

        # Identify first response action_id (first inh after first cust)
        first_cust_at = None
        first_response_aid = None
        for aid, cid, cname, party, cat, empty in rows:
            if party == "cust" and first_cust_at is None and cat:
                first_cust_at = cat
            if (first_response_aid is None
                    and first_cust_at is not None
                    and party == "inh" and cat):
                first_response_aid = aid
                break

        # Aggregate per participant
        participants: dict[str, dict] = {}
        for aid, cid, cname, party, cat, empty in rows:
            if not party or party not in ("inh", "cust"):
                continue
            pid = str(cid) if cid else f"{party}:{cname or 'unknown'}"
            if pid not in participants:
                participants[pid] = {
                    "participant_name": cname or None,
                    "participant_type": _PARTY_TO_TYPE.get(party, party),
                    "first_seen_at": cat,
                    "last_seen_at": cat,
                    "action_count": 0,
                    "first_response_flag": False,
                }
            p = participants[pid]
            p["action_count"] += 1
            if cat:
                if p["first_seen_at"] is None or cat < p["first_seen_at"]:
                    p["first_seen_at"] = cat
                if p["last_seen_at"] is None or cat > p["last_seen_at"]:
                    p["last_seen_at"] = cat
            if aid == first_response_aid:
                p["first_response_flag"] = True

        insert_rows = []
        for pid, p in participants.items():
            insert_rows.append((
                tid, tnum_map.get(tid), pid, p["participant_name"], p["participant_type"],
                p["first_seen_at"], p["last_seen_at"],
                p["action_count"], p["first_response_flag"],
                now, now,
            ))

        db.bulk_insert(
            "ticket_participants",
            ["ticket_id", "ticket_number", "participant_id", "participant_name", "participant_type",
             "first_seen_at", "last_seen_at", "action_count", "first_response_flag",
             "created_at", "updated_at"],
            insert_rows,
        )
        count += 1

    print(f"[rollups] Rebuilt participants for {count} ticket(s).", flush=True)
    return count


# ── E. Handoffs ──────────────────────────────────────────────────────

def rebuild_ticket_handoffs(ticket_ids: list[int]) -> int:
    """Rebuild ticket_handoffs by detecting party/participant transitions.

    A handoff is inferred whenever a different participant (or party) sends the
    next human action.  Full-refresh per ticket.
    """
    if not ticket_ids:
        return 0
    db.delete_for_tickets("ticket_handoffs", ticket_ids)

    now = datetime.now(timezone.utc)
    count = 0
    tnum_map = db.ticket_numbers_for_ids(ticket_ids)

    for tid in ticket_ids:
        rows = db.fetch_all(
            "SELECT action_id, creator_id, creator_name, party, created_at "
            "FROM ticket_actions WHERE ticket_id = %s AND party IN ('inh','cust') "
            "ORDER BY created_at;",
            (tid,),
        )

        insert_rows = []
        prev = None  # (party, participant_id, participant_name)
        for aid, cid, cname, party, cat in rows:
            pid = str(cid) if cid else f"{party}:{cname or 'unknown'}"
            if prev is not None:
                prev_party, prev_pid, _ = prev
                if prev_party != party or prev_pid != pid:
                    # Infer reason heuristic
                    reason = None
                    if prev_party != party:
                        reason = f"party_switch:{prev_party}->{party}"
                    else:
                        reason = f"participant_switch_within_{party}"
                    insert_rows.append((
                        tid, tnum_map.get(tid), prev_party, party, prev_pid, pid,
                        cat, reason, aid, 0.8, now,
                    ))
            prev = (party, pid, cname)

        db.bulk_insert(
            "ticket_handoffs",
            ["ticket_id", "ticket_number", "from_party", "to_party", "from_participant_id",
             "to_participant_id", "handoff_at", "handoff_reason",
             "inferred_from_action_id", "confidence", "created_at"],
            insert_rows,
        )
        count += 1

    print(f"[rollups] Rebuilt handoffs for {count} ticket(s).", flush=True)
    return count


# ── F. Wait states ───────────────────────────────────────────────────

# Mapping from action_class to a wait-state name.
_CLASS_TO_STATE = {
    "waiting_on_customer":      "waiting_on_customer",
    "customer_problem_statement": "waiting_on_support",
    "technical_work":           "active_work",
    "status_update":            "active_work",
    "delivery_confirmation":    "active_work",
    "scheduling":               "scheduled",
}


def _infer_state(action_class: str | None, party: str | None) -> str:
    """Map an action to a wait-state name.

    Heuristic (first pass):
    - If action_class maps directly, use it.
    - Otherwise fall back to party: customer action ⇒ waiting_on_support,
      inhance action ⇒ waiting_on_customer (the other side now waits).
    """
    if action_class and action_class in _CLASS_TO_STATE:
        return _CLASS_TO_STATE[action_class]
    if party == "cust":
        return "waiting_on_support"
    if party == "inh":
        return "waiting_on_customer"
    return "waiting_on_support"  # default


def rebuild_ticket_wait_states(ticket_ids: list[int]) -> int:
    """Rebuild ticket_wait_states from action stream.  Full-refresh per ticket.

    Each action opens a new segment whose state is inferred from the action_class
    and party.  The previous segment is closed at the new action's timestamp.
    The final segment remains open (end_at IS NULL) unless the ticket is closed.
    """
    if not ticket_ids:
        return 0
    db.delete_for_tickets("ticket_wait_states", ticket_ids)

    now = datetime.now(timezone.utc)
    count = 0
    tnum_map = db.ticket_numbers_for_ids(ticket_ids)

    for tid in ticket_ids:
        rows = db.fetch_all(
            "SELECT action_id, created_at, action_class, party "
            "FROM ticket_actions WHERE ticket_id = %s AND party IN ('inh','cust') "
            "ORDER BY created_at;",
            (tid,),
        )
        if not rows:
            count += 1
            continue

        # Fetch closed_at for closing the last segment
        trow = db.fetch_one(
            "SELECT closed_at FROM tickets WHERE ticket_id = %s;", (tid,),
        )
        closed_at = trow[0] if trow and trow[0] else None

        segments: list[tuple] = []
        prev_start = None
        prev_state = None
        prev_aids: list[int] = []

        for aid, cat, ac, party in rows:
            state = _infer_state(ac, party)
            if prev_start is not None and cat and prev_state is not None:
                dur = (cat - prev_start).total_seconds() / 60.0
                segments.append((
                    tid, tnum_map.get(tid), prev_state, prev_start, cat, round(dur, 2),
                    json.dumps(prev_aids), 0.75, "action_class_heuristic",
                    now, now,
                ))
            prev_start = cat
            prev_state = state
            prev_aids = [aid]

        # Close with ticket closed_at or leave open
        if prev_start is not None and prev_state is not None:
            end = closed_at
            dur = None
            if end and prev_start:
                dur = round((end - prev_start).total_seconds() / 60.0, 2)
            segments.append((
                tid, tnum_map.get(tid), prev_state, prev_start, end, dur,
                json.dumps(prev_aids), 0.75, "action_class_heuristic",
                now, now,
            ))

        db.bulk_insert(
            "ticket_wait_states",
            ["ticket_id", "ticket_number", "state_name", "start_at", "end_at", "duration_minutes",
             "inferred_from_action_ids", "confidence", "inference_method",
             "created_at", "updated_at"],
            segments,
        )
        count += 1

    print(f"[rollups] Rebuilt wait states for {count} ticket(s).", flush=True)
    return count


# ── G. Daily snapshots ───────────────────────────────────────────────

def snapshot_tickets_daily(
    snapshot_date: date | None = None,
    ticket_ids: list[int] | None = None,
) -> int:
    """Create/upsert ticket_snapshots_daily rows for the given date.

    If ticket_ids is None, snapshots ALL tickets.
    Joins latest priority, complexity, and wait-state data.
    """
    snap_date = snapshot_date or date.today()

    # Build ticket filter
    tid_clause = ""
    params: list = []
    if ticket_ids:
        placeholders = ",".join(["%s"] * len(ticket_ids))
        tid_clause = f"AND t.ticket_id IN ({placeholders})"
        params.extend(ticket_ids)

    rows = db.fetch_all(f"""
        SELECT
            t.ticket_id, t.ticket_number, t.ticket_name,
            t.status, t.assignee, t.product_name, t.customer,
            t.date_created, t.date_modified, t.source_updated_at,
            p.priority, c.overall_complexity
        FROM tickets t
        LEFT JOIN (
            SELECT DISTINCT ON (ticket_id) ticket_id, priority
            FROM ticket_priority_scores
            ORDER BY ticket_id, scored_at DESC, id DESC
        ) p ON p.ticket_id = t.ticket_id
        LEFT JOIN (
            SELECT DISTINCT ON (ticket_id) ticket_id, overall_complexity
            FROM ticket_complexity_scores
            ORDER BY ticket_id, scored_at DESC, id DESC
        ) c ON c.ticket_id = t.ticket_id
        WHERE TRUE {tid_clause};
    """, tuple(params))

    # Pre-fetch latest wait state per ticket
    ws_rows = db.fetch_all("""
        SELECT DISTINCT ON (ticket_id) ticket_id, state_name
        FROM ticket_wait_states
        ORDER BY ticket_id, start_at DESC;
    """)
    latest_ws: dict[int, str] = {r[0]: r[1] for r in ws_rows}

    count = 0
    for r in rows:
        (tid, tnum, tname, status, assignee, product, customer,
         date_created, date_modified, source_updated_at,
         priority, complexity) = r

        is_open = status is not None and status.lower() not in ("closed", "resolved")
        age_days = None
        if date_created:
            age_days = (datetime.now(timezone.utc) - date_created).days

        dsm = None
        if date_modified:
            dsm = (datetime.now(timezone.utc) - date_modified).days

        hp = priority is not None and priority <= 3
        hc = complexity is not None and complexity >= 4
        ws = latest_ws.get(tid)

        db.upsert_snapshot_daily({
            "snapshot_date": snap_date,
            "ticket_id": tid,
            "ticket_number": tnum,
            "ticket_name": tname,
            "status": status,
            "owner": assignee,
            "product_name": product,
            "customer": customer,
            "open_flag": is_open,
            "age_days": age_days,
            "days_since_modified": dsm,
            "priority": priority,
            "overall_complexity": complexity,
            "waiting_state": ws,
            "high_priority_flag": hp,
            "high_complexity_flag": hc,
            "source_updated_at": source_updated_at,
        })
        count += 1

    print(f"[rollups] Snapshot for {snap_date}: {count} ticket(s).", flush=True)
    return count


# ── H. Customer ticket health ────────────────────────────────────────

def rebuild_customer_ticket_health(as_of_date: date | None = None) -> int:
    """Refresh customer_ticket_health for the given date.  Full-refresh.

    Aggregates from tickets + latest priority/complexity/sentiment.
    ticket_load_pressure_score = open_ticket_count + 2*high_priority_count
                                 + 1.5*high_complexity_count + 3*frustration_count_90d
    """
    d = as_of_date or date.today()
    cutoff_90d = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    from datetime import timedelta
    cutoff_90d = cutoff_90d - timedelta(days=90)

    rows = db.fetch_all("""
        SELECT
            t.customer,
            COUNT(*) FILTER (WHERE t.status NOT IN ('Closed','Resolved')
                             OR t.closed_at IS NULL)                              AS open_ct,
            COUNT(*) FILTER (WHERE p.priority IS NOT NULL AND p.priority <= 3
                             AND (t.status NOT IN ('Closed','Resolved')
                                  OR t.closed_at IS NULL))                         AS hp_ct,
            COUNT(*) FILTER (WHERE c.overall_complexity >= 4
                             AND (t.status NOT IN ('Closed','Resolved')
                                  OR t.closed_at IS NULL))                         AS hc_ct,
            ROUND(AVG(c.overall_complexity), 2),
            ROUND(AVG(c.elapsed_drag), 2),
            COUNT(*) FILTER (WHERE s.frustrated = 'Yes')                           AS frust_ct,
            jsonb_agg(DISTINCT t.product_name)
                FILTER (WHERE t.product_name IS NOT NULL)                          AS top_products
        FROM tickets t
        LEFT JOIN (
            SELECT DISTINCT ON (ticket_id) ticket_id, priority
            FROM ticket_priority_scores ORDER BY ticket_id, scored_at DESC, id DESC
        ) p ON p.ticket_id = t.ticket_id
        LEFT JOIN (
            SELECT DISTINCT ON (ticket_id) ticket_id, overall_complexity, elapsed_drag
            FROM ticket_complexity_scores ORDER BY ticket_id, scored_at DESC, id DESC
        ) c ON c.ticket_id = t.ticket_id
        LEFT JOIN (
            SELECT DISTINCT ON (ticket_id) ticket_id, frustrated
            FROM ticket_sentiment ORDER BY ticket_id, scored_at DESC, id DESC
        ) s ON s.ticket_id = t.ticket_id
        WHERE t.customer IS NOT NULL
          AND (t.date_created >= %s
               OR t.closed_at IS NULL
               OR t.closed_at >= %s)
        GROUP BY t.customer;
    """, (cutoff_90d, cutoff_90d))

    # Pre-fetch cluster IDs per customer
    cluster_rows = db.fetch_all("""
        SELECT t.customer, jsonb_agg(DISTINCT tc.cluster_id)
        FROM tickets t
        JOIN ticket_clusters tc ON tc.ticket_id = t.ticket_id
        WHERE t.customer IS NOT NULL
        GROUP BY t.customer;
    """)
    cluster_map: dict[str, str] = {r[0]: r[1] for r in cluster_rows}

    count = 0
    for r in rows:
        (customer, open_ct, hp_ct, hc_ct, avg_c, avg_ed,
         frust_ct, top_products_json) = r
        # Pressure score: simple weighted formula (first-pass documented)
        pressure = (open_ct or 0) + 2 * (hp_ct or 0) + 1.5 * (hc_ct or 0) + 3 * (frust_ct or 0)

        top_clusters = cluster_map.get(customer)

        db.upsert_customer_health({
            "as_of_date": d,
            "customer": customer,
            "open_ticket_count": open_ct or 0,
            "high_priority_count": hp_ct or 0,
            "high_complexity_count": hc_ct or 0,
            "avg_complexity": avg_c,
            "avg_elapsed_drag": avg_ed,
            "reopen_count_90d": 0,  # Not inferable from current data; placeholder
            "frustration_count_90d": frust_ct or 0,
            "top_cluster_ids": psycopg2.extras.Json(top_clusters) if top_clusters else None,
            "top_products": psycopg2.extras.Json(top_products_json) if top_products_json else None,
            "ticket_load_pressure_score": round(pressure, 2),
        })
        count += 1

    print(f"[rollups] Customer health for {d}: {count} customer(s).", flush=True)
    return count


# ── I. Product ticket health ─────────────────────────────────────────

def rebuild_product_ticket_health(as_of_date: date | None = None) -> int:
    """Refresh product_ticket_health for the given date.  Full-refresh.

    Aggregates from tickets + latest complexity/sentiment + wait-state profile.
    - dev_touched_rate: fraction of tickets with action_class='technical_work'
    - customer_wait_rate: fraction of tickets whose latest wait state is waiting_on_customer
    """
    d = as_of_date or date.today()
    cutoff_90d = datetime(d.year, d.month, d.day, tzinfo=timezone.utc)
    from datetime import timedelta
    cutoff_90d = cutoff_90d - timedelta(days=90)

    rows = db.fetch_all("""
        SELECT
            t.product_name,
            COUNT(*)                                      AS vol,
            ROUND(AVG(c.overall_complexity), 2)           AS avg_c,
            ROUND(AVG(c.coordination_load), 2)            AS avg_cl,
            ROUND(AVG(c.elapsed_drag), 2)                 AS avg_ed
        FROM tickets t
        LEFT JOIN (
            SELECT DISTINCT ON (ticket_id) ticket_id,
                   overall_complexity, coordination_load, elapsed_drag
            FROM ticket_complexity_scores
            ORDER BY ticket_id, scored_at DESC, id DESC
        ) c ON c.ticket_id = t.ticket_id
        WHERE t.product_name IS NOT NULL
          AND (t.date_created >= %s
               OR t.closed_at IS NULL
               OR t.closed_at >= %s)
        GROUP BY t.product_name;
    """, (cutoff_90d, cutoff_90d))

    # Pre-fetch dev touched rate per product
    dev_rows = db.fetch_all("""
        SELECT t.product_name,
               ROUND(COUNT(DISTINCT ta.ticket_id)::numeric
                     / NULLIF(COUNT(DISTINCT t.ticket_id), 0), 4) AS dev_rate
        FROM tickets t
        LEFT JOIN ticket_actions ta ON ta.ticket_id = t.ticket_id
                                    AND ta.action_class = 'technical_work'
        WHERE t.product_name IS NOT NULL
          AND (t.date_created >= %s OR t.closed_at IS NULL OR t.closed_at >= %s)
        GROUP BY t.product_name;
    """, (cutoff_90d, cutoff_90d))
    dev_map = {r[0]: r[1] for r in dev_rows}

    # Pre-fetch customer wait rate per product
    cw_rows = db.fetch_all("""
        SELECT t.product_name,
               ROUND(COUNT(*) FILTER (WHERE ws.state_name = 'waiting_on_customer')::numeric
                     / NULLIF(COUNT(*), 0), 4) AS cw_rate
        FROM tickets t
        LEFT JOIN LATERAL (
            SELECT state_name FROM ticket_wait_states
            WHERE ticket_id = t.ticket_id ORDER BY start_at DESC LIMIT 1
        ) ws ON TRUE
        WHERE t.product_name IS NOT NULL
          AND (t.date_created >= %s OR t.closed_at IS NULL OR t.closed_at >= %s)
        GROUP BY t.product_name;
    """, (cutoff_90d, cutoff_90d))
    cw_map = {r[0]: r[1] for r in cw_rows}

    # Pre-fetch clusters + mechanisms per product
    cluster_rows = db.fetch_all("""
        SELECT t.product_name, jsonb_agg(DISTINCT tc.cluster_id)
        FROM tickets t
        JOIN ticket_clusters tc ON tc.ticket_id = t.ticket_id
        WHERE t.product_name IS NOT NULL
        GROUP BY t.product_name;
    """)
    cluster_map = {r[0]: r[1] for r in cluster_rows}

    mech_rows = db.fetch_all("""
        SELECT t.product_name, jsonb_agg(DISTINCT iss.mechanism_summary)
        FROM tickets t
        JOIN (
            SELECT DISTINCT ON (ticket_id) ticket_id, mechanism_summary
            FROM ticket_issue_summaries ORDER BY ticket_id, scored_at DESC, id DESC
        ) iss ON iss.ticket_id = t.ticket_id
        WHERE t.product_name IS NOT NULL AND iss.mechanism_summary IS NOT NULL
        GROUP BY t.product_name;
    """)
    mech_map = {r[0]: r[1] for r in mech_rows}

    count = 0
    for r in rows:
        product, vol, avg_c, avg_cl, avg_ed = r
        tc = cluster_map.get(product)
        tm = mech_map.get(product)
        db.upsert_product_health({
            "as_of_date": d,
            "product_name": product,
            "ticket_volume": vol or 0,
            "avg_complexity": avg_c,
            "avg_coordination_load": avg_cl,
            "avg_elapsed_drag": avg_ed,
            "top_clusters": psycopg2.extras.Json(tc) if tc else None,
            "top_mechanisms": psycopg2.extras.Json(tm) if tm else None,
            "dev_touched_rate": dev_map.get(product),
            "customer_wait_rate": cw_map.get(product),
        })
        count += 1

    print(f"[rollups] Product health for {d}: {count} product(s).", flush=True)
    return count


# ── Public orchestration helpers (called by run_ingest post-sync) ────

def run_analytics_for_tickets(ticket_ids: list[int]) -> None:
    """Run all analytics rebuild steps for the given ticket_ids.

    Steps: participants → handoffs → wait_states → snapshot → health.
    Called by run_ingest.py after a successful sync.
    """
    if not ticket_ids:
        return
    print(f"[analytics] Rebuilding analytics for {len(ticket_ids)} ticket(s)…", flush=True)
    rebuild_ticket_participants(ticket_ids)
    rebuild_ticket_handoffs(ticket_ids)
    rebuild_ticket_wait_states(ticket_ids)
    snapshot_tickets_daily(ticket_ids=ticket_ids)
    rebuild_customer_ticket_health()
    rebuild_product_ticket_health()
    print("[analytics] Done.", flush=True)


# ── CLI ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Rebuild classification, thread rollups, metrics, and analytics from DB state.",
    )
    sub = parser.add_subparsers(dest="command")

    _ALL_CMDS = {
        "classify":     "Classify all actions (deterministic rules).",
        "rollups":      "Rebuild ticket_thread_rollups.",
        "metrics":      "Rebuild ticket_metrics.",
        "all":          "Classify + rollups + metrics.",
        "participants": "Rebuild ticket_participants.",
        "handoffs":     "Rebuild ticket_handoffs.",
        "wait_states":  "Rebuild ticket_wait_states.",
        "snapshot":     "Write today's ticket_snapshots_daily rows.",
        "health":       "Refresh customer + product ticket health.",
        "analytics":    "Participants + handoffs + wait_states + snapshot + health.",
        "full":         "All + analytics (everything).",
    }
    for cmd, helptext in _ALL_CMDS.items():
        p = sub.add_parser(cmd, help=helptext)
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
    tids = _ticket_ids(ticket_number)

    if not tids:
        print("[rollups] No tickets to process.", flush=True)
        return

    print(f"[rollups] Processing {len(tids)} ticket(s) …", flush=True)

    cmd = args.command

    # ── Core rollups ──
    if cmd in ("classify", "all", "full"):
        classify_actions(tids)

    if cmd in ("rollups", "all", "full"):
        rebuild_rollups(tids)

    if cmd in ("metrics", "all", "full"):
        rebuild_metrics(tids)

    # ── Analytics ──
    if cmd in ("participants", "analytics", "full"):
        rebuild_ticket_participants(tids)

    if cmd in ("handoffs", "analytics", "full"):
        rebuild_ticket_handoffs(tids)

    if cmd in ("wait_states", "analytics", "full"):
        rebuild_ticket_wait_states(tids)

    if cmd in ("snapshot", "analytics", "full"):
        snapshot_tickets_daily(ticket_ids=tids)

    if cmd in ("health", "analytics", "full"):
        rebuild_customer_ticket_health()
        rebuild_product_ticket_health()

    print("[rollups] Done.", flush=True)


if __name__ == "__main__":
    main()
