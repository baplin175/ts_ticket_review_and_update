"""Shared rollup orchestration helpers."""

from __future__ import annotations


def run_full_rollups_pipeline(ticket_ids, *, classify, rollups, metrics, participants, handoffs, wait_states, snapshot, customer_health, product_health, daily_open_counts, db_enabled):
    """Run the complete rollup pipeline for the provided ticket ids."""
    if not db_enabled():
        return
    tids = ticket_ids()
    if not tids:
        print("[rollups] No tickets to process.", flush=True)
        return
    print(f"[rollups] Running full rollups for {len(tids)} ticket(s) …", flush=True)
    classify(tids)
    rollups(tids)
    metrics(tids)
    participants(tids)
    handoffs(tids)
    wait_states(tids)
    snapshot(ticket_ids=tids)
    customer_health()
    product_health()
    daily_open_counts()
    print("[rollups] Full rollups complete.", flush=True)


def run_analytics_pipeline(ticket_ids, *, participants, handoffs, wait_states, snapshot, customer_health, product_health, daily_open_counts):
    """Run analytics-only rebuild steps for the given ticket ids."""
    if not ticket_ids:
        return
    print(f"[analytics] Rebuilding analytics for {len(ticket_ids)} ticket(s)…", flush=True)
    participants(ticket_ids)
    handoffs(ticket_ids)
    wait_states(ticket_ids)
    snapshot()
    customer_health()
    product_health()
    daily_open_counts()
    print("[analytics] Done.", flush=True)
