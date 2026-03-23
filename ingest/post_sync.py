"""Post-sync rebuild and enrichment orchestration."""

from __future__ import annotations

import db


def rebuild_for_tickets(ticket_ids: list[int]) -> None:
    """Rebuild rollups and analytics for the touched ticket ids."""
    if not ticket_ids:
        return
    from run_rollups import (
        classify_actions,
        rebuild_metrics,
        rebuild_rollups,
        run_analytics_for_tickets,
    )

    print(
        f"\n[ingest] Post-sync: rebuilding rollups + analytics for {len(ticket_ids)} ticket(s)…",
        flush=True,
    )
    classify_actions(ticket_ids)
    rebuild_rollups(ticket_ids)
    rebuild_metrics(ticket_ids)
    run_analytics_for_tickets(ticket_ids)


def enrich_tickets(
    ticket_ids: list[int], *, sentiment: bool, complexity: bool = False, full_enrichment: bool
) -> None:
    """Run DB-backed enrichment for touched ticket ids."""
    if not ticket_ids:
        return

    num_map = db.ticket_numbers_for_ids(ticket_ids)
    touched_numbers = [num_map[tid] for tid in ticket_ids if tid in num_map]
    if not touched_numbers:
        return

    if sentiment or full_enrichment:
        from run_sentiment import main as sentiment_main

        print(
            f"\n[ingest] Post-sync: running sentiment for {len(touched_numbers)} ticket(s)…",
            flush=True,
        )
        try:
            sentiment_main(force=False, ticket_numbers=touched_numbers)
        except SystemExit:
            print("[ingest] Sentiment stage exited.", flush=True)
        except Exception as exc:
            print(f"[ingest] Sentiment error: {exc}", flush=True)

    if not (full_enrichment or complexity):
        return

    from run_complexity import main as complexity_main
    from run_rollups import rebuild_customer_ticket_health, rebuild_product_ticket_health
    if full_enrichment:
        from run_priority import main as priority_main

        print(
            f"\n[ingest] Post-sync: running priority for {len(touched_numbers)} ticket(s)…",
            flush=True,
        )
        try:
            priority_main(write_back=False, force=False, ticket_numbers=touched_numbers)
        except SystemExit:
            print("[ingest] Priority stage exited.", flush=True)
        except Exception as exc:
            print(f"[ingest] Priority error: {exc}", flush=True)

    print(
        f"\n[ingest] Post-sync: running complexity for {len(touched_numbers)} ticket(s)…",
        flush=True,
    )
    try:
        complexity_main(write_back=False, force=False, ticket_numbers=touched_numbers)
    except SystemExit:
        print("[ingest] Complexity stage exited.", flush=True)
    except Exception as exc:
        print(f"[ingest] Complexity error: {exc}", flush=True)

    print("\n[ingest] Post-enrich: rebuilding health rollups…", flush=True)
    try:
        rebuild_customer_ticket_health()
        rebuild_product_ticket_health()
    except Exception as exc:
        print(f"[ingest] Health rollup error: {exc}", flush=True)
