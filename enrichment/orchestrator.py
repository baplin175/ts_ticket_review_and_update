"""Shared DB-backed enrichment orchestration helpers."""

from __future__ import annotations

import time


def run_stage_batches(*, tickets, batch_size, label, runner, force, write_back=False):
    """Run an enrichment stage in batches and return counters."""
    total_scored = 0
    total_skipped = 0
    total_errors = 0

    for offset in range(0, len(tickets), batch_size):
        batch = tickets[offset:offset + batch_size]
        print(
            f"\n  [{label}] Batch {offset // batch_size + 1}/"
            f"{(len(tickets) + batch_size - 1) // batch_size} ({len(batch)} tickets)",
            flush=True,
        )
        try:
            results = runner(write_back=write_back, force=force, ticket_numbers=batch)
            scored = len(results)
            total_scored += scored
            total_skipped += len(batch) - scored
        except SystemExit:
            print(f"  [{label}] Batch failed (system exit). Continuing...", flush=True)
            total_errors += len(batch)
        except Exception as exc:
            print(f"  [{label}] Batch error: {exc}. Continuing...", flush=True)
            total_errors += len(batch)

    return total_scored, total_skipped, total_errors


def run_sentiment_stage(*, tickets, force, runner):
    """Run the sentiment stage with consistent logging."""
    print(f"\n[enrich] Stage 3: Sentiment analysis ({len(tickets)} tickets)", flush=True)
    try:
        runner(force=force, ticket_numbers=tickets)
    except SystemExit:
        print("[enrich] Sentiment stage exited.", flush=True)
    except Exception as exc:
        print(f"[enrich] Sentiment error: {exc}", flush=True)
    print("[enrich] Sentiment complete.", flush=True)


def elapsed_minutes(start_time: float) -> float:
    return (time.time() - start_time) / 60
