"""
Orchestrator — Run all pipeline stages in sequence.

Stages:
  1. Pull activities   (always runs — fetches from TS API + generates JSON)
  2. Sentiment analysis (RUN_SENTIMENT=1)
  3. AI priority        (RUN_PRIORITY=1)
  4. Complexity         (RUN_COMPLEXITY=1)
  5. Consolidated write-back to TeamSupport (single API call per ticket)

When DATABASE_URL is set, enrichment scripts also persist to DB and use
hash-based skipping.  Use --force to override hash checks.

Usage:
    python run_all.py
    TARGET_TICKET=29696 python run_all.py
    TARGET_TICKET=29696 python run_all.py --force
    RUN_SENTIMENT=0 RUN_COMPLEXITY=0 python run_all.py
    python run_all.py --no-writeback   # skip TS write-back (dry-run); TS_WRITEBACK=0 in config always wins
"""

import argparse
import os
import sys
from datetime import datetime, timezone

from config import FORCE_ENRICHMENT, LOG_TO_FILE, OUTPUT_DIR, RUN_COMPLEXITY, RUN_PRIORITY, RUN_SENTIMENT, SKIP_OUTPUT_FILES, TS_WRITEBACK
from run_pull_activities import main as pull_activities
from run_sentiment import main as run_sentiment
from run_priority import main as run_priority
from run_complexity import main as run_complexity
from ts_client import update_ticket

_log_fh = None


def _log(msg: str) -> None:
    print(msg, flush=True)
    if _log_fh is not None:
        _log_fh.write(msg + "\n")
        _log_fh.flush()


def _setup_log_file() -> None:
    """Redirect all print() output to a timestamped log file in OUTPUT_DIR."""
    global _log_fh
    if not LOG_TO_FILE or SKIP_OUTPUT_FILES:
        return
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_path = os.path.join(OUTPUT_DIR, f"pipeline_{ts}.log")
    _log_fh = open(log_path, "w", encoding="utf-8")
    # Tee stdout/stderr to the log file
    import io

    class _Tee(io.TextIOBase):
        def __init__(self, original, logfile):
            self._original = original
            self._logfile = logfile
        def write(self, s):
            self._original.write(s)
            self._logfile.write(s)
            self._logfile.flush()
            return len(s)
        def flush(self):
            self._original.flush()
            self._logfile.flush()

    sys.stdout = _Tee(sys.__stdout__, _log_fh)
    sys.stderr = _Tee(sys.__stderr__, _log_fh)


def main(*, force: bool = False, no_writeback: bool = False) -> None:
    force = force or FORCE_ENRICHMENT
    _setup_log_file()
    if not TS_WRITEBACK:
        write_back = False  # config says off — hard lock, CLI cannot override
    else:
        write_back = not no_writeback  # config says on — CLI can suppress with --no-writeback
    _log("=" * 60)
    _log("[orchestrator] Starting pipeline")
    if force:
        _log("[orchestrator] --force: hash-based skip checks disabled")
    if not write_back:
        _log("[orchestrator] TS write-back disabled"
             + (" (--no-writeback)" if no_writeback else " (TS_WRITEBACK=0)"))
    _log("=" * 60)

    # Part 1 — always runs
    _log("\n[orchestrator] Part 1: Pull activities")
    activities_file = pull_activities()

    # Part 2 — sentiment
    if RUN_SENTIMENT:
        _log("\n[orchestrator] Part 2: Sentiment analysis")
        run_sentiment(activities_file=activities_file, force=force)
    else:
        _log("\n[orchestrator] Part 2: Sentiment — skipped (RUN_SENTIMENT=0)")

    # Collect fields from each stage for a single consolidated write-back
    # per ticket.  Each runner returns {ticket_number: {ticket_id, fields, activities}}
    all_updates: dict[str, dict] = {}  # ticket_number -> merged data

    # Part 3 — priority
    if RUN_PRIORITY:
        _log("\n[orchestrator] Part 3: AI priority scoring")
        priority_results = run_priority(activities_file=activities_file, write_back=False, force=force)
        for tnum, data in priority_results.items():
            entry = all_updates.setdefault(
                tnum,
                {"ticket_id": data["ticket_id"], "fields": {}, "activities": data["activities"]},
            )
            entry["fields"].update(data["fields"])
    else:
        _log("\n[orchestrator] Part 3: Priority — skipped (RUN_PRIORITY=0)")

    # Part 4 — complexity
    if RUN_COMPLEXITY:
        _log("\n[orchestrator] Part 4: Complexity analysis")
        complexity_results = run_complexity(activities_file=activities_file, write_back=False, force=force)
        for tnum, data in complexity_results.items():
            entry = all_updates.setdefault(
                tnum,
                {"ticket_id": data["ticket_id"], "fields": {}, "activities": data["activities"]},
            )
            entry["fields"].update(data["fields"])
    else:
        _log("\n[orchestrator] Part 4: Complexity — skipped (RUN_COMPLEXITY=0)")

    # ── Consolidated write-back: one API call per ticket ──
    if all_updates and write_back:
        _log(f"\n[orchestrator] Writing back {len(all_updates)} ticket(s) (single call each)...")
        updated = 0
        deferred = 0
        for tnum, data in all_updates.items():
            try:
                update_ticket(data["ticket_id"], data["fields"], data["activities"])
                _log(f"  [ts] Updated ticket {tnum} — fields: {list(data['fields'].keys())}")
                updated += 1
            except Exception as e:
                if hasattr(e, 'response') and getattr(e.response, 'status_code', None) == 403:
                    _log(f"  [ts] API rate-limited for {tnum}; payload saved to dry-run file.")
                    deferred += 1
                else:
                    _log(f"  [ts] Failed to update ticket {tnum}: {e}")
        _log(f"[orchestrator] Write-back complete: {updated}/{len(all_updates)} updated, {deferred} deferred (rate-limited).")
    elif all_updates and not write_back:
        reason = "--no-writeback" if no_writeback else "TS_WRITEBACK=0"
        _log(f"\n[orchestrator] {len(all_updates)} ticket(s) scored but write-back skipped ({reason}).")
        for tnum, data in all_updates.items():
            _log(f"  {tnum}: {list(data['fields'].keys())}")

    _log("\n" + "=" * 60)
    _log("[orchestrator] Pipeline complete")
    _log("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the full pipeline.")
    parser.add_argument("--force", action="store_true",
                        help="Force enrichment rerun even if content hashes are unchanged.")
    parser.add_argument("--no-writeback", action="store_true",
                        help="Skip TeamSupport write-back (dry-run mode).")
    args = parser.parse_args()
    main(force=args.force, no_writeback=args.no_writeback)
