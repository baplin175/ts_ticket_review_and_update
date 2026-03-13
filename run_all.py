"""
Orchestrator — Run all pipeline stages in sequence.

Stages:
  1. Pull activities   (always runs)
  2. Sentiment analysis (RUN_SENTIMENT=1)
  3. AI priority        (RUN_PRIORITY=1)
  4. Complexity         (RUN_COMPLEXITY=1)

Usage:
    python run_all.py
    TARGET_TICKET=29696 python run_all.py
    RUN_SENTIMENT=0 RUN_COMPLEXITY=0 python run_all.py
"""

import sys

from config import RUN_COMPLEXITY, RUN_PRIORITY, RUN_SENTIMENT
from run_pull_activities import main as pull_activities
from run_sentiment import main as run_sentiment
from run_priority import main as run_priority
from run_complexity import main as run_complexity


def _log(msg: str) -> None:
    print(msg, flush=True)


def main() -> None:
    _log("=" * 60)
    _log("[orchestrator] Starting pipeline")
    _log("=" * 60)

    # Part 1 — always runs
    _log("\n[orchestrator] Part 1: Pull activities")
    activities_file = pull_activities()

    # Part 2 — sentiment
    if RUN_SENTIMENT:
        _log("\n[orchestrator] Part 2: Sentiment analysis")
        run_sentiment(activities_file=activities_file)
    else:
        _log("\n[orchestrator] Part 2: Sentiment — skipped (RUN_SENTIMENT=0)")

    # Part 3 — priority
    if RUN_PRIORITY:
        _log("\n[orchestrator] Part 3: AI priority scoring")
        run_priority(activities_file=activities_file)
    else:
        _log("\n[orchestrator] Part 3: Priority — skipped (RUN_PRIORITY=0)")

    # Part 4 — complexity
    if RUN_COMPLEXITY:
        _log("\n[orchestrator] Part 4: Complexity analysis")
        run_complexity(activities_file=activities_file)
    else:
        _log("\n[orchestrator] Part 4: Complexity — skipped (RUN_COMPLEXITY=0)")

    _log("\n" + "=" * 60)
    _log("[orchestrator] Pipeline complete")
    _log("=" * 60)


if __name__ == "__main__":
    main()
