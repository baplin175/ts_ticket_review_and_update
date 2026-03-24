#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────
# Import pass1–pass5 CSV results from a directory into the database.
#
# Auto-discovers the latest file for each pass by matching filenames
# like "pass1_results.csv", "pass1_results (5).csv", etc.  When
# multiple versions exist, the highest-numbered copy is used.
#
# Usage:
#   ./run_import_root_c.sh                     # default: root_c_files/
#   ./run_import_root_c.sh /path/to/csv_dir
# ──────────────────────────────────────────────────────────────────────
set -euo pipefail

DIR="${1:-root_c_files}"

if [[ ! -d "$DIR" ]]; then
    echo "ERROR: directory '$DIR' does not exist." >&2
    exit 1
fi

# Find the latest file for a given pass prefix (e.g. "pass1_results").
# Picks the file with the highest parenthesized version number, or the
# plain filename if no versioned copies exist.
find_latest() {
    local prefix="$1"
    local best=""
    local best_num=-1

    for f in "$DIR"/${prefix}*.csv; do
        [[ -f "$f" ]] || continue
        fname="$(basename "$f")"
        # Extract the number in parentheses, e.g. "pass1_results (5).csv" → 5
        if [[ "$fname" =~ \(([0-9]+)\) ]]; then
            num="${BASH_REMATCH[1]}"
        else
            num=0
        fi
        if (( num > best_num )); then
            best_num=$num
            best="$fname"
        fi
    done
    echo "$best"
}

PASS1_FILE="$(find_latest pass1_results)"
PASS3_FILE="$(find_latest pass3_results)"
PASS4_FILE="$(find_latest pass4_results)"
PASS5_FILE="$(find_latest pass5_results)"

echo "=== Import pass results from $DIR ==="
echo "  Pass 1: ${PASS1_FILE:-(not found)}"
echo "  Pass 3: ${PASS3_FILE:-(not found)}"
echo "  Pass 4: ${PASS4_FILE:-(not found)}"
echo "  Pass 5: ${PASS5_FILE:-(not found)}"
echo ""

CMD=(python3 run_csv_pipe_import.py --dir "$DIR" --force)
[[ -n "$PASS1_FILE" ]] && CMD+=(--pass1 "$PASS1_FILE")
[[ -n "$PASS3_FILE" ]] && CMD+=(--pass3 "$PASS3_FILE")
[[ -n "$PASS4_FILE" ]] && CMD+=(--pass4 "$PASS4_FILE")
[[ -n "$PASS5_FILE" ]] && CMD+=(--pass5 "$PASS5_FILE")

echo "Running: ${CMD[*]}"
echo ""
exec "${CMD[@]}"
