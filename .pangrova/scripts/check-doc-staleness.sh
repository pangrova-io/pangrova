#!/usr/bin/env bash
# Verify docs/architecture/*.md files have been updated within the last 60 days.
# Emits warnings for stale documentation that may be out of sync with code.
#
# Exit codes:
#   0 - all docs are fresh (or only warnings emitted)
#
# Note: This script warns but does not fail CI. Stale docs are a signal,
# not a blocker. Pipe output to CI annotations for visibility.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DOCS_DIR="${REPO_ROOT}/docs/architecture"
STALE_DAYS=${STALE_DAYS:-60}

if [ ! -d "$DOCS_DIR" ]; then
    echo "⚠️  docs/architecture/ directory not found — skipping staleness check."
    exit 0
fi

echo "=== Documentation Staleness Check (threshold: ${STALE_DAYS} days) ==="
echo ""

STALE=()
FRESH=0
TOTAL=0

# Use git log to determine last modification date for each file.
# Falls back to filesystem mtime if not in a git repo.
get_last_modified_days() {
    local file="$1"
    local last_commit_date

    if git -C "$REPO_ROOT" rev-parse --git-dir > /dev/null 2>&1; then
        last_commit_date=$(git -C "$REPO_ROOT" log -1 --format="%at" -- "$file" 2>/dev/null)
        if [ -z "$last_commit_date" ] || [ "$last_commit_date" = "" ]; then
            # File not tracked by git — use filesystem mtime
            if [[ "$OSTYPE" == "darwin"* ]]; then
                last_commit_date=$(stat -f "%m" "$file")
            else
                last_commit_date=$(stat -c "%Y" "$file")
            fi
        fi
    else
        # Not a git repo — use filesystem mtime
        if [[ "$OSTYPE" == "darwin"* ]]; then
            last_commit_date=$(stat -f "%m" "$file")
        else
            last_commit_date=$(stat -c "%Y" "$file")
        fi
    fi

    local now
    now=$(date +%s)
    local diff_seconds=$((now - last_commit_date))
    local diff_days=$((diff_seconds / 86400))
    echo "$diff_days"
}

for doc_file in "$DOCS_DIR"/*.md; do
    [ -f "$doc_file" ] || continue
    TOTAL=$((TOTAL + 1))

    days_ago=$(get_last_modified_days "$doc_file")
    basename_file=$(basename "$doc_file")

    if [ "$days_ago" -gt "$STALE_DAYS" ]; then
        STALE+=("${basename_file} (last updated ${days_ago} days ago)")
    else
        FRESH=$((FRESH + 1))
    fi
done

echo "Total docs: ${TOTAL} | Fresh: ${FRESH} | Stale: ${#STALE[@]}"
echo ""

if [ ${#STALE[@]} -gt 0 ]; then
    echo "⚠️  Stale architecture docs (not updated in >${STALE_DAYS} days):"
    for s in "${STALE[@]}"; do
        echo "  ⚠ ${s}"
    done
    echo ""
    echo "Consider reviewing and updating these documents to reflect current architecture."
    exit 0
fi

echo "✅ All ${TOTAL} architecture docs updated within ${STALE_DAYS} days."
exit 0
