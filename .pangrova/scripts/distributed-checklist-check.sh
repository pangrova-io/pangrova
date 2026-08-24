#!/usr/bin/env bash
# Distributed correctness checklist enforcement.
# Ensures PRs touching critical modules (coordinator, worker, shuffle, metadata)
# have the distributed correctness section filled in the PR body.
#
# Usage: distributed-checklist-check.sh --base-ref origin/main [--pr-body-file /path/to/body.md]
#
# In CI, the PR body is passed via file or environment variable (PR_BODY).
#
# Exit codes:
#   0 — checklist not required (no critical modules touched) or checklist filled
#   1 — checklist required but not filled

set -euo pipefail

BASE_REF="origin/main"
PR_BODY_FILE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --base-ref)      BASE_REF="$2"; shift 2 ;;
    --pr-body-file)  PR_BODY_FILE="$2"; shift 2 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

echo "=== Distributed Correctness Checklist Check ==="
echo "Base ref: ${BASE_REF}"
echo ""

# Critical module path patterns that require the distributed correctness checklist
CRITICAL_PATTERNS=(
  "pangrova-coordinator/"
  "pangrova-worker/"
  "pangrova-shuffle/"
  "shuffle-server/"
  "pangrova-metadata/"
)

# Collect changed files
CHANGED_FILES=$(git diff --name-only --diff-filter=ACMR "${BASE_REF}"...HEAD 2>/dev/null || \
                git diff --name-only --diff-filter=ACMR "${BASE_REF}" 2>/dev/null || true)

if [[ -z "${CHANGED_FILES}" ]]; then
  echo "No changed files detected. Skipping checklist check."
  exit 0
fi

# Check if any changed file touches a critical module
TOUCHES_CRITICAL=false
CRITICAL_FILES=""

for file in ${CHANGED_FILES}; do
  for pattern in "${CRITICAL_PATTERNS[@]}"; do
    if [[ "${file}" == *"${pattern}"* ]]; then
      TOUCHES_CRITICAL=true
      CRITICAL_FILES="${CRITICAL_FILES}  ${file}\n"
      break
    fi
  done
done

if [[ "${TOUCHES_CRITICAL}" == "false" ]]; then
  echo "No critical modules touched. Distributed checklist not required."
  exit 0
fi

echo "Critical modules touched:"
echo -e "${CRITICAL_FILES}"
echo ""
echo "Distributed correctness checklist is REQUIRED for this PR."
echo ""

# Get PR body content
PR_BODY=""
if [[ -n "${PR_BODY_FILE}" && -f "${PR_BODY_FILE}" ]]; then
  PR_BODY=$(cat "${PR_BODY_FILE}")
elif [[ -n "${PR_BODY:-}" ]]; then
  PR_BODY="${PR_BODY}"
else
  echo "ERROR: No PR body available."
  echo "Provide via --pr-body-file or PR_BODY environment variable."
  echo ""
  echo "In local development, ensure the PR description includes the"
  echo "Distributed Correctness section from the PR template."
  exit 1
fi

# Check if the distributed correctness section exists
if ! echo "${PR_BODY}" | grep -qi "## Distributed Correctness"; then
  echo "FAILED: PR body is missing the '## Distributed Correctness' section."
  echo ""
  echo "This PR touches critical distributed modules and must include the"
  echo "distributed correctness checklist. Please fill in the section from"
  echo "the PR template."
  exit 1
fi

# Extract the distributed correctness section
CHECKLIST_SECTION=$(echo "${PR_BODY}" | sed -n '/## Distributed Correctness/,/^## /p' | head -n -1)

# Check if the section is marked as N/A
if echo "${CHECKLIST_SECTION}" | grep -qi "N/A"; then
  echo "PASSED: Distributed correctness section marked as N/A."
  echo "(Author has explicitly declared this change does not affect distributed behavior.)"
  exit 0
fi

# Check that at least one checklist item is checked [x] or [X]
CHECKED_ITEMS=$(echo "${CHECKLIST_SECTION}" | grep -c '\[x\]\|\[X\]' || true)
TOTAL_ITEMS=$(echo "${CHECKLIST_SECTION}" | grep -c '\[ \]\|\[x\]\|\[X\]' || true)

if [[ "${TOTAL_ITEMS}" -eq 0 ]]; then
  echo "FAILED: Distributed correctness section exists but contains no checklist items."
  echo "Please use the PR template's distributed correctness checklist."
  exit 1
fi

if [[ "${CHECKED_ITEMS}" -eq 0 ]]; then
  echo "FAILED: None of the distributed correctness checklist items are checked."
  echo ""
  echo "This PR touches critical modules (coordinator, worker, shuffle, metadata)."
  echo "You must check at least one item and provide a brief explanation,"
  echo "or mark the section as N/A with justification."
  echo ""
  echo "Checklist items:"
  echo "  - Network partition behavior"
  echo "  - Dual-leader safety"
  echo "  - Crash recovery"
  echo "  - Slow-node tolerance"
  echo ""
  echo "See docs/distributed-correctness-checklist.md for examples."
  exit 1
fi

echo "PASSED: Distributed correctness checklist is filled (${CHECKED_ITEMS}/${TOTAL_ITEMS} items checked)."
exit 0
