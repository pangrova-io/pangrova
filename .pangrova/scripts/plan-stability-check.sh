#!/usr/bin/env bash
# Plan stability CI gate.
# Detects changes to golden plan files and enforces explicit approval.
#
# This script operates in two modes:
#   1. Git-diff mode (default for PRs): detects golden file changes via git diff
#   2. Test mode (--run-tests): runs plan stability tests and checks for diffs
#
# Golden plan files capture the expected query plan output for TPC-H/TPC-DS queries.
# Any change to these files indicates a planner behavior change that must be
# explicitly reviewed and approved before merge.
#
# Usage:
#   plan-stability-check.sh --base-ref origin/main
#   plan-stability-check.sh --run-tests
#
# Approval mechanism:
#   - PR must carry the label "plan-change-approved" to pass this gate
#   - OR the PR body must contain "Plan Change Approved: yes"
#
# To regenerate golden files after intentional plan changes:
#   mvn test -Pupdate-golden-plans -pl pangrova-planner
#
# Exit codes:
#   0 — no golden plan changes, or changes are explicitly approved
#   1 — golden plan changes detected without explicit approval

set -euo pipefail

BASE_REF="origin/main"
PR_LABELS=""
PR_BODY_FILE=""
RUN_TESTS=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --base-ref)      BASE_REF="$2"; shift 2 ;;
    --pr-labels)     PR_LABELS="$2"; shift 2 ;;
    --pr-body-file)  PR_BODY_FILE="$2"; shift 2 ;;
    --run-tests)     RUN_TESTS=true; shift ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

echo "=== Plan Stability Gate ==="
echo "Base ref: ${BASE_REF}"
echo "Run tests: ${RUN_TESTS}"
echo ""

# Golden plan file patterns
GOLDEN_PLAN_PATTERNS=(
  "pangrova-planner/src/test/resources/golden-plans/"
  "pangrova-testing/src/test/resources/golden-plans/"
)

# --- Mode 1: Run plan stability tests and detect generated diffs ---
if [[ "${RUN_TESTS}" == "true" ]]; then
  echo "Running plan stability tests to verify golden files are up-to-date..."
  echo ""

  # Record golden file state before test run
  GOLDEN_DIR="pangrova-planner/src/test/resources/golden-plans"
  BEFORE_HASH=""
  if [[ -d "${GOLDEN_DIR}" ]]; then
    BEFORE_HASH=$(find "${GOLDEN_DIR}" -type f -exec md5sum {} \; 2>/dev/null | sort | md5sum || true)
  fi

  # Run plan stability tests (these will generate golden files if missing,
  # or fail if existing golden files don't match generated plans)
  echo "Executing: mvn test -pl pangrova-planner -Dtest=TpcdsPlanStabilityTest -DfailIfNoTests=false"
  if ! mvn -B test -pl pangrova-planner \
      -Dtest="TpcdsPlanStabilityTest" \
      -DfailIfNoTests=false 2>&1; then
    echo ""
    echo "=== Plan Stability Tests FAILED ==="
    echo ""
    echo "Golden plan files do not match the current planner output."
    echo "This means the query planner behavior has changed."
    echo ""

    # Show what changed
    if command -v git &>/dev/null; then
      CHANGED=$(git diff --name-only -- "${GOLDEN_DIR}" 2>/dev/null || true)
      if [[ -n "${CHANGED}" ]]; then
        echo "Changed golden files:"
        echo "${CHANGED}" | sed 's/^/  /'
        echo ""
        echo "=== Diff ==="
        git diff -- "${GOLDEN_DIR}" 2>/dev/null | head -100 || true
        echo ""
      fi
    fi

    echo "=== How to Approve ==="
    echo ""
    echo "If the plan changes are intentional:"
    echo ""
    echo "  1. Regenerate golden files:"
    echo "     mvn test -Pupdate-golden-plans -pl pangrova-planner"
    echo ""
    echo "  2. Review the regenerated plans for correctness"
    echo ""
    echo "  3. Commit the updated golden files"
    echo ""
    echo "  4. Add the label 'plan-change-approved' to the PR"
    echo "     OR add 'Plan Change Approved: yes' to the PR description"
    echo ""
    echo "FAILED: Plan stability tests detected plan changes."
    exit 1
  fi

  # Check if golden files were newly generated (didn't exist before)
  AFTER_HASH=""
  if [[ -d "${GOLDEN_DIR}" ]]; then
    AFTER_HASH=$(find "${GOLDEN_DIR}" -type f -exec md5sum {} \; 2>/dev/null | sort | md5sum || true)
  fi

  if [[ "${BEFORE_HASH}" != "${AFTER_HASH}" ]]; then
    echo ""
    echo "NOTE: Golden files were generated (first run or new queries added)."
    echo "Please commit the generated golden files."
    echo ""
  fi

  echo "Plan stability tests PASSED. Golden files are up-to-date."
  exit 0
fi

# --- Mode 2: Git-diff based detection (PR gate) ---

# Collect changed files
CHANGED_FILES=$(git diff --name-only --diff-filter=ACMRD "${BASE_REF}"...HEAD 2>/dev/null || \
                git diff --name-only --diff-filter=ACMRD "${BASE_REF}" 2>/dev/null || true)

if [[ -z "${CHANGED_FILES}" ]]; then
  echo "No changed files detected. Skipping plan stability check."
  exit 0
fi

# Check if any changed file is a golden plan file
PLAN_CHANGES=""
PLAN_CHANGE_COUNT=0

for file in ${CHANGED_FILES}; do
  for pattern in "${GOLDEN_PLAN_PATTERNS[@]}"; do
    if [[ "${file}" == *"${pattern}"* ]]; then
      PLAN_CHANGES="${PLAN_CHANGES}  ${file}\n"
      PLAN_CHANGE_COUNT=$((PLAN_CHANGE_COUNT + 1))
      break
    fi
  done
done

if [[ "${PLAN_CHANGE_COUNT}" -eq 0 ]]; then
  echo "No golden plan files changed. Plan stability gate passed."
  exit 0
fi

echo "WARNING: Golden plan files modified (${PLAN_CHANGE_COUNT} file(s)):"
echo -e "${PLAN_CHANGES}"
echo ""
echo "Golden plan changes indicate query planner behavior has changed."
echo "This requires explicit approval before merge."
echo ""

# Check for approval via PR label
if [[ -n "${PR_LABELS}" ]]; then
  if echo "${PR_LABELS}" | grep -qi "plan-change-approved"; then
    echo "PASSED: PR has 'plan-change-approved' label. Plan changes are approved."
    exit 0
  fi
fi

# Check for approval via PR body
if [[ -n "${PR_BODY_FILE}" && -f "${PR_BODY_FILE}" ]]; then
  PR_BODY=$(cat "${PR_BODY_FILE}")
  if echo "${PR_BODY}" | grep -qi "Plan Change Approved:.*yes"; then
    echo "PASSED: PR body contains 'Plan Change Approved: yes'. Plan changes are approved."
    exit 0
  fi
fi

# No approval found — generate diff summary and fail
echo "=== Plan Diff Summary ==="
echo ""

for file in ${CHANGED_FILES}; do
  for pattern in "${GOLDEN_PLAN_PATTERNS[@]}"; do
    if [[ "${file}" == *"${pattern}"* ]]; then
      echo "--- ${file} ---"
      git diff "${BASE_REF}"...HEAD -- "${file}" 2>/dev/null | head -50 || \
        git diff "${BASE_REF}" -- "${file}" 2>/dev/null | head -50 || true
      echo ""
      break
    fi
  done
done

echo "=== How to Approve ==="
echo ""
echo "To approve golden plan changes, do ONE of the following:"
echo ""
echo "  1. Add the label 'plan-change-approved' to this PR"
echo "  2. Add 'Plan Change Approved: yes' to the PR description"
echo ""
echo "If the plan changes are intentional, regenerate golden files first:"
echo "  mvn test -Pupdate-golden-plans -pl pangrova-planner"
echo ""
echo "Before approving, verify that the plan changes are intentional and correct:"
echo "  - Review the diff above to understand what changed"
echo "  - Confirm the new plans are semantically correct"
echo "  - Ensure no unintended regressions in plan quality"
echo ""
echo "FAILED: Golden plan files changed without explicit approval."
exit 1
