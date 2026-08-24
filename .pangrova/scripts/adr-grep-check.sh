#!/usr/bin/env bash
#
# adr-grep-check.sh — Verify ADR status is consistent with code.
#
# For each ADR in docs/architecture/adr/:
#   1. Parse the "Status:" line.
#   2. Extract symbols/paths the ADR references with the trailer
#      "Removed-Symbols:" / "Deprecated-Symbols:" / "Required-Symbols:".
#   3. Run `git grep` for each symbol across production source code
#      (excluding the ADR itself, docs/, deps-download/, target/).
#   4. Enforce:
#         - status: Superseded / Removed  -> no production references allowed
#         - status: Deprecated            -> warn (info only)
#         - status: Accepted              -> Required-Symbols MUST be present
#
# Exit code: 0 on clean, 1 on contradiction (CI fails the build).
#
# ADR trailer format (one per line, anywhere in the doc):
#     Removed-Symbols: org.foo.Bar, org.baz.Qux
#     Deprecated-Symbols: org.foo.Old*
#     Required-Symbols: org.foo.New, org.bar.Required
#
# Symbols are matched as fixed strings against `git grep -F`.

set -euo pipefail

ADR_DIR="${ADR_DIR:-docs/architecture/adr}"
EXCLUDES=(
    ":(exclude)docs/**"
    ":(exclude)deps-download/**"
    ":(exclude)target/**"
    ":(exclude)**/target/**"
    ":(exclude)build/**"
    ":(exclude).github/**"
    ":(exclude)scripts/adr-grep-check.sh"
)

violations=0

if [ ! -d "${ADR_DIR}" ]; then
    echo "INFO: no ADR directory at ${ADR_DIR}; nothing to check."
    exit 0
fi

shopt -s nullglob
adrs=( "${ADR_DIR}"/*.md )
if [ "${#adrs[@]}" -eq 0 ]; then
    echo "INFO: no ADR files under ${ADR_DIR}; nothing to check."
    exit 0
fi

extract_status() {
    # First non-empty line containing "Status:" wins. Tolerant of "> Status: **Accepted**".
    grep -m1 -iE '^[[:space:]>*]*status:' "$1" 2>/dev/null \
        | sed -E 's/^[[:space:]>*]*[Ss]tatus:[[:space:]]*//; s/\*//g; s/[[:space:]]*$//' \
        | tr '[:upper:]' '[:lower:]' \
        | awk '{print $1}'
}

extract_symbols() {
    # $1=adr file, $2=trailer name (e.g. Removed-Symbols)
    grep -iE "^[[:space:]>*]*${2}:" "$1" 2>/dev/null \
        | sed -E "s/^[[:space:]>*]*${2}:[[:space:]]*//I" \
        | tr ',' '\n' \
        | sed -E 's/^[[:space:]]+//; s/[[:space:]]+$//' \
        | grep -v '^$' || true
}

grep_symbol() {
    # Returns matching lines (may be empty); never errors.
    git grep -nF -- "$1" "${EXCLUDES[@]}" 2>/dev/null || true
}

for adr in "${adrs[@]}"; do
    status=$(extract_status "${adr}")
    if [ -z "${status}" ]; then
        echo "WARN: ${adr} has no Status: line; skipping."
        continue
    fi

    echo "----"
    echo "ADR: ${adr}  status=${status}"

    removed_symbols=$(extract_symbols "${adr}" "Removed-Symbols")
    deprecated_symbols=$(extract_symbols "${adr}" "Deprecated-Symbols")
    required_symbols=$(extract_symbols "${adr}" "Required-Symbols")

    if [ -n "${removed_symbols}" ]; then
        while IFS= read -r sym; do
            [ -z "${sym}" ] && continue
            hits=$(grep_symbol "${sym}")
            if [ -n "${hits}" ]; then
                echo "FAIL: Removed-Symbols '${sym}' from ADR ${adr} still appears in code:"
                echo "${hits}" | sed 's/^/    /'
                violations=$((violations + 1))
            else
                echo "  ok: Removed-Symbols '${sym}' absent from code."
            fi
        done <<< "${removed_symbols}"
    fi

    if [ "${status}" = "superseded" ] || [ "${status}" = "removed" ]; then
        # The whole ADR is dead; ensure none of the Required-Symbols (legacy) leak.
        while IFS= read -r sym; do
            [ -z "${sym}" ] && continue
            hits=$(grep_symbol "${sym}")
            if [ -n "${hits}" ]; then
                echo "FAIL: ADR is ${status} but legacy symbol '${sym}' still in code:"
                echo "${hits}" | sed 's/^/    /'
                violations=$((violations + 1))
            fi
        done <<< "${required_symbols}"
    fi

    if [ -n "${deprecated_symbols}" ]; then
        while IFS= read -r sym; do
            [ -z "${sym}" ] && continue
            count=$(grep_symbol "${sym}" | wc -l | tr -d ' ')
            echo "  info: Deprecated-Symbols '${sym}' appears in ${count} place(s)."
        done <<< "${deprecated_symbols}"
    fi

    if [ "${status}" = "accepted" ] && [ -n "${required_symbols}" ]; then
        while IFS= read -r sym; do
            [ -z "${sym}" ] && continue
            hits=$(grep_symbol "${sym}")
            if [ -z "${hits}" ]; then
                echo "FAIL: ADR is accepted but Required-Symbols '${sym}' not found in code."
                violations=$((violations + 1))
            else
                echo "  ok: Required-Symbols '${sym}' present in code."
            fi
        done <<< "${required_symbols}"
    fi
done

echo "----"
if [ "${violations}" -gt 0 ]; then
    echo "ADR/code consistency check FAILED with ${violations} violation(s)."
    exit 1
fi
echo "ADR/code consistency check OK."
exit 0
