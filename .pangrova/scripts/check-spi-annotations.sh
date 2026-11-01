#!/usr/bin/env bash
# Enforces that all public top-level interfaces and abstract classes under
# the manifest-declared -api/-spi contract trees are annotated with one of:
#   @Stable, @Experimental, or @Internal.
#
# This guards the SPI contract: any new interface added without an explicit
# stability annotation will fail CI, forcing the author to declare intent.
#
# Annotation declaration packages (annotation/, api/annotation/) are exempt.
#
# Pre-existing unannotated types are listed in
# .pangrova/scripts/.spi-annotation-baseline.txt. CI fails when:
#   - a NEW unannotated type is added (not in the baseline), or
#   - a baseline entry no longer exists (rename/delete the baseline entry too), or
#   - a baseline entry has been annotated (positive drift — remove from baseline).
#
# Usage:  ./.pangrova/scripts/check-spi-annotations.sh
# Exit:   0 = OK, 1 = violations found.
#
# Compatible with bash 3.2 (macOS default).

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BASELINE_FILE="${ROOT_DIR}/.pangrova/scripts/.spi-annotation-baseline.txt"

# Contract roots are every artifact declared in .pangrova/modules.yaml whose
# artifactId is pangrova-spi or ends in -api or -spi. Adding a contract artifact to the manifest
# enrolls it in this gate automatically.
CONTRACT_ROOTS=()
while IFS= read -r artifact; do
    dir=$(awk -v a="${artifact}" '
        $0 == "  " a ":" { found = 1; next }
        found && /^  [A-Za-z0-9_.-]+:/ { exit }
        found && /^    path:/ { print $2; exit }
    ' "${ROOT_DIR}/.pangrova/modules.yaml")
    [[ -n "${dir}" && -d "${ROOT_DIR}/${dir}/src/main/java" ]] \
        && CONTRACT_ROOTS+=("${ROOT_DIR}/${dir}/src/main/java")
done < <(sed -n -e 's/^  \(pangrova-.*-api\):$/\1/p' \
    -e 's/^  \(pangrova-.*-spi\):$/\1/p' \
    -e 's/^  \(pangrova-spi\):$/\1/p' "${ROOT_DIR}/.pangrova/modules.yaml")

if [[ ${#CONTRACT_ROOTS[@]} -eq 0 ]]; then
    echo "ERROR: no -api/-spi contract roots found in .pangrova/modules.yaml" >&2
    exit 2
fi

TMPDIR=$(mktemp -d)
trap 'rm -rf "${TMPDIR}"' EXIT

CURRENT_FILE="${TMPDIR}/current.txt"
BASELINE_SORTED="${TMPDIR}/baseline.txt"

# Build current unannotated set.
: > "${CURRENT_FILE}"
while IFS= read -r -d '' file; do
    case "${file}" in
        */annotation/*) continue ;;
    esac

    if ! grep -qE '^public (interface|abstract class) ' "${file}"; then
        continue
    fi

    decl_line=$(grep -nE '^public (interface|abstract class) ' "${file}" | head -1 | cut -d: -f1)
    [[ -z "${decl_line}" ]] && continue

    header=$(sed -n "1,${decl_line}p" "${file}")
    if ! grep -qE '@(Stable|Experimental|Internal)\b' <<< "${header}"; then
        echo "${file#${ROOT_DIR}/}" >> "${CURRENT_FILE}"
    fi
done < <(find "${CONTRACT_ROOTS[@]}" -type f -name '*.java' -print0)

sort -u "${CURRENT_FILE}" -o "${CURRENT_FILE}"

# Load baseline (strip blank lines / comments).
if [[ -f "${BASELINE_FILE}" ]]; then
    { grep -vE '^(#|[[:space:]]*$)' "${BASELINE_FILE}" || true; } | sort -u > "${BASELINE_SORTED}"
else
    : > "${BASELINE_SORTED}"
fi

# NEW offenders: in current, not in baseline.
NEW_OFFENDERS=$(comm -23 "${CURRENT_FILE}" "${BASELINE_SORTED}" || true)
# STALE baseline: in baseline, not in current.
STALE_BASELINE=$(comm -13 "${CURRENT_FILE}" "${BASELINE_SORTED}" || true)

status=0

if [[ -n "${NEW_OFFENDERS}" ]]; then
    new_count=$(echo "${NEW_OFFENDERS}" | wc -l | tr -d ' ')
    echo "FAIL: ${new_count} NEW unannotated public SPI type(s):" >&2
    echo "${NEW_OFFENDERS}" | sed 's/^/  - /' >&2
    echo "" >&2
    echo "Annotate with one of:" >&2
    echo "  @Stable(since = \"<version>\")   public contract, no breaking changes within major" >&2
    echo "  @Experimental                    unstable; may change in minor releases" >&2
    echo "  @Internal                        not part of public API" >&2
    status=1
fi

if [[ -n "${STALE_BASELINE}" ]]; then
    stale_count=$(echo "${STALE_BASELINE}" | wc -l | tr -d ' ')
    echo "FAIL: ${stale_count} baseline entry(ies) are stale (now annotated or deleted)." >&2
    echo "Remove these lines from ${BASELINE_FILE#${ROOT_DIR}/}:" >&2
    echo "${STALE_BASELINE}" | sed 's/^/  - /' >&2
    status=1
fi

if [[ ${status} -eq 0 ]]; then
    annotated_count=0
    while IFS= read -r -d '' file; do
        case "${file}" in */annotation/*) continue ;; esac
        if grep -qE '@(Stable|Experimental|Internal)\b' "${file}" \
           && grep -qE '^public (interface|abstract class) ' "${file}"; then
            annotated_count=$((annotated_count + 1))
        fi
    done < <(find "${CONTRACT_ROOTS[@]}" -type f -name '*.java' -print0)

    baseline_count=$(wc -l < "${BASELINE_SORTED}" | tr -d ' ')
    echo "OK: SPI annotation gate passed."
    echo "  Annotated public types: ${annotated_count}"
    echo "  Baseline (pre-existing gap): ${baseline_count}"
fi

exit ${status}
