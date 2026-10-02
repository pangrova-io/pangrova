#!/usr/bin/env bash
# Reject contributor-machine paths from active repository files.
# Standard deployment paths, generated artifacts, and version-control metadata
# are intentionally outside this gate.
#
# Usage:
#   check-environment-specific-info.sh              scan the repository
#   check-environment-specific-info.sh --self-test  verify the pattern itself
#
# The --self-test mode exists because a gate that matches nothing looks
# identical to a clean repository. It feeds known-bad and known-good samples
# through the same PATTERN and fails if either verdict is wrong.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
PATTERN='/Users/[[:alnum:]_][[:alnum:]_.-]*/|/home/[[:alnum:]_][[:alnum:]_.-]*/|[A-Za-z]:\\Users\\[[:alnum:]_][[:alnum:]_.-]*\\|workspaceStorage/|/Applications/Visual Studio Code[.]app/'

self_test() {
    # Samples that MUST be rejected — one per alternative in PATTERN.
    local -a should_match=(
        '/Users/alice/workspace/pangrova/pom.xml'
        '/home/runner-1/build/pangrova'
        'C:\Users\bob\pangrova'
        'workspaceStorage/1a2b3c/state.json'
        '/Applications/Visual Studio Code.app/Contents/MacOS'
    )
    # Samples that MUST pass — standard deployment and portable references.
    local -a should_not_match=(
        '/opt/pangrova/lib/libpangrova_native.so'
        '/var/lib/pangrova/spill'
        '${HOME}/.m2/repository'
        '$PROJECT_ROOT/config/checkstyle'
        '/usr/local/include/arrow'
        '/tmp/pangrova-warehouse'
        '/Users'
        '/homebrew/bin'
    )

    local failures=0 sample
    for sample in "${should_match[@]}"; do
        if ! grep -Eq "$PATTERN" <<< "$sample"; then
            echo "SELF-TEST FAIL: pattern missed a contributor path: ${sample}" >&2
            failures=$((failures + 1))
        fi
    done
    for sample in "${should_not_match[@]}"; do
        if grep -Eq "$PATTERN" <<< "$sample"; then
            echo "SELF-TEST FAIL: pattern rejected a legitimate path: ${sample}" >&2
            failures=$((failures + 1))
        fi
    done

    if [[ ${failures} -gt 0 ]]; then
        echo "Self-test failed with ${failures} incorrect verdict(s)." >&2
        exit 1
    fi

    echo "Self-test passed: ${#should_match[@]} rejected, ${#should_not_match[@]} accepted."
}

if [[ "${1:-}" == "--self-test" ]]; then
    self_test
    exit 0
fi

# Guard the real scan with the self-test so a broken pattern can never report
# "check passed" on a repository it never actually inspected.
self_test

RESULTS_FILE="$(mktemp "${TMPDIR:-/tmp}/pangrova-environment-paths.XXXXXX")"
trap 'rm -f "$RESULTS_FILE"' EXIT

find "$REPO_ROOT" -type f \
    -not -path '*/.git/*' \
    -not -path '*/.pangrova/scripts/check-environment-specific-info.sh' \
    -not -path '*/.qoder/repowiki/*' \
    -not -path '*/target/*' \
    -not -path '*/build/*' \
    -not -path '*/build-*/*' \
    -not -name 'javac.*.args' \
    -not -path '*/website/node_modules/*' \
    -not -path '*/website/.docusaurus/*' \
    -not -path '*/.idea/*' \
    -not -path '*/.vscode/*' \
    -exec grep -IEn "$PATTERN" {} + > "$RESULTS_FILE" || true

if [[ -s "$RESULTS_FILE" ]]; then
    echo "ERROR: contributor-machine paths found in active repository files:"
    sed "s#${REPO_ROOT}/##" "$RESULTS_FILE"
    echo "Use PATH, HOME, JAVA_HOME, MAVEN_HOME, PROJECT_ROOT, or a documented application setting."
    exit 1
fi

echo "Environment-specific path check passed."
