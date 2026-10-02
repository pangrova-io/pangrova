#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
E2E_ROOT="${E2E_TRUTHFULNESS_ROOT:-${PROJECT_ROOT}/pangrova-testing/pangrova-test-e2e/src/test/java}"

if [[ ! -d "${E2E_ROOT}" ]]; then
  echo "E2E source root is missing: ${E2E_ROOT}" >&2
  exit 1
fi

PATTERN='assume(True|False)[[:space:]]*\(|Assumptions\.(assumeTrue|assumeFalse)[[:space:]]*\(|@Disabled\b|assertThat[[:space:]]*\([[:space:]]*(true|false)[[:space:]]*\)|assert(True|False)[[:space:]]*\([[:space:]]*(true|false)[[:space:]]*[,)]'
MASKED_FAILURE_PATTERN='catch[[:space:]]*\([^)]*AssertionError|doesNotContainIgnoringCase[[:space:]]*\([[:space:]]*"(syntax error|mismatched input|No match found for function signature)"'
COLLECTION_TAUTOLOGY_PATTERN='assertThat[[:space:]]*\([^\n;]*\.(size|length)[[:space:]]*\([[:space:]]*\)[[:space:]]*\)[[:space:]]*\.[[:space:]]*isGreaterThanOrEqualTo[[:space:]]*\([[:space:]]*0[[:space:]]*\)'
EMPTY_FAILURE_PATTERN='catch[[:space:]]*\([^)]*RuntimeException[^}]{0,600}return[[:space:]]+List\.of[[:space:]]*\('
SIMULATED_MULTI_WORKER_PATTERN='extends[[:space:]]+(SqlE2ETestBase|VirtualWorkerRoutingIntegrationTestBase|MultiWorkerTestBase|Simulated[A-Za-z0-9]*TestBase|SharedEndpoint[A-Za-z0-9]*TestBase)|registerVirtualWorkers|registerVirtualWorkerRoutes|registerWorkerNode[[:space:]]*\([[:space:]]*realHttpPort|virtual[ -]workers?|same (real )?worker endpoint|all pointing to (the )?(same|parent.s real)'

if rg --line-number --glob '*.java' "${PATTERN}" "${E2E_ROOT}"; then
  echo "E2E truthfulness gate failed: skips and constant assertions are forbidden." >&2
  echo "Unavailable required infrastructure must fail the test, and assertions must observe the public execution path." >&2
  exit 1
fi

if rg --line-number --glob '*.java' "${MASKED_FAILURE_PATTERN}" "${E2E_ROOT}" \
    || rg --multiline --line-number --glob '*.java' "${COLLECTION_TAUTOLOGY_PATTERN}" "${E2E_ROOT}" \
    || rg --multiline --line-number --glob '*.java' "${EMPTY_FAILURE_PATTERN}" "${E2E_ROOT}"; then
  echo "E2E truthfulness gate failed: caught assertions, parser-only error acceptance and collection tautologies cannot prove execution." >&2
  exit 1
fi

while IFS= read -r source; do
  if rg --line-number --ignore-case "${SIMULATED_MULTI_WORKER_PATTERN}" "${source}"; then
    echo "E2E truthfulness gate failed: multi-worker requires distinct worker processes and endpoints: ${source}" >&2
    echo "Virtual identities, a shared endpoint, and the single-worker E2E base are routing integration only." >&2
    exit 1
  fi
done < <(rg --files-with-matches --glob '*Test.java' \
  '@Tag[[:space:]]*\([[:space:]]*"multi-worker"[[:space:]]*\)' \
  "${E2E_ROOT}" || true)

echo "E2E truthfulness gate passed"
