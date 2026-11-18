#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
E2E_ROOT="${E2E_TRUTHFULNESS_ROOT:-${PROJECT_ROOT}/pangrova-testing/pangrova-testing-integration-tests/src/test/java}"

python3 - "${E2E_ROOT}" <<'PY'
import os
from pathlib import Path
import re
import sys

root = Path(sys.argv[1])
if not root.is_dir():
    print(f"E2E source root is missing: {root}", file=sys.stderr)
    sys.exit(1)

pattern = re.compile(r'assume(True|False)\s*\(|Assumptions\.(assumeTrue|assumeFalse)\s*\(|@Disabled\b|assertThat\s*\(\s*(true|false)\s*\)|assert(True|False)\s*\(\s*(true|false)\s*[,)]')
masked_failure = re.compile(r'catch\s*\([^)]*AssertionError|doesNotContainIgnoringCase\s*\(\s*"(syntax error|mismatched input|No match found for function signature)"')
collection_tautology = re.compile(r'assertThat\s*\([^\n;]*\.(size|length)\s*\(\s*\)\s*\)\s*\.\s*isGreaterThanOrEqualTo\s*\(\s*0\s*\)')
empty_failure = re.compile(r'catch\s*\([^)]*RuntimeException[^}]{0,600}return\s+List\.of\s*\(')
multi_worker_tag = re.compile(r'@Tag\s*\(\s*"multi-worker"\s*\)')
simulated_multi_worker = re.compile(r'extends\s+(SqlE2ETestBase|VirtualWorkerRoutingIntegrationTestBase|MultiWorkerTestBase|Simulated[A-Za-z0-9]*TestBase|SharedEndpoint[A-Za-z0-9]*TestBase)|registerVirtualWorkers|registerVirtualWorkerRoutes|registerWorkerNode\s*\(\s*realHttpPort|virtual[ -]workers?|same (real )?worker endpoint|all pointing to (the )?(same|parent.s real)', re.IGNORECASE)

sources = []
def raise_walk_error(error):
    raise error

for directory, dirs, files in os.walk(root, onerror=raise_walk_error):
    dirs[:] = [name for name in dirs if not name.startswith('.')]
    for name in files:
        if name.endswith('.java') and not name.startswith('.'):
            path = Path(directory) / name
            if not path.is_symlink():
                sources.append((path, path.read_text(encoding='utf-8', errors='replace')))

def report(path, text, match):
    line = text.count('\n', 0, match.start()) + 1
    print(f"{path}:{line}:{text.splitlines()[line - 1]}")

for path, text in sources:
    for number, line in enumerate(text.splitlines(), 1):
        if pattern.search(line):
            print(f"{path}:{number}:{line}")
            print("E2E truthfulness gate failed: skips and constant assertions are forbidden.", file=sys.stderr)
            print("Unavailable required infrastructure must fail the test, and assertions must observe the public execution path.", file=sys.stderr)
            sys.exit(1)

for path, text in sources:
    for number, line in enumerate(text.splitlines(), 1):
        if masked_failure.search(line):
            print(f"{path}:{number}:{line}")
            print("E2E truthfulness gate failed: caught assertions, parser-only error acceptance and collection tautologies cannot prove execution.", file=sys.stderr)
            sys.exit(1)
    match = collection_tautology.search(text) or empty_failure.search(text)
    if match:
        report(path, text, match)
        print("E2E truthfulness gate failed: caught assertions, parser-only error acceptance and collection tautologies cannot prove execution.", file=sys.stderr)
        sys.exit(1)

for path, text in sources:
    if not path.name.endswith('Test.java') or not any(multi_worker_tag.search(line) for line in text.splitlines()):
        continue
    for number, line in enumerate(text.splitlines(), 1):
        if simulated_multi_worker.search(line):
            print(f"{path}:{number}:{line}")
            print(f"E2E truthfulness gate failed: multi-worker requires distinct worker processes and endpoints: {path}", file=sys.stderr)
            print("Virtual identities, a shared endpoint, and the single-worker E2E base are routing integration only.", file=sys.stderr)
            sys.exit(1)

print("E2E truthfulness gate passed")
PY
