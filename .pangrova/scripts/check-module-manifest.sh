#!/usr/bin/env bash
# Verifies that the committed module fact baseline matches the reactor and that
# every modular artifact declares an ownership entry.
#
# The Java gate (ModuleDependencyPropertyTest) is authoritative for policy and
# package ownership. This script fails fast in CI before the Maven reactor starts
# and catches a stale baseline when tests are skipped.
#
# Usage:  .pangrova/scripts/check-module-manifest.sh
# Exit:   0 = current, 1 = stale/missing declaration, 2 = environment error.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
GENERATOR="${ROOT_DIR}/.pangrova/scripts/generate-module-baseline.py"
MANIFEST="${ROOT_DIR}/.pangrova/modules.yaml"

if ! command -v python3 >/dev/null 2>&1; then
    echo "ERROR: python3 is required to verify the module baseline." >&2
    exit 2
fi
if [[ ! -f "${GENERATOR}" || ! -f "${MANIFEST}" ]]; then
    echo "ERROR: module manifest files are missing." >&2
    exit 2
fi

"${GENERATOR}" --check

# Capture discovery first: process substitution would hide its exit status and
# could report success after a Python traceback or an unreadable source tree.
artifacts="$(python3 - "${GENERATOR}" <<'PYTHON'
import importlib.util
import sys

spec = importlib.util.spec_from_file_location("module_baseline", sys.argv[1])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
for artifact, facts in module.pom_artifacts().items():
    if facts["packaging"] != "pom" or facts["leaf"]:
        print(artifact)
PYTHON
)"

missing=0
while IFS= read -r artifact; do
    [[ -z "${artifact}" ]] && continue
    if ! grep -qE "^  ${artifact}:" "${MANIFEST}"; then
        echo "VIOLATION: artifact '${artifact}' has no modules.yaml entry" >&2
        missing=$((missing + 1))
    fi
done <<< "${artifacts}"

if [[ ${missing} -gt 0 ]]; then
    echo "FAIL: ${missing} artifact(s) missing from .pangrova/modules.yaml." >&2
    exit 1
fi

echo "OK: module manifest and baseline are current."
