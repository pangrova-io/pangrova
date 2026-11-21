#!/usr/bin/env bash
# Compare complete matching JMH runs. Missing calibration is an error, never a pass.
# Usage: jmh-regression-check.sh <current-results.json> <baseline.json> [threshold]
# Exit codes: 0 = within threshold, 1 = regression, 2 = invalid/non-comparable inputs.
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec python3 "${SCRIPT_DIR}/jmh-regression-compare.py" "$@"
