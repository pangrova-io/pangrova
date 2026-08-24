#!/usr/bin/env bash
# TPC-H SF=10 Performance Regression Check
#
# Compares all 22 TPC-H query execution times against the version-controlled baseline.
# Fails if any query regresses more than 15% from baseline P99.
# Optionally generates a monthly report with trend charts.
#
# Usage: ./tpch-regression-check.sh <results-json> [baseline-json] [options]
#   results-json:  path to the benchmark results file (produced by benchmark run)
#   baseline-json: path to the baseline file (default: ../baseline/tpch-performance-baseline.json)
#
# Options (via environment variables):
#   HISTORY_DIR:   directory containing historical result files for trend analysis
#   REPORT_OUTPUT: path to write the monthly report file
#   ALERT_WEBHOOK: webhook URL for regression alert notifications
#
# Exit codes:
#   0 - all queries within 15% threshold
#   1 - one or more queries regressed beyond threshold
#   2 - script error (missing files, parse error)
#
# Examples:
#   # Basic regression check
#   ./tpch-regression-check.sh results/2025-02-01.json
#
#   # With monthly report generation
#   HISTORY_DIR=results/ REPORT_OUTPUT=reports/2025-02.txt \
#     ./tpch-regression-check.sh results/2025-02-01.json
#
#   # With alert webhook
#   ALERT_WEBHOOK=https://hooks.slack.com/services/xxx \
#     ./tpch-regression-check.sh results/2025-02-01.json

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_BASELINE="${SCRIPT_DIR}/../baseline/tpch-performance-baseline.json"

RESULTS_FILE="${1:-}"
BASELINE_FILE="${2:-$DEFAULT_BASELINE}"

if [[ -z "$RESULTS_FILE" ]]; then
    echo "Usage: $0 <results-json> [baseline-json]"
    echo ""
    echo "Environment variables:"
    echo "  HISTORY_DIR    - directory with historical results for trend charts"
    echo "  REPORT_OUTPUT  - file path to write monthly report"
    echo "  ALERT_WEBHOOK  - webhook URL for regression alerts (>15%)"
    exit 2
fi

if [[ ! -f "$RESULTS_FILE" ]]; then
    echo "ERROR: Results file not found: $RESULTS_FILE"
    exit 2
fi

if [[ ! -f "$BASELINE_FILE" ]]; then
    echo "ERROR: Baseline file not found: $BASELINE_FILE"
    exit 2
fi

echo "============================================"
echo "  TPC-H SF=10 Performance Regression Check"
echo "============================================"
echo "  Baseline: $BASELINE_FILE"
echo "  Results:  $RESULTS_FILE"
if [[ -n "${HISTORY_DIR:-}" ]]; then
    echo "  History:  $HISTORY_DIR"
fi
if [[ -n "${REPORT_OUTPUT:-}" ]]; then
    echo "  Report:   $REPORT_OUTPUT"
fi
if [[ -n "${ALERT_WEBHOOK:-}" ]]; then
    echo "  Alerts:   enabled"
fi
echo "============================================"
echo ""

export BASELINE_PATH="$BASELINE_FILE"
export RESULTS_PATH="$RESULTS_FILE"
export HISTORY_DIR="${HISTORY_DIR:-}"
export REPORT_OUTPUT="${REPORT_OUTPUT:-}"
export ALERT_WEBHOOK="${ALERT_WEBHOOK:-}"

exec python3 "${SCRIPT_DIR}/tpch-regression-compare.py"
