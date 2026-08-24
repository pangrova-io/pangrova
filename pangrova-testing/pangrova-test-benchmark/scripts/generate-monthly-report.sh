#!/usr/bin/env bash
# Generate Monthly TPC-H Performance Regression Report
#
# This script is designed to be run monthly (e.g., via cron or CI schedule).
# It compares the latest benchmark results against the baseline, generates
# a report with trend charts from historical data, and sends alerts for
# any query with >15% regression.
#
# Prerequisites:
#   - Python 3.8+
#   - Benchmark results stored in HISTORY_DIR as dated JSON files
#   - Latest results file available
#
# Usage: ./generate-monthly-report.sh [options]
#
# Options:
#   --results-dir DIR    Directory containing benchmark result files (default: ../results)
#   --reports-dir DIR    Directory to write monthly reports (default: ../reports)
#   --webhook URL        Webhook URL for regression alerts
#   --latest FILE        Specific results file to use (default: most recent in results-dir)
#
# Cron example (1st of each month at 9am):
#   0 9 1 * * /path/to/generate-monthly-report.sh --webhook https://hooks.slack.com/xxx
#
# CI example (GitHub Actions):
#   schedule:
#     - cron: '0 9 1 * *'
#   steps:
#     - run: ./scripts/generate-monthly-report.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCHMARK_ROOT="${SCRIPT_DIR}/.."
BASELINE_FILE="${BENCHMARK_ROOT}/baseline/tpch-performance-baseline.json"

# Defaults
RESULTS_DIR="${BENCHMARK_ROOT}/results"
REPORTS_DIR="${BENCHMARK_ROOT}/reports"
WEBHOOK_URL=""
LATEST_FILE=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --results-dir)
            RESULTS_DIR="$2"
            shift 2
            ;;
        --reports-dir)
            REPORTS_DIR="$2"
            shift 2
            ;;
        --webhook)
            WEBHOOK_URL="$2"
            shift 2
            ;;
        --latest)
            LATEST_FILE="$2"
            shift 2
            ;;
        -h|--help)
            head -30 "$0" | tail -28
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 2
            ;;
    esac
done

# Validate baseline exists
if [[ ! -f "$BASELINE_FILE" ]]; then
    echo "ERROR: Baseline file not found: $BASELINE_FILE"
    exit 2
fi

# Find latest results file
if [[ -z "$LATEST_FILE" ]]; then
    if [[ ! -d "$RESULTS_DIR" ]]; then
        echo "ERROR: Results directory not found: $RESULTS_DIR"
        echo "  Run benchmarks first, or specify --results-dir"
        exit 2
    fi
    LATEST_FILE=$(find "$RESULTS_DIR" -name "*.json" -type f | sort | tail -1)
    if [[ -z "$LATEST_FILE" ]]; then
        echo "ERROR: No result files found in: $RESULTS_DIR"
        exit 2
    fi
fi

if [[ ! -f "$LATEST_FILE" ]]; then
    echo "ERROR: Results file not found: $LATEST_FILE"
    exit 2
fi

# Create reports directory
mkdir -p "$REPORTS_DIR"

# Generate report filename with current month
MONTH=$(date +%Y-%m)
REPORT_FILE="${REPORTS_DIR}/tpch-regression-report-${MONTH}.txt"

echo "============================================"
echo "  Monthly Performance Report Generation"
echo "============================================"
echo "  Month:    $MONTH"
echo "  Baseline: $BASELINE_FILE"
echo "  Latest:   $LATEST_FILE"
echo "  History:  $RESULTS_DIR"
echo "  Report:   $REPORT_FILE"
echo "============================================"
echo ""

# Run comparison with report generation
export BASELINE_PATH="$BASELINE_FILE"
export RESULTS_PATH="$LATEST_FILE"
export HISTORY_DIR="$RESULTS_DIR"
export REPORT_OUTPUT="$REPORT_FILE"
export ALERT_WEBHOOK="$WEBHOOK_URL"

EXIT_CODE=0
python3 "${SCRIPT_DIR}/tpch-regression-compare.py" || EXIT_CODE=$?

echo ""
if [[ $EXIT_CODE -eq 0 ]]; then
    echo "✅ Monthly report generated successfully: $REPORT_FILE"
    echo "   All queries within 15% regression threshold."
elif [[ $EXIT_CODE -eq 1 ]]; then
    echo "❌ Monthly report generated with REGRESSIONS: $REPORT_FILE"
    echo "   One or more queries exceeded 15% regression threshold."
    if [[ -n "$WEBHOOK_URL" ]]; then
        echo "   Alert notification sent."
    fi
else
    echo "⚠️  Report generation encountered an error (exit code: $EXIT_CODE)"
fi

exit $EXIT_CODE
