#!/usr/bin/env bash
# JMH Benchmark Regression Detection
#
# Compares current JMH results against a baseline file.
# Fails the build if any benchmark regresses by more than the threshold (default 10%).
#
# Usage: jmh-regression-check.sh <current-results.json> <baseline.json> [threshold]
#   current-results.json  - JMH JSON output from the current run
#   baseline.json         - Stored baseline JMH JSON results
#   threshold             - Regression threshold as decimal (default: 0.10 = 10%)
#
# Exit codes:
#   0 - All benchmarks within threshold
#   1 - One or more benchmarks regressed beyond threshold
#   2 - Usage error or missing files

set -euo pipefail

if [ $# -lt 2 ]; then
    echo "Usage: $0 <current-results.json> <baseline.json> [threshold]"
    exit 2
fi

CURRENT="$1"
BASELINE="$2"
THRESHOLD="${3:-0.10}"

if [ ! -f "$CURRENT" ]; then
    echo "ERROR: Current results file not found: $CURRENT"
    exit 2
fi

if [ ! -f "$BASELINE" ]; then
    echo "WARN: No baseline file found at $BASELINE. Skipping regression check (first run)."
    echo "Copy current results to baseline: cp $CURRENT $BASELINE"
    exit 0
fi

# Use python for JSON parsing and comparison
python3 - "$CURRENT" "$BASELINE" "$THRESHOLD" << 'PYTHON_SCRIPT'
import json
import sys

def load_jmh_results(path):
    """Load JMH JSON results into a dict keyed by benchmark name."""
    with open(path) as f:
        data = json.load(f)
    results = {}
    for entry in data:
        name = entry.get("benchmark", "")
        score = entry.get("primaryMetric", {}).get("score", 0)
        mode = entry.get("mode", "thrpt")
        results[name] = {"score": score, "mode": mode}
    return results

def check_regression(current_path, baseline_path, threshold):
    current = load_jmh_results(current_path)
    baseline = load_jmh_results(baseline_path)

    regressions = []
    improvements = []
    unchanged = []

    for name, base_data in baseline.items():
        if name not in current:
            continue

        base_score = base_data["score"]
        curr_score = current[name]["score"]
        mode = base_data["mode"]

        if base_score == 0:
            continue

        # For throughput (thrpt): higher is better, regression = current < baseline
        # For average time (avgt): lower is better, regression = current > baseline
        if mode == "thrpt":
            change = (curr_score - base_score) / base_score
            regressed = change < -threshold
        else:
            change = (curr_score - base_score) / base_score
            regressed = change > threshold

        entry = {
            "name": name.split(".")[-1],
            "baseline": base_score,
            "current": curr_score,
            "change_pct": change * 100,
            "mode": mode
        }

        if regressed:
            regressions.append(entry)
        elif abs(change) > threshold:
            improvements.append(entry)
        else:
            unchanged.append(entry)

    # Print report
    print("=" * 70)
    print("JMH Benchmark Regression Report")
    print(f"Threshold: {threshold * 100:.0f}%")
    print("=" * 70)

    if regressions:
        print(f"\n❌ REGRESSIONS ({len(regressions)}):")
        for r in regressions:
            print(f"  {r['name']}: {r['baseline']:.2f} -> {r['current']:.2f} "
                  f"({r['change_pct']:+.1f}%)")

    if improvements:
        print(f"\n✅ IMPROVEMENTS ({len(improvements)}):")
        for r in improvements:
            print(f"  {r['name']}: {r['baseline']:.2f} -> {r['current']:.2f} "
                  f"({r['change_pct']:+.1f}%)")

    if unchanged:
        print(f"\n⚪ UNCHANGED ({len(unchanged)}):")
        for r in unchanged:
            print(f"  {r['name']}: {r['change_pct']:+.1f}%")

    print("\n" + "=" * 70)

    if regressions:
        print(f"FAILED: {len(regressions)} benchmark(s) regressed beyond {threshold * 100:.0f}% threshold")
        return 1
    else:
        print("PASSED: All benchmarks within acceptable range")
        return 0

if __name__ == "__main__":
    current_path = sys.argv[1]
    baseline_path = sys.argv[2]
    threshold = float(sys.argv[3])
    sys.exit(check_regression(current_path, baseline_path, threshold))
PYTHON_SCRIPT
