#!/usr/bin/env python3
"""TPC-H performance regression comparison and monthly report generation.

Compares current benchmark results against the version-controlled baseline.
Alerts on >15% regression for any query. Generates monthly trend reports.
Exit codes: 0 = within threshold, 1 = regression, 2 = invalid input or uncalibrated baseline.
See tpch_regression_validation.py for the full-run JSON contract.

Environment variables:
    BASELINE_PATH: path to the baseline JSON file
    RESULTS_PATH:  path to the current results JSON file
    HISTORY_DIR:   (optional) directory containing historical result files for trend analysis
    REPORT_OUTPUT: (optional) path to write the monthly report (default: stdout)
    ALERT_WEBHOOK: (optional) webhook URL for regression alerts
"""

import json
import math
import os
import sys
from datetime import datetime
from pathlib import Path

from tpch_regression_validation import require_number, validate_results


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key {key!r}; remove ambiguous measurements")
        result[key] = value
    return result


def load_json(path, source="results"):
    category = "INVALID_BASELINE" if source == "baseline" else "INVALID_RESULTS"
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f, object_pairs_hook=unique_object)
    except FileNotFoundError as error:
        if source == "baseline":
            raise ValueError(
                f"UNCALIBRATED_BASELINE {path}: file missing; supply a calibrated matching-scale baseline"
            ) from error
        raise ValueError(f"{category} {path}: file missing; run the complete benchmark first") from error
    except (OSError, ValueError) as error:
        raise ValueError(f"{category} {path}: cannot read valid JSON: {error}") from error


def compute_change(baseline_ms, current_ms):
    require_number(baseline_ms, "INVALID_BASELINE comparison latency")
    require_number(current_ms, "INVALID_RESULTS comparison latency")
    try:
        change = (current_ms - baseline_ms) / baseline_ms
    except OverflowError as error:
        raise ValueError("INVALID_RESULTS: non-finite computed change; verify measurement units") from error
    if not math.isfinite(change):
        raise ValueError("INVALID_RESULTS: non-finite computed change; verify measurement units")
    return change


def compare_results(baseline, results):
    results, threshold = validate_results(baseline, results)
    queries = baseline["queries"]
    comparisons = []
    has_failure = False

    for qid in sorted(queries.keys(), key=lambda x: int(x[1:])):
        qdata = queries[qid]
        baseline_p99 = qdata["baselineP99Ms"]
        baseline_p50 = qdata.get("baselineP50Ms")
        current_p99 = results["queries"][qid]["p99Ms"]
        current_p50 = results["queries"][qid].get("p50Ms")

        change_p99 = compute_change(baseline_p99, current_p99)
        change_p50 = (compute_change(baseline_p50, current_p50)
                      if current_p50 is not None and baseline_p50 is not None else None)

        if change_p99 > threshold:
            status = "FAIL"
            has_failure = True
        elif change_p99 > threshold * 0.67:
            status = "WARN"
        else:
            status = "PASS"

        comparisons.append({
            "query": qid,
            "name": qdata.get("name", qid),
            "baselineP99": baseline_p99,
            "currentP99": current_p99,
            "changeP99": change_p99,
            "baselineP50": baseline_p50,
            "currentP50": current_p50,
            "changeP50": change_p50,
            "status": status
        })

    return comparisons, has_failure, threshold


def load_history(history_dir):
    """Load historical results in filename order (use date-prefixed filenames)."""
    if not history_dir:
        return []
    if not Path(history_dir).is_dir():
        raise ValueError(f"INVALID_RESULTS history {history_dir}: directory does not exist")

    history = []
    for f in sorted(Path(history_dir).glob("*.json")):
        data = load_json(f)
        history.append({"file": str(f), "data": data})
    return history


def compute_trends(baseline, history):
    """Compute per-query trends across historical runs."""
    if not history:
        return {}

    queries = baseline["queries"]
    trends = {}
    validated = []
    for entry in history:
        try:
            data, _ = validate_results(baseline, entry["data"])
        except ValueError as error:
            raise ValueError(f"History {entry['file']}: {error}") from error
        date_str = data.get("date", data.get("timestamp", Path(entry["file"]).stem))
        validated.append({"date": date_str, "data": data})

    for qid in queries:
        query_history = []
        for entry in validated:
            p99 = entry["data"]["queries"][qid]["p99Ms"]
            query_history.append({"date": entry["date"], "p99Ms": p99})
        if query_history:
            trends[qid] = query_history

    return trends


def format_ascii_chart(values, width=40, height=8):
    """Render a simple ASCII sparkline chart for trend visualization."""
    if not values or len(values) < 2:
        return "  (insufficient data for trend)"

    min_val = min(values)
    max_val = max(values)
    val_range = max_val - min_val if max_val != min_val else 1

    lines = []
    for row in range(height - 1, -1, -1):
        threshold = min_val + (val_range * row / (height - 1))
        line = ""
        for val in values:
            normalized = (val - min_val) / val_range * (height - 1)
            if abs(normalized - row) < 0.5:
                line += "●"
            elif normalized > row:
                line += "│"
            else:
                line += " "
        label = f"{threshold:>8.0f}ms"
        lines.append(f"  {label} |{line}")

    x_axis = "  " + " " * 10 + "+" + "─" * len(values)
    lines.append(x_axis)
    return "\n".join(lines)


def generate_report(comparisons, has_failure, threshold, baseline, history_dir):
    """Generate the monthly performance regression report."""
    report_lines = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    report_lines.append("=" * 72)
    report_lines.append(f"  Pangrova TPC-H SF={baseline['scaleFactor']} Monthly Performance Regression Report")
    report_lines.append("=" * 72)
    report_lines.append(f"  Generated: {now}")
    report_lines.append(f"  Baseline:  SF={baseline['scaleFactor']} ({baseline.get('lastUpdated', 'undated')})")
    report_lines.append(f"  Threshold: {threshold * 100:.0f}% regression triggers alert")
    report_lines.append(f"  Queries:   {len(comparisons)} TPC-H queries evaluated")
    report_lines.append("")

    # Summary
    passed = sum(1 for c in comparisons if c["status"] == "PASS")
    warned = sum(1 for c in comparisons if c["status"] == "WARN")
    failed = sum(1 for c in comparisons if c["status"] == "FAIL")

    report_lines.append("─" * 72)
    report_lines.append("  SUMMARY")
    report_lines.append("─" * 72)
    report_lines.append(f"  ✅ PASS: {passed}  ⚠️  WARN: {warned}  ❌ FAIL: {failed}")
    report_lines.append(f"  Overall: {'FAILED - Regression detected' if has_failure else 'PASSED - All within threshold'}")
    report_lines.append("")

    # Detailed results table
    report_lines.append("─" * 72)
    report_lines.append("  DETAILED RESULTS")
    report_lines.append("─" * 72)
    header = f"  {'Query':<6} {'Name':<35} {'Base(ms)':<10} {'Curr(ms)':<10} {'Change':<10} {'Status'}"
    report_lines.append(header)
    report_lines.append("  " + "-" * 68)

    for c in comparisons:
        sign = "+" if c["changeP99"] >= 0 else ""
        change_str = f"{sign}{c['changeP99'] * 100:.1f}%"
        status_icon = "✅" if c["status"] == "PASS" else ("⚠️" if c["status"] == "WARN" else "❌")
        report_lines.append(
            f"  {c['query']:<6} {c['name']:<35} {c['baselineP99']:<10} {c['currentP99']:<10.0f} {change_str:<10} {status_icon} {c['status']}"
        )

    report_lines.append("")

    # Regressions detail (if any)
    regressions = [c for c in comparisons if c["status"] == "FAIL"]
    if regressions:
        report_lines.append("─" * 72)
        report_lines.append("  ❌ REGRESSIONS REQUIRING INVESTIGATION")
        report_lines.append("─" * 72)
        for c in regressions:
            report_lines.append(f"  {c['query']} ({c['name']}):")
            report_lines.append(f"    Baseline P99: {c['baselineP99']}ms → Current P99: {c['currentP99']:.0f}ms")
            report_lines.append(f"    Regression: +{c['changeP99'] * 100:.1f}% (threshold: {threshold * 100:.0f}%)")
            report_lines.append("")

    # Trend charts (if history available)
    history = load_history(history_dir)
    if history:
        trends = compute_trends(baseline, history)
        report_lines.append("─" * 72)
        report_lines.append("  TREND CHARTS (last {} runs)".format(len(history)))
        report_lines.append("─" * 72)

        for qid in sorted(trends.keys(), key=lambda x: int(x[1:])):
            entries = trends[qid]
            values = [e["p99Ms"] for e in entries]
            report_lines.append(f"\n  {qid} - {baseline['queries'][qid].get('name', qid)}:")
            report_lines.append(f"  Range: {min(values):.0f}ms - {max(values):.0f}ms")
            report_lines.append(format_ascii_chart(values))
            dates = [e["date"] for e in entries]
            if dates:
                report_lines.append(f"  Period: {dates[0]} → {dates[-1]}")
            report_lines.append("")

    report_lines.append("─" * 72)
    report_lines.append("  END OF REPORT")
    report_lines.append("─" * 72)

    return "\n".join(report_lines)


def send_alert(webhook_url, regressions, threshold):
    """Send alert notification for regressions via webhook."""
    if not webhook_url:
        return

    try:
        import urllib.request

        alert_body = {
            "text": f"🚨 TPC-H Performance Regression Alert\n"
                    f"Threshold: >{threshold * 100:.0f}%\n"
                    f"Regressions detected: {len(regressions)}\n"
                    + "\n".join(
                        f"  • {r['query']} ({r['name']}): +{r['changeP99'] * 100:.1f}%"
                        for r in regressions
                    ),
            "timestamp": datetime.now().isoformat(),
            "severity": "critical"
        }

        data = json.dumps(alert_body).encode("utf-8")
        req = urllib.request.Request(
            webhook_url,
            data=data,
            headers={"Content-Type": "application/json"}
        )
        urllib.request.urlopen(req, timeout=10)
        print(f"  Alert sent to webhook: {len(regressions)} regression(s) reported")
    except Exception as e:
        print(f"  WARNING: Failed to send alert: {e}", file=sys.stderr)


def main():
    baseline_file = os.environ.get("BASELINE_PATH", "")
    results_file = os.environ.get("RESULTS_PATH", "")
    history_dir = os.environ.get("HISTORY_DIR", "")
    report_output = os.environ.get("REPORT_OUTPUT", "")
    alert_webhook = os.environ.get("ALERT_WEBHOOK", "")

    if not baseline_file or not results_file:
        print("ERROR: BASELINE_PATH and RESULTS_PATH environment variables required", file=sys.stderr)
        sys.exit(2)

    try:
        baseline = load_json(baseline_file, source="baseline")
        results = load_json(results_file)
        comparisons, has_failure, threshold = compare_results(baseline, results)
        report = generate_report(comparisons, has_failure, threshold, baseline, history_dir)
    except (ValueError, OSError) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        sys.exit(2)

    # Output report
    if report_output:
        try:
            Path(report_output).parent.mkdir(parents=True, exist_ok=True)
            with open(report_output, "w", encoding="utf-8") as f:
                f.write(report)
        except OSError as error:
            print(f"ERROR: cannot write report {report_output}: {error}", file=sys.stderr)
            sys.exit(2)
        print(f"Report written to: {report_output}")
    else:
        print(report)

    # Send alerts for regressions
    regressions = [c for c in comparisons if c["status"] == "FAIL"]
    if regressions and alert_webhook:
        send_alert(alert_webhook, regressions, threshold)

    # Exit code
    if has_failure:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
