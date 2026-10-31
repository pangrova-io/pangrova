#!/usr/bin/env python3
"""Compare complete matching JMH point estimates, without claiming significance."""

import argparse
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
import json
import math
from pathlib import Path
import sys


MODES = {"thrpt", "avgt", "sample", "ss"}
TIME_UNITS = {"ns", "us", "ms", "s", "min", "hr", "day"}
TEXT_SETTINGS = ("jmhVersion", "jdkVersion", "vmName", "vmVersion",
                 "warmupTime", "measurementTime")
INTEGER_SETTINGS = ("threads", "forks", "warmupIterations", "warmupBatchSize",
                    "measurementIterations", "measurementBatchSize")
SETTINGS = (*TEXT_SETTINGS, *INTEGER_SETTINGS, "jvmArgs")


@dataclass(frozen=True)
class Measurement:
    score: Decimal
    unit: str
    settings: tuple


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def positive_number(value, context):
    if type(value) not in (int, Decimal):
        raise ValueError(f"{context}: expected a positive finite JSON number")
    value = Decimal(value)
    if not value.is_finite() or value <= 0 or not math.isfinite(float(value)) or float(value) == 0:
        raise ValueError(f"{context}: expected a positive finite score; calibrate with real measurements")
    return value


def nonempty_string(value, context):
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{context}: expected a non-empty string")
    return value


def harness_settings(entry, context):
    for field in TEXT_SETTINGS:
        nonempty_string(entry.get(field), f"{context} {field}")
    for field in INTEGER_SETTINGS:
        value = entry.get(field)
        minimum = 0 if field == "warmupIterations" else 1
        if type(value) is not int or value < minimum:
            raise ValueError(f"{context} {field}: expected integer >= {minimum}")
    args = entry.get("jvmArgs")
    if not isinstance(args, list) or any(not isinstance(arg, str) for arg in args):
        raise ValueError(f"{context} jvmArgs: expected an array of strings")
    return tuple(tuple(entry[field]) if field == "jvmArgs" else entry[field] for field in SETTINGS)


def load_measurements(path, category):
    try:
        with Path(path).open(encoding="utf-8") as source:
            entries = json.load(source, parse_float=Decimal, object_pairs_hook=unique_object)
    except (OSError, ValueError) as error:
        raise ValueError(f"{category} {path}: {error}") from error
    if not isinstance(entries, list) or not entries:
        raise ValueError(f"{category} {path}: expected non-empty JMH results; calibrate a complete run")
    measurements = {}
    for index, entry in enumerate(entries):
        context = f"{category} entry[{index}]"
        if not isinstance(entry, dict):
            raise ValueError(f"{context}: expected a JSON object")
        name = nonempty_string(entry.get("benchmark"), f"{context} benchmark")
        mode = entry.get("mode")
        if not isinstance(mode, str) or mode not in MODES:
            raise ValueError(f"{context} mode: expected one of {sorted(MODES)}")
        params = entry.get("params", {})
        if not isinstance(params, dict) or any(
                not key or not isinstance(value, str) for key, value in params.items()):
            raise ValueError(f"{context} params: expected string keys and string values")
        key = (name, tuple(sorted(params.items())), mode)
        if key in measurements:
            raise ValueError(f"{context}: duplicate benchmark/params/mode {key!r}")
        metric = entry.get("primaryMetric")
        if not isinstance(metric, dict):
            raise ValueError(f"{context} primaryMetric: expected a JSON object")
        score = positive_number(metric.get("score"), f"{context} primaryMetric.score")
        unit = nonempty_string(metric.get("scoreUnit"), f"{context} primaryMetric.scoreUnit")
        valid_units = {f"ops/{time_unit}" if mode == "thrpt" else f"{time_unit}/op"
                       for time_unit in TIME_UNITS}
        if unit not in valid_units:
            raise ValueError(f"{context}: scoreUnit {unit!r} incompatible with mode {mode!r}")
        measurements[key] = Measurement(score, unit, harness_settings(entry, context))
    return measurements


def compare(current, baseline, threshold):
    missing, extra = baseline.keys() - current.keys(), current.keys() - baseline.keys()
    if missing or extra:
        raise ValueError(f"BENCHMARK_SET_MISMATCH: missing={sorted(missing)!r}; extra={sorted(extra)!r}")
    comparisons = []
    for key in sorted(baseline):
        previous, measured = baseline[key], current[key]
        if previous.unit != measured.unit:
            raise ValueError(f"UNIT_MISMATCH {key!r}: {previous.unit!r} vs {measured.unit!r}")
        if previous.settings != measured.settings:
            changed = [field for field, before, after in zip(
                SETTINGS, previous.settings, measured.settings) if before != after]
            raise ValueError(f"HARNESS_MISMATCH {key!r}: {', '.join(changed)}; recalibrate matching runs")
        # Multiplication keeps an exact threshold equality within the gate.
        if key[2] == "thrpt":
            regressed = measured.score < previous.score * (1 - threshold)
        else:
            regressed = measured.score > previous.score * (1 + threshold)
        change = (measured.score - previous.score) / previous.score * 100
        comparisons.append((key, previous, measured, change, regressed))
    return comparisons


def threshold_value(text):
    try:
        value = Decimal(text)
    except InvalidOperation as error:
        raise argparse.ArgumentTypeError("threshold must be finite and in [0, 1)") from error
    if not value.is_finite() or not 0 <= value < 1:
        raise argparse.ArgumentTypeError("threshold must be finite and in [0, 1)")
    return value


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("current")
    parser.add_argument("baseline")
    parser.add_argument("threshold", nargs="?", type=threshold_value, default=Decimal("0.10"))
    args = parser.parse_args()
    try:
        baseline = load_measurements(args.baseline, "UNCALIBRATED_OR_INVALID_BASELINE")
        current = load_measurements(args.current, "INVALID_RESULTS")
        comparisons = compare(current, baseline, args.threshold)
    except (ValueError, InvalidOperation) as error:
        print(f"ERROR: {error}", file=sys.stderr)
        return 2
    print(f"JMH point-estimate regression report; threshold={args.threshold * 100}%")
    for (name, params, mode), previous, measured, change, regressed in comparisons:
        status = "FAIL" if regressed else "PASS"
        print(f"{status} {name} params={dict(params)!r} mode={mode}: "
              f"{previous.score} -> {measured.score} {measured.unit} ({change:+.2f}%)")
    failed = sum(row[-1] for row in comparisons)
    print(f"{'FAILED' if failed else 'PASSED'}: compared={len(comparisons)}, regressed={failed}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
