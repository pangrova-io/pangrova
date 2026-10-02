"""Validate full TPC-H runs before comparison; never infer scale or calibration.

Results require benchmark=tpch, a positive scaleFactor and all 22 queries with
status=passed and positive finite p99Ms. P50 remains optional. When supplied,
P50, samples, iteration counts and run summaries must be valid and consistent.
Query IDs Q1/q01 (case and zero padding) are equivalent, but duplicates are not.
"""

import math
import re


QUERY_IDS = {f"Q{i}" for i in range(1, 23)}


def require_object(value, context):
    if not isinstance(value, dict):
        raise ValueError(f"{context}: expected a JSON object")
    return value


def require_number(value, context, allow_zero=False):
    valid = type(value) in (int, float)
    if valid:
        try:
            valid = math.isfinite(value) and (value >= 0 if allow_zero else value > 0)
        except OverflowError:
            valid = False
    if not valid:
        bound = "non-negative" if allow_zero else "positive"
        raise ValueError(f"{context}: expected a {bound} finite JSON number; got {value!r}")
    return value


def query_id(key, context):
    if not isinstance(key, str) or not re.fullmatch(r"[Qq](?:0?[1-9]|1[0-9]|2[0-2])", key):
        raise ValueError(f"{context}: invalid query ID {key!r}; expected Q1..Q22 or q01..q22")
    return f"Q{int(key[1:])}"


def query_map(document, context, baseline=False):
    queries = document.get("queries")
    if queries is None and baseline:
        raise ValueError("UNCALIBRATED_BASELINE: missing queries; calibrate all 22 TPC-H queries")
    require_object(queries, f"{context} queries")
    normalized = {}
    for key, record in queries.items():
        qid = query_id(key, context)
        if qid in normalized:
            raise ValueError(f"{context}: duplicate query {qid} via alias {key!r}")
        normalized[qid] = require_object(record, f"{context} {qid}")
    missing = QUERY_IDS - normalized.keys()
    if missing:
        category = "UNCALIBRATED_BASELINE" if baseline else context
        names = ", ".join(sorted(missing, key=lambda q: int(q[1:])))
        action = "calibrate" if baseline else "rerun"
        raise ValueError(f"{category}: missing queries {names}; {action} all 22 TPC-H queries")
    return normalized


def validate_baseline(baseline):
    context = "INVALID_BASELINE"
    require_object(baseline, context)
    scale = require_number(baseline.get("scaleFactor"), f"{context} scaleFactor")
    if baseline.get("benchmark", "tpch") != "tpch":
        raise ValueError(f"{context} benchmark: expected tpch")
    threshold = require_number(baseline.get("regressionThreshold", 0.15),
                               f"{context} regressionThreshold", allow_zero=True)
    queries = query_map(baseline, context, baseline=True)
    for qid, record in queries.items():
        for field in ("baselineP99Ms", "baselineP50Ms"):
            if field == "baselineP50Ms" and field not in record:
                continue
            value = record.get(field)
            if field not in record or (type(value) in (int, float) and value == 0):
                raise ValueError(
                    f"UNCALIBRATED_BASELINE SF={scale} {qid} {field}: missing or zero; "
                    "calibrate with real measurements on matching-scale benchmark hardware"
                )
            require_number(value, f"{context} {qid} {field}")
        if record.get("baselineP50Ms", 0) > record["baselineP99Ms"]:
            raise ValueError(f"{context} {qid} baselineP50Ms exceeds baselineP99Ms")
        if "name" in record and not isinstance(record["name"], str):
            raise ValueError(f"{context} {qid} name: expected a string")
    return scale, threshold


def validate_run_metadata(results):
    context = "INVALID_RESULTS"
    if results.get("benchmark") != "tpch":
        raise ValueError(f"{context} benchmark: expected tpch; use run-tpc-benchmark.sh output")
    if "status" in results and results["status"] != "passed":
        raise ValueError(f"{context} status: run did not pass; rerun the benchmark")
    iterations = None
    if "config" in results:
        config = require_object(results["config"], f"{context} config")
        iterations = config.get("iterations")
        if type(iterations) is not int or iterations <= 0:
            raise ValueError(f"{context} config.iterations: expected a positive integer")
    if "summary" in results:
        summary = require_object(results["summary"], f"{context} summary")
        for field, expected in (("total", 22), ("passed", 22), ("failed", 0)):
            if type(summary.get(field)) is not int or summary[field] != expected:
                raise ValueError(
                    f"{context} summary.{field}: expected {expected}; "
                    "run failed or is incomplete/inconsistent; rerun all 22 queries"
                )
    return iterations


def validate_samples(record, context, iterations):
    if "samples" not in record:
        return
    samples = record["samples"]
    if not isinstance(samples, list) or not samples:
        raise ValueError(f"{context} samples: expected a non-empty array of measured times")
    if iterations is not None and len(samples) != iterations:
        raise ValueError(f"{context} samples: count must match config.iterations={iterations}")
    for index, sample in enumerate(samples):
        require_number(sample, f"{context} samples[{index}]")
    ordered = sorted(samples)
    # The shell runner reports the lower median and maximum, not an interpolated percentile.
    if record["p99Ms"] != ordered[-1]:
        raise ValueError(f"{context} samples: p99Ms must equal the runner's maximum sample")
    if "p50Ms" in record and record["p50Ms"] != ordered[(len(ordered) - 1) // 2]:
        raise ValueError(f"{context} samples: p50Ms must equal the runner's lower median sample")


def validate_results(baseline, results):
    """Return results keyed like the baseline plus its threshold, without mutating inputs."""
    scale, threshold = validate_baseline(baseline)
    context = "INVALID_RESULTS"
    require_object(results, context)
    result_scale = require_number(results.get("scaleFactor"), f"{context} scaleFactor")
    if result_scale != scale:
        raise ValueError(
            f"SCALE_MISMATCH: baseline SF={scale}, results SF={result_scale}; "
            "use a calibrated baseline and complete results from the same scale"
        )
    iterations = validate_run_metadata(results)
    queries = query_map(results, context)
    for qid, record in queries.items():
        query_context = f"{context} {qid}"
        if record.get("status") != "passed":
            raise ValueError(f"{query_context} status: expected passed; rerun failed or unconfirmed query")
        require_number(record.get("p99Ms"), f"{query_context} p99Ms")
        if "p50Ms" in record:
            require_number(record["p50Ms"], f"{query_context} p50Ms")
            if record["p50Ms"] > record["p99Ms"]:
                raise ValueError(f"{query_context} p50Ms exceeds p99Ms")
        validate_samples(record, query_context, iterations)
    aligned = {key: queries[query_id(key, "INVALID_BASELINE")] for key in baseline["queries"]}
    return {**results, "queries": aligned}, threshold