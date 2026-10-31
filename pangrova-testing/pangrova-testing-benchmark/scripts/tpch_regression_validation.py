"""Validate full TPC-H runs before comparison; never infer scale or calibration.

Both runs and calibration require explicit R7, CLI end-to-end timing, at least
1000 measured samples per query and successful warmup. Raw samples are mandatory;
reported percentiles must match recomputation. Historical maximum-only summaries
must be recalibrated. Query ID case/zero padding is normalized, never duplicated.
"""

import math
import re

from tpc_statistics import MINIMUM_SAMPLES, PERCENTILE_METHOD, TIMING_SCOPE, sample_statistics


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
    if baseline.get("benchmark") != "tpch":
        raise ValueError(f"{context} benchmark: expected tpch")
    threshold = require_number(baseline.get("regressionThreshold", 0.15),
                               f"{context} regressionThreshold", allow_zero=True)
    if threshold >= 1:
        raise ValueError(f"{context} regressionThreshold: expected a value < 1")
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
    validate_statistical_contract(baseline, context)
    iterations = validate_run_metadata(baseline, context)
    for qid, record in queries.items():
        validate_samples(record, f"{context} {qid}", iterations, baseline, baseline=True)
    validate_execution_ids(queries, context)
    return scale, threshold


def validate_statistical_contract(document, context):
    if document.get("percentileMethod") != PERCENTILE_METHOD:
        raise ValueError(f"{context} percentileMethod: expected R7; recalibrate historical maxima")
    if document.get("timingScope") != TIMING_SCOPE:
        raise ValueError(f"{context} timingScope: expected {TIMING_SCOPE}")
    minimum = document.get("minimumSamples")
    if type(minimum) is not int or minimum < MINIMUM_SAMPLES:
        raise ValueError(f"{context} minimumSamples: expected integer >= {MINIMUM_SAMPLES}")


def validate_run_metadata(results, context="INVALID_RESULTS"):
    if results.get("benchmark") != "tpch":
        raise ValueError(f"{context} benchmark: expected tpch; use run-tpc-benchmark.sh output")
    if results.get("status") != "passed":
        raise ValueError(f"{context} status: run did not pass; rerun the benchmark")
    config = require_object(results.get("config"), f"{context} config")
    iterations = config.get("iterations")
    if type(iterations) is not int or iterations <= 0:
        raise ValueError(f"{context} config.iterations: expected a positive integer")
    for field, minimum in (("warmup", 0), ("timeout", 1), ("queryTimeout", 1)):
        if type(config.get(field)) is not int or config[field] < minimum:
            raise ValueError(f"{context} config.{field}: expected integer >= {minimum}")
    if config["queryTimeout"] >= config["timeout"]:
        raise ValueError(f"{context} config.timeout must exceed config.queryTimeout")
    summary = require_object(results.get("summary"), f"{context} summary")
    for field, expected in (("total", 22), ("passed", 22), ("failed", 0)):
        if type(summary.get(field)) is not int or summary[field] != expected:
            raise ValueError(f"{context} summary.{field}: expected {expected}; rerun all 22 queries")
    return iterations


def validate_samples(record, context, iterations, document, baseline=False):
    if record.get("status") != "passed":
        raise ValueError(f"{context} status: expected passed")
    completed = record.get("warmupCompleted")
    if type(completed) is not int or completed != document["config"]["warmup"]:
        raise ValueError(f"{context} warmupCompleted: expected config.warmup")
    digest = record.get("sqlSha256")
    if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise ValueError(f"{context} sqlSha256: expected exact SQL file digest")
    measured_ids = validate_query_ids(record.get("queryIds"), iterations, f"{context} queryIds")
    warmup_ids = validate_query_ids(record.get("warmupQueryIds"), completed, f"{context} warmupQueryIds")
    if set(measured_ids) & set(warmup_ids):
        raise ValueError(f"{context} queryIds: warmup must not be counted as measurement")
    samples = record.get("samples")
    if not isinstance(samples, list) or not samples:
        raise ValueError(f"{context} samples: expected a non-empty array of measured times")
    if len(samples) != iterations:
        raise ValueError(f"{context} samples: count must match config.iterations={iterations}")
    try:
        statistics = sample_statistics(samples, document["minimumSamples"])
    except ValueError as error:
        raise ValueError(f"{context} samples: {error}") from error
    count = record.get("sampleCount")
    if type(count) is not int or count != statistics["sampleCount"]:
        raise ValueError(f"{context} sampleCount: must equal raw samples count")
    if type(record.get("minimumSamples")) is not int or record["minimumSamples"] != document["minimumSamples"]:
        raise ValueError(f"{context} minimumSamples: must equal the run's threshold")
    if record.get("percentileQualified") is not True or not statistics["percentileQualified"]:
        raise ValueError(f"{context} percentileQualified: insufficient samples for P99; rerun with >= {document['minimumSamples']}")
    for statistic, field in (("p99Ms", "baselineP99Ms" if baseline else "p99Ms"),
                             ("p50Ms", "baselineP50Ms" if baseline else "p50Ms")):
        if statistic == "p50Ms" and field not in record:
            continue
        value = require_number(record.get(field), f"{context} {field}")
        if not math.isclose(value, statistics[statistic], rel_tol=1e-12, abs_tol=0):
            raise ValueError(f"{context} samples: {field} must match recomputed R7 {statistic}")


def validate_query_ids(value, count, context):
    if (not isinstance(value, list) or len(value) != count
            or any(not isinstance(qid, str) or not qid.strip() for qid in value)):
        raise ValueError(f"{context}: expected {count} non-empty execution IDs")
    if len(set(value)) != len(value):
        raise ValueError(f"{context}: duplicate execution ID cannot count as a sample")
    return value


def validate_execution_ids(queries, context):
    executions = [qid for record in queries.values() for field in ("queryIds", "warmupQueryIds")
                  for qid in record[field]]
    if len(set(executions)) != len(executions):
        raise ValueError(f"{context} queryIds: execution IDs must be distinct across the run")


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
    validate_statistical_contract(results, context)
    for field in ("percentileMethod", "minimumSamples", "timingScope"):
        if results[field] != baseline[field]:
            raise ValueError(f"STATISTICAL_CONTRACT_MISMATCH {field}: recalibrate matching runs")
    iterations = validate_run_metadata(results)
    for field in ("warmup", "timeout", "queryTimeout"):
        if results["config"][field] != baseline["config"][field]:
            raise ValueError(f"RUN_CONFIG_MISMATCH {field}: recalibrate matching runs")
    queries = query_map(results, context)
    baseline_queries = query_map(baseline, "INVALID_BASELINE", baseline=True)
    for qid, record in queries.items():
        query_context = f"{context} {qid}"
        previous = baseline_queries[qid]
        if record.get("sqlSha256") != previous["sqlSha256"]:
            raise ValueError(f"SQL_INPUT_MISMATCH {qid}: recalibrate for the exact SQL file")
        if record.get("status") != "passed":
            raise ValueError(f"{query_context} status: expected passed; rerun failed or unconfirmed query")
        if record.get("percentileQualified") is False:
            raise ValueError(f"{query_context} percentileQualified: insufficient measured samples for P99")
        require_number(record.get("p99Ms"), f"{query_context} p99Ms")
        if "p50Ms" in record:
            require_number(record["p50Ms"], f"{query_context} p50Ms")
            if record["p50Ms"] > record["p99Ms"]:
                raise ValueError(f"{query_context} p50Ms exceeds p99Ms")
        validate_samples(record, query_context, iterations, results)
    validate_execution_ids(queries, context)
    aligned = {key: queries[query_id(key, "INVALID_BASELINE")] for key in baseline["queries"]}
    return {**results, "queries": aligned}, threshold
