#!/usr/bin/env python3
"""Run TPC query workloads via the batch CLI; no claim of certified TPC scores.

End-to-end latency includes process startup, submission and complete result decode.
Every successful invocation must produce a receipt for the exact SQL file. Warmup
failures are failures. Small runs retain descriptive statistics without P99.
"""

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tempfile
import time

from tpc_statistics import MINIMUM_SAMPLES, PERCENTILE_METHOD, TIMING_SCOPE, sample_statistics


BENCHMARK_ROOT = Path(__file__).resolve().parent.parent


def integer_at_least(minimum):
    def parse(value):
        try:
            result = int(value)
        except ValueError as error:
            raise argparse.ArgumentTypeError(f"expected integer >= {minimum}") from error
        if result < minimum:
            raise argparse.ArgumentTypeError(f"expected integer >= {minimum}")
        return result
    return parse


def arguments():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--benchmark", choices=("tpch", "tpcds"), required=True)
    parser.add_argument("--scale-factor", type=integer_at_least(1), default=100)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=integer_at_least(1), default=8080)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--queries", help="comma-separated Q1/q01 IDs; default all")
    parser.add_argument("--queries-dir", type=Path, default=BENCHMARK_ROOT / "queries")
    parser.add_argument("--warmup", type=integer_at_least(0), default=1)
    parser.add_argument("--iterations", type=integer_at_least(1), default=3)
    parser.add_argument("--minimum-samples", type=integer_at_least(MINIMUM_SAMPLES), default=MINIMUM_SAMPLES)
    parser.add_argument("--timeout", type=integer_at_least(1), default=600,
                        help="overall CLI process deadline in seconds, including startup (default 600)")
    parser.add_argument("--query-timeout", type=integer_at_least(1), default=300,
                        help="CLI query deadline after submission in seconds (default 300); must be less than --timeout")
    parser.add_argument("--catalog", default="tpc")
    parser.add_argument("--schema", help="default sf<scale-factor>")
    parser.add_argument("--cli-path", default="pangrova-cli")
    parser.add_argument("--token-env", help="name of an environment variable containing the Bearer token; never pass a token value")
    args = parser.parse_args()
    if args.port > 65535:
        parser.error("--port must be <= 65535")
    if args.query_timeout >= args.timeout:
        parser.error("--timeout must exceed --query-timeout")
    args.schema = args.schema or f"sf{args.scale_factor}"
    if args.output is None:
        date = datetime.now(timezone.utc).date()
        args.output = BENCHMARK_ROOT / "results" / f"{args.benchmark}-sf{args.scale_factor}-{date}.json"
    return args


def query_files(args):
    maximum = 22 if args.benchmark == "tpch" else 99
    requested = args.queries.split(",") if args.queries is not None else [f"q{i:02d}" for i in range(1, maximum + 1)]
    files = {}
    for name in requested:
        if not re.fullmatch(r"[qQ][0-9]{1,2}", name) or not 1 <= int(name[1:]) <= maximum:
            raise ValueError(f"invalid {args.benchmark} query ID {name!r}")
        canonical = f"q{int(name[1:]):02d}"
        if canonical in files:
            raise ValueError(f"duplicate query {canonical}")
        path = args.queries_dir / args.benchmark / f"{canonical}.sql"
        if not path.is_file():
            raise ValueError(f"query file missing: {canonical}.sql; no queries were executed")
        if not path.read_bytes().strip():
            raise ValueError(f"empty SQL query: {canonical}.sql")
        files[canonical] = path
    return files


def unique_object(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate receipt key")
        result[key] = value
    return result


def execution_receipt(path, sql_digest):
    try:
        with path.open(encoding="utf-8") as source:
            receipt = json.load(source, object_pairs_hook=unique_object)
    except (OSError, ValueError) as error:
        raise ValueError("missing/invalid batch execution receipt") from error
    if (not isinstance(receipt, dict) or receipt.get("state") != "FINISHED"
            or receipt.get("resultConsumed") is not True
            or receipt.get("sqlSha256") != sql_digest
            or not isinstance(receipt.get("queryId"), str) or not receipt["queryId"].strip()):
        raise ValueError("batch receipt must prove FINISHED, consumed results and exact SQL digest")
    return receipt["queryId"]


def execute_query(args, query_file, sql_digest):
    with tempfile.TemporaryDirectory(prefix="pangrova-tpc-execution-") as temporary:
        receipt = Path(temporary) / "receipt.json"
        result_output = Path(temporary) / "result.txt"
        command = [args.cli_path, "--host", args.host, "--port", str(args.port),
                   "--catalog", args.catalog, "--schema", args.schema,
                   "--query-timeout", f"{args.query_timeout}s", "--file", str(query_file),
                   "--output", str(result_output), "--execution-receipt", str(receipt)]
        if args.token_env is not None:
            # Only the variable's name is put in argv. The real CLI resolves it
            # in the inherited environment; the secret never enters a report.
            command.extend(("--token-env", args.token_env))
        with tempfile.TemporaryFile() as stderr:
            started = time.perf_counter_ns()
            process = subprocess.Popen(command, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                       stderr=stderr, start_new_session=True)
            try:
                code = process.wait(timeout=args.timeout)
            except subprocess.TimeoutExpired as error:
                raise ValueError(f"CLI timeout after {args.timeout}s; no completed sample") from error
            finally:
                if process.poll() is None:
                    # The launcher group may exit between poll and kill; wait always reaps it.
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait()
            elapsed = (time.perf_counter_ns() - started) / 1_000_000
            if code:
                # Client/server diagnostics can contain credentials, SQL and
                # local paths. Preserve exit/phase without publishing stderr.
                raise ValueError(f"CLI failed exit={code}; no completed sample")
        return elapsed, execution_receipt(receipt, sql_digest)


def run_query(args, path, used_query_ids):
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    samples, query_ids, warmup_ids = [], [], []
    phase = "warmup"
    try:
        for phase, count in (("warmup", args.warmup), ("measurement", args.iterations)):
            for _ in range(count):
                elapsed, query_id = execute_query(args, path, digest)
                if query_id in used_query_ids:
                    raise ValueError(f"duplicate queryId {query_id!r}; cannot count a replay as a new sample")
                used_query_ids.add(query_id)
                if phase == "warmup":
                    warmup_ids.append(query_id)
                else:
                    samples.append(elapsed)
                    query_ids.append(query_id)
    except (OSError, ValueError) as error:
        detail = error.strerror if isinstance(error, OSError) else str(error)
        return {"status": "failed", "failurePhase": phase, "error": detail,
                "warmupCompleted": len(warmup_ids), "samples": samples, "sampleCount": len(samples),
                "sqlSha256": digest, "queryIds": query_ids, "warmupQueryIds": warmup_ids}
    return {"status": "passed", "warmupCompleted": len(warmup_ids), "samples": samples,
            "sqlSha256": digest, "queryIds": query_ids, "warmupQueryIds": warmup_ids,
            **sample_statistics(samples, args.minimum_samples)}


def write_results(path, results):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                         prefix=f".{path.name}.", delete=False) as output:
            temporary_path = Path(output.name)
            json.dump(results, output, indent=2, allow_nan=False)
            output.write("\n")
        os.replace(temporary_path, path)
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)


def main():
    args = arguments()
    try:
        files = query_files(args)
        if args.output.resolve() in {path.resolve() for path in files.values()}:
            raise ValueError("output must not overwrite a SQL query file")
        if args.token_env is not None:
            if not re.fullmatch(r"[A-Za-z_][A-Za-z_0-9]*", args.token_env):
                raise ValueError("--token-env must be an environment variable name")
            if not os.environ.get(args.token_env, "").strip():
                raise ValueError("Bearer token environment variable must be set and nonblank")
        if shutil.which(args.cli_path) is None:
            raise ValueError("batch CLI executable not found")
        results = {
            "benchmark": args.benchmark, "scaleFactor": args.scale_factor,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "percentileMethod": PERCENTILE_METHOD, "minimumSamples": args.minimum_samples,
            "timingScope": TIMING_SCOPE,
            "environment": {"host": args.host, "port": args.port, "catalog": args.catalog,
                            "schema": args.schema,
                            "authentication": "bearer-environment" if args.token_env is not None else "none"},
            "config": {"warmup": args.warmup, "iterations": args.iterations, "timeout": args.timeout,
                       "queryTimeout": args.query_timeout},
            "queries": {},
        }
        used_query_ids = set()
        for name, path in files.items():
            record = run_query(args, path, used_query_ids)
            results["queries"][name] = record
            if record["status"] == "failed":
                print(f"{name}: FAILED {record['failurePhase']}: {record['error']}", flush=True)
            elif record["percentileQualified"]:
                print(f"{name}: P50={record['p50Ms']:.3f}ms P99(R7)={record['p99Ms']:.3f}ms "
                      f"samples={record['sampleCount']}", flush=True)
            else:
                print(f"{name}: P50={record['p50Ms']:.3f}ms max={record['maxMs']:.3f}ms "
                      f"samples={record['sampleCount']}; insufficient samples for P99", flush=True)
        passed = sum(record["status"] == "passed" for record in results["queries"].values())
        failed = len(files) - passed
        results["status"] = "failed" if failed else "passed"
        results["summary"] = {"total": len(files), "passed": passed, "failed": failed}
        write_results(args.output, results)
        print(f"Results: {passed} passed, {failed} failed; JSON report written")
        return 1 if failed else 0
    except (OSError, ValueError) as error:
        detail = error.strerror if isinstance(error, OSError) else str(error)
        print(f"ERROR: {detail}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
