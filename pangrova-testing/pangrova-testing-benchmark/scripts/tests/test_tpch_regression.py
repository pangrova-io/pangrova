"""Deterministic unit and shell-entry-point tests; fixtures are not calibration data."""

import copy
import importlib.util
import json
import hashlib
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SCRIPT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPT_DIR))
SPEC = importlib.util.spec_from_file_location(
    "tpch_regression_compare", SCRIPT_DIR / "tpch-regression-compare.py"
)
compare = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(compare)


def oracle_samples(p99=100):
    """R7 P50=80/P99=p99 with sufficient samples; no performance claim."""
    return [70] * 499 + [80] * 490 + [p99] * 11


def baseline_fixture(scale=100):
    """Synthetic numbers used only to exercise the comparator's arithmetic."""
    return {
        "benchmark": "tpch", "status": "passed",
        "percentileMethod": "R7", "timingScope": "cli-end-to-end", "minimumSamples": 1000,
        "config": {"warmup": 1, "iterations": 1000, "timeout": 600, "queryTimeout": 300},
        "summary": {"total": 22, "passed": 22, "failed": 0},
        "scaleFactor": scale,
        "lastUpdated": "2026-01-01",
        "regressionThreshold": 0.15,
        "queries": {
            f"Q{i}": {
                "name": f"Query {i}", "baselineP50Ms": 80, "baselineP99Ms": 100,
                "sqlSha256": hashlib.sha256(f"arithmetic oracle Q{i}".encode()).hexdigest(),
                "queryIds": [f"baseline-Q{i}-{n}" for n in range(1000)],
                "warmupQueryIds": [f"baseline-warmup-Q{i}"],
                "status": "passed", "sampleCount": 1000, "minimumSamples": 1000,
                "percentileQualified": True, "warmupCompleted": 1, "samples": oracle_samples(),
            }
            for i in range(1, 23)
        },
    }


def results_fixture(scale=100):
    """Match the JSON emitted by run-tpc-benchmark.sh, including sample statistics."""
    return {
        "benchmark": "tpch",
        "scaleFactor": scale,
        "status": "passed", "percentileMethod": "R7",
        "timingScope": "cli-end-to-end", "minimumSamples": 1000,
        "timestamp": "2026-01-02T00:00:00Z",
        "config": {"warmup": 1, "iterations": 1000, "timeout": 600, "queryTimeout": 300},
        "queries": {
            f"q{i:02d}": {
                "status": "passed", "p50Ms": 80, "p99Ms": 100,
                "samples": oracle_samples(), "sampleCount": 1000, "minimumSamples": 1000,
                "sqlSha256": hashlib.sha256(f"arithmetic oracle Q{i}".encode()).hexdigest(),
                "queryIds": [f"current-Q{i}-{n}" for n in range(1000)],
                "warmupQueryIds": [f"current-warmup-Q{i}"],
                "percentileQualified": True, "warmupCompleted": 1,
            }
            for i in range(1, 23)
        },
        "summary": {"total": 22, "passed": 22, "failed": 0},
    }


class ComparisonTest(unittest.TestCase):
    def setUp(self):
        self.baseline = baseline_fixture()
        self.results = results_fixture()

    def assert_invalid(self, message):
        with self.assertRaisesRegex(ValueError, message):
            compare.compare_results(self.baseline, self.results)

    def test_query_and_process_deadlines_are_explicit_valid_and_match_calibration(self):
        for owner in ("baseline", "results"):
            for value in (None, 0, -1, True, "300", 300.0, 600, 601):
                with self.subTest(owner=owner, value=value):
                    self.setUp()
                    getattr(self, owner)["config"]["queryTimeout"] = value
                    self.assert_invalid("config\\.(queryTimeout|timeout)")
            self.setUp()
            del getattr(self, owner)["config"]["queryTimeout"]
            self.assert_invalid("config.queryTimeout")
        self.setUp()
        self.results["config"]["queryTimeout"] = 299
        self.assert_invalid("RUN_CONFIG_MISMATCH queryTimeout")

    def test_runner_format_and_query_aliases_preserve_valid_comparison(self):
        original = copy.deepcopy((self.baseline, self.results))
        comparisons, failed, threshold = compare.compare_results(self.baseline, self.results)
        self.assertFalse(failed)
        self.assertEqual(threshold, 0.15)
        self.assertEqual([c["query"] for c in comparisons], [f"Q{i}" for i in range(1, 23)])
        self.assertTrue(all(c["status"] == "PASS" for c in comparisons))
        self.assertTrue(all(c["changeP50"] == 0 for c in comparisons))
        self.assertEqual((self.baseline, self.results), original)

    def test_threshold_and_warning_boundaries_remain_strictly_greater(self):
        for threshold in (0.15, 0.20):
            warning = threshold * 0.67
            for change, expected in ((-0.1, "PASS"), (0, "PASS"),
                                     (warning - 0.001, "PASS"), (warning + 0.001, "WARN"),
                                     (threshold, "WARN"), (threshold + 0.001, "FAIL")):
                with self.subTest(threshold=threshold, change=change):
                    self.baseline["regressionThreshold"] = threshold
                    query = self.results["queries"]["q01"]
                    query["p99Ms"] = 100 + change * 100
                    query["samples"] = oracle_samples(query["p99Ms"])
                    comparisons, failed, _ = compare.compare_results(self.baseline, self.results)
                    self.assertEqual(comparisons[0]["status"], expected)
                    self.assertEqual(failed, expected == "FAIL")

    def test_default_threshold_and_optional_p50(self):
        del self.baseline["regressionThreshold"]
        del self.baseline["queries"]["Q1"]["baselineP50Ms"]
        del self.results["queries"]["q01"]["p50Ms"]
        comparisons, failed, threshold = compare.compare_results(self.baseline, self.results)
        self.assertFalse(failed)
        self.assertEqual(threshold, 0.15)
        self.assertIsNone(comparisons[0]["changeP50"])

    def test_aggregate_only_results_without_raw_samples_or_metadata_are_rejected(self):
        for field in ("config", "summary"):
            with self.subTest(field=field):
                self.setUp()
                del self.results[field]
                self.assert_invalid("INVALID_RESULTS.*" + field)
        self.setUp()
        del self.results["queries"]["q01"]["samples"]
        self.assert_invalid("INVALID_RESULTS.*samples")

    def test_lowercase_baseline_and_uppercase_results_are_aligned(self):
        self.baseline["queries"] = {
            f"q{i:02d}": self.baseline["queries"][f"Q{i}"] for i in range(1, 23)
        }
        self.results["queries"] = {
            f"Q{i}": self.results["queries"][f"q{i:02d}"] for i in range(1, 23)
        }
        comparisons, failed, _ = compare.compare_results(self.baseline, self.results)
        self.assertFalse(failed)
        self.assertEqual([c["query"] for c in comparisons], [f"q{i:02d}" for i in range(1, 23)])

    def test_missing_queries_are_errors_not_skips(self):
        for owner, key, category in (("baseline", "Q22", "UNCALIBRATED_BASELINE"),
                                     ("results", "q22", "INVALID_RESULTS")):
            with self.subTest(owner=owner):
                self.setUp()
                del getattr(self, owner)["queries"][key]
                self.assert_invalid(category + ".*Q22")

    def test_missing_query_maps_are_errors(self):
        for owner, category in (("baseline", "UNCALIBRATED_BASELINE"),
                                ("results", "INVALID_RESULTS")):
            with self.subTest(owner=owner):
                self.setUp()
                del getattr(self, owner)["queries"]
                self.assert_invalid(category + ".*queries")

    def test_empty_and_malformed_query_maps_and_documents(self):
        for owner in ("baseline", "results"):
            for value in (None, [], {}, "invalid", 22):
                with self.subTest(owner=owner, value=value):
                    self.setUp()
                    getattr(self, owner)["queries"] = value
                    self.assert_invalid("BASELINE|INVALID_RESULTS")
            for value in (None, [], "invalid", 22):
                with self.subTest(document=owner, value=value):
                    self.setUp()
                    setattr(self, owner, value)
                    self.assert_invalid("INVALID_BASELINE|INVALID_RESULTS")

    def test_query_ids_must_be_unambiguous_tpch_ids(self):
        for owner in ("baseline", "results"):
            for key in ("Q01", "q001", "Q0", "q23", "other"):
                with self.subTest(owner=owner, key=key):
                    self.setUp()
                    queries = getattr(self, owner)["queries"]
                    queries[key] = copy.deepcopy(next(iter(queries.values())))
                    self.assert_invalid("duplicate query|invalid query")

    def test_missing_or_zero_baselines_require_real_calibration(self):
        for field in ("baselineP99Ms", "baselineP50Ms"):
            with self.subTest(field=field):
                self.setUp()
                self.baseline["queries"]["Q1"][field] = 0
                self.assert_invalid("UNCALIBRATED_BASELINE.*Q1.*" + field + ".*calibrat")
        self.setUp()
        del self.baseline["queries"]["Q1"]["baselineP99Ms"]
        self.assert_invalid("UNCALIBRATED_BASELINE.*Q1.*baselineP99Ms")

    def test_invalid_measurements_are_rejected_in_both_percentiles(self):
        invalid = (None, 0, -1, True, "100", [], {}, float("nan"),
                   float("inf"), -float("inf"), 10 ** 400)
        for owner, key, fields in (
            ("baseline", "Q1", ("baselineP99Ms", "baselineP50Ms")),
            ("results", "q01", ("p99Ms", "p50Ms")),
        ):
            for field in fields:
                for value in invalid:
                    with self.subTest(owner=owner, field=field, value=value):
                        self.setUp()
                        getattr(self, owner)["queries"][key][field] = value
                        self.assert_invalid("BASELINE|INVALID_RESULTS")

    def test_missing_p99_and_malformed_query_records(self):
        del self.results["queries"]["q01"]["p99Ms"]
        self.assert_invalid("INVALID_RESULTS.*Q1.*p99Ms")
        for owner, key in (("baseline", "Q1"), ("results", "q01")):
            for value in (None, [], "passed"):
                with self.subTest(owner=owner, value=value):
                    self.setUp()
                    getattr(self, owner)["queries"][key] = value
                    self.assert_invalid("BASELINE|INVALID_RESULTS")

    def test_failed_or_unconfirmed_queries_are_not_measurements(self):
        for value in (None, "failed", "timeout", "skipped", "PASS", True):
            with self.subTest(status=value):
                self.results["queries"]["q01"]["status"] = value
                self.assert_invalid("INVALID_RESULTS.*Q1.*status.*passed")
        del self.results["queries"]["q01"]["status"]
        self.assert_invalid("INVALID_RESULTS.*Q1.*status")

    def test_failed_run_and_inconsistent_summary_are_rejected(self):
        for field, value in (("failed", 1), ("passed", 21), ("total", 23),
                             ("failed", False), ("passed", "22"), ("total", None)):
            with self.subTest(field=field, value=value):
                self.setUp()
                self.results["summary"][field] = value
                self.assert_invalid("INVALID_RESULTS.*summary")
        self.setUp()
        self.results["status"] = "failed"
        self.assert_invalid("INVALID_RESULTS.*status")

    def test_present_run_metadata_cannot_be_malformed(self):
        for field in ("config", "summary"):
            for value in (None, [], "passed", {}):
                with self.subTest(field=field, value=value):
                    self.setUp()
                    self.results[field] = value
                    self.assert_invalid("INVALID_RESULTS.*" + field)

    def test_scale_is_required_positive_finite_numeric_and_must_match(self):
        for owner in ("baseline", "results"):
            for value in (None, 0, -1, True, "100", float("nan"), float("inf")):
                with self.subTest(owner=owner, value=value):
                    self.setUp()
                    getattr(self, owner)["scaleFactor"] = value
                    self.assert_invalid("scaleFactor")
            self.setUp()
            del getattr(self, owner)["scaleFactor"]
            self.assert_invalid("scaleFactor")
        self.setUp()
        self.results["scaleFactor"] = 10
        self.assert_invalid("SCALE_MISMATCH.*100.*10")

    def test_wrong_benchmark_is_rejected(self):
        for owner in ("baseline", "results"):
            for value in ("tpcds", None, 100):
                with self.subTest(owner=owner, value=value):
                    self.setUp()
                    getattr(self, owner)["benchmark"] = value
                    self.assert_invalid("benchmark")
        self.setUp()
        del self.results["benchmark"]
        self.assert_invalid("benchmark")

    def test_invalid_threshold_cannot_disable_regression_detection(self):
        for value in (None, -0.1, True, "0.15", float("nan"), float("inf")):
            with self.subTest(value=value):
                self.baseline["regressionThreshold"] = value
                self.assert_invalid("INVALID_BASELINE.*regressionThreshold")

    def test_zero_threshold_is_valid_and_detects_any_slowdown(self):
        self.baseline["regressionThreshold"] = 0
        self.results["queries"]["q01"].update(p99Ms=101, samples=oracle_samples(101))
        comparisons, failed, threshold = compare.compare_results(self.baseline, self.results)
        self.assertTrue(failed)
        self.assertEqual(threshold, 0)
        self.assertEqual(comparisons[0]["status"], "FAIL")

    def test_invalid_samples_and_iteration_counts_are_rejected(self):
        for samples in (None, [], "100", [0, 80, 100], [-1, 80, 100], [True, 80, 100],
                        ["70", 80, 100], [float("nan"), 80, 100], [float("inf"), 80, 100],
                        [70, 80], [70, 80, 99]):
            with self.subTest(samples=samples):
                self.setUp()
                self.results["queries"]["q01"]["samples"] = samples
                self.assert_invalid("INVALID_RESULTS.*Q1.*samples")
        for iterations in (None, 0, -1, True, 3.0, "3", 4):
            with self.subTest(iterations=iterations):
                self.setUp()
                self.results["config"]["iterations"] = iterations
                self.assert_invalid("INVALID_RESULTS.*iterations|INVALID_RESULTS.*queryIds")

    def test_percentile_order_and_sample_median_are_checked(self):
        self.baseline["queries"]["Q1"]["baselineP50Ms"] = 101
        self.assert_invalid("INVALID_BASELINE.*Q1.*baselineP50Ms")
        self.setUp()
        self.results["queries"]["q01"]["p50Ms"] = 101
        self.assert_invalid("INVALID_RESULTS.*Q1.*p50Ms")
        self.results["queries"]["q01"]["p50Ms"] = 79
        self.assert_invalid("INVALID_RESULTS.*Q1.*samples")

    def test_even_sample_counts_use_interpolated_r7_median(self):
        from tpc_statistics import sample_statistics
        self.assertEqual(sample_statistics([70, 80, 90, 100])["p50Ms"], 85)
        self.assertNotIn("p99Ms", sample_statistics([70, 80, 90, 100]))

    def test_nonfinite_computed_changes_cannot_reach_reporting(self):
        previous = self.baseline["queries"]["Q1"]
        previous.update(baselineP99Ms=1e-308, baselineP50Ms=1e-308, samples=[1e-308] * 1000)
        current = self.results["queries"]["q01"]
        current.update(p99Ms=1e308, p50Ms=1e308, samples=[1e308] * 1000)
        self.assert_invalid("INVALID_RESULTS.*non-finite")

    def test_historical_maximum_contract_requires_real_recalibration(self):
        for owner in ("baseline", "results"):
            for method in (None, "maximum", "nearest-rank", "R6"):
                with self.subTest(owner=owner, method=method):
                    self.setUp()
                    getattr(self, owner)["percentileMethod"] = method
                    self.assert_invalid("percentileMethod.*recalibrat")

    def test_insufficient_samples_cannot_be_claimed_as_p99(self):
        for owner, key in (("baseline", "Q1"), ("results", "q01")):
            with self.subTest(owner=owner):
                self.setUp()
                document = getattr(self, owner)
                document["config"]["iterations"] = 3
                record = document["queries"][key]
                record.update(samples=[70, 80, 100], sampleCount=3, queryIds=record["queryIds"][:3])
                self.assert_invalid("percentileQualified.*insufficient")
        for field, value in (("sampleCount", 999), ("sampleCount", True),
                             ("percentileQualified", 1), ("percentileQualified", False),
                             ("minimumSamples", 3), ("warmupCompleted", 0)):
            with self.subTest(field=field, value=value):
                self.setUp()
                self.results["queries"]["q01"][field] = value
                self.assert_invalid(field)
        for minimum in (3, 999, True, "1000", None):
            self.setUp()
            self.results["minimumSamples"] = minimum
            self.assert_invalid("minimumSamples")

    def test_changed_timing_scope_or_warmup_configuration_is_rejected(self):
        self.results["timingScope"] = "server-execution"
        self.assert_invalid("timingScope")
        self.setUp()
        self.results["config"]["warmup"] = 0
        self.assert_invalid("RUN_CONFIG_MISMATCH.*warmup")

    def test_exact_sql_digest_and_distinct_execution_ids_are_required(self):
        self.results["queries"]["q01"]["sqlSha256"] = "0" * 64
        self.assert_invalid("SQL_INPUT_MISMATCH")
        for owner, key in (("baseline", "Q1"), ("results", "q01")):
            for invalid in (None, [], ["repeated"] * 1000, [""] * 1000):
                with self.subTest(owner=owner, invalid=type(invalid).__name__):
                    self.setUp()
                    getattr(self, owner)["queries"][key]["queryIds"] = invalid
                    self.assert_invalid("queryIds")
        self.setUp()
        self.results["queries"]["q02"]["queryIds"][0] = self.results["queries"]["q01"]["queryIds"][0]
        self.assert_invalid("queryIds.*distinct across")



class ScriptContractTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="tpch-regression-test-")
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.baseline_path = self.directory / "baseline.json"
        self.results_path = self.directory / "results.json"
        self.report_path = self.directory / "report.txt"
        self.write_json(self.baseline_path, baseline_fixture())
        self.write_json(self.results_path, results_fixture())
        self.environment = os.environ.copy()
        self.environment.update(
            BASELINE_PATH=str(self.baseline_path), RESULTS_PATH=str(self.results_path),
            HISTORY_DIR="", REPORT_OUTPUT="", ALERT_WEBHOOK="", PYTHONDONTWRITEBYTECODE="1",
        )

    @staticmethod
    def write_json(path, value):
        path.write_text(json.dumps(value), encoding="utf-8")

    def run_check(self, direct=False):
        command = ([sys.executable, str(SCRIPT_DIR / "tpch-regression-compare.py")]
                   if direct else ["bash", str(SCRIPT_DIR / "tpch-regression-check.sh"),
                                   str(self.results_path), str(self.baseline_path)])
        return subprocess.run(command, env=self.environment, cwd=self.directory,
                              capture_output=True, text=True, check=False)

    def assert_input_error(self, process, message):
        self.assertEqual(process.returncode, 2, process.stdout + process.stderr)
        self.assertIn(message, process.stderr)
        self.assertNotIn("Traceback", process.stderr)
        self.assertNotIn("PASSED", process.stdout)
        self.assertFalse(self.report_path.exists())

    def test_both_entry_points_accept_complete_valid_results(self):
        for direct in (False, True):
            with self.subTest(direct=direct):
                process = self.run_check(direct)
                self.assertEqual(process.returncode, 0, process.stderr)
                self.assertIn("PASSED - All within threshold", process.stdout)
                self.assertIn("SF=100", process.stdout)
                self.assertNotIn("SF=10 ", process.stdout)

    def test_real_regression_returns_one_and_writes_report(self):
        results = results_fixture()
        results["queries"]["q22"].update(p99Ms=116, samples=oracle_samples(116))
        self.write_json(self.results_path, results)
        self.environment["REPORT_OUTPUT"] = str(self.report_path)
        process = self.run_check()
        self.assertEqual(process.returncode, 1, process.stderr)
        report = self.report_path.read_text(encoding="utf-8")
        self.assertIn("FAILED - Regression detected", report)
        self.assertIn("+16.0%", report)

    def test_sf100_zero_template_is_an_uncalibrated_error_not_a_regression(self):
        baseline = baseline_fixture()
        baseline["queries"] = {
            f"q{i:02d}": {"name": f"Query {i}", "baselineP99Ms": 0, "baselineP50Ms": 0}
            for i in range(1, 23)
        }
        self.write_json(self.baseline_path, baseline)
        self.environment["REPORT_OUTPUT"] = str(self.report_path)
        process = self.run_check()
        self.assert_input_error(process, "UNCALIBRATED_BASELINE")
        self.assertIn("SF=100", process.stderr)
        self.assertIn("calibrat", process.stderr)
        self.assertEqual(json.loads(self.baseline_path.read_text()), baseline)

    def test_invalid_latest_run_does_not_write_success_report(self):
        results = results_fixture()
        del results["queries"]["q22"]
        self.write_json(self.results_path, results)
        self.environment["REPORT_OUTPUT"] = str(self.report_path)
        self.assert_input_error(self.run_check(), "INVALID_RESULTS")

    def test_failed_run_and_scale_mismatch_return_input_error_not_regression(self):
        failed = results_fixture()
        failed["queries"]["q01"]["status"] = "failed"
        failed["summary"].update(passed=21, failed=1)
        for results, message in ((failed, "INVALID_RESULTS"),
                                 (results_fixture(10), "SCALE_MISMATCH")):
            with self.subTest(message=message):
                self.write_json(self.results_path, results)
                self.assert_input_error(self.run_check(), message)

    def test_usage_and_missing_environment_return_two(self):
        process = subprocess.run(
            ["bash", str(SCRIPT_DIR / "tpch-regression-check.sh")],
            env=self.environment, cwd=self.directory, capture_output=True, text=True, check=False,
        )
        self.assertEqual(process.returncode, 2)
        self.assertIn("Usage:", process.stdout)
        self.environment["BASELINE_PATH"] = ""
        self.assert_input_error(self.run_check(direct=True), "environment variables required")

    def test_simplified_ci_results_without_scale_are_rejected(self):
        self.write_json(self.results_path, {"queries": {
            "Q1": {"p99Ms": 100}, "Q5": {"p99Ms": 100}, "Q9": {"p99Ms": 100}
        }})
        self.assert_input_error(self.run_check(), "INVALID_RESULTS")

    def test_missing_files_fail_in_shell_and_python_entry_points(self):
        for path, message in ((self.baseline_path, "UNCALIBRATED_BASELINE"),
                              (self.results_path, "INVALID_RESULTS")):
            contents = path.read_text()
            path.unlink()
            for direct in (False, True):
                with self.subTest(path=path.name, direct=direct):
                    self.assert_input_error(self.run_check(direct), message)
            path.write_text(contents)

    def test_malformed_json_duplicate_keys_and_nonfinite_numbers_fail_cleanly(self):
        for path, category in ((self.baseline_path, "INVALID_BASELINE"),
                               (self.results_path, "INVALID_RESULTS")):
            contents = path.read_text()
            for payload in ("{", '{"queries": {}, "queries": {}}', "null", "[]"):
                with self.subTest(path=path.name, payload=payload):
                    path.write_text(payload)
                    self.assert_input_error(self.run_check(), category)
            path.write_text(contents)
        for token in ("NaN", "Infinity", "-Infinity", "1e999"):
            with self.subTest(token=token):
                self.results_path.write_text(
                    json.dumps(results_fixture()).replace('"p99Ms": 100', '"p99Ms": ' + token, 1)
                )
                self.assert_input_error(self.run_check(), "INVALID_RESULTS")

    def test_valid_history_supports_runner_keys_and_timestamp(self):
        history = self.directory / "history"
        history.mkdir()
        self.write_json(history / "first.json", results_fixture())
        second = results_fixture()
        second["timestamp"] = "2026-01-03T00:00:00Z"
        self.write_json(history / "second.json", second)
        self.environment["HISTORY_DIR"] = str(history)
        process = self.run_check()
        self.assertEqual(process.returncode, 0, process.stderr)
        self.assertIn("TREND CHARTS (last 2 runs)", process.stdout)
        self.assertIn("2026-01-02T00:00:00Z", process.stdout)

    def test_invalid_or_mismatched_history_cannot_appear_as_scale_evidence(self):
        history = self.directory / "history"
        history.mkdir()
        self.environment.update(HISTORY_DIR=str(history), REPORT_OUTPUT=str(self.report_path))
        invalid = results_fixture()
        invalid["queries"]["q01"]["status"] = "failed"
        for value, message in ((invalid, "INVALID_RESULTS"),
                               (results_fixture(10), "SCALE_MISMATCH")):
            with self.subTest(message=message):
                self.write_json(history / "bad.json", value)
                self.assert_input_error(self.run_check(), message)
        (history / "bad.json").write_text("{")
        self.assert_input_error(self.run_check(), "INVALID_RESULTS")

    def test_missing_requested_history_directory_is_not_silently_ignored(self):
        self.environment["HISTORY_DIR"] = str(self.directory / "missing-history")
        self.assert_input_error(self.run_check(), "INVALID_RESULTS history")

    def test_report_write_error_returns_two_without_traceback(self):
        self.environment["REPORT_OUTPUT"] = str(self.directory)
        self.assert_input_error(self.run_check(), "cannot write report")


if __name__ == "__main__":
    unittest.main()
