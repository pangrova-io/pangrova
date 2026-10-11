"""Exercise the real shell/Python gate. Numbers are arithmetic oracles, not benchmarks."""

import copy
import json
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT_DIR = Path(__file__).resolve().parents[1]


def measurement(score=100, mode="thrpt", params=None):
    return {
        "jmhVersion": "1.37", "benchmark": "org.pangrova.Arithmetic.oracle",
        "mode": mode, "threads": 1, "forks": 1, "jvmArgs": [],
        "jdkVersion": "17.0.1", "vmName": "OpenJDK", "vmVersion": "17.0.1",
        "warmupIterations": 2, "warmupTime": "1 s", "warmupBatchSize": 1,
        "measurementIterations": 3, "measurementTime": "1 s", "measurementBatchSize": 1,
        "params": params or {},
        "primaryMetric": {"score": score, "scoreUnit": "ops/s" if mode == "thrpt" else "ns/op"},
    }


class JmhGateTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.current_path = Path(self.directory.name) / "current.json"
        self.baseline_path = Path(self.directory.name) / "baseline.json"

    def run_gate(self, current, baseline, threshold="0.10"):
        self.current_path.write_text(json.dumps(current), encoding="utf-8")
        self.baseline_path.write_text(json.dumps(baseline), encoding="utf-8")
        return self.invoke(threshold)

    def invoke(self, threshold="0.10"):
        return subprocess.run(
            ["bash", str(SCRIPT_DIR / "jmh-regression-check.sh"), str(self.current_path),
             str(self.baseline_path), threshold], capture_output=True, text=True, check=False,
        )

    def test_all_modes_use_correct_direction_and_exact_threshold(self):
        for mode in ("thrpt", "avgt", "sample", "ss"):
            boundary = 90 if mode == "thrpt" else 110
            regression = 89.9999 if mode == "thrpt" else 110.0001
            improvement = 120 if mode == "thrpt" else 80
            for score, code in ((100, 0), (boundary, 0), (regression, 1), (improvement, 0)):
                with self.subTest(mode=mode, score=score):
                    result = self.run_gate([measurement(score, mode)], [measurement(100, mode)])
                    self.assertEqual(result.returncode, code, result.stderr)

    def test_parameter_variants_are_compared_independently(self):
        baseline = [measurement(params={"size": "10"}), measurement(params={"size": "20"})]
        current = [measurement(80, params={"size": "10"}), measurement(params={"size": "20"})]
        result = self.run_gate(current, baseline)
        self.assertEqual(result.returncode, 1, result.stderr)
        self.assertIn("compared=2, regressed=1", result.stdout)
        self.assertIn("params={'size': '10'}", result.stdout)

    def test_multiple_modes_for_same_params_are_valid(self):
        entries = [measurement(), measurement(mode="avgt")]
        result = self.run_gate(entries, entries)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("compared=2", result.stdout)

    def test_parameter_order_and_absent_empty_params_are_equivalent(self):
        current = measurement(params={"size": "1", "kind": "small"})
        baseline = measurement(params={"kind": "small", "size": "1"})
        self.assertEqual(self.run_gate([current], [baseline]).returncode, 0)
        empty = measurement()
        del empty["params"]
        self.assertEqual(self.run_gate([empty], [measurement()]).returncode, 0)

    def test_missing_or_extra_benchmark_and_mode_drift_are_errors(self):
        for current in ([measurement(params={"size": "2"})], [measurement(mode="avgt")],
                        [measurement(), measurement(params={"size": "2"})]):
            result = self.run_gate(current, [measurement()])
            self.assertEqual(result.returncode, 2)
            self.assertIn("BENCHMARK_SET_MISMATCH", result.stderr)

    def test_empty_and_invalid_documents_cannot_pass(self):
        for value in ([], {}, None, "bad", [None]):
            for side in ("baseline", "current"):
                with self.subTest(value=value, side=side):
                    result = self.run_gate(value if side == "current" else [measurement()],
                                           value if side == "baseline" else [measurement()])
                    self.assertEqual(result.returncode, 2)
                    self.assertNotIn("PASSED", result.stdout)

    def test_missing_files_are_errors(self):
        result = self.invoke()
        self.assertEqual(result.returncode, 2)
        self.assertIn("BASELINE", result.stderr)
        self.baseline_path.write_text(json.dumps([measurement()]))
        result = self.invoke()
        self.assertEqual(result.returncode, 2)
        self.assertIn("INVALID_RESULTS", result.stderr)

    def test_nonpositive_nonfinite_or_nonnumeric_scores_are_errors(self):
        for score in (0, -1, True, False, "100", None, float("nan"), float("inf"), -float("inf")):
            for side in ("baseline", "current"):
                with self.subTest(score=score, side=side):
                    result = self.run_gate([measurement(score if side == "current" else 100)],
                                           [measurement(score if side == "baseline" else 100)])
                    self.assertEqual(result.returncode, 2)

    def test_missing_fields_and_invalid_params_are_errors(self):
        for field in ("benchmark", "mode", "primaryMetric", "measurementTime", "forks"):
            entry = measurement()
            del entry[field]
            with self.subTest(field=field):
                self.assertEqual(self.run_gate([entry], [measurement()]).returncode, 2)
        for params in ([], None, {"size": 1}, {"": "1"}):
            entry = measurement()
            entry["params"] = params
            self.assertEqual(self.run_gate([entry], [measurement()]).returncode, 2)

    def test_duplicate_entries_and_json_keys_are_errors(self):
        for side in ("baseline", "current"):
            duplicate = [measurement(), measurement()]
            result = self.run_gate(duplicate if side == "current" else [measurement()],
                                   duplicate if side == "baseline" else [measurement()])
            self.assertEqual(result.returncode, 2)
            self.assertIn("duplicate benchmark", result.stderr)
        self.run_gate([measurement()], [measurement()])
        self.current_path.write_text(json.dumps([measurement()]).replace(
            '"score": 100', '"score": 1, "score": 100'))
        result = self.invoke()
        self.assertEqual(result.returncode, 2)
        self.assertIn("duplicate JSON key", result.stderr)

    def test_unit_changes_and_unit_mode_mismatch_are_errors(self):
        for unit in ("ops/ms", "ns/op", "ops/fake", "", None):
            entry = measurement()
            entry["primaryMetric"]["scoreUnit"] = unit
            result = self.run_gate([entry], [measurement()])
            self.assertEqual(result.returncode, 2)

    def test_nonrepresentable_scores_and_malformed_json_fail_cleanly(self):
        self.run_gate([measurement()], [measurement()])
        for payload in ("{", json.dumps([measurement()]).replace('"score": 100', '"score": 1e999999999'),
                        json.dumps([measurement()]).replace('"score": 100', '"score": 1e-999999999')):
            self.current_path.write_text(payload)
            result = self.invoke()
            self.assertEqual(result.returncode, 2)
            self.assertNotIn("Traceback", result.stderr)

    def test_harness_drift_and_invalid_integer_settings_are_errors(self):
        for field, value in (("threads", 2), ("forks", 2), ("warmupIterations", 3),
                             ("jdkVersion", "21"), ("jmhVersion", "1.36"),
                             ("jvmArgs", ["-Xmx4g"]), ("measurementTime", "2 s")):
            entry = measurement()
            entry[field] = value
            with self.subTest(field=field):
                result = self.run_gate([entry], [measurement()])
                self.assertEqual(result.returncode, 2)
                self.assertIn("HARNESS_MISMATCH", result.stderr)
        for value in (0, -1, True, 1.5, "1"):
            entry = measurement()
            entry["forks"] = value
            self.assertEqual(self.run_gate([entry], [measurement()]).returncode, 2)

    def test_threshold_rejects_values_that_would_disable_gate(self):
        for threshold in ("NaN", "Infinity", "-0.1", "1", "1.1", "invalid"):
            result = self.run_gate([measurement()], [measurement()], threshold)
            self.assertEqual(result.returncode, 2)
        self.assertEqual(self.run_gate([measurement(99.9)], [measurement()], "0").returncode, 1)

    def test_invalid_late_record_does_not_emit_partial_success_report(self):
        entries = [measurement(), measurement(params={"size": "2"})]
        current = copy.deepcopy(entries)
        current[1]["primaryMetric"]["scoreUnit"] = "ops/ms"
        result = self.run_gate(current, entries)
        self.assertEqual(result.returncode, 2)
        self.assertNotIn("PASS", result.stdout)


if __name__ == "__main__":
    unittest.main()
