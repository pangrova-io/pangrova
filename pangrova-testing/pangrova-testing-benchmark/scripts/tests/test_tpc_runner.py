"""Statistics/receipt oracles and real CLI rejection tests; not SQL performance evidence."""

import copy
import hashlib
import json
import os
from pathlib import Path
import random
import shutil
import subprocess
import sys
import tempfile
import unittest


SCRIPT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPT_DIR))
from run_tpc_benchmark import execution_receipt, write_results
from tpc_statistics import sample_statistics


class StatisticsTest(unittest.TestCase):
    def test_r7_interpolation_differs_from_maximum_and_lower_median(self):
        summary = sample_statistics(list(range(1, 1001)))
        self.assertEqual(summary["sampleCount"], 1000)
        self.assertEqual(summary["p50Ms"], 500.5)
        self.assertAlmostEqual(summary["p99Ms"], 990.01)
        self.assertEqual(summary["maxMs"], 1000)
        self.assertEqual(summary["meanMs"], 500.5)
        self.assertTrue(summary["percentileQualified"])

    def test_insufficient_samples_never_emit_p99(self):
        for count in (1, 3, 999):
            with self.subTest(count=count):
                summary = sample_statistics([2] * count)
                self.assertEqual(summary["sampleCount"], count)
                self.assertEqual(summary["p50Ms"], 2)
                self.assertEqual(summary["maxMs"], 2)
                self.assertFalse(summary["percentileQualified"])
                self.assertNotIn("p99Ms", summary)

    def test_increasing_minimum_is_supported_but_lowering_is_rejected(self):
        self.assertNotIn("p99Ms", sample_statistics([1] * 1000, 2000))
        for minimum in (0, 999, True, "1000", 1000.0):
            with self.subTest(minimum=minimum):
                with self.assertRaisesRegex(ValueError, "minimumSamples"):
                    sample_statistics([1], minimum)

    def test_statistics_are_order_independent_and_do_not_mutate_inputs(self):
        source = list(range(1, 1001))
        expected = sample_statistics(source)
        original = source.copy()
        shuffled = source.copy()
        random.Random(0).shuffle(shuffled)
        for samples in (source, list(reversed(source)), shuffled):
            self.assertEqual(sample_statistics(samples), expected)
        self.assertEqual(source, original)

    def test_invalid_samples_cannot_be_summarized(self):
        for samples in ([], None, "1", [0], [-1], [True], ["1"], [float("nan")],
                        [float("inf")], [10 ** 400]):
            with self.subTest(samples=samples):
                with self.assertRaisesRegex(ValueError, "samples"):
                    sample_statistics(samples)


class ReceiptTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.receipt_path = self.directory / "receipt.json"
        self.digest = hashlib.sha256(b"SELECT 1;\n").hexdigest()
        self.receipt = {"queryId": "arithmetic-oracle-only", "state": "FINISHED",
                        "sqlSha256": self.digest, "resultConsumed": True}

    def test_complete_receipt_for_exact_sql_is_accepted(self):
        self.receipt_path.write_text(json.dumps(self.receipt))
        self.assertEqual(execution_receipt(self.receipt_path, self.digest), "arithmetic-oracle-only")

    def test_missing_incomplete_and_ambiguous_receipts_are_rejected(self):
        for field, value in (("state", "FAILED"), ("state", "RUNNING"),
                             ("sqlSha256", "0" * 64), ("resultConsumed", False),
                             ("resultConsumed", 1), ("queryId", ""), ("queryId", None)):
            receipt = copy.deepcopy(self.receipt)
            receipt[field] = value
            self.receipt_path.write_text(json.dumps(receipt))
            with self.subTest(field=field, value=value):
                with self.assertRaisesRegex(ValueError, "batch receipt"):
                    execution_receipt(self.receipt_path, self.digest)
        for payload in ("{", "null", '{"state":"FAILED","state":"FINISHED"}'):
            self.receipt_path.write_text(payload)
            with self.assertRaises(ValueError):
                execution_receipt(self.receipt_path, self.digest)
        self.receipt_path.unlink()
        with self.assertRaisesRegex(ValueError, "missing/invalid"):
            execution_receipt(self.receipt_path, self.digest)

    def test_atomic_json_write_preserves_values_and_has_no_temporary_files(self):
        write_results(self.receipt_path, self.receipt)
        self.assertEqual(json.loads(self.receipt_path.read_text()), self.receipt)
        self.assertEqual(list(self.directory.iterdir()), [self.receipt_path])
        with self.assertRaises(ValueError):
            write_results(self.receipt_path, {"invalid": float("nan")})
        self.assertEqual(json.loads(self.receipt_path.read_text()), self.receipt)
        self.assertEqual(list(self.directory.iterdir()), [self.receipt_path])


class RunnerRejectionTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.query_root = self.directory / "queries"
        self.query_dir = self.query_root / "tpch"
        self.query_dir.mkdir(parents=True)
        self.sql = self.query_dir / "q01.sql"
        self.sql.write_text("SELECT 1;\n")
        self.output = self.directory / "result.json"

    def run_runner(self, *extra, env=None):
        return subprocess.run(
            ["bash", str(SCRIPT_DIR / "run-tpc-benchmark.sh"), "--benchmark", "tpch",
             "--queries-dir", str(self.query_root), "--queries", "q01",
             "--output", str(self.output), *extra],
            stdin=subprocess.DEVNULL, capture_output=True, text=True, check=False, env=env,
        )

    def test_missing_selected_sql_fails_preflight_without_skipping(self):
        process = self.run_runner("--queries", "q01,q02")
        self.assertEqual(process.returncode, 2)
        self.assertIn("query file missing", process.stderr)
        self.assertFalse(self.output.exists())

    def test_duplicate_aliases_invalid_ids_and_empty_sql_fail_preflight(self):
        for queries in ("Q1,q01", "", "q00", "q23", "../q01", "q01,"):
            with self.subTest(queries=queries):
                process = self.run_runner("--queries", queries)
                self.assertEqual(process.returncode, 2)
                self.assertFalse(self.output.exists())
        self.sql.write_text(" \n")
        process = self.run_runner()
        self.assertEqual(process.returncode, 2)
        self.assertIn("empty SQL", process.stderr)

    def test_invalid_options_and_missing_cli_cannot_produce_results(self):
        for option, value in (("--iterations", "0"), ("--iterations", "bad"),
                              ("--warmup", "-1"), ("--timeout", "0"),
                              ("--query-timeout", "0"), ("--query-timeout", "600"),
                              ("--port", "65536"), ("--minimum-samples", "3"),
                              ("--cli-path", str(self.directory / "missing-cli"))):
            with self.subTest(option=option, value=value):
                self.assertEqual(self.run_runner(option, value).returncode, 2)
                self.assertFalse(self.output.exists())

    def test_process_deadline_must_exceed_the_explicit_query_deadline(self):
        for query, process in ((30, 30), (31, 30), (300, 1)):
            with self.subTest(query=query, process=process):
                result = self.run_runner("--query-timeout", str(query), "--timeout", str(process))
                self.assertEqual(result.returncode, 2)
                self.assertIn("must exceed", result.stderr)
                self.assertFalse(self.output.exists())

    def test_output_cannot_overwrite_query(self):
        process = self.run_runner("--output", str(self.sql))
        self.assertEqual(process.returncode, 2)
        self.assertEqual(self.sql.read_text(), "SELECT 1;\n")

    def test_environment_authentication_requires_a_name_and_nonblank_secret(self):
        environment = os.environ.copy()
        environment.pop("PANGROVA_TPC_TEST_TOKEN", None)
        for name, value in (("PANGROVA_TPC_TEST_TOKEN", None),
                            ("PANGROVA_TPC_TEST_TOKEN", " \n"), ("not-a-variable", "secret")):
            with self.subTest(name=name, value=value):
                if value is None:
                    environment.pop(name, None)
                else:
                    environment[name] = value
                process = self.run_runner("--token-env", name, env=environment)
                self.assertEqual(process.returncode, 2)
                self.assertFalse(self.output.exists())
                self.assertNotIn("secret", process.stdout + process.stderr)
                self.assertNotIn(str(self.directory), process.stdout + process.stderr)

    def test_real_process_exit_zero_without_receipt_is_rejected_during_warmup(self):
        # This is the installed OS command, not a SQL substitute or performance sample.
        process = self.run_runner("--cli-path", shutil.which("true"))
        self.assertEqual(process.returncode, 1, process.stderr)
        result = json.loads(self.output.read_text())
        self.assertEqual(result["status"], "failed")
        self.assertEqual(result["summary"], {"total": 1, "passed": 0, "failed": 1})
        record = result["queries"]["q01"]
        self.assertEqual(record["failurePhase"], "warmup")
        self.assertEqual(record["samples"], [])
        self.assertEqual(record["warmupCompleted"], 0)
        self.assertIn("receipt", record["error"])
        self.assertNotIn("p99Ms", record)

    def test_real_process_error_is_preserved_and_warmup_zero_starts_measurement(self):
        # Python rejects --host; exercise a real process failure without an imitation CLI.
        process = self.run_runner("--cli-path", sys.executable, "--warmup", "0")
        self.assertEqual(process.returncode, 1)
        record = json.loads(self.output.read_text())["queries"]["q01"]
        self.assertEqual(record["failurePhase"], "measurement")
        self.assertIn("CLI failed exit=", record["error"])
        self.assertEqual(record["samples"], [])


if __name__ == "__main__":
    unittest.main()
