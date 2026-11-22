"""Process-level smoke harness with loopback health and a fake CLI, not Pangrova."""

import contextlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest

ROOT = Path(__file__).resolve().parents[3]
CLI = '''
import hashlib
import json
import os
from pathlib import Path
import sys
assert sys.argv[sys.argv.index("--host") + 1] == "127.0.0.1"
assert "PANGROVA_COORDINATOR_URL" not in os.environ
where = Path(__file__).parent
sql = Path(sys.argv[sys.argv.index("--file") + 1]).read_bytes()
assert sys.argv[sys.argv.index("--port") + 1] == (where / "port").read_text()
with (where / "queries").open("ab") as out:
    out.write(sql + b"\\n")
mode = (where / "mode").read_text()
if mode == "authenticated":
    assert sys.argv[sys.argv.index("--token-env") + 1] == "PANGROVA_ICEBERG_TRIAL_TOKEN"
    assert os.environ["PANGROVA_ICEBERG_TRIAL_TOKEN"] == "fixture-token"
    assert "fixture-token" not in sys.argv
if mode in ("insert failure", "insert and cleanup failure") and sql.startswith(b"INSERT"):
    sys.exit("injected insert error")
if mode == "ambiguous create" and sql.startswith(b"CREATE SCHEMA"):
    sys.exit("injected create error")
if mode in ("cleanup failure", "insert and cleanup failure") and sql.startswith(b"DROP TABLE"):
    sys.exit("injected cleanup error")
result = "...| event_type | cnt |\\n| click | 2 |\\n| purchase | 1 |\\n2 rows\\n"
if mode == "bad result":
    result = result.replace("purchase | 1", "purchase | 2")
Path(sys.argv[sys.argv.index("--output") + 1]).write_text(
    result if sql.startswith(b"SELECT") else "OK (no result set)\\n")
Path(sys.argv[sys.argv.index("--execution-receipt") + 1]).write_text(json.dumps({
    "queryId": "test-id", "state": "FAILED" if mode == "invalid receipt" else "FINISHED",
    "resultConsumed": True, "sqlSha256": hashlib.sha256(sql).hexdigest(),
}))
'''


@contextlib.contextmanager
def coordinator(has_catalogs):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            assert self.path == "/v1/health"
            health = {"ready": has_catalogs, "nodeType": "COORDINATOR", "checks": {
                "http": True, "transport": True, "hasWorkers": True, "hasCatalogs": has_catalogs,
            }}
            self.send_response(200 if has_catalogs else 503)
            self.end_headers()
            self.wfile.write(json.dumps(health).encode())

        def log_message(self, *_):
            pass

    with ThreadingHTTPServer(("127.0.0.1", 0), Handler) as server:
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.01})
        thread.start()
        try:
            yield server.server_port
        finally:
            server.shutdown()
            thread.join()


class IcebergSmokeTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        for relative in ("bin", "examples", "tmp"):
            (self.root / relative).mkdir()
        for relative in ("examples/iceberg-smoke.py", "examples/iceberg-smoke.sql"):
            shutil.copy(ROOT / relative, self.root / relative)
        launcher = self.root / "bin/pangrova-cli.sh"
        launcher.write_text(f"#!{sys.executable}\n" + CLI, encoding="utf-8")
        launcher.chmod(0o755)

    def run_trial(self, mode, *, has_catalogs=False, catalog=None, token=None):
        (self.root / "bin/mode").write_text(mode, encoding="utf-8")
        env = {**os.environ, "TMPDIR": str(self.root / "tmp"),
               "PANGROVA_COORDINATOR_URL": "http://do-not-use.invalid"}
        env.pop("PANGROVA_ICEBERG_TRIAL_TOKEN", None)
        if token is not None:
            env["PANGROVA_ICEBERG_TRIAL_TOKEN"] = token
        catalog_args = ["--catalog", catalog] if catalog is not None else []
        with coordinator(has_catalogs) as port:
            (self.root / "bin/port").write_text(str(port), encoding="utf-8")
            return subprocess.run(
                [sys.executable, str(self.root / "examples/iceberg-smoke.py"),
                 "--local-standalone-port", str(port), *catalog_args], capture_output=True, text=True,
                env=env, timeout=10)

    def queries(self):
        return (self.root / "bin/queries").read_text()

    def test_local_paths_and_confirmed_cleanup(self):
        result = self.run_trial("success")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("click=2, purchase=1", result.stdout)
        sql = self.queries()
        self.assertNotIn("file:///tmp/iceberg-warehouse", sql)
        self.assertRegex(sql, r"CREATE CATALOG iceberg_trial_[0-9a-f]{32}")
        self.assertRegex(sql, r"(?s)DROP TABLE .+?\n\nDROP CATALOG")
        self.assertNotIn("DROP SCHEMA", sql)
        self.assertEqual(list((self.root / "tmp").iterdir()), [])

    def test_nonempty_coordinator_is_rejected_before_any_sql(self):
        result = self.run_trial("success", has_catalogs=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("fresh Coordinator", result.stderr)
        self.assertFalse((self.root / "bin/queries").exists())

    def test_authenticated_trial_uses_preprovisioned_catalog_for_sql_and_cleanup(self):
        result = self.run_trial("authenticated", catalog="iceberg_trial_grant", token="fixture-token")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("CREATE CATALOG iceberg_trial_grant\n", self.queries())
        self.assertIn("DROP CATALOG iceberg_trial_grant\n", self.queries())

    def test_catalog_rejects_sql_punctuation_before_running_any_sql(self):
        result = self.run_trial("success", catalog="iceberg_trial_invalid;")
        self.assertEqual(2, result.returncode, result.stderr)
        self.assertFalse((self.root / "bin/queries").exists())

    def test_insert_error_stops_queries_and_cleans_confirmed_objects(self):
        result = self.run_trial("insert failure")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("statement-4 failed", result.stderr)
        self.assertNotIn("SELECT", self.queries())
        self.assertIn("DROP CATALOG", self.queries())
        self.assertEqual(list((self.root / "tmp").iterdir()), [])

    def test_ambiguous_create_stops_queries_and_keeps_warehouse(self):
        result = self.run_trial("ambiguous create")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("statement-2 failed", result.stderr)
        self.assertIn("files retained", result.stderr)
        self.assertNotIn("CREATE TABLE", self.queries())
        self.assertNotIn("DROP", self.queries())
        self.assertEqual(len(list((self.root / "tmp").iterdir())), 1)

    def test_invalid_receipt_is_not_counted_as_creation(self):
        result = self.run_trial("invalid receipt")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Invalid execution receipt for statement-1", result.stderr)
        self.assertNotIn("CREATE SCHEMA", self.queries())
        self.assertNotIn("DROP", self.queries())

    def test_cleanup_failure_prevents_success_and_preserves_evidence(self):
        result = self.run_trial("cleanup failure")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("drop-table failed", result.stderr)
        self.assertIn("files retained", result.stderr)
        self.assertNotIn("DROP CATALOG", self.queries())
        self.assertNotIn("passed", result.stdout)

    def test_wrong_aggregate_is_rejected_even_when_cleanup_succeeds(self):
        result = self.run_trial("bad result")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Unexpected Iceberg result", result.stderr)
        self.assertIn("DROP CATALOG", self.queries())
        self.assertEqual(list((self.root / "tmp").iterdir()), [])

    def test_failed_cleanup_retains_the_original_trial_failure(self):
        result = self.run_trial("insert and cleanup failure")
        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn("statement-4 failed: injected insert error", result.stderr)
        self.assertIn("drop-table failed: injected cleanup error", result.stderr)
        self.assertIn("files retained", result.stderr)


if __name__ == "__main__":
    unittest.main()
