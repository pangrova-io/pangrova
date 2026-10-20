import importlib.util
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import textwrap
import time
import unittest


SCRIPT = Path(__file__).parents[1] / "bin/run-native-tests.py"
SPEC = importlib.util.spec_from_file_location("run_native_tests", SCRIPT)
RUNNER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(RUNNER)


class RunNativeTestsTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.pid = self.root / "pid"
        self.child = self.root / "broker.py"
        self.child.write_text(textwrap.dedent("""
            import json, os, signal, socket, struct, sys
            from pathlib import Path
            mode, pid, endpoint, ready = sys.argv[1:]
            Path(pid).write_text(str(os.getpid()))
            if mode == 'exit':
                sys.exit(17)
            if mode == 'ignore-term':
                signal.signal(signal.SIGTERM, signal.SIG_IGN)
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
                server.bind(endpoint)
                server.listen()
                if mode != 'never-ready':
                    Path(ready).touch()
                while True:
                    connection, _ = server.accept()
                    with connection:
                        size = struct.unpack('!I', connection.recv(4))[0]
                        data = bytearray()
                        while len(data) < size:
                            data.extend(connection.recv(size - len(data)))
                        request = json.loads(data)
                        accepted = (mode != 'reject' and request['nonce'] ==
                                    os.environ['PANGROVA_LANCE_FIXTURE_NONCE'])
                        response = json.dumps({'success': accepted, 'catalog': 'fixture'}).encode()
                        connection.sendall(struct.pack('!I', len(response)) + response)
        """))

    def fixture(self, mode="normal", timeout=3):
        return RUNNER.fixture(
            [sys.executable, str(self.child), mode, str(self.pid)],
            self.root, self.root / "broker.log", timeout=timeout, stop_timeout=0.2)

    def assert_stopped(self):
        with self.assertRaises(ProcessLookupError):
            os.kill(int(self.pid.read_text()), 0)

    def test_live_authenticated_fixture_and_child_exit_code(self):
        with self.fixture() as environment:
            endpoint = Path(environment["PANGROVA_LANCE_FIXTURE_SOCKET"])
            self.assertEqual(stat.S_IMODE(endpoint.parent.stat().st_mode), 0o700)
            self.assertGreaterEqual(len(environment["PANGROVA_LANCE_FIXTURE_NONCE"]), 32)
            code = subprocess.call([sys.executable, "-c", "import sys; sys.exit(7)"],
                                   env=environment)
            self.assertEqual(code, 7)
        self.assertFalse(endpoint.parent.exists())
        self.assert_stopped()

    def test_startup_failure_never_admits_tests(self):
        with self.assertRaisesRegex(RuntimeError, "exited 17"):
            with self.fixture("exit"):
                self.fail("Tests admitted before fixture readiness")
        self.assert_stopped()

    def test_rejected_readiness_stops_fixture(self):
        with self.assertRaisesRegex(RuntimeError, "rejected readiness"):
            with self.fixture("reject"):
                self.fail("Tests admitted after rejected authority handshake")
        self.assert_stopped()

    def test_readiness_timeout_stops_live_unready_fixture(self):
        with self.assertRaisesRegex(RuntimeError, "readiness timed out"):
            with self.fixture("never-ready", timeout=0.4):
                self.fail("Tests admitted without fixture readiness")
        self.assert_stopped()

    def test_interrupt_stops_fixture(self):
        with self.assertRaises(KeyboardInterrupt):
            with self.fixture():
                raise KeyboardInterrupt()
        self.assert_stopped()

    def test_unresponsive_shutdown_is_reported_and_process_reaped(self):
        with self.assertRaisesRegex(RuntimeError, "did not shut down cleanly"):
            with self.fixture("ignore-term"):
                pass
        self.assert_stopped()

    def test_passing_and_failing_output_survives_subsequent_real_ctest_discovery(self):
        (self.root / "CTestTestfile.cmake").write_text(
            'add_test(pass-output "' + sys.executable + '" "-c" "print(\\\"passing-diagnostic\\\")")\n'
            'add_test(fail-output "' + sys.executable
            + '" "-c" "print(\\\"failing-diagnostic\\\"); raise SystemExit(3)")\n')
        logs = self.root / "owned"
        logs.mkdir()
        started = time.time_ns()
        result = subprocess.run(["ctest", "--test-dir", str(self.root), "--output-on-failure"],
                                capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        RUNNER.retain_ctest_log(self.root, logs, started)
        original = (logs / "ctest.log").read_bytes()
        self.assertIn(b"passing-diagnostic", original)
        self.assertIn(b"failing-diagnostic", original)
        subprocess.run(["ctest", "--test-dir", str(self.root), "--show-only=json-v1"],
                       capture_output=True, text=True, check=True)
        self.assertEqual((logs / "ctest.log").read_bytes(), original)
        self.assertNotIn(b"passing-diagnostic",
                         (self.root / "Testing/Temporary/LastTest.log").read_bytes())

    def test_old_per_test_log_cannot_be_attributed_to_a_new_invocation(self):
        source = self.root / "Testing/Temporary/LastTest.log"
        source.parent.mkdir(parents=True)
        source.write_text("previous run")
        logs = self.root / "owned"
        logs.mkdir()
        with self.assertRaisesRegex(RuntimeError, "fresh per-test log"):
            RUNNER.retain_ctest_log(self.root, logs, source.stat().st_mtime_ns + 1)
        self.assertFalse((logs / "ctest.log").exists())


if __name__ == "__main__":
    unittest.main()
