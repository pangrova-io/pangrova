import importlib.util
import os
from pathlib import Path
import stat
import subprocess
import sys
import tempfile
import textwrap
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


if __name__ == "__main__":
    unittest.main()
