"""Check macOS Native Docker readiness and diagnostics without a hypervisor."""
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[3]
SETUP = ROOT / ".pangrova/scripts/setup-macos-native-test-docker.sh"


class MacOSNativeDockerTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory(prefix="native-docker-test-", dir="/tmp")
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.github_env = self.root / "github-env"
        self.env = {**os.environ, "PATH": f"{self.bin}{os.pathsep}{os.defpath}",
                    "RUNNER_TEMP": str(self.root), "GITHUB_ENV": str(self.github_env)}
        self.executable("uname", "echo x86_64")
        self.executable("curl", 'if [ "$1" = --unix-socket ]; then echo "$LIMA_TEST_PING"; fi')
        self.executable("shasum", "cat >/dev/null")
        self.executable("tar", 'mkdir -p "$4/bin"; cp "$RUNNER_TEMP/bin/limactl" "$4/bin/"')
        self.executable("ssh", "echo 'Docker provisioning reported a missing package'")
        self.executable("limactl", """
import os
from pathlib import Path
import socket

instance = Path(os.environ["LIMA_HOME"]) / "native-ci"
(instance / "sock").mkdir(parents=True)
(instance / "ha.stderr.log").write_text("Lima did not reach running\\n")
(instance / "serial.log").write_text("Guest kernel booted\\n")
(instance / "ssh.config").touch()
status = int(os.environ["LIMA_TEST_EXIT"])
if not status:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
        server.bind(str(instance / "sock/docker.sock"))
raise SystemExit(status)
""", interpreter=sys.executable)

    def executable(self, name, command, interpreter="/bin/sh"):
        path = self.bin / name
        path.write_text(f"#!{interpreter}\n{command}\n")
        path.chmod(0o755)

    def run_setup(self, status="0", ping="OK"):
        return subprocess.run(["bash", str(SETUP)], capture_output=True, text=True,
                              env={**self.env, "LIMA_TEST_EXIT": status, "LIMA_TEST_PING": ping})

    def test_boot_failure_preserves_status_and_reports_host_and_guest_logs(self):
        result = self.run_setup(status="42")
        self.assertEqual(result.returncode, 42, result.stdout + result.stderr)
        self.assertIn("Lima did not reach running", result.stderr)
        self.assertIn("Guest kernel booted", result.stderr)
        self.assertIn("Docker provisioning reported a missing package", result.stdout)
        self.assertFalse(self.github_env.exists())

    def test_failed_ping_leaves_docker_unpublished(self):
        result = self.run_setup(ping="unhealthy")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("Guest kernel booted", result.stderr)
        self.assertFalse(self.github_env.exists())

    def test_success_publishes_the_verified_socket_and_loopback_host(self):
        result = self.run_setup()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        env = self.github_env.read_text().splitlines()
        self.assertIn(f"DOCKER_HOST=unix://{self.root}/native-lima/native-ci/sock/docker.sock", env)
        self.assertIn("TESTCONTAINERS_HOST_OVERRIDE=127.0.0.1", env)
        self.assertNotIn("Lima startup log", result.stderr)
