"""Check the Java fixture preparation entry points without starting CTest."""

import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]
RUNNER = ROOT / "pangrova-worker-native/bin/run-native-tests.py"
MODULE = "pangrova-connector/pangrova-connector-lance"
BROKER = "org/pangrova/connector/lance/LanceManagedSourceFixtureBroker.class"


class NativeJavaFixtureTest(unittest.TestCase):
    def setUp(self):
        fixture = tempfile.TemporaryDirectory(prefix="native java fixture ")
        self.addCleanup(fixture.cleanup)
        self.root = Path(fixture.name)
        self.runner = self.root / "pangrova-worker-native/bin/run-native-tests.py"
        self.runner.parent.mkdir(parents=True)
        shutil.copyfile(RUNNER, self.runner)
        broker = self.root / MODULE / "target/test-classes" / BROKER
        broker.parent.mkdir(parents=True)
        broker.touch()
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.calls = self.root / "calls"
        self.env = dict(os.environ, PATH=f"{self.bin}{os.pathsep}{os.defpath}",
                        JAVA_HOME=str(self.root), CALLS=str(self.calls))
        self.executable("java", 'echo java >> "$CALLS"; exit 1')

    def executable(self, name, command):
        path = self.bin / name
        path.write_text("#!/bin/sh\n" + command + "\n")
        path.chmod(0o755)

    def run_fixture(self, *arguments):
        return subprocess.run([sys.executable, str(self.runner), *map(str, arguments)],
                              env=self.env, capture_output=True, text=True, timeout=10)

    def test_preparation_runs_maven_without_a_native_build(self):
        self.executable("mvn", """
printf '%s\\n' "$*" >> "$CALLS"
for argument in "$@"; do
    case "$argument" in
        -Dmdep.outputFile=*) printf fixture.jar > "${argument#*=}" ;;
    esac
done
""")
        directory = self.root / "prepared"
        result = self.run_fixture("--prepare-java", directory)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        calls = self.calls.read_text().splitlines()
        self.assertEqual(len(calls), 2)
        self.assertTrue(calls[0].endswith(" install"), calls)
        self.assertTrue(calls[1].endswith(" dependency:build-classpath"), calls)
        self.assertEqual((directory / "classpath.txt").read_text(), "fixture.jar")

    def test_invalid_prepared_classpath_fails_without_rebuilding_or_running_tests(self):
        self.executable("ctest", 'echo ctest >> "$CALLS"; exit 1')
        self.executable("mvn", 'echo maven >> "$CALLS"; exit 1')
        directory = self.root / "prepared"
        directory.mkdir()
        (directory / "classpath.txt").touch()
        build = self.root / "build"
        build.mkdir()
        (build / "CTestTestfile.cmake").touch()
        result = self.run_fixture("--prepared-java", directory, "--build-dir", build)
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn("Prepared Java fixture dependency classpath is empty", result.stderr)
        self.assertFalse(self.calls.exists())


if __name__ == "__main__":
    unittest.main()
