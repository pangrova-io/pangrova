"""Darwin Boost arguments and probe failures; no actual macOS link on Linux."""

import os
from pathlib import Path
import shlex
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[3]
ADAPTER = ROOT / "pangrova-worker-native/bin/darwin-x86-boost.sh"
BOOTSTRAP = ADAPTER.with_name("setup-native-build-env.sh")


class DarwinX86BoostTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="boost sdk ")
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        bindir = self.root / "bin"
        sdk = self.root / "Xcode SDK"
        self.include = sdk / "usr/include/c++/v1"
        self.include.mkdir(parents=True)
        (self.include / "exception").touch()
        bindir.mkdir()
        self.executable(bindir / "uname", 'case "$1" in -s) echo Darwin;; -m) echo "$TEST_ARCH";; esac')
        self.executable(bindir / "clang++", """
printf '%s\0' "$@" > "$TRACE/clang"
[ "$TEST_MODE" = "link_failure" ] && exit 71
while [ "$#" -gt 1 ]; do
    if [ "$1" = "-o" ]; then
        printf '#!/bin/sh\nprintf "ran" > "$TRACE/probe-run"\n' > "$2"
        chmod +x "$2"
        break
    fi
    shift
done
""")
        self.executable(bindir / "otool", """
if [ "$TEST_MODE" = "foreign_runtime" ]; then
    echo '  /opt/llvm/lib/c++/libc++.1.dylib (compatibility version 1.0.0)'
else
    echo '  /usr/lib/libc++.1.dylib (compatibility version 1.0.0)'
fi
""")
        self.executable(self.root / "bootstrap.sh", 'printf "%s\\0" "$@" > "$TRACE/bootstrap"')
        self.executable(self.root / "b2", """
printf '%s\0' "$@" > "$TRACE/b2"
command -v clang++ > "$TRACE/compiler-path"
[ "$TEST_MODE" = "b2_failure" ] && exit 42
exit 0
""")
        self.env = dict(os.environ, PATH=f"{bindir}{os.pathsep}{os.defpath}",
                        CXX=str(bindir / "clang++"), SDKROOT=str(sdk), TMPDIR=str(self.root),
                        TEST_ARCH="x86_64", TEST_MODE="", TRACE=str(self.root), SUDO="")

    @staticmethod
    def executable(path, source):
        path.write_text("#!/bin/sh\n" + source.replace("\x00", r"\0") + "\n")
        path.chmod(0o755)

    def run_adapter(self, *args, **env):
        return subprocess.run(["/bin/bash", str(ADAPTER), *args], cwd=self.root,
                              env={**self.env, **env}, capture_output=True, text=True)

    def read_args(self, operation):
        return (self.root / operation).read_text().rstrip("\0").split("\0")

    def test_sdk_pair_reaches_compile_and_link_without_bundled_runtime(self):
        prefix = str(self.root / "install prefix")
        result = self.run_adapter("--build", prefix, "2")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.read_args("bootstrap"), [f"--prefix={prefix}"])
        args = self.read_args("b2")
        self.assertEqual(args[:5], ["-j2", "-d0", "install", "threading=multi", "--without-python"])
        self.assertEqual(shlex.split(args[5].split("=", 1)[1]),
                         ["-nostdinc++", "-isysroot", self.env["SDKROOT"], "-isystem", str(self.include)])
        self.assertEqual(shlex.split(args[6].split("=", 1)[1]), ["-isysroot", self.env["SDKROOT"]])
        self.assertEqual((self.root / "compiler-path").read_text().strip(), self.env["CXX"])

    def test_preflight_runs_same_sdk_flags_and_system_runtime_probe(self):
        result = self.run_adapter("--check-runtime")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.read_args("clang")[:6],
                         ["-std=c++11", "-nostdinc++", "-isysroot", self.env["SDKROOT"],
                          "-isystem", str(self.include)])
        self.assertEqual((self.root / "probe-run").read_text(), "ran")
        self.assertFalse(list(self.root.glob("pangrova-cxx-abi.*")))

    def test_specific_compile_failure_fails_closed(self):
        result = self.run_adapter("--check-runtime", TEST_MODE="link_failure")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("libc++ link probe failed", result.stderr)
        self.assertFalse((self.root / "probe-run").exists())

    def test_bundled_runtime_is_rejected_before_execution(self):
        result = self.run_adapter("--check-runtime", TEST_MODE="foreign_runtime")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("must link the system libc++", result.stderr)
        self.assertFalse((self.root / "probe-run").exists())

    def test_missing_sdk_headers_and_non_x86_refuse_to_build(self):
        (self.include / "exception").unlink()
        for override, message in (({}, "headers are missing"), ({"TEST_ARCH": "arm64"}, "only for macOS x86_64")):
            with self.subTest(override=override):
                result = self.run_adapter("--build", "/", "2", **override)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(message, result.stderr)
                self.assertFalse((self.root / "bootstrap").exists())

    def test_sdk_path_cannot_escape_generated_b2_shell_quotes(self):
        sdk = self.root / "SDK's folder"
        header = sdk / "usr/include/c++/v1/exception"
        header.parent.mkdir(parents=True)
        header.touch()
        result = self.run_adapter("--build", "/", "2", SDKROOT=str(sdk))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unsupported shell characters", result.stderr)
        self.assertFalse((self.root / "bootstrap").exists())

    def test_actual_build_failure_is_propagated(self):
        result = self.run_adapter("--build", "/", "2", TEST_MODE="b2_failure")
        self.assertEqual(result.returncode, 42)

    def test_bootstrap_scopes_the_override_and_preflights_before_initial_builds(self):
        source = BOOTSTRAP.read_text()
        self.assertIn('if [[ "$ARCH" == x86_64 ]]; then\n      install_boost() {', source)
        self.assertIn('if [[ "$OS" == Darwin && "$ARCH" == x86_64 && "$SKIP_VELOX" != 1 &&', source)
        self.assertLess(source.index('darwin-x86-boost.sh" --check-runtime'), source.index("\n    build_abseil\n"))


if __name__ == "__main__":
    unittest.main()
