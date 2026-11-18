"""Verify the CI compiler selection reaches the native bootstrap."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]
ACTION = ROOT / ".github/actions/setup-native-build/action.yml"
BOOTSTRAP = ROOT / "pangrova-worker-native/bin/setup-native-build-env.sh"


def action_script(name):
    action = ACTION.read_text()
    step = action.split(f"    - name: {name}\n", 1)[1].split("\n    - name:", 1)[0]
    return "\n".join(line[8:] for line in step.split("      run: |\n", 1)[1].splitlines())


class NativeToolchainTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="native-toolchain-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.bin = self.root / "bin"
        self.bin.mkdir()
        self.env = dict(os.environ, HOME=str(self.root), PATH=f"{self.bin}{os.pathsep}{os.defpath}")

    @staticmethod
    def executable(path, body):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("#!/bin/sh\n" + body + "\n")
        path.chmod(0o755)

    def run_action(self, name):
        github_env = self.root / "github-env"
        github_path = self.root / "github-path"
        github_env.touch()
        github_path.touch()
        env = dict(self.env, GITHUB_ENV=str(github_env), GITHUB_PATH=str(github_path),
                   TRACE=str(self.root / "trace"), LLVM_PREFIX=str(self.root / "llvm"))
        result = subprocess.run(["bash", "-e", "-c", action_script(name)], env=env,
                                text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return github_env.read_text(), github_path.read_text(), (self.root / "trace").read_text()

    def test_linux_action_installs_and_exports_matching_compilers(self):
        self.executable(self.bin / "sudo", 'printf "%s\\n" "$*" >> "$TRACE"')
        environment, _, trace = self.run_action("Install build tools (Linux)")
        self.assertIn("gcc-13 g++-13", trace)
        self.assertEqual(environment, "CC=gcc-13\nCXX=g++-13\n")

    def test_macos_action_exports_the_installed_llvm_prefix(self):
        self.executable(self.bin / "brew", """
if [ "$1" = "--prefix" ]; then
  case "$2" in
    llvm@17) printf '%s\\n' "$LLVM_PREFIX" ;;
    bison|flex) printf '%s/%s\\n' "$HOME" "$2" ;;
    *) exit 1 ;;
  esac
else
  printf '%s\\n' "$*" >> "$TRACE"
fi
""")
        environment, path, trace = self.run_action("Install build tools (macOS)")
        self.assertIn("llvm@17", trace)
        self.assertEqual(environment, f"PANGROVA_LLVM_PREFIX={self.root / 'llvm'}\n")
        self.assertIn(f"{self.root / 'llvm' / 'bin'}\n", path)

    def bootstrap(self, prefix=None):
        self.executable(self.bin / "uname", """
case "$1" in
  -s) echo Darwin ;;
  -m) echo arm64 ;;
esac
""")
        for name in ("cmake", "ninja", "git", "cargo", "rustc", "autoconf", "automake",
                     "pkg-config", "glibtoolize", "xcode-select", "brew"):
            self.executable(self.bin / name, "exit 0")
        default = self.root / ".pangrova/llvm/bin/clang++"
        self.executable(default, "echo default-clang")
        env = dict(self.env, PANGROVA_HOME=str(self.root / ".pangrova"),
                   CMAKE_BUILD_PARALLEL_LEVEL="1")
        env.pop("PANGROVA_LLVM_PREFIX", None)
        if prefix is not None:
            env["PANGROVA_LLVM_PREFIX"] = str(prefix)
        return subprocess.run(["bash", str(BOOTSTRAP), "--check-tools"], env=env,
                              text=True, capture_output=True)

    def test_macos_bootstrap_prefers_explicit_llvm_prefix(self):
        prefix = self.root / "selected toolchain"
        self.executable(prefix / "bin/clang++", "echo selected-clang")
        result = self.bootstrap(prefix)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("Using selected-clang", result.stdout)
        self.assertNotIn("default-clang", result.stdout)

    def test_macos_bootstrap_rejects_an_invalid_explicit_prefix(self):
        result = self.bootstrap(self.root / "missing")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("check PANGROVA_LLVM_PREFIX", result.stderr)

    def test_macos_bootstrap_keeps_the_existing_default(self):
        result = self.bootstrap()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("Using default-clang", result.stdout)


if __name__ == "__main__":
    unittest.main()
