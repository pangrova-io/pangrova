"""Verify the CI compiler selection reaches the native bootstrap."""

import os
from pathlib import Path
import re
import shlex
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
                   TRACE=str(self.root / "trace"), LLVM_PREFIX=str(self.root / "llvm"),
                   TEST_SDKROOT=str(self.root / "macos sdk"), RUNNER_TEMP=str(self.root))
        result = subprocess.run(["bash", "-e", "-c", action_script(name)], env=env,
                                text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return github_env.read_text(), github_path.read_text(), (self.root / "trace").read_text()

    def test_macos_disk_cleanup_keeps_the_selected_xcode(self):
        self.executable(self.bin / "sudo", 'printf "%s\\n" "$*" >> "$TRACE"')
        xcodes = self.root / "Applications"
        for name in ("Xcode_15.4.app", "Xcode_16.4.app"):
            (xcodes / name).mkdir(parents=True)
        self.executable(self.bin / "xcode-select",
                        f'printf "{xcodes / "Xcode_16.4.app"}/Contents/Developer\\n"')
        self.env["TRACE"] = str(self.root / "trace")
        script = action_script("Free runner disk space (macOS)").replace(
            "/Applications/Xcode*.app", str(xcodes / "Xcode*.app"))
        result = subprocess.run(["bash", "-e", "-c", script], env=self.env,
                                text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        trace = (self.root / "trace").read_text()
        self.assertIn("Xcode_15.4.app", trace)
        self.assertNotIn("Xcode_16.4.app", trace)

    def test_linux_action_installs_and_exports_matching_compilers(self):
        self.executable(self.bin / "sudo", 'printf "%s\\n" "$*" >> "$TRACE"')
        environment, _, trace = self.run_action("Install build tools (Linux)")
        self.assertIn("gcc-13 g++-13", trace)
        self.assertIn("libgflags-dev", trace)
        self.assertIn("ripgrep", trace)
        self.assertEqual(environment, "CC=gcc-13\nCXX=g++-13\n")

    def test_linux_action_checks_and_runs_a_static_gflags_consumer(self):
        compiler = self.bin / "cxx"
        self.executable(compiler, """
printf '%s\\n' "$*" >> "$TRACE"
cat >> "$TRACE"
while [ "$#" -gt 0 ]; do
  if [ "$1" = "-o" ]; then
    shift
    printf '#!/bin/sh\\nprintf "ABI check ran\\\\n" >> "$TRACE"\\n' > "$1"
    chmod +x "$1"
    exit 0
  fi
  shift
done
exit 1
""")
        self.env["CXX"] = str(compiler)
        _, _, trace = self.run_action("Verify Linux system C++ ABI")
        self.assertIn("GFLAGS_NAMESPACE::ParseCommandLineFlags", trace)
        self.assertIn("-Wl,-Bstatic -lgflags -Wl,-Bdynamic", trace)
        self.assertIn("ABI check ran", trace)

    def test_linux_action_propagates_a_gflags_link_failure(self):
        compiler = self.bin / "cxx"
        self.executable(compiler, "exit 1")
        env = dict(self.env, CXX=str(compiler), RUNNER_TEMP=str(self.root))
        result = subprocess.run(["bash", "-e", "-o", "pipefail", "-c",
                                 action_script("Verify Linux system C++ ABI")],
                                env=env, text=True, capture_output=True)
        self.assertNotEqual(result.returncode, 0)

    def macos_tooling(self, llvm_formula="llvm@17"):
        for name in ("cmake", "ninja", "autoconf", "automake", "glibtoolize",
                     "pkg-config", "bison", "flex", "rg"):
            self.executable(self.bin / name, "exit 0")
        prefix = self.root / llvm_formula.replace("@", "")
        self.executable(prefix / "bin/clang++",
                        'echo "Homebrew clang version 17.0.6"')
        self.executable(self.bin / "brew", f"""
printf 'auto-update=%s install-upgrade=%s\\n' "${{HOMEBREW_NO_AUTO_UPDATE:-}}" "${{HOMEBREW_NO_INSTALL_UPGRADE:-}}" >> "$TRACE"
printf '%s\\n' "$*" >> "$TRACE"
if [ "$1" = "--prefix" ]; then
  case "$2" in
    {llvm_formula}) printf '%s\\n' "{prefix}" ;;
    bison|flex) printf '%s/%s\\n' "$HOME" "$2" ;;
    *) exit 1 ;;
  esac
fi
""")
        sdk_root = self.root / "macos sdk"
        (sdk_root / "usr/include").mkdir(parents=True)
        self.executable(self.bin / "xcrun", 'printf "%s\\n" "$TEST_SDKROOT"')
        return sdk_root, prefix

    def test_macos_action_exports_the_selected_llvm_prefix(self):
        sdk_root, prefix = self.macos_tooling()
        environment, path, trace = self.run_action("Install build tools (macOS)")
        self.assertIn("auto-update=1 install-upgrade=1", trace)
        self.assertNotIn("install llvm@17", trace)
        self.assertNotIn("ccache", trace)
        self.assertNotIn("update --quiet", trace)
        self.assertEqual(environment, f"PANGROVA_LLVM_PREFIX={prefix}\nSDKROOT={sdk_root}\n")
        self.assertIn(f"{prefix / 'bin'}\n", path)

    def test_macos_action_prefers_an_installed_newer_llvm_without_installing(self):
        _, prefix = self.macos_tooling(llvm_formula="llvm@18")
        environment, path, _ = self.run_action("Install build tools (macOS)")
        self.assertEqual(environment, f"PANGROVA_LLVM_PREFIX={prefix}\n"
                                      f"SDKROOT={self.root / 'macos sdk'}\n")
        self.assertIn(f"{prefix / 'bin'}\n", path)

    def bootstrap(self, prefix=None, sdkroot=None, full=False, build_deps=False, arch="arm64"):
        self.executable(self.bin / "uname", f"""
case "$1" in
  -s) echo Darwin ;;
  -m) echo {arch} ;;
esac
""")
        for name in ("cmake", "ninja", "git", "cargo", "rustc", "rg", "autoconf",
                     "automake", "pkg-config", "glibtoolize", "xcode-select", "brew"):
            self.executable(self.bin / name, "exit 0")
        self.executable(self.bin / "xcrun", 'printf "%s\\n" "$TEST_SDKROOT"')
        sdk = self.root / "macos sdk"
        (sdk / "usr/include").mkdir(parents=True, exist_ok=True)
        default = self.root / ".pangrova/llvm/bin/clang++"
        self.executable(default, "echo default-clang")
        env = dict(self.env, PANGROVA_HOME=str(self.root / ".pangrova"),
                   PANGROVA_NATIVE_BUILD_DIR=str(self.root / "build"),
                   TEST_SDKROOT=str(sdk), CMAKE_BUILD_PARALLEL_LEVEL="1")
        env.pop("PANGROVA_LLVM_PREFIX", None)
        env.pop("SDKROOT", None)
        if prefix is not None:
            env["PANGROVA_LLVM_PREFIX"] = str(prefix)
        if sdkroot is not None:
            env["SDKROOT"] = str(sdkroot)
        if full:
            env.update(SKIP_DEPS="1", SKIP_VELOX="1")
            self.executable(self.root / "build/re2-contract-build/pangrova_re2_contract", "exit 0")
            deps = self.root / ".pangrova/native-deps"
            for name in ("bin/protoc", "bin/grpc_cpp_plugin"):
                self.executable(deps / name, "exit 0")
            for name in ("lib/cmake/protobuf/protobuf-config.cmake", "lib/cmake/grpc/gRPCConfig.cmake"):
                path = deps / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.touch()
        if build_deps:
            env["SKIP_DEPS"] = "0"
            env["TRACE"] = str(self.root / "cmake-trace")
            self.executable(self.bin / "cmake", 'printf "%s\\n" "$@" >> "$TRACE"')
            for name in ("abseil-cpp", "protobuf", "re2", "grpc"):
                (self.root / "build" / name).mkdir(parents=True, exist_ok=True)
            copts = self.root / "build/abseil-cpp/absl/copts/AbseilConfigureCopts.cmake"
            copts.parent.mkdir(parents=True)
            copts.write_text('foreach(_arch IN ITEMS "x86_64" "arm64")\n')
        args = [] if full else ["--check-tools"]
        return subprocess.run(["/bin/bash", str(BOOTSTRAP), *args], env=env,
                              text=True, capture_output=True)

    def test_macos_intel_bootstrap_reaches_dependency_configure(self):
        result = self.bootstrap(full=True, build_deps=True, arch="x86_64")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        trace = (self.root / "cmake-trace").read_text()
        self.assertIn(str(self.root / "build/abseil-cpp"), trace)
        self.assertIn(str(self.root / "build/grpc"), trace)
        self.assertNotIn("-DCMAKE_OSX_ARCHITECTURES=arm64", trace)

    def test_macos_arm_bootstrap_keeps_the_architecture_argument(self):
        result = self.bootstrap(full=True, build_deps=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("-DCMAKE_OSX_ARCHITECTURES=arm64",
                      (self.root / "cmake-trace").read_text())

    def test_optional_argument_arrays_preserve_empty_and_quoted_values(self):
        for script in (BOOTSTRAP, BOOTSTRAP.with_name("setup-native-asan-env.sh")):
            expansions = [line.strip().rstrip("\\").strip()
                          for line in script.read_text().splitlines()
                          if "[@]" in line and not line.lstrip().startswith("#")]
            self.assertTrue(expansions)
            for expansion in expansions:
                names = sorted(set(re.findall(r'\$\{(\w+)\[@\]', expansion)))
                for values in ([], ["-DPREFIX=/path with spaces", "-DOPTION=ON"]):
                    with self.subTest(script=script.name, expansion=expansion, values=values):
                        items = " ".join(shlex.quote(value) for value in values)
                        assignments = "; ".join(f"{name}=({items})" for name in names)
                        command = (f"set -eu; {assignments}; "
                                   'capture() { printf "%s\\n" "$#" "$@"; }; '
                                   f"capture {expansion}")
                        result = subprocess.run(["/bin/bash", "-c", command], env=self.env,
                                                text=True, capture_output=True)
                        self.assertEqual(result.returncode, 0, result.stderr)
                        self.assertEqual(result.stdout.splitlines(),
                                         [str(len(values) * len(names)), *values * len(names)])

    def test_dependency_phase_prepares_sources_without_compiling_release_velox(self):
        sdk = self.root / "velox"
        (sdk / ".git").mkdir(parents=True)
        aws = sdk / "deps-install/lib/cmake/AWSSDK/AWSSDKConfig.cmake"
        aws.parent.mkdir(parents=True)
        aws.touch()
        self.executable(sdk / "scripts/setup-ubuntu.sh", 'printf "prepared\\n" >> "$TRACE"')
        self.executable(self.bin / "git", "exit 0")
        self.executable(self.bin / "bash", "exit 0")
        self.executable(self.bin / "cmake", 'printf "compiled\\n" >> "$TRACE"; exit 73')
        source = BOOTSTRAP.read_text()
        function = "build_velox() {" + source.split("\nbuild_velox() {", 1)[1].split("\n}\n", 1)[0] + "\n}"
        env = dict(self.env, VELOX_DIR=str(sdk), WORK_DIR=str(self.root),
                   VELOX_REF="fixture", GRPC_VERSION="fixture", PROTOBUF_VERSION="fixture",
                   ABSEIL_VERSION="fixture", OS="Linux", NPROC="1", CC="cc", CXX="c++",
                   PANGROVA_PRODUCTION_BUILD="OFF", CMAKE_LAUNCHER_FLAGS="",
                   PANGROVA_CMAKE_BIN=str(self.bin / "cmake"), DEPS_PREFIX=str(self.root / "deps"),
                   PANGROVA_BOOTSTRAP_SCRIPT_DIR=str(BOOTSTRAP.parent))
        for phase, expected in (("dependencies", 0), ("velox", 73)):
            with self.subTest(phase=phase):
                trace = self.root / f"{phase}-trace"
                command = ('set -euo pipefail\nlog() { :; }\nstep() { :; }\n'
                           'ensure_folly_compiler_abi() { :; }\n' + function + '\nbuild_velox\n')
                result = subprocess.run(["/bin/bash", "-c", command], text=True, capture_output=True,
                                        env=dict(env, PANGROVA_NATIVE_BUILD_PHASE=phase, TRACE=str(trace)))
                self.assertEqual(result.returncode, expected, result.stdout + result.stderr)
                self.assertIn("prepared", trace.read_text())
                self.assertEqual("compiled" in trace.read_text(), phase == "velox")

    def darwin_libevent(self, mode, fixture):
        prefix = self.root / "brew formula"
        trace = self.root / "brew-log"
        self.executable(self.bin / "brew", """
printf '%s\\n' "$*" >> "$TRACE"
case "$*" in
  '--prefix libevent') [ "$FIXTURE" = no-prefix ] || printf '%s\\n' "$PREFIX" ;;
  'install --formula libevent')
    [ "$FIXTURE" != install-fails ] || exit 8
    [ "$HOMEBREW_NO_AUTO_UPDATE:$HOMEBREW_NO_INSTALL_UPGRADE" = 1:1 ] || exit 9
    mkdir -p "$PREFIX/include" "$PREFIX/lib"
    touch "$PREFIX/include/event.h" "$PREFIX/lib/libevent.dylib" ;;
  *) exit 10 ;;
esac
""")
        if fixture == "present":
            (prefix / "include").mkdir(parents=True)
            (prefix / "lib").mkdir()
            (prefix / "include/event.h").touch()
            (prefix / "lib/libevent.dylib").touch()
        helper = BOOTSTRAP.with_name("resolve-darwin-libevent.sh")
        cmd = ('die() { echo "$1" >&2; exit 1; }; '
               'source "$1"; selected="$(resolve_darwin_libevent "$2" "$3")"; '
               'printf "configure:%s\\n" "$selected" >> "$TRACE"')
        result = subprocess.run(
            ["/bin/bash", "-eu", "-c", cmd, "test", str(helper), str(self.bin / "brew"), mode],
            env=dict(self.env, TRACE=str(trace), PREFIX=str(prefix), FIXTURE=fixture),
            text=True, capture_output=True,
        )
        return result, prefix, trace.read_text().splitlines()

    def test_darwin_libevent_installs_before_configure(self):
        result, prefix, trace = self.darwin_libevent("install", "absent")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(trace, ["--prefix libevent", "install --formula libevent",
                                 "--prefix libevent", f"configure:{prefix}"])

    def test_darwin_libevent_reuses_an_installed_formula(self):
        result, prefix, trace = self.darwin_libevent("install", "present")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(trace, ["--prefix libevent", f"configure:{prefix}"])

    def test_darwin_libevent_fails_before_configure_on_install_error(self):
        result, _, trace = self.darwin_libevent("install", "install-fails")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("could not install libevent", result.stderr)
        self.assertEqual(trace, ["--prefix libevent", "install --formula libevent"])

    def test_darwin_libevent_fails_before_configure_without_resolved_prefix(self):
        result, _, trace = self.darwin_libevent("install", "no-prefix")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("libevent headers or library not found", result.stderr)
        self.assertEqual(trace, ["--prefix libevent", "install --formula libevent", "--prefix libevent"])

    def test_darwin_asan_libevent_check_does_not_install(self):
        result, _, trace = self.darwin_libevent("check", "absent")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("run setup-native-build-env.sh", result.stderr)
        self.assertEqual(trace, ["--prefix libevent"])

    def test_darwin_folly_builds_pass_the_formula_prefix(self):
        release = BOOTSTRAP.read_text()
        asan = BOOTSTRAP.with_name("setup-native-asan-env.sh").read_text()
        self.assertIn('resolve_darwin_libevent "$PANGROVA_BREW" install', release)
        self.assertIn('$INSTALL_PREFIX;$libevent_prefix;$HOMEBREW_PREFIX', release)
        self.assertIn('ensure_folly_compiler_abi "$libevent_prefix"', release)
        self.assertIn('$VELOX_DIR/deps-install;$libevent_prefix', release)
        self.assertIn('resolve_darwin_libevent "$PANGROVA_BREW"', asan)
        self.assertIn('$VELOX_HOME/deps-install${libevent_prefix:+;$libevent_prefix}', asan)

    def test_macos_bootstrap_prefers_explicit_llvm_prefix(self):
        prefix = self.root / "selected toolchain"
        self.executable(prefix / "bin/clang++", "echo selected-clang")
        result = self.bootstrap(prefix)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("Using selected-clang", result.stdout)
        self.assertIn(f"Using macOS SDK {self.root / 'macos sdk'}", result.stdout)
        self.assertNotIn("default-clang", result.stdout)

    def test_macos_bootstrap_rejects_an_invalid_explicit_prefix(self):
        result = self.bootstrap(self.root / "missing")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("check PANGROVA_LLVM_PREFIX", result.stderr)

    def test_macos_bootstrap_keeps_the_existing_default(self):
        result = self.bootstrap()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("Using default-clang", result.stdout)

    def test_macos_bootstrap_preserves_the_requested_sdk(self):
        sdk = self.root / "requested sdk"
        (sdk / "usr/include").mkdir(parents=True)
        result = self.bootstrap(sdkroot=sdk)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn(f"Using macOS SDK {sdk}", result.stdout)

    def test_macos_bootstrap_resolves_a_named_sdk(self):
        result = self.bootstrap(sdkroot="macosx")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn(f"Using macOS SDK {self.root / 'macos sdk'}", result.stdout)

    def test_macos_bootstrap_rejects_an_invalid_sdk(self):
        result = self.bootstrap(sdkroot=self.root / "missing sdk")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("macOS SDK headers not found", result.stderr)

    def test_macos_bootstrap_records_the_sdk_for_later_builds(self):
        result = self.bootstrap(full=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        env_file = self.root / ".pangrova/env.sh"
        result = subprocess.run(["bash", "-c", 'source "$1"; printf "%s" "$SDKROOT"',
                                 "test", str(env_file)], env=self.env,
                                text=True, capture_output=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout, str(self.root / "macos sdk"))


if __name__ == "__main__":
    unittest.main()
