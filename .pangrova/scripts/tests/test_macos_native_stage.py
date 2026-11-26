"""Exercise the real stage archive and identity contract, not a Native build."""
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import tarfile
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[3]
STAGE = ROOT / ".pangrova/scripts/macos-native-stage.sh"


class MacOSNativeStageTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory(prefix="macos-native-stage-")
        self.addCleanup(temp.cleanup)
        self.temp = Path(temp.name)
        self.home = self.temp / "home"
        self.root = self.home / ".pangrova"
        self.bin = self.temp / "bin"
        self.bundle = self.temp / "bundle"
        self.llvm = self.temp / "llvm"
        for path in (self.home, self.bin, self.llvm / "bin"):
            path.mkdir(parents=True)
        self.executable(self.bin / "uname",
                        'case "$1" in -s) echo Darwin;; -m) echo "${TEST_ARCH:-arm64}";; esac')
        self.executable(self.bin / "sw_vers", 'echo "${TEST_OS_BUILD:-24D70}"')
        self.executable(self.bin / "xcodebuild", "echo 'Xcode 16.4'")
        self.executable(self.bin / "xcrun", """
if [ "$1" = lipo ]; then
    # Xcode lipo expects input files before -verify_arch; every following
    # argument is interpreted as an architecture, not another input file.
    [ "$#" -eq 4 ] && [ -f "$2" ] && [ "$3" = -verify_arch ] || exit 2
    [ "$4" = "${TEST_ARCH:-arm64}" ] || exit 2
    [ "${TEST_WRONG_BINARY:-}" != "$2" ] || exit 1
else
    echo 15.5
fi
""")
        self.executable(self.bin / "brew", """
echo "openssl@3 ${TEST_OPENSSL_VERSION:-3.5.1}"
echo "libevent ${TEST_LIBEVENT_VERSION:-2.1.12}"
""")
        self.executable(self.llvm / "bin/clang++", 'echo "clang version ${TEST_CLANG_VERSION:-18.1.8}"')
        self.env = dict(os.environ, HOME=str(self.home),
                        PATH=f"{self.bin}{os.pathsep}{os.defpath}",
                        PANGROVA_LLVM_PREFIX=str(self.llvm),
                        PANGROVA_HOME=str(self.root),
                        SDKROOT="/test/MacOSX.sdk", GITHUB_RUN_ID="12",
                        GITHUB_SHA="a" * 40, GITHUB_WORKSPACE=str(ROOT),
                        ImageVersion="test-image")

    @staticmethod
    def executable(path, command):
        path.write_text("#!/bin/sh\n" + command + "\n")
        path.chmod(0o755)

    def create(self, name, value="fixture\n"):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value)
        return path

    def prepare(self, sdk=False):
        for name in ("bin/protoc", "bin/grpc_cpp_plugin",
                     "lib/cmake/protobuf/protobuf-config.cmake",
                     "lib/cmake/grpc/gRPCConfig.cmake",
                     "lib/cmake/re2/re2Config.cmake", "lib/libre2.a"):
            self.create("native-deps/" + name)
        for tool in ("protoc", "grpc_cpp_plugin"):
            (self.root / "native-deps/bin" / tool).chmod(0o755)
        for name in ("velox/core/PlanNode.h", ".git/HEAD", "deps-install/lib/libfolly.a",
                     "deps-install/.pangrova-folly-alignment", "deps-install/.pangrova-fbthrift-ready",
                     "deps-install/lib/cmake/Snappy/SnappyConfig.cmake", "deps-install/lib/libsnappy.a",
                     "deps-install/lib/cmake/AWSSDK/AWSSDKConfig.cmake"):
            self.create("velox/" + name)
        if sdk:
            self.create("native-deps/lib/libarrow_flight.18.dylib")
            (self.root / "native-deps/lib/libarrow_flight.dylib").symlink_to(
                "libarrow_flight.18.dylib")
            for name in ("velox/core/PlanNode.h", "_build/release/lib/libvelox.a",
                         "_build/release/lib/libvelox.a.pangrova-provenance",
                         "_build/release/CMakeCache.txt", ".git/HEAD",
                         "_build/release/velox/generated.h"):
                self.create("velox/" + name)
            self.create("velox/_build/release/temporary.o")
            self.create("velox/archive-cache/source.tar.gz")

    def run_stage(self, operation, stage, **overrides):
        return subprocess.run(["bash", str(STAGE), operation, stage, str(self.bundle)],
                              env=dict(self.env, **overrides),
                              text=True, capture_output=True)

    def assert_ok(self, result):
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def fresh(self):
        shutil.rmtree(self.root)

    def test_core_round_trip_preserves_executable_and_package_config(self):
        self.prepare()
        self.assert_ok(self.run_stage("pack", "core"))
        self.fresh()
        self.assert_ok(self.run_stage("restore", "core"))
        for tool in ("protoc", "grpc_cpp_plugin"):
            self.assertTrue((self.root / "native-deps/bin" / tool).stat().st_mode & 0o100)
        self.assertTrue((self.root / "velox/deps-install/lib/libfolly.a").is_file())
        self.assertFalse((self.root / "velox/_build/release/lib/libvelox.a").exists())

    def test_baseline_core_bundle_carries_its_archive_cache(self):
        self.prepare()
        self.create("velox/archive-cache/source.tar.gz")
        self.assert_ok(self.run_stage("pack", "core", PANGROVA_MACOS_STAGE_ARCHIVES="1"))
        self.fresh()
        self.assert_ok(self.run_stage("restore", "core"))
        self.assertEqual((self.root / "velox/archive-cache/source.tar.gz").read_text(), "fixture\n")

    def test_intel_core_round_trip_uses_x86_64_architecture(self):
        self.prepare()
        self.assert_ok(self.run_stage("pack", "core", TEST_ARCH="x86_64"))
        self.fresh()
        self.assert_ok(self.run_stage("restore", "core", TEST_ARCH="x86_64"))

    def test_sdk_round_trip_preserves_libraries_symlinks_headers_and_source_identity(self):
        self.prepare(sdk=True)
        self.assert_ok(self.run_stage("pack", "sdk"))
        self.fresh()
        self.assert_ok(self.run_stage("restore", "sdk"))
        self.assertTrue((self.root / "native-deps/lib/libarrow_flight.dylib").is_symlink())
        self.assertEqual((self.root / "velox/_build/release/lib/libvelox.a").read_text(), "fixture\n")
        self.assertTrue((self.root / "velox/.git/HEAD").is_file())
        self.assertTrue((self.root / "velox/_build/release/velox/generated.h").is_file())
        self.assertFalse((self.root / "velox/_build/release/temporary.o").exists())
        self.assertFalse((self.root / "velox/archive-cache").exists())

    def test_missing_output_refuses_to_package(self):
        self.prepare(sdk=True)
        (self.root / "velox/_build/release/lib/libvelox.a.pangrova-provenance").unlink()
        result = self.run_stage("pack", "sdk")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("lacks required output", result.stderr)

    def test_required_package_config_must_be_regular_nonempty_file(self):
        self.prepare()
        config = self.root / "native-deps/lib/cmake/protobuf/protobuf-config.cmake"
        config.unlink()
        config.mkdir()
        (config / "README").write_text("not a CMake package config")
        result = self.run_stage("pack", "core")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("lacks required output", result.stderr)

    def test_both_build_tools_must_be_executable(self):
        self.prepare()
        for tool in ("protoc", "grpc_cpp_plugin"):
            with self.subTest(tool=tool):
                path = self.root / "native-deps/bin" / tool
                path.chmod(0o644)
                result = self.run_stage("pack", "core")
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("not executable", result.stderr)
                path.chmod(0o755)

    def test_restored_tool_must_be_executable(self):
        self.prepare()
        self.assert_ok(self.run_stage("pack", "core"))
        (self.root / "native-deps/bin/grpc_cpp_plugin").chmod(0o644)
        archive = self.bundle / "prefix.tar.gz"
        # Model an older genuine stage archive with an incorrect tool mode.
        with tarfile.open(archive, "w:gz") as bundle:
            for directory in ("native-deps", "velox"):
                bundle.add(self.root / directory, arcname=directory)
        checksum = hashlib.sha256(archive.read_bytes()).hexdigest()
        (self.bundle / "prefix.sha256").write_text(f"{checksum}  prefix.tar.gz\n")
        self.fresh()
        result = self.run_stage("restore", "core")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("not executable", result.stderr)

    def test_both_build_tools_must_match_runner_architecture(self):
        self.prepare()
        for tool in ("protoc", "grpc_cpp_plugin"):
            with self.subTest(tool=tool):
                path = self.root / "native-deps/bin" / tool
                result = self.run_stage("pack", "core", TEST_WRONG_BINARY=str(path))
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("incompatible binary architecture", result.stderr)

    def test_revision_run_architecture_and_toolchain_mismatch_are_rejected(self):
        self.prepare()
        self.assert_ok(self.run_stage("pack", "core"))
        self.fresh()
        for overrides in ({"GITHUB_SHA": "b" * 40}, {"GITHUB_RUN_ID": "13"},
                          {"TEST_ARCH": "x86_64"}, {"TEST_OS_BUILD": "24D71"},
                          {"SDKROOT": "/different/sdk"}, {"TEST_CLANG_VERSION": "19.1.1"},
                          {"TEST_OPENSSL_VERSION": "3.5.2"}, {"TEST_LIBEVENT_VERSION": "2.1.13"}):
            with self.subTest(overrides=overrides):
                result = self.run_stage("restore", "core", **overrides)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("does not match", result.stderr)
                self.assertFalse(self.root.exists())

    def test_runner_image_rollout_preserves_compatible_stage_restore(self):
        self.prepare(sdk=True)
        self.assert_ok(self.run_stage("pack", "sdk", ImageVersion="new-image"))
        self.fresh()
        restored = self.run_stage("restore", "sdk", ImageVersion="previous-image")
        self.assert_ok(restored)
        self.assertIn("previous-image", restored.stdout)
        self.assertTrue((self.root / "velox/_build/release/lib/libvelox.a").is_file())

    def test_wrong_binary_architecture_refuses_to_package(self):
        self.prepare(sdk=True)
        wrong = self.root / "velox/_build/release/lib/libvelox.a"
        result = self.run_stage("pack", "sdk", TEST_WRONG_BINARY=str(wrong))
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("incompatible binary architecture", result.stderr)

    def test_corrupted_archive_is_rejected_before_extraction(self):
        self.prepare()
        self.assert_ok(self.run_stage("pack", "core"))
        self.fresh()
        with (self.bundle / "prefix.tar.gz").open("ab") as output:
            output.write(b"changed")
        result = self.run_stage("restore", "core")
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(self.root.exists())

    def test_existing_prefix_is_not_overlaid(self):
        self.prepare()
        self.assert_ok(self.run_stage("pack", "core"))
        result = self.run_stage("restore", "core")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("requires a fresh dependency prefix", result.stderr)


if __name__ == "__main__":
    unittest.main()
