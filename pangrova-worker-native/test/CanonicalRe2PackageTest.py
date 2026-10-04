"""Exercise the real canonical RE2 gate with CMake-generated SDK caches."""

import argparse
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
OPTIONS = None


class CanonicalRe2PackageTest(unittest.TestCase):
    def configure(self, cache_option, deps_prefix=None, cxx_flags=None, linker_flags=None):
        deps_prefix = deps_prefix or OPTIONS.deps_prefix
        with tempfile.TemporaryDirectory(prefix="pangrova-re2-contract-") as directory:
            root = Path(directory)
            sdk = root / "sdk"
            sdk.mkdir()
            (sdk / "CMakeLists.txt").write_text(
                "cmake_minimum_required(VERSION 3.20)\n"
                "project(DependencySelection NONE)\n", encoding="utf-8")
            sdk_build = root / "sdk-build"
            sdk_command = [OPTIONS.cmake, "-S", str(sdk), "-B", str(sdk_build)]
            if cache_option is not None:
                sdk_command.append(cache_option)
            sdk_result = subprocess.run(sdk_command, capture_output=True, text=True)
            self.assertEqual(sdk_result.returncode, 0, sdk_result.stdout + sdk_result.stderr)
            main = root / "main"
            main.mkdir()
            (main / "CMakeLists.txt").write_text(
                "cmake_minimum_required(VERSION 3.20)\n"
                "project(CanonicalDependencyConsumer CXX)\n"
                f'include("{(ROOT / "cmake/PangrovaRe2.cmake").as_posix()}")\n',
                encoding="utf-8")
            return subprocess.run([
                OPTIONS.cmake, "-S", str(main), "-B", str(root / "main-build"),
                f"-DCMAKE_CXX_COMPILER={OPTIONS.compiler}",
                f"-DPANGROVA_DEPS_PREFIX={deps_prefix}",
                f"-Dabsl_DIR:PATH={OPTIONS.absl_package or OPTIONS.deps_prefix / 'lib/cmake/absl'}",
                f"-DCMAKE_CXX_FLAGS={OPTIONS.cxx_flags if cxx_flags is None else cxx_flags}",
                f"-DCMAKE_EXE_LINKER_FLAGS={OPTIONS.linker_flags if linker_flags is None else linker_flags}",
                f"-DVELOX_BUILD_DIR={sdk_build}",
            ], capture_output=True, text=True)

    def test_explicit_path_cache_accepts_the_real_package_and_abi(self):
        result = self.configure(f"-Dre2_DIR:PATH={OPTIONS.deps_prefix}/lib/cmake/re2")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_untyped_command_line_cache_accepts_the_same_real_package_and_abi(self):
        result = self.configure(f"-Dre2_DIR={OPTIONS.deps_prefix}/lib/cmake/re2")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_a_different_sdk_package_is_rejected_before_linking(self):
        result = self.configure(f"-Dre2_DIR:PATH={OPTIONS.deps_prefix}/different-re2")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Velox RE2 package differs", result.stdout + result.stderr)

    def test_missing_sdk_selection_is_rejected(self):
        result = self.configure(None)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Velox does not record its selected RE2 package", result.stdout + result.stderr)

    def test_incompatible_sanitizer_template_layout_is_rejected_at_runtime(self):
        compile_flags = "" if "-fsanitize=address" in OPTIONS.cxx_flags else "-fsanitize=address"
        result = self.configure(f"-Dre2_DIR:PATH={OPTIONS.deps_prefix}/lib/cmake/re2",
                                cxx_flags=compile_flags, linker_flags="-fsanitize=address")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Canonical RE2 runtime ABI mismatch", result.stdout + result.stderr)

    def test_duplicate_archive_members_are_rejected_before_linking(self):
        with tempfile.TemporaryDirectory(prefix="pangrova-corrupt-re2-") as directory:
            root = Path(directory)
            deps = root / "deps"
            cmake_dir = deps / "lib/cmake"
            cmake_dir.mkdir(parents=True)
            (deps / "include").symlink_to(OPTIONS.deps_prefix / "include", target_is_directory=True)
            shutil.copytree(OPTIONS.deps_prefix / "lib/cmake/re2", cmake_dir / "re2")
            archive = deps / "lib/libre2.a"
            shutil.copy2(OPTIONS.deps_prefix / "lib/libre2.a", archive)
            members = subprocess.run([OPTIONS.ar, "t", str(archive)],
                                     capture_output=True, text=True, check=True).stdout.splitlines()
            self.assertTrue(members, "Canonical RE2 archive has no object members")
            subprocess.run([OPTIONS.ar, "x", str(archive), members[0]], cwd=root, check=True)
            subprocess.run([OPTIONS.ar, "q", str(archive), members[0]], cwd=root, check=True)
            result = self.configure(f"-Dre2_DIR:PATH={cmake_dir / 're2'}", deps)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Canonical RE2 archive contains duplicate object members",
                          result.stdout + result.stderr)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cmake", required=True)
    parser.add_argument("--deps-prefix", type=Path, required=True)
    parser.add_argument("--compiler", required=True)
    parser.add_argument("--ar", required=True)
    parser.add_argument("--absl-package", type=Path)
    parser.add_argument("--cxx-flags", default="")
    parser.add_argument("--linker-flags", default="")
    OPTIONS, remaining = parser.parse_known_args()
    unittest.main(argv=[__file__, *remaining])
