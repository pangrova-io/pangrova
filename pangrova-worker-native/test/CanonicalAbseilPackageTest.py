"""Verify real Abseil package selection and SDK archive-to-target ownership."""

import argparse
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
OPTIONS = None


class CanonicalAbseilPackageTest(unittest.TestCase):
    def configure(self, cache_option, inspect_archives=False):
        with tempfile.TemporaryDirectory(prefix="pangrova-abseil-contract-") as directory:
            root = Path(directory)
            sdk = root / "sdk"
            sdk.mkdir()
            (sdk / "CMakeLists.txt").write_text(
                "cmake_minimum_required(VERSION 3.20)\nproject(SdkDependencySelection NONE)\n",
                encoding="utf-8")
            sdk_build = root / "sdk-build"
            command = [OPTIONS.cmake, "-S", str(sdk), "-B", str(sdk_build)]
            if cache_option is not None:
                command.append(cache_option)
            result = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
            source = root / "main"
            source.mkdir()
            body = (
                "cmake_minimum_required(VERSION 3.20)\nproject(AbseilConsumer CXX)\n"
                f'include("{ROOT / "cmake/PangrovaAbseil.cmake"}")\n')
            if inspect_archives:
                archives = sorted((OPTIONS.sdk_deps / "lib").glob("libabsl_*.a"))
                self.assertTrue(archives, "SDK has no Abseil archives to verify")
                body += "pangrova_select_abseil_targets(selected\n"
                body += "".join(f'  "{archive}"\n' for archive in archives) + ")\n"
                body += "foreach(target IN LISTS selected)\n"
                body += '  if(NOT target MATCHES "^absl::")\n'
                body += '    message(FATAL_ERROR "Raw SDK Abseil archive remains")\n  endif()\n'
                body += '  get_target_property(location "${target}" IMPORTED_LOCATION_RELEASE)\n'
                prefix = OPTIONS.package.resolve().parents[2] / "lib"
                body += f'  string(FIND "${{location}}" "{prefix}/" prefix_position)\n'
                body += '  if(NOT prefix_position EQUAL 0)\n'
                body += '    message(FATAL_ERROR "Target escaped selected package: ${location}")\n'
                body += "  endif()\nendforeach()\n"
            (source / "CMakeLists.txt").write_text(body, encoding="utf-8")
            return subprocess.run([
                OPTIONS.cmake, "-S", str(source), "-B", str(root / "main-build"),
                f"-DCMAKE_CXX_COMPILER={OPTIONS.compiler}",
                f"-Dabsl_DIR:PATH={OPTIONS.package}", f"-DVELOX_BUILD_DIR={sdk_build}",
            ], capture_output=True, text=True)

    def test_explicit_package_and_every_sdk_archive_use_the_real_targets(self):
        result = self.configure(f"-Dabsl_DIR:PATH={OPTIONS.package}", True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_untyped_command_line_selection_is_accepted(self):
        result = self.configure(f"-Dabsl_DIR={OPTIONS.package}")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_different_sdk_selection_is_rejected(self):
        result = self.configure(f"-Dabsl_DIR:PATH={OPTIONS.package}/other-package")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Velox Abseil package differs", result.stdout + result.stderr)

    def test_missing_sdk_selection_is_rejected(self):
        result = self.configure(None)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Velox does not record its selected Abseil package", result.stdout + result.stderr)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cmake", required=True)
    parser.add_argument("--package", type=Path, required=True)
    parser.add_argument("--sdk-deps", type=Path, required=True)
    parser.add_argument("--compiler", required=True)
    OPTIONS, remaining = parser.parse_known_args()
    unittest.main(argv=[__file__, *remaining])
