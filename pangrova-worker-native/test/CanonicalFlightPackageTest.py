"""Exercise mandatory Flight package admission without a simulated execution path."""

import argparse
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
OPTIONS = None


class CanonicalFlightPackageTest(unittest.TestCase):
    def configure(self, flight_prefix, cached_library=None):
        with tempfile.TemporaryDirectory(prefix="pangrova-flight-package-") as directory:
            root = Path(directory)
            (root / "CMakeLists.txt").write_text(
                "cmake_minimum_required(VERSION 3.20)\nproject(FlightConsumer CXX)\n"
                f'include("{ROOT / "cmake/FindArrowFlight.cmake"}")\n'
                'get_target_property(closure arrow_flight_imported INTERFACE_LINK_LIBRARIES)\n'
                'if(NOT closure STREQUAL "gRPC::grpc++;gRPC::grpc")\n'
                '  message(FATAL_ERROR "Direct server API dependency is absent")\nendif()\n',
                encoding="utf-8")
            arguments = [
                OPTIONS.cmake, "-S", str(root), "-B", str(root / "build"),
                f"-DCMAKE_CXX_COMPILER={OPTIONS.compiler}",
                f"-DPANGROVA_DEPS_PREFIX={OPTIONS.deps_prefix}",
                f"-DCMAKE_PREFIX_PATH={OPTIONS.deps_prefix}",
                f"-DARROW_FLIGHT_HOME={flight_prefix}",
            ]
            if cached_library is not None:
                arguments.append(f"-DARROW_FLIGHT_LIB={cached_library}")
            return subprocess.run(arguments, capture_output=True, text=True)

    def real_library(self):
        libraries = sorted((OPTIONS.flight_prefix / "lib").glob("libarrow_flight.*"))
        self.assertTrue(libraries, "The selected Flight package has no actual library")
        return libraries[0]

    def test_cached_library_from_selected_package_is_admitted(self):
        result = self.configure(OPTIONS.flight_prefix, self.real_library())
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_cached_library_cannot_admit_a_different_package_prefix(self):
        with tempfile.TemporaryDirectory(prefix="pangrova-stale-flight-prefix-") as directory:
            result = self.configure(Path(directory), self.real_library())
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Flight library differs from the selected package", result.stdout + result.stderr)

    def test_real_package_declares_direct_grpc_dependency_without_telemetry(self):
        result = self.configure(OPTIONS.flight_prefix)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_headers_without_library_do_not_admit_an_incomplete_runtime(self):
        with tempfile.TemporaryDirectory(prefix="pangrova-flight-headers-") as directory:
            prefix = Path(directory)
            header = prefix / "include/arrow/flight/types.h"
            header.parent.mkdir(parents=True)
            shutil.copy2(OPTIONS.flight_prefix / "include/arrow/flight/types.h", header)
            result = self.configure(prefix)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("requires the Arrow Flight library", result.stdout + result.stderr)

    def test_missing_package_is_rejected(self):
        with tempfile.TemporaryDirectory(prefix="pangrova-no-flight-") as directory:
            result = self.configure(Path(directory))
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("requires the Arrow Flight library", result.stdout + result.stderr)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cmake", required=True)
    parser.add_argument("--deps-prefix", type=Path, required=True)
    parser.add_argument("--flight-prefix", type=Path, required=True)
    parser.add_argument("--compiler", required=True)
    OPTIONS, remaining = parser.parse_known_args()
    unittest.main(argv=[__file__, *remaining])
