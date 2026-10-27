"""Exercise the RE2 probe's actual child environment and failure propagation."""

from pathlib import Path
import shlex
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]
MODULE = ROOT / "pangrova-worker-native/cmake/PangrovaRe2.cmake"


class NativeSanitizerProbeTest(unittest.TestCase):
    def run_probe(self, apple, exit_code=0):
        with tempfile.TemporaryDirectory(prefix="native sanitizer probe ") as directory:
            root = Path(directory)
            package = root / "deps/lib/cmake/re2"
            package.mkdir(parents=True)
            (package / "re2Config.cmake").write_text("# Test package marker\n")
            sdk = root / "sdk"
            sdk.mkdir()
            (sdk / "CMakeCache.txt").write_text(f"re2_DIR:PATH={package}\n")
            output = root / "child-environment"
            probe = root / "probe"
            probe.write_text("#!/bin/sh\n"
                             f"printf '%s' \"$ASAN_OPTIONS\" > {shlex.quote(str(output))}\n"
                             f"exit {exit_code}\n")
            probe.chmod(0o755)
            # Substitute only dependency discovery/compilation. The production
            # module still launches the child through its real cmake -E env and
            # must propagate its actual process result. This is not an ABI test.
            (root / "CMakeLists.txt").write_text(f"""
cmake_minimum_required(VERSION 3.20)
project(ProbeEnvironment NONE)
set(APPLE {"TRUE" if apple else "FALSE"})
set(PANGROVA_DEPS_PREFIX "{root / 'deps'}")
set(VELOX_BUILD_DIR "{sdk}")
function(find_package package)
  if(package STREQUAL "re2")
    add_library(re2::re2 INTERFACE IMPORTED)
    set_property(TARGET re2::re2 PROPERTY IMPORTED_CONFIGURATIONS RELEASE)
    set_property(TARGET re2::re2 PROPERTY IMPORTED_LOCATION_RELEASE unused.fixture)
  endif()
endfunction()
function(try_compile result directory)
  file(MAKE_DIRECTORY "${{directory}}")
  file(COPY_FILE "{probe}" "${{directory}}/pangrova_re2_contract")
  set(${{result}} TRUE PARENT_SCOPE)
endfunction()
include("{MODULE}")
""")
            result = subprocess.run(["cmake", "-S", str(root), "-B", str(root / "build")],
                                    text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                    check=False)
            return result, output.read_text() if output.exists() else None

    def test_darwin_child_runs_address_checks(self):
        result, environment = self.run_probe(apple=True)
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(environment, "detect_leaks=0:halt_on_error=1")

    def test_linux_child_retains_leak_checks(self):
        result, environment = self.run_probe(apple=False)
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertEqual(environment, "detect_leaks=1:halt_on_error=1")

    def test_failed_child_still_rejects_the_configuration(self):
        result, environment = self.run_probe(apple=True, exit_code=23)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Canonical RE2 runtime ABI mismatch (23)", result.stdout)
        self.assertEqual(environment, "detect_leaks=0:halt_on_error=1")


if __name__ == "__main__":
    unittest.main()
