import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "stage-java-native-test-runtime.sh"


class StageJavaNativeTestRuntimeTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.build = self.root / "build"
        self.stage = self.root / "stage with spaces"
        self.build.mkdir()
        self.fixture = self.build / "pangrova_lance_fixture_tool"
        self.fixture.write_text('#!/bin/bash\nprintf "%s\\n" "$LD_LIBRARY_PATH" "$@"\n')
        (self.build / "libpangrova_native.so").touch()
        self.arrow = self.root / "libarrow-real.so"
        self.arrow.write_bytes(b"arrow fixture")
        self.system = self.root / "libc.so.6"
        self.system.touch()
        self.ldd = self.root / "ldd"
        self.ldd.write_text('#!/bin/bash\ncat "$1.ldd"\n')
        self.ldd.chmod(0o755)

    def run_stage(self, fixture_dependencies, native_dependencies):
        self.fixture.with_suffix(".ldd").write_text(fixture_dependencies)
        (self.build / "libpangrova_native.so.ldd").write_text(native_dependencies)
        return subprocess.run(
            ["bash", SCRIPT, self.build, self.stage],
            env={**os.environ, "PATH": f"{self.root}:{os.environ['PATH']}"},
            text=True, capture_output=True,
        )

    def test_stages_soname_and_scope_library_path_to_executable_fixture(self):
        alias = self.root / "arrow link"
        alias.symlink_to(self.arrow)
        result = self.run_stage(
            f"libarrow.so.1800 => {alias} (0x1)\nlibc.so.6 => {self.system} (0x2)\n",
            f"libarrow.so.1800 => {self.arrow} (0x3)\n/lib64/ld-linux-x86-64.so.2 (0x4)\n",
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        library = self.stage / "lib/libarrow.so.1800"
        self.assertEqual(library.read_bytes(), self.arrow.read_bytes())
        self.assertFalse(library.is_symlink())
        self.assertFalse((self.stage / "lib/libc.so.6").exists())
        launched = subprocess.check_output(
            [self.stage / self.fixture.name, "one argument"],
            env={**os.environ, "LD_LIBRARY_PATH": "/previous/lib"}, text=True,
        )
        self.assertEqual(launched.splitlines(), [f"{self.stage}/lib:/previous/lib", "one argument"])

    def test_missing_or_absolute_dependency_fails_staging(self):
        for dependencies, expected in (
            ("libarrow.so.1800 => not found\n", "Unresolved dependency libarrow.so.1800"),
            (f"{self.arrow} (0x1)\n", "Absolute dependency"),
        ):
            with self.subTest(dependencies=dependencies):
                result = self.run_stage(dependencies, "")
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(expected, result.stderr)

    def test_different_libraries_with_the_same_soname_fail_staging(self):
        conflicting = self.root / "different.so"
        conflicting.write_bytes(b"different")
        result = self.run_stage(
            f"libarrow.so.1800 => {self.arrow} (0x1)\n",
            f"libarrow.so.1800 => {conflicting} (0x2)\n",
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("incompatible copies of libarrow.so.1800", result.stderr)
