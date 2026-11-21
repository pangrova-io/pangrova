#!/usr/bin/env python3
"""Apply the SDK portability patch and execute its actual platform selection."""
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[3]
PATCH = ROOT / "pangrova-worker-native/patches/velox-darwin-atomic-link.patch"
ORIGINAL = '''if(
  "${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang"
  AND "${CMAKE_CXX_COMPILER_VERSION}" VERSION_GREATER_EQUAL 15
)
  set(CMAKE_EXE_LINKER_FLAGS "-latomic")
endif()

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
'''


class AtomicLinkPatchTest(unittest.TestCase):
    def check_platform(self, apple, compiler, version, expected):
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            source = root / "CMakeLists.txt"
            source.write_text(ORIGINAL)
            subprocess.run(["git", "apply", "--check", str(PATCH)], cwd=root, check=True)
            subprocess.run(["git", "apply", str(PATCH)], cwd=root, check=True)
            subprocess.run(["git", "apply", "--reverse", "--check", str(PATCH)], cwd=root, check=True)
            script = root / "probe.cmake"
            script.write_text(
                'cmake_minimum_required(VERSION 3.20)\n'
                f'set(APPLE {apple})\n'
                f'set(CMAKE_CXX_COMPILER_ID "{compiler}")\n'
                f'set(CMAKE_CXX_COMPILER_VERSION "{version}")\n'
                'set(CMAKE_EXE_LINKER_FLAGS "-fsanitize=address")\n'
                + source.read_text()
                + f'file(WRITE "{root / "result"}" "${{CMAKE_EXE_LINKER_FLAGS}}")\n')
            subprocess.run(["cmake", "-P", str(script)], check=True, capture_output=True, text=True)
            self.assertEqual((root / "result").read_text(), expected)

    def test_darwin_clang_keeps_sanitizer_without_linux_library(self):
        self.check_platform("TRUE", "Clang", "17.0.6", "-fsanitize=address")

    def test_linux_clang_appends_atomic_without_losing_sanitizer(self):
        self.check_platform("FALSE", "Clang", "17.0.6", "-fsanitize=address -latomic")

    def test_other_compilers_keep_their_original_flags(self):
        self.check_platform("FALSE", "GNU", "13.0", "-fsanitize=address")

    def test_older_clang_keeps_original_selection(self):
        self.check_platform("FALSE", "Clang", "14.0", "-fsanitize=address")


if __name__ == "__main__":
    unittest.main()
