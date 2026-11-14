import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCANNER = Path(__file__).resolve().parents[1] / "check-native-includes.py"


class NativeIncludeGraphTest(unittest.TestCase):
    def scan(self, files):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, value in files.items():
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(value)
            return subprocess.run([sys.executable, str(SCANNER), str(root), "--components"],
                                  text=True, capture_output=True)

    def test_cycle_in_relative_and_source_root_headers_reports_each_source(self):
        result = self.scan({"a/one.h": '#include "../b/two.h"\n',
                            "b/two.h": '// outside header\n#include <a/one.h>\n'})
        self.assertEqual(1, result.returncode, result.stderr)
        self.assertIn("a/one.h:1 includes b/two.h", result.stderr)
        self.assertIn("b/two.h:2 includes a/one.h", result.stderr)

    def test_bidirectional_component_calls_with_acyclic_headers(self):
        result = self.scan({"a/api.h": "", "b/api.h": "",
                            "a/source.cpp": '#include "b/api.h"\n',
                            "b/source.cpp": '#include <a/api.h>\n'})
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("a -> b:", result.stdout)
        self.assertIn("b -> a:", result.stdout)
        self.assertIn("Cyclic source-component groups: 1\n  a, b\n", result.stdout)
        self.assertIn("2 local edges, 0 cycles", result.stdout)

    def test_reports_separate_groups_even_when_one_reaches_another(self):
        files = {name + "/api.h": "" for name in "abcdef"}
        for origin, targets in {"a": "b", "b": "c", "c": "ad", "d": "e", "e": "df"}.items():
            files[origin + "/impl.cpp"] = "".join(f'#include "{t}/api.h"\n' for t in targets)
        result = self.scan(files)
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("Cyclic source-component groups: 2\n  a, b, c\n  d, e\n", result.stdout)

    def test_paths_in_comments_and_raw_strings_are_not_edges(self):
        result = self.scan({"engine/a.cpp": '/*\n#include "a.cpp"\n*/\n'
                            'auto s = R"x(\n#include "a.cpp"\n)x";\n'
                            '#include \\\n "header.h"\n',
                            "engine/header.h": '#include <external/sdk.h>\n'})
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("1 local edges, 0 cycles", result.stdout)

    def test_empty_source_is_an_error(self):
        self.assertNotEqual(0, self.scan({}).returncode)
