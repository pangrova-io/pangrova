import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCANNER = Path(__file__).resolve().parents[1] / "check-native-includes.py"


class NativeIncludeGraphTest(unittest.TestCase):
    def scan(self, files, *options):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, value in files.items():
                path = root / name
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(value)
            return subprocess.run([sys.executable, str(SCANNER), str(root), "--components", *options],
                                  text=True, capture_output=True)

    def test_cycle_in_relative_and_source_root_headers_reports_each_source(self):
        result = self.scan({"a/one.h": '#include "../b/two.h"\n',
                            "b/two.h": '// outside header\n#include <a/one.h>\n'})
        self.assertEqual(1, result.returncode, result.stderr)
        self.assertIn("a/one.h:1 includes b/two.h", result.stderr)
        self.assertIn("b/two.h:2 includes a/one.h", result.stderr)

    def test_engine_cannot_recover_the_command_handle_registry(self):
        result = self.scan({"command/resource/HandleRegistry.h": "",
                            "engine/runner.cpp": '#include "command/resource/HandleRegistry.h"\n'})
        self.assertEqual(1, result.returncode)
        self.assertIn("engine -> command: engine/runner.cpp:1 includes command/resource/HandleRegistry.h", result.stderr)

    def test_lance_cannot_load_command_headers_for_storage_authorization(self):
        result = self.scan({"command/task/Storage.h": "",
                            "lance/control.cpp": '#include <command/task/Storage.h>\n'})
        self.assertEqual(1, result.returncode)
        self.assertIn("lance -> command: lance/control.cpp:1 includes command/task/Storage.h", result.stderr)

    def test_jni_assembles_lance_memory_and_udf_error_reporting(self):
        for origin, target in (("lance", "jni"), ("udf", "command")):
            with self.subTest(origin=origin, target=target):
                result = self.scan({target + "/entry.h": "",
                                    origin + "/impl.cpp": f'#include "{target}/entry.h"\n'})
                self.assertEqual(1, result.returncode)
                self.assertIn(f"{origin} -> {target}: {origin}/impl.cpp:1", result.stderr)

    def test_bidirectional_component_calls_with_acyclic_headers(self):
        result = self.scan({"a/api.h": "", "b/api.h": "",
                            "a/source.cpp": '#include "b/api.h"\n',
                            "b/source.cpp": '#include <a/api.h>\n'})
        self.assertEqual(1, result.returncode)
        self.assertIn("New cyclic Native component edge a -> b: a/source.cpp:1", result.stderr)
        self.assertIn("a -> b:", result.stdout)
        self.assertIn("b -> a:", result.stdout)
        self.assertIn("Cyclic source-component groups: 1\n  a, b\n", result.stdout)

    def test_existing_reciprocal_edges_and_new_one_way_edge(self):
        result = self.scan({"engine/api.h": "", "jni/api.h": "", "isolated/api.h": "",
                            "jni/init.cpp": '#include "engine/api.h"\n',
                            "engine/run.cpp": '#include "jni/api.h"\n#include "isolated/api.h"\n'})
        self.assertEqual(0, result.returncode, result.stderr)
        self.assertIn("3 local edges, 0 cycles", result.stdout)

    def test_new_edge_inside_existing_component_group(self):
        files = {name + "/api.h": "" for name in ("engine", "jni", "vector")}
        files["engine/run.cpp"] = '#include "jni/api.h"\n#include "vector/api.h"\n'
        files["vector/run.cpp"] = '#include "engine/api.h"\n'
        files["jni/init.cpp"] = '#include "engine/api.h"\n#include "vector/api.h"\n'
        result = self.scan(files)
        self.assertEqual(1, result.returncode)
        self.assertIn("New cyclic Native component edge jni -> vector: jni/init.cpp:2", result.stderr)

    def test_edge_that_is_now_acyclic_must_be_removed_from_the_baseline(self):
        result = self.scan({"engine/api.h": "", "jni/init.cpp": '#include "engine/api.h"\n'},
                           "--verify-component-baseline")
        self.assertEqual(1, result.returncode)
        self.assertIn("Remove resolved cyclic edge from the Native baseline: jni -> engine", result.stderr)

    def test_reports_separate_groups_even_when_one_reaches_another(self):
        files = {name + "/api.h": "" for name in "abcdef"}
        for origin, targets in {"a": "b", "b": "c", "c": "ad", "d": "e", "e": "df"}.items():
            files[origin + "/impl.cpp"] = "".join(f'#include "{t}/api.h"\n' for t in targets)
        result = self.scan(files)
        self.assertEqual(1, result.returncode)
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
