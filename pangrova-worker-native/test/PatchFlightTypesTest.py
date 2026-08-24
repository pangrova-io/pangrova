import argparse
import importlib.util
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch


SCRIPT_PATH = Path(__file__).parents[1] / "patch_flight_types.py"
SPEC = importlib.util.spec_from_file_location("patch_flight_types", SCRIPT_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class PatchFlightTypesTest(unittest.TestCase):
    def setUp(self):
        self.temp_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_directory.name)
        self.header = self.root / MODULE.HEADER_RELATIVE_PATH
        self.header.parent.mkdir(parents=True)
        self.header.write_text(
            "namespace arrow {\n"
            "namespace flight {\n"
            "class FlightInfo {\n"
            f"{MODULE.OLD_LINE}"
            "};\n"
            f"{MODULE.NAMESPACE_MARKER}"
            "}  // namespace arrow\n",
            encoding="utf-8",
        )

    def tearDown(self):
        self.temp_directory.cleanup()

    def test_explicit_header_is_patched_idempotently(self):
        MODULE.patch_header(self.header)
        first_result = self.header.read_text(encoding="utf-8")

        MODULE.patch_header(self.header)

        self.assertEqual(first_result, self.header.read_text(encoding="utf-8"))
        self.assertIn(MODULE.NEW_LINE, first_result)
        self.assertIn("inline FlightInfo::FlightInfo(Data data)", first_result)

    def test_deps_prefix_argument_resolves_header(self):
        args = argparse.Namespace(header=None, deps_prefix=self.root)

        self.assertEqual(self.header, MODULE.resolve_header(args))

    def test_environment_prefix_resolves_header(self):
        args = argparse.Namespace(header=None, deps_prefix=None)

        with patch.dict(os.environ, {"PANGROVA_DEPS_PREFIX": str(self.root)}, clear=True):
            self.assertEqual(self.header, MODULE.resolve_header(args))

    def test_missing_header_has_actionable_error(self):
        missing_header = self.root / "missing.h"

        with self.assertRaisesRegex(FileNotFoundError, "Set PANGROVA_DEPS_PREFIX"):
            MODULE.patch_header(missing_header)


if __name__ == "__main__":
    unittest.main()
