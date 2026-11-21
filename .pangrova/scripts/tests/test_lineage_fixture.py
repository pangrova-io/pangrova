"""Reject missing or substituted reference-backend artifacts before image creation."""

import importlib.util
from pathlib import Path
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "prepare-lineage-test-image.py"
SPEC = importlib.util.spec_from_file_location("prepare_lineage_test_image", SCRIPT)
FIXTURE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(FIXTURE)


class ApplicationIntegrityTest(unittest.TestCase):
    def test_substituted_application_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "marquez-api-0.50.0.jar").write_bytes(b"substituted application")
            (root / "entrypoint.sh").write_text("exit 0\n")
            with self.assertRaisesRegex(ValueError, "pinned upstream artifact"):
                FIXTURE.verify_application(root)

    def test_missing_application_is_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaises(FileNotFoundError):
                FIXTURE.verify_application(Path(temporary))


if __name__ == "__main__":
    unittest.main()
