"""Verify the actual SDK archive cohort through the production CMake admission gate."""

import argparse
from contextlib import contextmanager
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[1]
OPTIONS = None


class CanonicalFunctionProvenanceTest(unittest.TestCase):
    @contextmanager
    def source_checkout(self):
        """Use real pinned Git objects, checking out only the patch's owned files."""
        patch = (ROOT / "patches/velox-function-metadata-snapshot.patch").read_text(encoding="utf-8")
        paths = re.findall(r"^\+\+\+ b/(.+)$", patch, re.MULTILINE)
        self.assertTrue(paths)
        with tempfile.TemporaryDirectory(prefix="pangrova-source-patch-") as directory:
            source = Path(directory) / "sdk"
            subprocess.run(["git", "clone", "--shared", "--no-checkout", str(OPTIONS.source), str(source)],
                           capture_output=True, text=True, check=True, timeout=60)
            subprocess.run(["git", "-C", str(source), "sparse-checkout", "set", "--no-cone", "--stdin"],
                           input="".join(f"/{path}\n" for path in paths),
                           capture_output=True, text=True, check=True, timeout=60)
            subprocess.run(["git", "-C", str(source), "checkout", "--detach"],
                           capture_output=True, text=True, check=True, timeout=60)
            yield source

    @staticmethod
    def source_action(source, action):
        return subprocess.run(["bash", str(ROOT / "bin/apply-velox-patches.sh"), action, str(source)],
                              capture_output=True, text=True, timeout=60)

    def test_full_patch_applies_to_actual_pinned_source(self):
        with self.source_checkout() as source:
            applied = self.source_action(source, "apply")
            self.assertEqual(applied.returncode, 0, applied.stdout + applied.stderr)
            verified = self.source_action(source, "verify")
            self.assertEqual(verified.returncode, 0, verified.stdout + verified.stderr)

    def test_complete_prior_source_checkpoint_upgrades_without_dropping_changes(self):
        with self.source_checkout() as source:
            applied = self.source_action(source, "apply")
            self.assertEqual(applied.returncode, 0, applied.stdout + applied.stderr)
            script = (ROOT / "bin/apply-velox-patches.sh").read_text(encoding="utf-8")
            upgrade = re.search(r"cat <<'PANGROVA_PRIOR_UPGRADE'\n(.*?)\nPANGROVA_PRIOR_UPGRADE", script, re.DOTALL)
            self.assertIsNotNone(upgrade)
            subprocess.run(["git", "-C", str(source), "apply", "--unidiff-zero", "--reverse", "-"],
                           input=upgrade.group(1) + "\n", capture_output=True,
                           text=True, check=True, timeout=60)
            subprocess.run(
                ["git", "-C", str(source), "apply", "--unidiff-zero", "--reverse",
                 "--include=velox/connectors/hive/storage_adapters/s3fs/*",
                 str(ROOT / "patches/velox-function-metadata-snapshot.patch")],
                capture_output=True, text=True, check=True, timeout=60)
            upgraded = self.source_action(source, "apply")
            self.assertEqual(upgraded.returncode, 0, upgraded.stdout + upgraded.stderr)
            verified = self.source_action(source, "verify")
            self.assertEqual(verified.returncode, 0, verified.stdout + verified.stderr)

    def test_unreviewed_source_drift_is_rejected(self):
        with self.source_checkout() as source:
            applied = self.source_action(source, "apply")
            self.assertEqual(applied.returncode, 0, applied.stdout + applied.stderr)
            with (source / "velox/vector/arrow/Bridge.cpp").open("a", encoding="utf-8") as output:
                output.write("\n// Deliberate source drift outside the reviewed patch.\n")
            rejected = self.source_action(source, "apply")
            self.assertNotEqual(rejected.returncode, 0)
            self.assertIn("unreviewed drift", rejected.stderr)

    def configure(self, change=None, corrupt_header=False):
        with tempfile.TemporaryDirectory(prefix="pangrova-function-provenance-") as directory:
            root = Path(directory)
            archive = root / "libvelox.a"
            archive.symlink_to(OPTIONS.archive.resolve(strict=True))
            proof = Path(str(OPTIONS.archive) + ".pangrova-provenance").read_text(encoding="utf-8")
            if change is not None:
                proof = change(proof)
            Path(str(archive) + ".pangrova-provenance").write_text(proof, encoding="utf-8")
            include = OPTIONS.include
            if corrupt_header:
                include = root / "include"
                patch = (ROOT / "patches/velox-function-metadata-snapshot.patch").read_text(encoding="utf-8")
                headers = re.findall(r"^\+\+\+ b/(.*\.h)$", patch, re.MULTILINE)
                self.assertTrue(headers, "Actual SDK patch declares no selected public headers")
                for header in headers:
                    destination = include / header
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(OPTIONS.include / header, destination)
                with (include / headers[0]).open("a", encoding="utf-8") as output:
                    output.write("\n// Deliberately mismatched selected header.\n")
            project = root / "project"
            project.mkdir()
            (project / "CMakeLists.txt").write_text(
                "cmake_minimum_required(VERSION 3.20)\n"
                "project(FunctionArchiveAdmission NONE)\n"
                f'include("{ROOT / "cmake/PangrovaFunctionProvenance.cmake"}")\n'
                f'pangrova_verify_function_provenance("{OPTIONS.source}" "{include}" "{archive}")\n',
                encoding="utf-8")
            return subprocess.run([OPTIONS.cmake, "-S", str(project), "-B", str(root / "build"),
                                   f"-DPython3_EXECUTABLE={OPTIONS.python}"],
                                  capture_output=True, text=True, timeout=60)

    def assert_rejected(self, change, diagnostic):
        result = self.configure(change)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn(diagnostic, result.stdout + result.stderr)

    def test_actual_rebuilt_archive_and_input_cohort_is_accepted(self):
        result = self.configure()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_legacy_three_line_proof_is_rejected(self):
        self.assert_rejected(lambda proof: "\n".join(
            proof.splitlines()[index] for index in (1, 2, 4)) + "\n", "Malformed registration archive provenance")

    def test_different_generator_input_identity_is_rejected(self):
        self.assert_rejected(lambda proof: self.replace_line(proof, 3),
                             "Registration source/patch/generation/archive provenance differs")

    def test_different_sdk_patch_identity_is_rejected(self):
        self.assert_rejected(lambda proof: self.replace_line(proof, 2),
                             "Registration source/patch/generation/archive provenance differs")

    def test_different_archive_identity_is_rejected(self):
        self.assert_rejected(lambda proof: self.replace_line(proof, 4),
                             "Registration source/patch/generation/archive provenance differs")

    def test_extra_provenance_line_is_rejected(self):
        self.assert_rejected(lambda proof: proof + "extra\n", "Malformed registration archive provenance")

    def test_selected_public_header_drift_is_rejected(self):
        result = self.configure(corrupt_header=True)
        self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertIn("Selected registration header differs from rebuilt source", result.stdout + result.stderr)

    @staticmethod
    def replace_line(proof, index):
        lines = proof.splitlines()
        lines[index] = "0" * 64 if lines[index] != "0" * 64 else "1" * 64
        return "\n".join(lines) + "\n"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cmake", required=True)
    parser.add_argument("--python", required=True)
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--include", type=Path, required=True)
    parser.add_argument("--archive", type=Path, required=True)
    OPTIONS, remaining = parser.parse_known_args()
    unittest.main(argv=[__file__, *remaining])
