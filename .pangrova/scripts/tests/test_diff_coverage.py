"""Real temporary Git/index/report fixtures for the coverage gate."""
import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
import sys

sys.path.insert(0, str(Path(__file__).parents[1]))
from classfile_evidence import package_evidence

spec = importlib.util.spec_from_file_location("coverage_gate", Path(__file__).parents[1] / "diff_coverage.py")
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)


class DiffCoverageTest(unittest.TestCase):
    def setUp(self):
        package_evidence.cache_clear()
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.name = "module/src/main/java/example/Thing.java"
        self.source = self.root / self.name
        self.source.parent.mkdir(parents=True)
        self.source.write_text("package example;\nclass Thing {\n  int f() { return 1; }\n}\n")
        self.command("init", "-q")
        self.command("add", ".")
        self.command("-c", "user.name=Gate Test", "-c", "user.email=gate@example.invalid", "commit", "-qm", "base")

    def command(self, *arguments):
        subprocess.check_call(["git", "-C", str(self.root), *arguments], stdout=subprocess.DEVNULL)

    def report(self, body):
        file = self.root / "module/target/site/jacoco/jacoco.xml"
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text('<report><package name="example"><sourcefile name="Thing.java">'
                        + body + '</sourcefile></package></report>')

    def test_unstaged_and_staged_lines_are_measured(self):
        self.source.write_text("package example;\nclass Thing {\n  int f() { return 2; }\n}\n")
        self.assertEqual(gate.changed_lines(self.root, "HEAD")[self.name], {3})
        self.command("add", self.name)
        self.assertEqual(gate.changed_lines(self.root, "HEAD")[self.name], {3})

    def test_untracked_production_file_is_not_invisible(self):
        new = self.source.with_name("New.java")
        new.write_text("package example;\nclass New {}\n")
        self.assertEqual(gate.changed_lines(self.root, "HEAD")[str(new.relative_to(self.root))], {1, 2})

    def test_missing_report_fails_instead_of_skipping(self):
        hit, total, _, missing = gate.measure(self.root, {self.name: {3}})
        self.assertEqual((hit, total), (0, 0))
        self.assertEqual(len(missing), 1)

    def test_only_changed_executable_lines_count(self):
        self.report('<line nr="2" mi="0" ci="2"/><line nr="3" mi="2" ci="0"/>')
        hit, total, _, missing = gate.measure(self.root, {self.name: {3, 4}})
        self.assertEqual((hit, total, missing), (0, 1, []))

    def test_covered_changed_line_is_a_real_hit(self):
        self.report('<line nr="3" mi="0" ci="2"/>')
        hit, total, _, missing = gate.measure(self.root, {self.name: {3}})
        self.assertEqual((hit, total, missing), (1, 1, []))

    def test_stale_report_does_not_credit_a_new_source_revision(self):
        self.report('<line nr="3" mi="0" ci="2"/>')
        report = self.root / "module/target/site/jacoco/jacoco.xml"
        os.utime(report, (1, 1))
        hit, total, _, missing = gate.measure(self.root, {self.name: {3}})
        self.assertEqual((hit, total), (0, 0))
        self.assertIn("Stale coverage report", missing[0])

    def test_report_for_another_source_does_not_hide_missing_measurement(self):
        self.report('<line nr="3" mi="0" ci="2"/>')
        other = self.source.with_name("Absent.java")
        other.write_text("package example;\nclass Absent {}\n")
        self.report('<line nr="3" mi="0" ci="2"/>')
        name = str(other.relative_to(self.root))
        hit, total, _, missing = gate.measure(self.root, {name: {2}})
        self.assertEqual((hit, total), (0, 0))
        self.assertIn("No source coverage", missing[0])

    def test_same_source_name_in_another_module_cannot_borrow_coverage(self):
        self.report('<line nr="3" mi="0" ci="2"/>')
        name = self.name.replace("module/", "other-module/", 1)
        other = self.root / name
        other.parent.mkdir(parents=True)
        other.write_text(self.source.read_text())
        hit, total, _, missing = gate.measure(self.root, {name: {3}})
        self.assertEqual((hit, total), (0, 0))
        self.assertIn("other-module/target/site/jacoco", missing[0])

    def test_missing_base_is_not_a_zero_change_pass(self):
        with self.assertRaises(subprocess.CalledProcessError):
            gate.changed_lines(self.root, "missing-reference")

    def compile_source(self, text):
        self.source.write_text(text)
        classes = self.root / "module/target/classes"
        classes.mkdir(parents=True, exist_ok=True)
        subprocess.check_call(["javac", "-d", str(classes), str(self.source)])

    def test_fresh_real_abstract_interface_has_no_executable_lines(self):
        self.compile_source("package example; public interface Thing { int value(); }\n")
        hit, total, results, missing = gate.measure(self.root, {self.name: {1}})
        self.assertEqual((hit, total, missing), (0, 0, []))
        self.assertEqual(results, [(self.name, 0, 0)])

    def test_default_interface_method_still_requires_a_report(self):
        self.compile_source("package example; public interface Thing { default int value() { return 1; } }\n")
        _, _, _, missing = gate.measure(self.root, {self.name: {1}})
        self.assertIn("Missing report", missing[0])

    def test_other_top_level_class_in_same_source_cannot_hide_code(self):
        self.compile_source("package example; interface Thing {} class Other { int value() { return 1; } }\n")
        _, _, _, missing = gate.measure(self.root, {self.name: {1}})
        self.assertIn("Missing report", missing[0])

    def test_stale_abstract_interface_is_not_evidence_for_new_source(self):
        self.compile_source("package example; public interface Thing {}\n")
        compiled = self.root / "module/target/classes/example/Thing.class"
        os.utime(compiled, (1, 1))
        _, _, _, missing = gate.measure(self.root, {self.name: {1}})
        self.assertIn("Missing report", missing[0])

    def test_corrupt_class_file_is_a_failure_not_empty_coverage(self):
        self.compile_source("package example; public interface Thing {}\n")
        compiled = self.root / "module/target/classes/example/Thing.class"
        compiled.write_bytes(b"bad class")
        _, _, _, missing = gate.measure(self.root, {self.name: {1}})
        self.assertIn("Invalid class file", missing[0])

    def test_truncated_real_class_is_a_failure_not_empty_coverage(self):
        self.compile_source("package example; public interface Thing {}\n")
        compiled = self.root / "module/target/classes/example/Thing.class"
        compiled.write_bytes(compiled.read_bytes()[:-1])
        _, _, _, missing = gate.measure(self.root, {self.name: {1}})
        self.assertIn("Truncated class file", missing[0])

    def test_real_package_annotation_has_no_executable_lines(self):
        self.name = "module/src/main/java/example/package-info.java"
        self.source = self.root / self.name
        self.compile_source("@Deprecated package example;\n")
        hit, total, _, missing = gate.measure(self.root, {self.name: {1}})
        self.assertEqual((hit, total, missing), (0, 0, []))

    def test_string_named_code_does_not_invent_a_method_attribute(self):
        self.compile_source('package example; public interface Thing { String MARKER = "Code"; }\n')
        hit, total, _, missing = gate.measure(self.root, {self.name: {1}})
        self.assertEqual((hit, total, missing), (0, 0, []))


if __name__ == "__main__":
    unittest.main()
