"""Regression tests for the documentation class-reference gate."""

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
GATE = ROOT / '.pangrova/scripts/check-doc-class-refs.sh'


class DocumentationClassReferenceTest(unittest.TestCase):
    def run_gate(self, documents, path=None):
        with tempfile.TemporaryDirectory() as directory:
            for name, content in documents.items():
                (Path(directory) / name).write_text(content, encoding='utf-8')
            environment = dict(os.environ)
            environment['PANGROVA_DOC_CLASS_REFS_DIR'] = directory
            if path is not None:
                environment['PATH'] = path
            result = subprocess.run([str(GATE)], cwd=ROOT, env=environment,
                                    capture_output=True, text=True)
        reported = {
            line.split()[1]
            for line in result.stdout.splitlines()
            if line.strip().startswith('\u2717 ')
        }
        return result.returncode, reported

    def test_active_ai_type_on_a_removed_row_is_reported(self):
        content = (
            '| range | disposition | production | evidence |\n'
            '|---|---|---|---|\n'
            '| `MissingRemovedAiFactory` | Removed | '
            '`MissingActiveAiProvider` is still used | none |\n')
        status, reported = self.run_gate({'ai.md': content})
        self.assertEqual(1, status)
        self.assertEqual({'MissingActiveAiProvider'}, reported)

    def test_active_streaming_type_after_a_retired_reference_is_reported(self):
        content = (
            'Retired `MissingLegacyStreamReader` '
            'only implemented `MissingStreamSource`.\n')
        status, reported = self.run_gate({'streaming.md': content})
        self.assertEqual(1, status)
        self.assertEqual({'MissingStreamSource'}, reported)

    def test_removal_notes_in_a_later_column_do_not_hide_production_types(self):
        content = (
            '| original | disposition | production | evidence |\n'
            '|---|---|---|---|\n'
            '| `MissingIndexLifecycleListener` | Removed the unwired class | '
            'Production `MissingCurrentCollector` reads from the repository | '
            'Check callers and wiring before removal |\n')
        status, reported = self.run_gate({'dispositions.md': content})
        self.assertEqual(1, status)
        self.assertIn('MissingCurrentCollector', reported)
        self.assertNotIn('MissingIndexLifecycleListener', reported)

    def test_removed_sentence_does_not_hide_the_next_sentence(self):
        content = ('`MissingOldPlanner` was removed. '
                   'The new `MissingActivePlanner` owns planning.\n')
        status, reported = self.run_gate({'planner.md': content})
        self.assertEqual(1, status)
        self.assertEqual({'MissingActivePlanner'}, reported)

    def test_historical_rows_alone_pass(self):
        content = (
            '| original | disposition | production | evidence |\n'
            '|---|---|---|---|\n'
            '| `MissingRetiredAdapter` | Removed | none | none |\n')
        status, reported = self.run_gate({'removed.md': content})
        self.assertEqual(0, status)
        self.assertEqual(set(), reported)

    def test_gate_does_not_require_ripgrep(self):
        with tempfile.TemporaryDirectory() as directory:
            ripgrep = Path(directory) / 'rg'
            ripgrep.write_text('#!/bin/sh\nexit 127\n', encoding='utf-8')
            ripgrep.chmod(0o755)
            path = directory + os.pathsep + os.environ['PATH']
            status, reported = self.run_gate(
                {'classes.md': '`QueryId` and `MissingExampleClass`.\n'}, path=path)
        self.assertEqual(1, status)
        self.assertEqual({'MissingExampleClass'}, reported)


if __name__ == '__main__':
    unittest.main()
