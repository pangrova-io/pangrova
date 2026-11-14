"""Regression tests for the documentation class-reference gate."""

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
GATE = ROOT / '.pangrova/scripts/check-doc-class-refs.sh'


class DocumentationClassReferenceTest(unittest.TestCase):
    def run_gate(self, documents):
        with tempfile.TemporaryDirectory() as directory:
            for name, content in documents.items():
                (Path(directory) / name).write_text(content, encoding='utf-8')
            environment = dict(os.environ)
            environment['PANGROVA_DOC_CLASS_REFS_DIR'] = directory
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
            '| `MissingRemovedAiFactory` | \u5df2\u5220\u9664 | '
            '`MissingActiveAiProvider` \u4ecd\u5728\u4f7f\u7528 | none |\n')
        status, reported = self.run_gate({'ai.md': content})
        self.assertEqual(1, status)
        self.assertEqual({'MissingActiveAiProvider'}, reported)

    def test_active_streaming_type_after_a_retired_reference_is_reported(self):
        content = (
            '\u9000\u5f79\u7684 `MissingLegacyStreamReader` '
            '\u4ec5\u5b9e\u73b0 `MissingStreamSource`\u3002\n')
        status, reported = self.run_gate({'streaming.md': content})
        self.assertEqual(1, status)
        self.assertEqual({'MissingStreamSource'}, reported)

    def test_removal_notes_in_a_later_column_do_not_hide_production_types(self):
        content = (
            '| original | disposition | production | evidence |\n'
            '|---|---|---|---|\n'
            '| `MissingIndexLifecycleListener` | \u5220\u9664\u672a\u88c5\u914d\u7684\u7c7b | '
            '\u751f\u4ea7 `MissingCurrentCollector` \u4ece\u4ed3\u50a8\u8bfb\u53d6 | '
            '\u5220\u9664\u524d\u68c0\u67e5\u8c03\u7528\u4e0e\u88c5\u914d |\n')
        status, reported = self.run_gate({'dispositions.md': content})
        self.assertEqual(1, status)
        self.assertIn('MissingCurrentCollector', reported)
        self.assertNotIn('MissingIndexLifecycleListener', reported)

    def test_historical_rows_alone_pass(self):
        content = (
            '| original | disposition | production | evidence |\n'
            '|---|---|---|---|\n'
            '| `MissingRetiredAdapter` | \u5df2\u5220\u9664 | none | none |\n')
        status, reported = self.run_gate({'removed.md': content})
        self.assertEqual(0, status)
        self.assertEqual(set(), reported)


if __name__ == '__main__':
    unittest.main()
