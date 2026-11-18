"""Regression tests for the E2E truthfulness gate."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[3]
GATE = ROOT / '.pangrova/scripts/check-e2e-truthfulness.sh'


class E2ETruthfulnessTest(unittest.TestCase):
    def run_gate(self, files):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            sources = root / 'sources'
            sources.mkdir()
            for name, content in files.items():
                (sources / name).write_text(content, encoding='utf-8')
            ripgrep = root / 'rg'
            ripgrep.write_text('#!/bin/sh\nexit 127\n', encoding='utf-8')
            ripgrep.chmod(0o755)
            environment = dict(os.environ, E2E_TRUTHFULNESS_ROOT=str(sources),
                               PATH=directory + os.pathsep + os.environ['PATH'])
            return subprocess.run(['bash', str(GATE)], env=environment,
                                  capture_output=True, text=True)

    def test_clean_sources_pass_without_ripgrep(self):
        result = self.run_gate({'ExampleTest.java': 'assertThat(result).isEqualTo(expected);\n'})
        self.assertEqual(0, result.returncode, result.stderr)

    def test_forbidden_patterns_fail_without_ripgrep(self):
        cases = [
            ('Example.java', 'assumeTrue(ready);', 'skips and constant assertions'),
            ('Example.java', 'assertThat(true);', 'skips and constant assertions'),
            ('Example.java', 'catch (AssertionError e) {}', 'caught assertions'),
            ('Example.java', 'assertThat(values.size()).isGreaterThanOrEqualTo(0);', 'collection tautologies'),
            ('Example.java', 'catch (RuntimeException e) {\n  return List.of();\n}', 'caught assertions'),
            ('ExampleTest.java', '@Tag("multi-worker")\nclass Example extends SqlE2ETestBase {}',
             'multi-worker requires distinct worker processes'),
        ]
        for name, content, message in cases:
            with self.subTest(content=content):
                result = self.run_gate({name: content})
                self.assertEqual(1, result.returncode, result.stderr)
                self.assertIn(message, result.stderr)
                self.assertIn(name + ':', result.stdout)

    def test_multi_worker_check_requires_test_file_and_tag(self):
        for name, content in [
            ('ExampleTest.java', 'class Example extends SqlE2ETestBase {}'),
            ('Example.java', '@Tag("multi-worker")\nclass Example extends SqlE2ETestBase {}'),
        ]:
            with self.subTest(name=name):
                result = self.run_gate({name: content})
                self.assertEqual(0, result.returncode, result.stderr)

    def test_missing_source_root_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            environment = dict(os.environ, E2E_TRUTHFULNESS_ROOT=str(Path(directory) / 'missing'))
            result = subprocess.run(['bash', str(GATE)], env=environment,
                                    capture_output=True, text=True)
        self.assertEqual(1, result.returncode)
        self.assertIn('E2E source root is missing', result.stderr)


if __name__ == '__main__':
    unittest.main()
