"""Regression tests for gate scope, PR parsing, and structural-policy alignment."""

import importlib.util
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[3]
SCRIPTS = ROOT / '.pangrova/scripts'
spec = importlib.util.spec_from_file_location('scope', SCRIPTS / 'ci_change_scope.py')
scope = importlib.util.module_from_spec(spec)
spec.loader.exec_module(scope)


class PerformanceScopeTest(unittest.TestCase):
    def test_docs_and_unrelated_tests_do_not_trigger_performance(self):
        paths = ['README.md', 'pangrova-worker-native/README.md',
                 'pangrova-worker/src/test/java/example/WorkerTest.java',
                 '.pangrova/scripts/tests/test_gate_policy.py']
        self.assertFalse(scope.needs_performance(paths))

    def test_runtime_dependency_and_benchmark_inputs_trigger_performance(self):
        paths = ['pangrova-parser/src/main/java/example/Parser.java',
                 'pangrova-worker/src/main/resources/worker.yml',
                 'pangrova-worker/src/main/resources/docs/template.md',
                 'pangrova-worker/src/main/java/org/pangrova/docs/DocRenderer.java',
                 'pangrova-worker-native/test/ExampleTest.cpp',
                 'pangrova-client/pangrova-client-cli/pom.xml',
                 'pangrova-testing/pangrova-test-benchmark/baseline/jmh.json',
                 'pangrova-testing/pangrova-test-e2e/src/test/java/'
                 'org/pangrova/testing/e2e/performance/SqlLatencyBaselineTest.java',
                 '.github/workflows/ci.yml', '.pangrova/modules.yaml']
        for path in paths:
            with self.subTest(path=path):
                self.assertTrue(scope.needs_performance([path]))

    def test_mixed_diff_and_unknown_inputs_are_conservative(self):
        self.assertTrue(scope.needs_performance(['README.md', 'new-build-input.lock']))
        self.assertFalse(scope.needs_performance([]))


class GitGateTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.git('init', '-q')
        self.git('config', 'user.email', 'gate-test@example.invalid')
        self.git('config', 'user.name', 'Gate Test')
        self.commit_file('README.md', 'base\n')
        self.base = self.git('rev-parse', 'HEAD').strip()

    def git(self, *args):
        return subprocess.check_output(['git', *args], cwd=self.root, text=True,
                                       stderr=subprocess.STDOUT)

    def commit_file(self, name, content='changed\n'):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        self.git('add', name)
        self.git('commit', '-qm', 'fixture')

    def checklist(self, body=None, base=None, env_body=None):
        env = os.environ.copy()
        env.pop('PR_BODY', None)
        if env_body is not None:
            env['PR_BODY'] = env_body
        args = ['bash', str(SCRIPTS / 'distributed-checklist-check.sh'),
                '--base-ref', base or self.base]
        if body is not None:
            path = self.root / 'body.md'
            path.write_text(body)
            args += ['--pr-body-file', str(path)]
        return subprocess.run(args, cwd=self.root, env=env, capture_output=True, text=True)

    def performance(self, base=None):
        return subprocess.run(['python3', str(SCRIPTS / 'ci_change_scope.py'),
                               '--base-ref', base or self.base], cwd=self.root,
                              capture_output=True, text=True)

    def test_doc_and_test_only_changes_do_not_require_pr_body(self):
        self.commit_file('pangrova-coordinator/README.md')
        self.commit_file('pangrova-worker/src/test/java/example/Test.java')
        self.assertEqual(self.checklist().returncode, 0)
        result = self.performance()
        self.assertEqual((result.returncode, result.stdout.strip()), (0, 'performance=false'))

    def test_runtime_change_requires_pr_body(self):
        self.commit_file('pangrova-worker/src/main/java/example/Worker.java')
        self.commit_file('pangrova-worker/src/main/resources/docs/template.md')
        self.assertNotEqual(self.checklist().returncode, 0)
        self.assertEqual(self.performance().stdout.strip(), 'performance=true')

    def test_runtime_resources_and_docs_named_packages_are_not_documentation(self):
        self.commit_file('pangrova-worker/src/main/resources/docs/template.md')
        self.assertNotEqual(self.checklist().returncode, 0)
        self.commit_file('pangrova-worker/src/main/java/org/pangrova/docs/DocRenderer.java')
        self.assertNotEqual(self.checklist().returncode, 0)

    def test_configuration_change_requires_checklist(self):
        self.commit_file('pangrova-metadata/pom.xml')
        self.assertNotEqual(self.checklist().returncode, 0)

    def test_environment_body_is_preserved_and_last_section_is_read(self):
        self.commit_file('pangrova-coordinator/src/main/java/example/Coordinator.java')
        result = self.checklist(env_body='## Distributed Correctness\n- [x] Crash recovery\n')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_justified_na_is_accepted(self):
        self.commit_file('pangrova-worker/src/main/java/example/Worker.java')
        result = self.checklist('## Distributed Correctness\nN/A: Logging wording only.\n')
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_bare_na_does_not_replace_risk_evidence(self):
        self.commit_file('pangrova-worker/src/main/java/example/Worker.java')
        result = self.checklist('## Distributed Correctness\nN/A\n')
        self.assertNotEqual(result.returncode, 0)

    def test_checked_item_in_another_section_is_not_evidence(self):
        self.commit_file('pangrova-coordinator/src/main/java/example/Coordinator.java')
        body = '## Distributed Correctness\n- [ ] Crash recovery\n## Checklist\n- [x] Style\n'
        self.assertNotEqual(self.checklist(body).returncode, 0)

    def test_deleted_runtime_path_still_requires_checks(self):
        name = 'pangrova-shuffle/shuffle-core/src/main/java/example/Exchange.java'
        self.commit_file(name)
        self.base = self.git('rev-parse', 'HEAD').strip()
        self.git('rm', name)
        self.git('commit', '-qm', 'delete')
        self.assertNotEqual(self.checklist().returncode, 0)
        self.assertEqual(self.performance().stdout.strip(), 'performance=true')

    def test_missing_base_fails_instead_of_reporting_no_changes(self):
        self.assertNotEqual(self.checklist(base='missing-reference').returncode, 0)
        self.assertNotEqual(self.performance(base='missing-reference').returncode, 0)


class StructuralPolicyTest(unittest.TestCase):
    def test_readability_backstops_match_documented_policy(self):
        config = ET.parse(ROOT / 'config/checkstyle/checkstyle.xml')
        policy = (ROOT / '.pangrova/rules/code-standards.md').read_text()
        for module, maximum in [('FileLength', 800), ('MethodLength', 100),
                                ('CyclomaticComplexity', 15), ('ParameterNumber', 10)]:
            prop = config.find(f'.//module[@name="{module}"]/property[@name="max"]')
            self.assertEqual(int(prop.get('value')), maximum, module)
            self.assertIn(str(maximum), policy)

    def test_retired_checks_have_no_active_files_or_ci_calls(self):
        workflow = (ROOT / '.github/workflows/ci.yml').read_text()
        for name in ('check-bom-usage.sh', 'check-doc-staleness.sh', 'adr-grep-check.sh'):
            self.assertFalse((SCRIPTS / name).exists())
            self.assertNotIn(name, workflow)

    def test_layout_check_never_recreates_tool_adapters(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            policy = root / '.pangrova/rules'
            policy.mkdir(parents=True)
            script = root / '.pangrova/setup.sh'
            script.write_text((ROOT / '.pangrova/setup.sh').read_text())
            (root / 'AGENTS.md').write_text('Canonical entry point')
            for name in ('architecture', 'code-standards', 'development', 'testing',
                         'build-pipeline', 'ci-gates', 'workflow'):
                (policy / f'{name}.md').write_text(name)
            result = subprocess.run(['bash', str(script), '--check'], capture_output=True)
            self.assertEqual(result.returncode, 0)
            self.assertFalse((root / '.claude').exists())
            self.assertFalse((root / '.qoder').exists())
            (policy / 'testing.md').unlink()
            self.assertNotEqual(subprocess.run(['bash', str(script), '--check'],
                                              capture_output=True).returncode, 0)

    def test_nesting_and_execution_correctness_gates_remain_active(self):
        config = ET.parse(ROOT / 'config/checkstyle/checkstyle.xml')
        for module, maximum in [('NestedIfDepth', 3), ('NestedForDepth', 3), ('NestedTryDepth', 2)]:
            prop = config.find(f'.//module[@name="{module}"]/property[@name="max"]')
            self.assertEqual(int(prop.get('value')), maximum)
        workflow = (ROOT / '.github/workflows/ci.yml').read_text()
        self.assertIn('native-runtime,full-regression,coverage verify', workflow)
        self.assertIn('--threshold 60', workflow)
        self.assertEqual(workflow.count('verify -Pstatic-analysis'), 1)


if __name__ == '__main__':
    unittest.main()
