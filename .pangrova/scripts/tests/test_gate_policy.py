"""Regression tests for validation selection, preserved checks and release wiring."""

import importlib.util
import json
import os
import re
import sys
import textwrap
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
                 'pangrova-testing/pangrova-testing-benchmark/baseline/jmh.json',
                 'pangrova-testing/pangrova-testing-integration-tests/src/test/java/'
                 'org/pangrova/testing/integration/e2e/performance/SqlLatencyBaselineTest.java',
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

    def selection(self, base=None):
        return subprocess.run([sys.executable, str(SCRIPTS / 'ci_change_scope.py'),
                               '--base-ref', base or self.base], cwd=self.root,
                              capture_output=True, text=True)

    def selected(self):
        result = self.selection()
        self.assertEqual(result.returncode, 0, result.stderr)
        outputs = dict(line.split('=', 1) for line in result.stdout.splitlines())
        matrix = json.loads(outputs['native_matrix'])
        self.assertEqual(len(matrix), 4 if outputs['native'] == 'true' else 1)
        return {name: outputs[name] for name in ['runtime', 'performance']}

    def test_doc_only_diff_skips_runtime_and_performance(self):
        self.commit_file('pangrova-coordination/pangrova-coordination-control/README.md')
        self.assertEqual(self.selected(), {'runtime': 'false', 'performance': 'false'})

    def test_test_only_diff_retains_correctness(self):
        self.commit_file('pangrova-worker/src/test/java/example/WorkerTest.java')
        self.assertEqual(self.selected(), {'runtime': 'true', 'performance': 'false'})

    def test_production_diff_requires_both(self):
        self.commit_file('pangrova-worker/src/main/java/example/Worker.java')
        self.assertEqual(self.selected(), {'runtime': 'true', 'performance': 'true'})

    def test_deleted_runtime_path_still_requires_checks(self):
        name = 'pangrova-shuffle/shuffle-core/src/main/java/example/Exchange.java'
        self.commit_file(name)
        self.base = self.git('rev-parse', 'HEAD').strip()
        self.git('rm', name)
        self.git('commit', '-qm', 'delete')
        self.assertEqual(self.selected(), {'runtime': 'true', 'performance': 'true'})

    def test_rename_from_runtime_to_docs_still_requires_checks(self):
        name = 'pangrova-worker/src/main/java/example/Worker.java'
        self.commit_file(name)
        self.base = self.git('rev-parse', 'HEAD').strip()
        (self.root / 'docs').mkdir()
        self.git('mv', name, 'docs/retired.md')
        self.git('commit', '-qm', 'rename')
        self.assertEqual(self.selected(), {'runtime': 'true', 'performance': 'true'})

    def test_space_in_runtime_path_is_not_split(self):
        self.commit_file('pangrova-worker/src/main/resources/data files/template.md')
        self.assertEqual(self.selected(), {'runtime': 'true', 'performance': 'true'})

    def test_missing_base_fails_instead_of_reporting_no_changes(self):
        self.assertNotEqual(self.selection(base='missing-base').returncode, 0)


class StructuralPolicyTest(unittest.TestCase):
    def test_lightweight_module_checks_always_have_jdk_before_source_parsing(self):
        workflow = (ROOT / '.github/workflows/ci.yml').read_text()
        lint = workflow.split('  lint:\n', 1)[1].split('\n  static-analysis:', 1)[0]
        steps = lint.split('      - ')
        java = next(step for step in steps if 'actions/setup-java@' in step)
        manifest = next(step for step in steps if 'check-module-manifest.sh' in step)
        tests = next(step for step in steps if 'unittest discover' in step)
        self.assertIn("java-version: '17'", java)
        self.assertNotIn('if:', java)
        self.assertNotIn('if:', manifest)
        self.assertLess(steps.index(java), steps.index(tests))
        self.assertLess(steps.index(java), steps.index(manifest))

    def test_structural_diagnostics_remain_visible_without_blocking_correctness(self):
        config = ET.parse(ROOT / 'config/checkstyle/checkstyle.xml')
        policy = (ROOT / '.pangrova/rules/code-standards.md').read_text()
        for module, maximum in [('FileLength', 800), ('MethodLength', 100),
                                ('CyclomaticComplexity', 15), ('ParameterNumber', 10)]:
            prop = config.find(f'.//module[@name="{module}"]/property[@name="max"]')
            self.assertEqual(int(prop.get('value')), maximum, module)
            self.assertIn(str(maximum), policy)
            severity = config.find(f'.//module[@name="{module}"]/property[@name="severity"]')
            self.assertEqual(severity.get("value"), "warning", module)
        pom = ET.parse(ROOT / "pom.xml")
        ns = {"m": "http://maven.apache.org/POM/4.0.0"}
        self.assertEqual(pom.find(".//m:violationSeverity", ns).text, "error")

    def test_retired_checks_have_no_active_files_or_ci_calls(self):
        workflow = (ROOT / '.github/workflows/ci.yml').read_text()
        for name in ('check-bom-usage.sh', 'check-doc-staleness.sh', 'adr-grep-check.sh',
                     'plan-stability-check.sh', 'distributed-checklist-check.sh'):
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
        self.assertRegex(workflow, r'native-runtime,full-regression verify')
        self.assertNotIn('-Pcoverage', workflow)
        self.assertNotIn('diff-coverage-check.sh', workflow)
        self.assertEqual(workflow.count('verify -Pstatic-analysis'), 1)

    def test_production_regression_retains_same_source_image_identity(self):
        workflow = (ROOT / '.github/workflows/ci.yml').read_text()
        production = workflow.split('  production-regression:\n', 1)[1].split(
            '\n  tpch-performance:', 1)[0]
        normalized = re.sub(r'\s+', ' ', production.replace('\\\n', ' '))
        self.assertIn('IMAGE="$PANGROVA_E2E_IMAGE" SOURCE_COMMIT="$GITHUB_SHA"', normalized)
        self.assertIn('docker buildx bake --load', normalized)
        self.assertIn('docker image inspect --format', production)
        runtime_stage = (ROOT / 'deploy/docker/Dockerfile').read_text().rsplit('\nFROM ', 1)[1]
        self.assertIn('ARG SOURCE_COMMIT\n', runtime_stage)
        self.assertIn('org.opencontainers.image.revision="${SOURCE_COMMIT}"', runtime_stage)
        self.assertIn('"PANGROVA_E2E_IMAGE_ID=$image_id"', production)
        self.assertIn('stage-production-regression-native.sh "$PANGROVA_E2E_IMAGE_ID"', normalized)
        self.assertNotIn('pangrova/pangrova:latest', production)
        for property_name, variable in [('pangrova.e2e.image', 'PANGROVA_E2E_IMAGE'),
                                        ('pangrova.e2e.image-id', 'PANGROVA_E2E_IMAGE_ID')]:
            # Full regression and the isolated owner suites both require this identity.
            self.assertEqual(production.count(f'-D{property_name}="${variable}"'), 2)

    def test_local_file_acceptance_runs_in_its_own_native_process(self):
        workflow = (ROOT / '.github/workflows/ci.yml').read_text()
        production = workflow.split('  production-regression:\n', 1)[1].split(
            '\n  tpch-performance:', 1)[0]
        owners = production.split('for suite in ', 1)[1].split('\n          done', 1)[0]
        self.assertIn('LocalGovernedFileSqlE2ETest; do', owners)
        self.assertIn('file_backend=s3', owners)
        self.assertIn('if [[ "$suite" == LocalGovernedFileSqlE2ETest ]]; '
                      'then file_backend=local; fi', owners)
        self.assertIn('-Dpangrova.e2e.file.backend="$file_backend"', owners)
        self.assertIn('-Dpangrova.e2e.excludedGroups=none -Dtest="$suite"', owners)


class RuntimeScopeTest(unittest.TestCase):
    def test_native_checks_follow_native_inputs_and_java_boundary_owners(self):
        for path in ['pangrova-worker-native/src/executor/Executor.cpp',
                     'pangrova-worker/src/main/java/example/NativeRuntime.java',
                     'pangrova-protocol/src/main/proto/task.proto',
                     'pangrova-native-bundle/pom.xml', 'pom.xml',
                     '.github/workflows/ci.yml', 'unknown-build-input.lock']:
            with self.subTest(path=path):
                self.assertTrue(scope.needs_native_checks([path]))
        for path in ['docs/architecture/overview.md',
                     'pangrova-planner/src/main/java/example/Planner.java',
                     'pangrova-parser/src/test/java/example/ParserTest.java',
                     'pangrova-indexing/pangrova-indexing-api/src/main/java/example/Contract.java',
                     'pangrova-client/client-cli/src/main/java/example/Cli.java']:
            with self.subTest(path=path):
                self.assertFalse(scope.needs_native_checks([path]))

    def test_full_event_selects_all_platforms_without_a_diff(self):
        result = subprocess.run([sys.executable, str(SCRIPTS / 'ci_change_scope.py'),
                                 '--full'], capture_output=True, text=True, check=True)
        outputs = dict(line.split('=', 1) for line in result.stdout.splitlines())
        self.assertEqual(outputs['runtime'], 'true')
        self.assertEqual(outputs['native'], 'true')
        self.assertEqual(outputs['performance'], 'false')
        matrix = json.loads(outputs['native_matrix'])
        self.assertEqual({item['classifier'] for item in matrix},
                         {'linux-x86_64', 'linux-aarch_64', 'osx-x86_64', 'osx-aarch_64'})
        self.assertEqual(matrix[0]['name'], 'linux-x86_64')

    def test_docs_policy_website_and_gate_tools_use_lightweight_checks(self):
        paths = ['AGENTS.md', '.pangrova/rules/testing.md', '.pangrova/setup.sh',
                 'docs/architecture/overview.md', 'pangrova-worker-native/README.md',
                 'website/src/pages/index.tsx', 'website/pnpm-lock.yaml',
                 '.pangrova/scripts/check-spi-annotations.sh',
                 '.pangrova/scripts/tests/test_gate_policy.py',
                 '.github/workflows/deploy-website.yml', '.github/ISSUE_TEMPLATE/bug.yml']
        for path in paths:
            with self.subTest(path=path):
                self.assertFalse(scope.needs_runtime([path]))
                self.assertFalse(scope.needs_performance([path]))

    def test_runtime_tests_resources_dependencies_and_unknown_inputs_are_conservative(self):
        paths = ['pangrova-worker-native/test/ExampleTest.cpp',
                 'pangrova-worker/src/main/resources/docs/template.md',
                 'pangrova-worker/src/test/resources/docs/fixture.md',
                 'pangrova-client/pangrova-client-cli/pom.xml',
                 '.pangrova/scripts/proto-descriptors/canonical-v2.descriptor',
                 '.pangrova/scripts/stage-production-regression-native.sh',
                 '.pangrova/scripts/build-iceberg-sdk-candidate.sh',
                 '.pangrova/scripts/ci_change_scope.py', '.pangrova/modules.yaml',
                 '.github/workflows/ci.yml', '.github/workflows/release.yml',
                 '.github/actions/setup-iceberg-sdk/action.yml',
                 '.github/actions/new-runtime-action/action.yml',
                 '.github/workflows/new-runtime-build.yml',
                 'config/checkstyle/checkstyle.xml', 'new-build-input.lock']
        for path in paths:
            with self.subTest(path=path):
                self.assertTrue(scope.needs_runtime([path]))
        self.assertTrue(scope.needs_runtime(['README.md', 'new-build-input.lock']))
        self.assertFalse(scope.needs_runtime([]))


class WorkflowPolicyTest(unittest.TestCase):
    def setUp(self):
        self.workflow = (ROOT / '.github/workflows/ci.yml').read_text()

    def test_golden_and_wiring_tests_stay_in_selected_maven_suites(self):
        pom = ET.parse(ROOT / 'pom.xml')
        ns = {'m': 'http://maven.apache.org/POM/4.0.0'}
        surefire = pom.find(".//m:plugin[m:artifactId='maven-surefire-plugin']", ns)
        self.assertIn('**/*Test.java', [e.text for e in surefire.findall('.//m:include', ns)])
        self.assertNotIn('plan-stability', (ROOT / 'pangrova-planner/pom.xml').read_text())
        for name in ['TpcdsPlanStabilityTest', 'TpchPlanStabilityTest']:
            source = (ROOT / 'pangrova-planner/src/test/java/org/pangrova/planner/analysis' / f'{name}.java').read_text()
            self.assertIn('.isRegularFile()', source)
            self.assertIn('.isEqualTo(expectedPlan)', source)
        for name in ['SpiWiringVerificationTest', 'BootstrapReachabilityTest']:
            source = (ROOT / 'pangrova-testing/pangrova-testing-integration-tests/src/test/java/'
                      'org/pangrova/testing/integration/e2e/arch' / f'{name}.java').read_text()
            self.assertNotIn('@Tag(', source)
        e2e = (ROOT / 'pangrova-testing/pangrova-testing-integration-tests/pom.xml').read_text()
        self.assertNotIn('<exclude>', e2e)
        smoke = ROOT / ('pangrova-testing/pangrova-testing-integration-tests/src/test/java/'
                        'org/pangrova/testing/integration/e2e/smoke/ActivationSmokeE2ETest.java')
        self.assertTrue(smoke.is_file())
        self.assertNotIn('Run full E2E and real activation smoke', self.workflow)

    def test_release_waits_for_reusable_validation_and_artifacts_do_not_collide(self):
        release = (ROOT / '.github/workflows/release.yml').read_text()
        self.assertIn('workflow_call:', self.workflow)
        self.assertIn('uses: ./.github/workflows/ci.yml', release)
        self.assertIn('needs: [native-build, validation]', release)
        self.assertIn('name: release-native-bundle-', release)
        self.assertNotIn('runner: macos-13', self.workflow + release)

    def test_reference_generator_is_manual_and_no_longer_a_pr_engine_gate(self):
        reference = (ROOT / '.github/workflows/nightly-operator-fuzzer.yml').read_text()
        self.assertIn('workflow_dispatch:', reference)
        self.assertNotIn('schedule:', reference)
        self.assertNotIn('  fuzzer:', self.workflow)

    def result(self, runtime='false', performance='false', native=None, pr=True, overrides=None):
        # Execute the workflow's actual final-check program with synthetic GitHub
        # results. This verifies skip/failure semantics without copying its logic.
        final_job = self.workflow.split('  validation:\n', 1)[1]
        program = textwrap.dedent(final_job.split("python3 - <<'PY'\n", 1)[1].rsplit('          PY', 1)[0])
        jobs = {name: {'result': 'success'} for name in
                ['change-scope', 'lint', 'doc-consistency', 'dead-stub-detection',
                 'static-analysis', 'native-build', 'java-build', 'native-sanitizers',
                 'production-regression', 'tpch-performance', 'jmh-benchmarks']}
        jobs['change-scope']['outputs'] = {'runtime': runtime, 'performance': performance,
                                         'native': runtime if native is None else native}
        for name, result in (overrides or {}).items():
            jobs[name]['result'] = result
        return subprocess.run([sys.executable, '-c', program], capture_output=True,
                              text=True, env={**os.environ, 'RESULTS': json.dumps(jobs),
                                              'PR_EVENT': str(pr).lower()})

    def test_docs_can_skip_only_unselected_checks(self):
        result = self.result(overrides={'native-build': 'skipped', 'java-build': 'skipped',
                                       'production-regression': 'skipped'})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotEqual(self.result(overrides={'lint': 'skipped'}).returncode, 0)

    def test_selected_checks_cannot_fail_cancel_or_disappear(self):
        for name in ['native-build', 'java-build', 'native-sanitizers', 'production-regression']:
            for outcome in ['failure', 'cancelled', 'skipped']:
                with self.subTest(name=name, outcome=outcome):
                    self.assertNotEqual(self.result(runtime='true', overrides={name: outcome}).returncode, 0)
        self.assertNotEqual(self.result(runtime='true', performance='true',
                                       overrides={'tpch-performance': 'skipped'}).returncode, 0)
        self.assertNotEqual(self.result(runtime='', pr=True).returncode, 0)
        self.assertNotEqual(self.result(performance='', pr=True).returncode, 0)
        self.assertNotEqual(self.result(runtime='false', performance='true').returncode, 0)

    def test_pushes_and_release_calls_cannot_select_docs_only_validation(self):
        self.assertNotEqual(self.result(runtime='false', pr=False).returncode, 0)
        self.assertEqual(self.result(runtime='true', pr=False).returncode, 0)
        self.assertNotEqual(self.result(runtime='true', native='false', pr=False).returncode, 0)

    def test_java_only_changes_may_skip_standalone_native_sanitizers(self):
        result = self.result(runtime='true', native='false',
                             overrides={'native-sanitizers': 'skipped'})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotEqual(self.result(runtime='true', native='false',
                                       overrides={'production-regression': 'skipped'}).returncode, 0)
        self.assertNotEqual(self.result(runtime='true', native='').returncode, 0)


if __name__ == '__main__':
    unittest.main()
