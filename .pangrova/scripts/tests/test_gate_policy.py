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
            names = ('architecture', 'code-standards', 'development', 'testing',
                     'build-pipeline', 'ci-gates')
            (root / 'AGENTS.md').write_text(' '.join(f'`{name}.md`' for name in names))
            for name in names:
                (policy / f'{name}.md').write_text(name)
            result = subprocess.run(['bash', str(script), '--check'], capture_output=True)
            self.assertEqual(result.returncode, 0)
            self.assertFalse((root / '.claude').exists())
            self.assertFalse((root / '.qoder').exists())
            (policy / 'unindexed.md').write_text('unindexed')
            result = subprocess.run(['bash', str(script), '--check'], capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn(b'unindexed.md', result.stderr)
            (policy / 'unindexed.md').unlink()
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

    def test_native_jobs_take_their_toolchain_from_the_verified_action(self):
        action = (ROOT / '.github/actions/setup-native-build/action.yml').read_text()
        self.assertIn('setup-native-build-env.sh --check-tools', action)
        self.assertIn('rustup', action)
        self.assertIn('autoconf', action)
        self.assertIn('llvm', action)
        self.assertIn('Free runner disk space', action)
        self.assertIn('libssl-dev', action)
        self.assertIn('zlib1g-dev', action)
        bootstrap = (ROOT / 'pangrova-worker-native/bin/setup-native-build-env.sh').read_text()
        self.assertIn('/usr/include/openssl/ssl.h', bootstrap)
        self.assertIn('/usr/include/zlib.h', bootstrap)
        self.assertIn('PANGROVA_CMAKE_BIN', bootstrap)
        self.assertIn('CMAKE_LAUNCHER_FLAGS', bootstrap)
        self.assertIn('ccache', action)
        self.assertIn('install_folly() {', bootstrap)
        self.assertIn(
            '-DCMAKE_PREFIX_PATH=$INSTALL_PREFIX;$libevent_prefix;$HOMEBREW_PREFIX', bootstrap
        )
        self.assertLess(bootstrap.index('install_folly() {'),
                        bootstrap.index('install_velox_deps'))
        asan = (ROOT / 'pangrova-worker-native/bin/setup-native-asan-env.sh').read_text()
        self.assertIn('PANGROVA_CMAKE_BIN', asan)
        self.assertIn('--check-tools', asan)
        self.assertIn('setup-native-asan-env.sh --check-tools',
                      (ROOT / '.github/workflows/ci.yml').read_text())
        self.assertIn('20 GB', action)
        for workflow_name in ('ci.yml', 'release.yml'):
            workflow = (ROOT / '.github/workflows' / workflow_name).read_text()
            # A whole Velox tree exceeds the 10 GB repository cache limit and a
            # cold build exceeds the runner disk; cache only the install prefixes.
            self.assertIn('~/.pangrova/velox/deps-install', workflow, workflow_name)
            self.assertIn('~/.cache/ccache', workflow, workflow_name)
            self.assertNotRegex(workflow, r'\n\s+~/\.pangrova/velox\n', workflow_name)
        for workflow_name in ('ci.yml', 'release.yml'):
            workflow = (ROOT / '.github/workflows' / workflow_name).read_text()
            matches = list(re.finditer(r'\n  [a-z][a-z0-9-]*:\n', workflow))
            jobs = [workflow[m.end():(matches[index + 1].start() if index + 1 < len(matches)
                                   else len(workflow))] for index, m in enumerate(matches)]
            bootstrapping = [job for job in jobs if 'setup-native-build-env.sh' in job]
            self.assertTrue(bootstrapping, workflow_name)
            for job in bootstrapping:
                self.assertIn('./.github/actions/setup-native-build', job, workflow_name)
                self.assertNotRegex(job, r'apt-get install|brew install cmake', workflow_name)

    def test_native_failures_publish_the_complete_log_tail(self):
        workflow = (ROOT / '.github/workflows/ci.yml').read_text()
        self.assertNotIn("awk 'length {print \"::error::\"", workflow)
        self.assertEqual(workflow.count('report_failure() {'), 6)
        self.assertEqual(workflow.count('awk \'length {printf "%s%%0A", $0}\''), 6)
        self.assertEqual(workflow.count("'--- error diagnostics ---'"), 6)
        self.assertEqual(workflow.count('bash -x'), 5)
        for marker in ('setup-native-build-env.sh 2>&1', 'setup-native-asan-env.sh 2>&1'):
            self.assertIn(marker, workflow)

    def test_asan_prepares_dependencies_without_building_release_velox(self):
        workflow = (ROOT / '.github/workflows/ci.yml').read_text()
        native = workflow.split('  native-build:\n', 1)[1].split('\n  java-build:', 1)[0]
        sanitizer = workflow.split('  native-sanitizers:\n', 1)[1].split('\n  jmh-benchmarks:', 1)[0]
        self.assertNotIn('PANGROVA_NATIVE_BUILD_PHASE:', native)
        self.assertIn("PANGROVA_NATIVE_BUILD_PHASE: ${{ matrix.sanitizer.name == 'asan' "
                      "&& 'dependencies' || 'all' }}", sanitizer)
        self.assertIn('setup-native-asan-env.sh --check-tools', sanitizer)
        self.assertIn('setup-native-asan-env.sh 2>&1', sanitizer)
        self.assertIn('run-native-tests.py --build-dir build-${{ matrix.sanitizer.name }}', sanitizer)
        bootstrap = (ROOT / 'pangrova-worker-native/bin/setup-native-build-env.sh').read_text()
        self.assertIn('dependencies|velox) build_velox ;;', bootstrap)

    def test_native_configure_build_and_tests_publish_filtered_diagnostics(self):
        workflow = (ROOT / '.github/workflows/ci.yml').read_text()
        action_path = ROOT / '.github/actions/run-native-step/action.yml'
        action = action_path.read_text()
        self.assertEqual(workflow.count('uses: ./.github/actions/run-native-step'), 6)
        self.assertIn('bash -e -o pipefail -c "$STEP_COMMAND"', action)
        self.assertIn("'--- error diagnostics ---'", action)
        self.assertIn('working-directory: ${{ inputs.working-directory }}', action)

    def test_macos_release_builds_dependencies_before_lance_and_configure(self):
        release = (ROOT / '.github/workflows/release.yml').read_text()
        native = release.split('  native-build:\n', 1)[1].split('\n  release:', 1)[0]
        steps = re.split(r'\n      - ', native)
        tools = next(index for index, step in enumerate(steps)
                     if 'uses: ./.github/actions/setup-native-build' in step)
        setup = [(index, step) for index, step in enumerate(steps)
                 if re.search(r'^\s+run: ./pangrova-worker-native/bin/'
                              r'setup-native-build-env\.sh\s*$', step, re.MULTILINE)]
        self.assertEqual(len(setup), 1, 'Release must run the full dependency bootstrap')
        index, step = setup[0]
        self.assertIn('if: "!matrix.platform.docker-build"', step)
        lance = next(index for index, step in enumerate(steps)
                     if 'run: ./pangrova-worker-native/bin/build-lance-ffi.sh' in step)
        configure = next(index for index, step in enumerate(steps)
                         if 'cmake --preset dev-release' in step)
        self.assertLess(tools, index)
        self.assertLess(index, lance)
        self.assertLess(lance, configure)

    def test_daily_tests_exclude_end_to_end_suites(self):
        pom = (ROOT / 'pom.xml').read_text()
        self.assertIn('routing-integration,native-required,performance,e2e', pom)
        self.assertIn('pangrova.e2e.excludedClasses', pom)
        self.assertIn('<exclude>${pangrova.e2e.excludedClasses}</exclude>', pom)
        self.assertIn('"none"', pom)
        base = (ROOT / 'pangrova-testing/pangrova-testing-integration-tests/src/test/java/'
                       'org/pangrova/testing/integration/e2e/SqlE2ETestBase.java').read_text()
        self.assertIn('@E2ETest', base)

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

    def test_linux_native_and_sanitizers_share_a_pinned_runner_release(self):
        platforms = {item['name']: item['runner'] for item in scope.NATIVE_PLATFORMS}
        runner = platforms['linux-x86_64']
        self.assertRegex(runner, r'^ubuntu-\d+\.\d+$')
        self.assertEqual(platforms['linux-aarch_64'], f'{runner}-arm')
        workflow = (ROOT / '.github/workflows/ci.yml').read_text()
        sanitizer = workflow.split('  native-sanitizers:\n', 1)[1].split(
            '\n  jmh-benchmarks:', 1)[0]
        self.assertIn(f'    runs-on: {runner}\n', sanitizer)

    def test_native_dependency_cache_is_scoped_to_runner_and_toolchain(self):
        workflow = (ROOT / '.github/workflows/ci.yml').read_text()
        native = workflow.split('  native-build:\n', 1)[1].split('\n  java-build:', 1)[0]
        sanitizer = workflow.split('  native-sanitizers:\n', 1)[1].split(
            '\n  jmh-benchmarks:', 1)[0]
        caches = [section.split('      - name: Cache native deps + Velox\n', 1)[1].split(
            '\n      - name:', 1)[0] for section in (native, sanitizer)]
        keys = [re.search(r'^\s+key: (.+)$', cache, re.MULTILINE).group(1)
                for cache in caches]
        runner = next(item['runner'] for item in scope.NATIVE_PLATFORMS
                      if item['name'] == 'linux-x86_64')
        self.assertEqual(keys[0].replace('${{ matrix.platform.runner }}', runner), keys[1])
        self.assertIn("hashFiles('.github/actions/setup-native-build/action.yml', "
                      "'pangrova-worker-native/bin/setup-native-build-env.sh', "
                      "'pangrova-worker-native/patches/*')", keys[0])
        for cache in caches:
            self.assertNotIn('restore-keys:', cache)

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

    def test_scheduled_benchmark_requires_provisioned_fleet(self):
        benchmark = (ROOT / '.github/workflows/nightly-tpc-benchmark.yml').read_text()
        self.assertIn("if: github.event_name != 'schedule' || vars.TPC_BENCHMARK_CONFIG_DIR != ''",
                      benchmark)

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
