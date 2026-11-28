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
WORKFLOWS = ROOT / '.github/workflows'
SANITIZER_WORKFLOW = WORKFLOWS / 'native-sanitizer.yml'
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
        matrix = [json.loads(outputs['linux_native']), *json.loads(outputs['native_matrix'])]
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

    def test_stacked_pr_uses_target_branch_merge_base_for_scope(self):
        self.git('checkout', '-qb', 'feature-base')
        self.commit_file('pangrova-worker-native/src/example/Parent.cpp')
        parent = self.git('rev-parse', 'HEAD').strip()
        self.git('checkout', '-qb', 'feature-child')
        self.commit_file('docs/child.md')
        # The parent commit is part of the target branch, not the child's delta.
        child = self.selection(base=parent)
        self.assertEqual(child.returncode, 0, child.stderr)
        outputs = dict(line.split('=', 1) for line in child.stdout.splitlines())
        self.assertEqual([outputs[key] for key in ('runtime', 'native', 'performance')],
                         ['false', 'false', 'false'])
        # Against main the cumulative stack still requires the full matrix.
        cumulative = self.selection(base=self.base)
        self.assertEqual(cumulative.returncode, 0, cumulative.stderr)
        outputs = dict(line.split('=', 1) for line in cumulative.stdout.splitlines())
        self.assertEqual([outputs[key] for key in ('runtime', 'native', 'performance')],
                         ['true', 'true', 'true'])
        # A child runtime change must retain its existing correctness chain.
        self.commit_file('pangrova-query/src/main/java/example/Child.java')
        child = self.selection(base=parent)
        self.assertEqual(child.returncode, 0, child.stderr)
        outputs = dict(line.split('=', 1) for line in child.stdout.splitlines())
        self.assertEqual([outputs[key] for key in ('runtime', 'native', 'performance')],
                         ['true', 'false', 'true'])

    def test_missing_base_fails_instead_of_reporting_no_changes(self):
        self.assertNotEqual(self.selection(base='missing-base').returncode, 0)


class StructuralPolicyTest(unittest.TestCase):
    def test_ci_runs_stack_targets_as_ordinary_pull_requests(self):
        workflow = (ROOT / '.github/workflows/ci.yml').read_text()
        events = workflow.split('\non:\n', 1)[1].split('\nconcurrency:', 1)[0]
        self.assertIn('  workflow_call:', events)
        self.assertIn('  push:\n    branches: [main, develop]', events)
        # Do not restrict PR target branches; stacks can target any feature branch.
        pull_request = events.split('  pull_request:', 1)[1]
        self.assertFalse(re.search(r'^    (branches|branches-ignore|paths|paths-ignore):',
                                   pull_request, re.MULTILINE))
        self.assertNotIn('pull_request_target:', events)
        self.assertIn("if: github.event_name == 'pull_request'", workflow)
        self.assertIn('BASE_SHA: ${{ github.event.pull_request.base.sha }}', workflow)
        self.assertIn('HEAD_SHA: ${{ github.event.pull_request.head.sha }}', workflow)
        self.assertIn('--base-ref "$BASE_SHA" --head-ref "$HEAD_SHA"', workflow)
        self.assertIn('--full >> "$GITHUB_OUTPUT"', workflow)

    def test_only_pull_request_ci_cancels_an_in_progress_run(self):
        workflow = (ROOT / '.github/workflows/ci.yml').read_text()
        concurrency = workflow.split('\nconcurrency:\n', 1)[1].split('\n\n', 1)[0]
        self.assertIn('group: ${{ github.workflow }}-${{ github.ref }}', concurrency)
        self.assertIn("cancel-in-progress: ${{ github.event_name == 'pull_request' }}",
                      concurrency)

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
                      SANITIZER_WORKFLOW.read_text())
        self.assertIn('20 GB', action)
        for workflow_name in ('native-sanitizer.yml', 'native-platform.yml', 'release.yml'):
            workflow = (ROOT / '.github/workflows' / workflow_name).read_text()
            # A whole Velox tree exceeds the 10 GB repository cache limit and a
            # cold build exceeds the runner disk; cache only the install prefixes.
            self.assertIn('~/.pangrova/velox/deps-install', workflow, workflow_name)
            self.assertIn('~/.cache/ccache', workflow, workflow_name)
            self.assertNotRegex(workflow, r'\n\s+~/\.pangrova/velox\n', workflow_name)
        for workflow_name in ('native-sanitizer.yml', 'native-platform.yml', 'release.yml'):
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
        workflow = ((ROOT / '.github/workflows/ci.yml').read_text() +
                    (ROOT / '.github/workflows/native-platform.yml').read_text())
        self.assertNotIn("awk 'length {print \"::error::\"", workflow)
        self.assertEqual(workflow.count('report_failure() {'), 3)
        self.assertEqual(workflow.count('awk \'length {printf "%s%%0A", $0}\''), 3)
        self.assertEqual(workflow.count("'--- error diagnostics ---'"), 3)
        self.assertIn('setup-native-build-env.sh 2>&1', workflow)

    def test_asan_prepares_dependencies_without_building_release_velox(self):
        native = (WORKFLOWS / 'native-platform.yml').read_text().split('  native-build:\n', 1)[1]
        producer, consumer = SANITIZER_WORKFLOW.read_text().split('  native-tests:\n', 1)
        self.assertNotIn('PANGROVA_NATIVE_BUILD_PHASE:', native)
        self.assertIn('command: PANGROVA_NATIVE_BUILD_PHASE=dependencies bash -x', producer)
        self.assertIn('setup-native-asan-env.sh --check-tools', producer)
        self.assertIn('command: PANGROVA_ASAN_CI_PRUNE_RELEASE=1 bash -x', producer)
        self.assertIn('run-native-tests.py --build-dir build-${{ inputs.name }}', consumer)
        bootstrap = (ROOT / 'pangrova-worker-native/bin/setup-native-build-env.sh').read_text()
        self.assertIn('dependencies|velox) build_velox ;;', bootstrap)

    def test_asan_stages_require_verified_same_run_dependencies(self):
        caller = (WORKFLOWS / 'ci.yml').read_text().split('  native-sanitizers:\n', 1)[1]
        caller = caller.split('  jmh-benchmarks:\n', 1)[0]
        self.assertIn('uses: ./.github/workflows/native-sanitizer.yml', caller)
        producer, consumer = SANITIZER_WORKFLOW.read_text().split('  native-tests:\n', 1)
        self.assertIn("if: inputs.name == 'asan'", producer)
        self.assertIn('linux-asan-stage.sh pack', producer)
        self.assertIn('if-no-files-found: error', producer)
        self.assertNotIn('prepare-native-java-fixture', producer)
        self.assertIn('needs: asan-dependencies', consumer)
        self.assertIn("inputs.name == 'asan' && needs.asan-dependencies.result == 'success'", consumer)
        self.assertIn("inputs.name == 'ubsan' && needs.asan-dependencies.result == 'skipped'", consumer)
        self.assertIn('digest-mismatch: error', consumer)
        self.assertLess(consumer.index('linux-asan-stage.sh restore'), consumer.index('CMake configure'))
        self.assertIn('name: linux-asan-dependencies', producer)
        self.assertIn('name: linux-asan-dependencies', consumer)

    def test_native_configure_build_and_tests_publish_filtered_diagnostics(self):
        platform = (WORKFLOWS / 'native-platform.yml').read_text()
        sanitizer = SANITIZER_WORKFLOW.read_text()
        action_path = ROOT / '.github/actions/run-native-step/action.yml'
        action = action_path.read_text()
        self.assertEqual(platform.count('uses: ./.github/actions/run-native-step'), 5)
        for log in ('native-setup', 'native-asan-setup', 'native-asan-restore',
                    'native-lance', 'native-sanitizer-configure', 'native-sanitizer-build',
                    'native-sanitizer-tests'):
            self.assertRegex(sanitizer, rf'uses: ./\.github/actions/run-native-step\s+with:\s+log: {log}\b')
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
        image = workflow.split('  production-image:\n', 1)[1].split('\n  production-regression:', 1)[0]
        normalized = re.sub(r'\s+', ' ', production.replace('\\\n', ' '))
        self.assertIn('IMAGE="$PANGROVA_E2E_IMAGE" SOURCE_COMMIT="$GITHUB_SHA"', image)
        self.assertIn('docker buildx bake --load', image)
        self.assertIn('--set native-build.args.NATIVE_BUILD_JOBS=2', image)
        self.assertIn('docker image save "$PANGROVA_E2E_IMAGE"', image)
        self.assertIn('if-no-files-found: error', image)
        self.assertNotIn('docker buildx', production)
        self.assertIn('docker image load --input', production)
        self.assertIn('digest-mismatch: error', production)
        self.assertIn('needs.production-image.outputs.artifact', production)
        self.assertIn('needs.production-image.outputs.image-id', production)
        self.assertIn('needs.production-image.outputs.image ', production)
        self.assertIn('[[ "$image_id" != "$EXPECTED_IMAGE_ID" || "$revision" != "$GITHUB_SHA" ]]', production)
        self.assertIn("needs.production-image.result == 'success'", production)
        self.assertIn('org.opencontainers.image.revision', production)
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

    def test_production_cache_uses_the_canonical_dependency_stage_and_trusted_writer(self):
        workflow = (WORKFLOWS / 'ci.yml').read_text()
        image = workflow.split('  production-image:\n', 1)[1].split('  production-regression:', 1)[0]
        publisher = (WORKFLOWS / 'native-production-cache.yml').read_text()
        cache_tag = 'build-cache:linux-amd64-v1'
        self.assertIn(cache_tag, image)
        self.assertIn(cache_tag, publisher)
        self.assertIn('packages: read', image)
        self.assertNotIn('cache-to', image)
        self.assertIn('native-build.cache-from=type=registry', image)
        self.assertIn("if: github.ref == 'refs/heads/main'", publisher)
        self.assertIn('packages: write', publisher)
        self.assertIn('native-production-dependencies.args.NATIVE_BUILD_JOBS=2', publisher)
        self.assertIn('native-production-dependencies.cache-to=type=registry', publisher)
        bake = (ROOT / 'deploy/docker/docker-bake.hcl').read_text()
        target = bake.split('target "native-production-dependencies" {', 1)[1].split('}', 1)[0]
        self.assertIn('inherits = ["native-build"]', target)
        self.assertIn('target = "builder"', target)
        self.assertIn('output = ["type=cacheonly"]', target)

    def test_java_jobs_only_wait_for_the_native_platform_that_produces_their_fixtures(self):
        workflow = (WORKFLOWS / 'ci.yml').read_text()
        jobs = dict(re.findall(r'^  ([\w-]+):\n(.*?)(?=^  [\w-]+:\n|\Z)',
                               workflow.split('\njobs:\n', 1)[1], re.MULTILINE | re.DOTALL))
        self.assertIn("needs.change-scope.outputs.linux_native", jobs['linux-native'])
        self.assertIn("needs.change-scope.outputs.native == 'true'", jobs['native-build'])
        self.assertIn('needs: [linux-native, iceberg-sdk]', jobs['java-build'])
        regression = jobs['production-regression']
        self.assertIn('needs: [change-scope, iceberg-sdk, linux-native, production-image]', regression)
        self.assertIn("needs.linux-native.result == 'success'", regression)
        self.assertNotIn('needs.native-sanitizers', regression)
        self.assertIn("'native-build', 'native-sanitizers'", jobs['validation'])

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

    def test_production_consumers_install_native_classifier_as_jar(self):
        for path in ('.github/workflows/ci.yml', 'deploy/docker/Dockerfile'):
            with self.subTest(path=path):
                source = (ROOT / path).read_text().replace('\\\n', ' ')
                install = next(line for line in source.splitlines()
                               if 'install:install-file' in line and
                               '-DpomFile=pangrova-native-bundle/pom.xml' in line)
                self.assertIn('-Dclassifier=', install)
                self.assertIn('-Dpackaging=jar', install)


class RuntimeScopeTest(unittest.TestCase):
    def test_native_fixture_bootstrap_selects_native_checks(self):
        for path in ('.pangrova/scripts/setup-macos-native-test-docker.sh',
                     '.pangrova/scripts/setup-macos-native-test-docker.yaml',
                     '.pangrova/scripts/linux-asan-stage.sh'):
            with self.subTest(path=path):
                self.assertTrue(scope.needs_runtime([path]))
                self.assertTrue(scope.needs_native_checks([path]))
                self.assertFalse(scope.needs_performance([path]))

    def test_native_checks_follow_native_inputs_and_java_boundary_owners(self):
        for path in ['pangrova-worker-native/src/executor/Executor.cpp',
                     'pangrova-worker/src/main/java/example/NativeRuntime.java',
                     'pangrova-protocol/src/main/proto/task.proto',
                     'pangrova-native-bundle/pom.xml', 'pom.xml',
                     '.mvn/ci-settings.xml',
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
        matrix = [json.loads(outputs['linux_native']), *json.loads(outputs['native_matrix'])]
        self.assertEqual({item['classifier'] for item in matrix},
                         {'linux-x86_64', 'linux-aarch_64', 'osx-x86_64', 'osx-aarch_64'})
        self.assertEqual(matrix[0]['name'], 'linux-x86_64')

    def test_linux_native_and_sanitizers_share_a_pinned_runner_release(self):
        platforms = {item['name']: item['runner'] for item in scope.NATIVE_PLATFORMS}
        runner = platforms['linux-x86_64']
        self.assertRegex(runner, r'^ubuntu-\d+\.\d+$')
        self.assertEqual(platforms['linux-aarch_64'], f'{runner}-arm')
        self.assertEqual(SANITIZER_WORKFLOW.read_text().count(f'    runs-on: {runner}\n'), 2)

    def test_native_fixture_jobs_prepare_java_before_building(self):
        action = (ROOT / '.github/actions/prepare-native-java-fixture/action.yml').read_text()
        steps = ('actions/cache/restore@', 'uses: ./.github/actions/restore-iceberg-sdk',
                 '--prepare-java ', 'actions/cache/save@')
        positions = [action.index(step) for step in steps]
        self.assertEqual(positions, sorted(positions))
        native = (WORKFLOWS / 'native-platform.yml').read_text().split('  native-build:\n', 1)[1]
        sanitizer = SANITIZER_WORKFLOW.read_text().split('  native-tests:\n', 1)[1]
        for start, job in (('native-build', native), ('native-sanitizers', sanitizer)):
            with self.subTest(job=start):
                self.assertEqual(job.count('actions/setup-java@'), 1)
                self.assertLess(job.index('uses: ./.github/actions/prepare-native-java-fixture'),
                                job.index('Setup native build environment'))
                self.assertIn('--prepared-java "$RUNNER_TEMP/native-java-fixture"', job)

    def test_runtime_validation_shares_the_tested_iceberg_sdk(self):
        workflow = (ROOT / '.github/workflows/ci.yml').read_text()
        jobs = dict(re.findall(r'^  ([\w-]+):\n(.*?)(?=^  [\w-]+:\n|\Z)',
                               workflow.split('\njobs:\n', 1)[1], re.MULTILINE | re.DOTALL))
        producer = jobs['iceberg-sdk']
        self.assertIn("if: needs.change-scope.outputs.runtime == 'true'", producer)
        self.assertLess(producer.index('/setup-iceberg-sdk'), producer.index('actions/upload-artifact@'))
        self.assertEqual(workflow.count('uses: ./.github/actions/setup-iceberg-sdk'), 1)
        self.assertIn('name: pinned-iceberg-sdk', producer)
        self.assertIn('if-no-files-found: error', producer)
        for name in ('linux-native', 'native-build', 'native-sanitizers', 'java-build', 'static-analysis',
                     'production-image', 'production-regression', 'jmh-benchmarks', 'validation'):
            with self.subTest(name=name):
                self.assertIn('iceberg-sdk', re.search(r'^    needs: .*', jobs[name], re.MULTILINE)[0])
        restore = (ROOT / '.github/actions/restore-iceberg-sdk/action.yml').read_text()
        self.assertIn('name: pinned-iceberg-sdk', restore)
        self.assertIn('digest-mismatch: error', restore)
        self.assertIn('pangrova.contexts.iceberg-sdk=$RUNNER_TEMP/pangrova-iceberg-sdk',
                      jobs['production-image'])
        self.assertIn('--allow "fs.read=$RUNNER_TEMP/pangrova-iceberg-sdk"',
                      jobs['production-image'])
        self.assertNotIn('BUILDX_BAKE_ENTITLEMENTS_FS', jobs['production-image'])
        self.assertIn("needs.iceberg-sdk.result == 'success'", jobs['production-regression'])

    def test_hosted_maven_builds_use_one_checksum_enforced_central_mirror(self):
        settings = ET.parse(ROOT / '.mvn/ci-settings.xml').getroot()
        mirrors = settings.findall('{*}mirrors/{*}mirror')
        self.assertEqual(len(mirrors), 1)
        self.assertEqual(mirrors[0].findtext('{*}mirrorOf'), 'central')
        self.assertEqual(mirrors[0].findtext('{*}url'),
                         'https://maven-central.storage-download.googleapis.com/maven2/')
        for workflow in ('ci', 'native-platform', 'native-sanitizer', 'release'):
            contents = (WORKFLOWS / f'{workflow}.yml').read_text()
            self.assertIn('MAVEN_ARGS: -C --global-settings ${{ github.workspace }}/.mvn/ci-settings.xml',
                          contents, workflow)
        ci = (WORKFLOWS / 'ci.yml').read_text()
        self.assertIn('pangrova.args.MAVEN_ARGS=-C --global-settings /build/.mvn/ci-settings.xml', ci)
        dockerfile = (ROOT / 'deploy/docker/Dockerfile').read_text()
        builder, runtime = dockerfile.split('FROM java-build-base AS builder', 1)[1].rsplit('\nFROM ', 1)
        self.assertLess(builder.index('ARG MAVEN_ARGS'), builder.index('RUN --mount=type=cache,target=/root/.m2'))
        self.assertNotIn('MAVEN_ARGS', runtime)

    def test_pinned_iceberg_sdk_uses_bounded_gradle_repository_backoff(self):
        script = (ROOT / '.pangrova/scripts/build-iceberg-sdk-candidate.sh').read_text()
        # Gradle 8.14.4 dependency resolution has its own retry settings;
        # the Maven Resolver configuration does not cover these invocations.
        self.assertIn('-Dorg.gradle.internal.repository.max.tentatives=3', script)
        self.assertIn('-Dorg.gradle.internal.repository.initial.backoff=30000', script)
        self.assertEqual(script.count('"$gradle_bin" "${gradle_repository_args[@]}"'), 2)
        self.assertIn('--no-daemon --max-workers=2', script)
        self.assertIn(':iceberg-core:test --tests org.apache.iceberg.avro.TestFixedByteBufferWriter', script)
        for module in ('api', 'bundled-guava', 'common', 'core', 'orc', 'parquet'):
            self.assertEqual(script.count(f':iceberg-{module}:publishToMavenLocal'), 1)

    def test_native_fixture_uses_bounded_maven_transport_backoff(self):
        action = (ROOT / '.github/actions/prepare-native-java-fixture/action.yml').read_text()
        self.assertIn('-Daether.connector.basic.threads=2', action)
        self.assertIn('-Daether.connector.http.retryHandler.count=3', action)
        self.assertIn('-Daether.connector.http.retryHandler.interval=30000', action)
        self.assertIn('-Daether.connector.http.retryHandler.intervalMax=120000', action)
        self.assertEqual(action.count('--prepare-java '), 1)
        self.assertNotIn('continue-on-error', action)

    @staticmethod
    def _cache_steps(section):
        steps = re.split(r'\n      - name: ', section)[1:]
        return {step.split('\n', 1)[0].strip(): step for step in steps
                if 'actions/cache' in step}

    def test_native_dependency_cache_is_layered_and_scoped(self):
        native = (WORKFLOWS / 'native-platform.yml').read_text().split('  native-build:\n', 1)[1]
        asan_deps, sanitizer = SANITIZER_WORKFLOW.read_text().split('  native-tests:\n', 1)
        runner = next(item['runner'] for item in scope.NATIVE_PLATFORMS
                      if item['name'] == 'linux-x86_64')
        native_caches = self._cache_steps(native)
        sanitizer_caches = self._cache_steps(sanitizer)
        deps = native_caches['Cache native dependency prefixes']
        deps_key = re.search(r'^\s+key: (.+)$', deps, re.MULTILINE).group(1)
        self.assertEqual(
            deps_key.replace('${{ fromJSON(inputs.platform).runner }}', runner),
            re.search(r'^\s+key: (.+)$', sanitizer_caches[
                'Cache native dependency prefixes'], re.MULTILINE).group(1))
        self.assertIn("hashFiles('pangrova-worker-native/dependency-versions.txt', "
                      "'.github/actions/setup-native-build/action.yml', "
                      "'pangrova-worker-native/patches/*')", deps_key)
        self.assertNotIn('setup-native-build-env.sh', deps_key)
        for name in ('Cache dependency archives', 'Cache native dependency prefixes',
                     'Cache Velox build layer'):
            self.assertNotIn('restore-keys:', native_caches[name])
        velox = native_caches['Cache Velox build layer']
        for path in ('_build/release/_deps', '_build/release/lib', 'CMakeCache.txt'):
            self.assertIn(path, velox)
        self.assertIn("'pangrova-worker-native/patches/velox-*.patch'", velox)
        self.assertNotIn("'pangrova-worker-native/patches/*'", velox)
        ccache = native_caches['Cache compiler results']
        self.assertIn('${{ github.sha }}', ccache)
        self.assertIn('restore-keys:', ccache)
        self.assertNotIn('patches/*', ccache)
        self.assertIn('inputs.name', sanitizer_caches['Cache compiler results'])
        self.assertIn('-asan-deps-', self._cache_steps(asan_deps)['Cache compiler results'])

    def test_native_platform_stage_names_do_not_depend_on_skipped_input_evaluation(self):
        workflow = (ROOT / '.github/workflows/ci.yml').read_text()
        platform_workflow = (ROOT / '.github/workflows/native-platform.yml').read_text()
        self.assertIn('name: Native Build (${{ matrix.platform.name }})', workflow)
        for job, name in (('macos-native-core', 'macOS Native prerequisites'),
                          ('macos-native-sdk', 'macOS Native SDK'),
                          ('native-build', 'Build and test')):
            with self.subTest(job=job):
                # The caller already names the platform. Skipped child jobs can
                # be displayed before their input expressions are evaluated.
                self.assertIn(f'  {job}:\n    name: {name}\n', platform_workflow)

    def test_macos_cold_build_stages_preserve_native_test_and_report_gates(self):
        workflow = (ROOT / '.github/workflows/ci.yml').read_text()
        platform_workflow = (ROOT / '.github/workflows/native-platform.yml').read_text()
        core = platform_workflow.split('  macos-native-core:\n', 1)[1].split('  macos-native-sdk:', 1)[0]
        sdk = platform_workflow.split('  macos-native-sdk:\n', 1)[1].split('  native-build:', 1)[0]
        native = (ROOT / '.github/workflows/native-platform.yml').read_text().split('  native-build:\n', 1)[1]
        self.assertIn('uses: ./.github/workflows/native-platform.yml', workflow)
        self.assertIn('platform: ${{ toJSON(matrix.platform) }}', workflow)
        self.assertIn('fail-fast: false', workflow)
        for stage in (core, sdk):
            self.assertIn("fromJSON(inputs.platform).os == 'osx'", stage)
            self.assertIn('runs-on: ${{ fromJSON(inputs.platform).runner }}', stage)
            self.assertIn('if-no-files-found: error', stage)
            self.assertIn('./.github/actions/setup-native-build', stage)
            self.assertIn('./.github/actions/setup-macos-native-cache', stage)
        self.assertIn('PANGROVA_NATIVE_BUILD_PHASE=dependencies bash pangrova-worker-native/bin/setup-native-build-env.sh', core)
        self.assertIn('macos-native-stage.sh pack core', core)
        self.assertLess(core.index('Build canonical prerequisite packages'),
                        core.index('actions/cache/save@'))
        self.assertLess(sdk.index('Build Velox and Arrow Flight from verified prerequisites'),
                        sdk.index('actions/cache/save@'))
        self.assertLess(native.index('Save completed macOS Native compiler results'),
                        native.index('Run native tests (GTest via CTest)'))
        self.assertIn('needs: macos-native-core', sdk)
        self.assertLess(sdk.index('restore core'), sdk.index('SKIP_DEPS=1 PANGROVA_NATIVE_BUILD_PHASE=prepared-velox'))
        self.assertIn('macos-native-stage.sh pack sdk', sdk)
        self.assertIn('needs: [macos-native-core, macos-native-sdk]', native)
        self.assertIn("fromJSON(inputs.platform).os == 'linux'", native)
        self.assertIn("needs.macos-native-sdk.result == 'success'", native)
        self.assertLess(native.index('restore sdk'), native.index('export SKIP_DEPS=1 SKIP_VELOX=1'))
        self.assertLess(native.index('Prepare Docker for macOS Native tests'),
                        native.index('Run native tests (GTest via CTest)'))
        self.assertIn('id: native-tests', native)
        self.assertIn("steps.native-tests.outcome == 'success'", native)
        self.assertIn('test -s pangrova-worker-native/build/test-results.xml', native)
        self.assertIn("!cancelled() && hashFiles('pangrova-worker-native/build/test-results.xml') != ''", native)
        self.assertIn('fail-on-error: true', native)
        self.assertNotIn('continue-on-error', native)

    def test_macos_warm_sdk_requires_matching_inputs_and_a_trusted_artifact(self):
        workflow = (WORKFLOWS / 'native-platform.yml').read_text()
        core, tail = workflow.split('  macos-native-sdk:\n', 1)
        sdk, native = tail.split('  native-build:\n', 1)
        self.assertIn('macos-native-stage.sh cache-key sdk', core)
        self.assertIn('.workflow_run.repository_id == .workflow_run.head_repository_id', core)
        self.assertIn('.workflow_run.head_branch == "main"', core)
        self.assertIn('.github/workflows/ci.yml|.github/workflows/native-deps-baseline.yml', core)
        self.assertIn("steps.cached-sdk.outputs.run == ''", core)
        self.assertIn("needs.macos-native-core.outputs.cached-sdk-run == ''", sdk)
        self.assertIn('name=macos-sdk-index-$PLATFORM-$key', sdk)
        self.assertIn("if: github.ref == 'refs/heads/main'", sdk)
        self.assertIn("needs.macos-native-core.result == 'success'", native)
        self.assertIn('macos-native-stage.sh restore-cached sdk', native)
        self.assertIn('digest-mismatch: error', native)

        platform_gate = native.split('  native-validation:\n', 1)[1]
        self.assertIn('needs: native-build', platform_gate)
        self.assertIn('!cancelled() && !inputs.sdk-only', platform_gate)
        self.assertIn('[[ "$NATIVE_RESULT" != success ]]', platform_gate)

    def test_macos_docker_boots_before_installing_the_verified_engine(self):
        script = (SCRIPTS / 'setup-macos-native-test-docker.sh').read_text()
        config = (SCRIPTS / 'setup-macos-native-test-docker.yaml').read_text()
        self.assertEqual(len(re.findall(r'^ +docker_digest=[0-9a-f]{64}$', script, re.MULTILINE)), 2)
        self.assertLess(script.index('"$docker_digest" "$docker_archive" | shasum'),
                        script.index('"$lima" start'))
        self.assertLess(script.index('"$lima" start'), script.index('sudo -n tar -xzf -'))
        self.assertLess(script.index('systemd-run --unit=pangrova-docker'), script.index('localhost/_ping'))
        self.assertIn('guestSocket: /var/run/docker.sock', config)
        self.assertIn('system: false', config)
        self.assertIn('user: false', config)
        self.assertNotIn('get.docker.com', script + config)

    def test_dependency_lock_matches_the_bootstrap(self):
        lock = {}
        for line in (ROOT / 'pangrova-worker-native/dependency-versions.txt').read_text().splitlines():
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            key, value = line.split('=', 1)
            lock[key] = value
        self.assertIn('PANGROVA_DEPENDENCY_BUILD', lock)
        bootstrap = (ROOT / 'pangrova-worker-native/bin/setup-native-build-env.sh').read_text()
        self.assertIn(f'VELOX_REF="${{VELOX_REF:-{lock["VELOX_REF"]}}}"', bootstrap)
        self.assertIn(f'xxHash/archive/refs/tags/v{lock["XXHASH_VERSION"]}.tar.gz',
                      bootstrap)

    def test_baseline_workflow_warms_the_same_layers(self):
        baseline = (ROOT / '.github/workflows/native-deps-baseline.yml').read_text()
        self.assertIn('schedule:', baseline)
        self.assertIn('push:\n    branches: [main]\n    paths:', baseline)
        self.assertIn('pangrova-v1-deps-${{ matrix.platform.runner }}-${{ runner.arch }}-', baseline)
        self.assertIn('pangrova-v1-velox-${{ matrix.platform.runner }}-${{ runner.arch }}-', baseline)
        linux, macos = baseline.split('  warm-macos:\n', 1)
        self.assertNotIn('macos-15', linux)
        self.assertIn('uses: ./.github/workflows/native-platform.yml', macos)
        self.assertIn('sdk-only: true', macos)
        platform = (WORKFLOWS / 'native-platform.yml').read_text()
        sdk, native = platform.split('  native-build:\n', 1)
        self.assertIn('native-deps-${{ fromJSON(inputs.platform).name }}.tar.zst', sdk)
        self.assertIn('if: inputs.sdk-only', sdk)
        self.assertIn('!cancelled() && !inputs.sdk-only', native)

    def test_dependency_baseline_consumers_are_opt_in(self):
        workflow = (ROOT / '.github/workflows/ci.yml').read_text()
        native = (ROOT / '.github/workflows/native-platform.yml').read_text().split('  native-build:\n', 1)[1]
        # Linux keeps its opt-in prefix and macOS verifies the selected SDK.
        self.assertNotIn("vars.PANGROVA_NATIVE_DEPS_ARTIFACT == 'true'", native)
        self.assertIn("vars.PANGROVA_NATIVE_DEPS_IMAGE == 'ghcr'", native)
        self.assertIn('name: macos-sdk-${{ fromJSON(inputs.platform).name }}', native)
        self.assertIn('macos-native-stage.sh restore sdk', native)
        revision = ("tr -d '\\n' < pangrova-worker-native/dependency-versions.txt "
                    "| sha256sum | cut -c1-16")
        self.assertIn(revision, native)
        baseline = (ROOT / '.github/workflows/native-deps-baseline.yml').read_text()
        self.assertIn(revision, baseline)

    def test_linux_dependency_image_uses_the_built_prefix(self):
        baseline = (WORKFLOWS / 'native-deps-baseline.yml').read_text()
        linux = baseline.split('  warm:\n', 1)[1].split('  warm-macos:', 1)[0]
        self.assertIn('setup-native-build-env.sh\n\n      - name: Publish Linux dependency prefix', linux)
        publish = linux.split('      - name: Publish Linux dependency prefix', 1)[1]
        self.assertIn("vars.PANGROVA_NATIVE_DEPS_IMAGE == 'ghcr'", publish)
        self.assertIn('docker build -t "$image" -f - "$HOME/.pangrova/native-deps"', publish)
        self.assertIn('COPY . /opt/pangrova/native-deps/', publish)
        platform = (WORKFLOWS / 'native-platform.yml').read_text()
        restore = platform.split('      - name: Restore Linux dependency baseline image', 1)[1]
        restore = restore.split('      - name:', 1)[0]
        self.assertIn('docker create "$image" /bin/false', restore)
        self.assertIn('docker cp "$container:/opt/pangrova/native-deps" "$HOME/.pangrova/"', restore)
        self.assertNotIn('/opt/pangrova/velox', restore)

    def test_jmh_compares_real_base_and_candidate_measurements(self):
        workflow = (WORKFLOWS / 'ci.yml').read_text()
        job = workflow.split('  jmh-benchmarks:\n', 1)[1].split('\n  dead-stub-detection:', 1)[0]
        self.assertIn('ref: ${{ github.event.pull_request.base.sha }}', job)
        self.assertIn('[[ "$(git -C jmh-base rev-parse HEAD)" == "$BASE_SHA" ]]', job)
        self.assertIn('(cd jmh-base && mvn "${benchmark_args[@]}")', job)
        self.assertIn('measure "jmh-base/$jar" base', job)
        self.assertIn('measure "$jar" candidate', job)
        self.assertIn('-foe true', job)
        self.assertIn('target/jmh/candidate.json target/jmh/base.json 0.10', job)
        self.assertIn('path: target/jmh/', job)

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
                 '.pangrova/scripts/macos-native-stage.sh',
                 '.pangrova/scripts/build-iceberg-sdk-candidate.sh',
                 '.pangrova/scripts/ci_change_scope.py', '.pangrova/modules.yaml',
                 '.github/workflows/ci.yml', '.github/workflows/release.yml',
                 '.github/actions/setup-iceberg-sdk/action.yml',
                 '.github/actions/restore-iceberg-sdk/action.yml',
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
                 'iceberg-sdk', 'static-analysis', 'linux-native', 'native-build', 'java-build', 'native-sanitizers',
                 'production-image', 'production-regression', 'tpch-performance', 'jmh-benchmarks']}
        jobs['change-scope']['outputs'] = {'runtime': runtime, 'performance': performance,
                                         'native': runtime if native is None else native}
        for name, result in (overrides or {}).items():
            jobs[name]['result'] = result
        return subprocess.run([sys.executable, '-c', program], capture_output=True,
                              text=True, env={**os.environ, 'RESULTS': json.dumps(jobs),
                                              'PR_EVENT': str(pr).lower()})

    def test_docs_can_skip_only_unselected_checks(self):
        result = self.result(overrides={'iceberg-sdk': 'skipped', 'linux-native': 'skipped', 'native-build': 'skipped',
                                       'java-build': 'skipped', 'production-image': 'skipped',
                                       'production-regression': 'skipped'})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotEqual(self.result(overrides={'lint': 'skipped'}).returncode, 0)

    def test_selected_checks_cannot_fail_cancel_or_disappear(self):
        for name in ['iceberg-sdk', 'linux-native', 'native-build', 'java-build', 'native-sanitizers',
                     'production-image', 'production-regression']:
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
                             overrides={'native-sanitizers': 'skipped', 'native-build': 'skipped'})
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotEqual(self.result(runtime='true', native='false',
                                       overrides={'production-regression': 'skipped'}).returncode, 0)
        self.assertNotEqual(self.result(runtime='true', native='').returncode, 0)


if __name__ == '__main__':
    unittest.main()
