"""Launcher tests: select the actual role closure without scanning unrelated target JARs."""
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[3]
HELPER = ROOT / 'bin' / 'pangrova-classpath.sh'


class RuntimeClasspathTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix='pangrova role classpath ')
        self.addCleanup(self.directory.cleanup)
        self.home = Path(self.directory.name)

    def resolve(self, role):
        return subprocess.run(
            ['bash', '-c', 'source "$1"; pangrova_runtime_classpath "$2"',
             'classpath-test', str(HELPER), role],
            env={**os.environ, 'PANGROVA_HOME': str(self.home)},
            text=True, capture_output=True)

    def package(self, module, artifact, dependencies):
        target = self.home / module / 'target'
        (target / 'maven-archiver').mkdir(parents=True)
        (target / 'maven-archiver/pom.properties').write_text(
            f'artifactId={artifact}\nversion=1-test\n')
        jar = target / f'{artifact}-1-test.jar'
        jar.touch()
        (target / 'runtime-classpath.txt').write_text(':'.join(map(str, dependencies)))
        return jar

    def test_nested_coordinator_uses_only_resolved_runtime_dependencies(self):
        dependency = self.home / 'shared dependency.jar'
        dependency.touch()
        jar = self.package('pangrova-server/pangrova-server-coordinator',
                           'pangrova-server-coordinator', [dependency])
        unrelated = self.home / 'pangrova-worker/target/worker.jar'
        unrelated.parent.mkdir(parents=True)
        unrelated.touch()
        result = self.resolve('coordinator')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip().split(':'), [str(jar), str(dependency)])

    def test_missing_dependency_is_rejected_without_scanning_other_artifacts(self):
        self.package('pangrova-worker/pangrova-worker-core', 'pangrova-worker-core',
                     [self.home / 'absent.jar'])
        result = self.resolve('worker')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('dependency is missing', result.stderr)
        self.assertEqual(result.stdout, '')

    def test_empty_entry_is_rejected_instead_of_adding_current_directory(self):
        self.package('pangrova-standalone', 'pangrova-standalone', ['', ''])
        self.assertNotEqual(self.resolve('standalone').returncode, 0)

    def test_role_distribution_takes_precedence_over_shared_lib(self):
        role_lib = self.home / 'lib/coordinator'
        role_lib.mkdir(parents=True)
        role_jar = role_lib / 'server.jar'
        role_jar.touch()
        (self.home / 'lib/worker.jar').touch()
        result = self.resolve('coordinator')
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), str(role_jar))

    def test_unbuilt_role_fails_with_actionable_build_command(self):
        result = self.resolve('coordinator')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('pangrova-server/pangrova-server-coordinator', result.stderr)


if __name__ == '__main__':
    unittest.main()
