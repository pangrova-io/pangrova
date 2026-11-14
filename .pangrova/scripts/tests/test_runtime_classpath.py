"""Launcher tests: select the actual role closure without scanning unrelated target JARs."""
import json
import os
import sys
from pathlib import Path
import subprocess
import tempfile
import unittest
import importlib.util
from zipfile import ZipFile

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
        self.package('pangrova-server/pangrova-server-worker', 'pangrova-server-worker',
                     [self.home / 'absent.jar'])
        result = self.resolve('worker')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('dependency is missing', result.stderr)
        self.assertEqual(result.stdout, '')

    def test_empty_entry_is_rejected_instead_of_adding_current_directory(self):
        self.package('pangrova-standalone', 'pangrova-standalone', ['', ''])
        self.assertNotEqual(self.resolve('standalone').returncode, 0)

    def test_incomplete_role_distribution_cannot_load_unlisted_jars(self):
        role_lib = self.home / 'lib/coordinator'
        role_lib.mkdir(parents=True)
        role_jar = role_lib / 'server.jar'
        role_jar.touch()
        (self.home / 'lib/worker.jar').touch()
        result = self.resolve('coordinator')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('classpath.list', result.stderr)
        self.assertEqual(result.stdout, '')

    def test_role_manifest_preserves_maven_order_and_rejects_a_missing_member(self):
        lib = self.home / 'lib/coordinator'
        lib.mkdir(parents=True)
        jars = [lib / name for name in ('z.jar', 'a.jar')]
        for jar in jars:
            jar.touch()
        (lib / 'classpath.list').write_text('z.jar\na.jar\n')
        (lib / 'unlisted.jar').touch()
        self.assertEqual(self.resolve('coordinator').stdout.strip().split(':'), list(map(str, jars)))
        jars[0].unlink()
        result = self.resolve('coordinator')
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, '')

    def test_role_scripts_pass_each_config_path_as_one_argument(self):
        java = self.home / 'jdk/bin/java'
        java.parent.mkdir(parents=True)
        java.write_text(f'#!{sys.executable}\nimport json, sys\n'
                        """if "-version" in sys.argv: print('java version "17"')\n"""
                        'else: print(json.dumps(sys.argv[1:]))\n')
        java.chmod(0o755)
        config = str(self.home / 'explicit [x] role.yml')
        for role in ('coordinator', 'worker', 'standalone'):
            with self.subTest(role=role):
                lib = self.home / 'lib' / role
                lib.mkdir(parents=True)
                (lib / 'entry.jar').touch()
                (lib / 'classpath.list').write_text('entry.jar\n')
                result = subprocess.run(
                    ['bash', str(ROOT / 'bin' / f'pangrova-{role}.sh'), '--config', config],
                    env={**os.environ, 'JAVA_HOME': str(java.parents[1]), 'PANGROVA_HOME': str(self.home)},
                    text=True, capture_output=True)
                self.assertEqual(result.returncode, 0, result.stderr)
                argv = json.loads(result.stdout.splitlines()[-1])
                self.assertEqual(argv[argv.index('--config') + 1:], [config])

    def test_stager_checks_binary_contents_even_under_unrelated_artifact_names(self):
        spec = importlib.util.spec_from_file_location("stage", ROOT / "deploy/docker/stage-role-classpaths.py")
        stage = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(stage)
        for role, member in (
            ("coordinator", "org/pangrova/worker/jni/NativeRuntime.class"),
            ("coordinator", "META-INF/native/linux/aarch64/libpangrova_native.so"),
            ("worker", "META-INF/versions/17/org/pangrova/planner/analysis/SqlCompiler.class"),
        ):
            with self.subTest(role=role, member=member):
                unrelated = self.home / "shared-dependency.jar"
                with ZipFile(unrelated, "w") as jar:
                    jar.writestr(member, b"")
                with self.assertRaisesRegex(ValueError, "runtime"):
                    stage.check_contents(role, unrelated, self.home / "server.jar", stage.RoleContents())

    def test_stager_checks_ownership_across_all_jars_in_one_role(self):
        spec = importlib.util.spec_from_file_location("stage", ROOT / "deploy/docker/stage-role-classpaths.py")
        stage = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(stage)
        service = "META-INF/services/org.pangrova.example.Factory"

        def make_jar(name, module, members):
            path = self.home / name
            with ZipFile(path, "w") as jar:
                jar.writestr("META-INF/MANIFEST.MF",
                             "Manifest-Version: 1.0\r\nAutomatic-Module-Name: " + module + "\r\n\r\n")
                for entry, content in members.items():
                    jar.writestr(entry, content)
            return path

        first = make_jar("first.jar", "org.pangrova.\r\n first", {
            "org/pangrova/example/A.class": b"", "META-INF/versions/17/org/pangrova/example/A.class": b"",
            service: "example.Provider # selected factory\n"})
        for module, entries, message in (
                ("another", {"org/pangrova/example/B.class": b""}, "Conflicting runtime owners"),
                ("org.pangrova.first", {"org/pangrova/other/C.class": b""}, "Conflicting runtime owners"),
                ("another", {service: "example.Provider\n"}, "Duplicate standalone service"),
                ("", {"org/pangrova/other/C.class": b""}, "Automatic-Module-Name")):
            with self.subTest(entries=entries, module=module):
                contents = stage.RoleContents()
                stage.check_contents("standalone", first, self.home / "server.jar", contents)
                other = make_jar("other.jar", module, entries)
                with self.assertRaisesRegex(ValueError, message):
                    stage.check_contents("standalone", other, self.home / "server.jar", contents)

    def test_unbuilt_role_fails_with_actionable_build_command(self):
        result = self.resolve('coordinator')
        self.assertNotEqual(result.returncode, 0)
        self.assertIn('pangrova-server/pangrova-server-coordinator', result.stderr)


if __name__ == '__main__':
    unittest.main()
