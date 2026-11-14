"""Preserve stub findings while avoiding ignored dependency/build tree scans."""

from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / 'detect-dead-stubs.sh'
STUB = '''package example;
public class Example {
    public void run() {
        throw new UnsupportedOperationException();
    }
}
'''


class StubDetectionTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.script = self.root / '.pangrova/scripts/detect-dead-stubs.sh'
        self.script.parent.mkdir(parents=True)
        shutil.copyfile(SCRIPT, self.script)
        shutil.copyfile(SCRIPT.with_name("DeadStubCheck.java"), self.script.with_name("DeadStubCheck.java"))
        self.git('init', '-q')

    def git(self, *args):
        subprocess.run(['git', *args], cwd=self.root, check=True,
                       capture_output=True)

    def source(self, name='module/src/main/java/example/Example.java', content=STUB):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        return name

    def check(self):
        return subprocess.run(['bash', str(self.script)], capture_output=True, text=True)

    def test_untracked_production_stub_is_rejected(self):
        self.source()
        result = self.check()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn('Found 1 dead stub', result.stdout)
        self.assertNotIn('integer expression', result.stderr)

    def test_tracked_source_is_checked_even_when_ignore_rule_matches(self):
        name = self.source()
        self.git('add', name)
        (self.root / '.gitignore').write_text('module/\n')
        self.assertEqual(self.check().returncode, 1)

    def test_test_and_generated_sources_are_outside_stub_gate(self):
        for name in ['module/src/test/java/example/Test.java',
                     'module/target/generated/src/main/java/example/Generated.java']:
            self.source(name)
        result = self.check()
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_tracked_implementation_and_method_issue_are_recognized(self):
        self.source(content=STUB.replace('public class', '@ExperimentalImplementation(trackingIssue = "OPS-7")\npublic class'))
        self.source('module/src/main/java/example/Tracked.java',
                    STUB.replace('public void', '// https://github.com/example/repo/issues/1\n    public void'))
        self.assertEqual(self.check().returncode, 0)

    def test_api_stability_and_nearby_real_method_do_not_hide_default_stub(self):
        self.source('domain-api/src/main/java/example/Contract.java', """
            @Experimental interface Contract {
                default void available() {
                    int a = 1;
                    int b = 2;
                    report(a + b);
                }
                default void pending() {
                    throw new UnsupportedOperationException(
                    );
                }
                void report(int value);
            }""")
        result = self.check()
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertIn('pending', result.stdout)
        self.assertNotIn('available', result.stdout)

    def test_ignored_dependency_sources_do_not_produce_findings(self):
        (self.root / '.gitignore').write_text('vendor/\n')
        self.source('vendor/sdk/src/main/java/example/ThirdParty.java')
        self.assertEqual(self.check().returncode, 0)

    def test_missing_git_repository_fails_instead_of_reporting_clean(self):
        shutil.rmtree(self.root / '.git')
        self.assertNotEqual(self.check().returncode, 0)


if __name__ == '__main__':
    unittest.main()
