"""Check rolling-upgrade command ordering without SSH or a live cluster."""

import os
from pathlib import Path
import shlex
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "bin" / "pangrova-rolling-upgrade.sh"


class RollingUpgradeTest(unittest.TestCase):
    def run_upgrade(self, backup_exit=0, existing_backup=False, missing_library=False, quoted_paths=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            release = root / ("release's stage" if quoted_paths else "release")
            (release / "lib").mkdir(parents=True)
            stubs = root / "stubs"
            stubs.mkdir()
            commands = {
                "ssh": """#!/usr/bin/env bash
printf '%s\\n' "${@: -1}" >> "$SSH_TRACE"
case "${@: -1}" in
  mkdir\\ *lib.backup*)
    [ "$BACKUP_EXIT" = 0 ] || exit "$BACKUP_EXIT"
    exec bash -c "${@: -1}"
    ;;
esac
exit 0
""",
                "curl": "#!/usr/bin/env bash\nprintf 200\n",
            }
            for name, contents in commands.items():
                script = stubs / name
                script.write_text(contents, encoding="utf-8")
                script.chmod(0o755)
            trace = root / "ssh.trace"
            install = root / ("current's install" if quoted_paths else "current")
            install.mkdir()
            if not missing_library:
                (install / "lib").mkdir()
                (install / "lib" / "current.jar").write_text("current", encoding="utf-8")
            backup = install / "lib.backup"
            if existing_backup:
                backup.mkdir()
                (backup / "previous.jar").write_text("previous", encoding="utf-8")
            result = subprocess.run(
                ["bash", str(SCRIPT), "--new-version-dir", str(release),
                 "--workers", "worker.invalid:8081"],
                capture_output=True, text=True, timeout=10, check=False,
                env={**os.environ, "PATH": f"{stubs}:{os.environ['PATH']}",
                     "PANGROVA_HOME": str(install), "SSH_TRACE": str(trace),
                     "BACKUP_EXIT": str(backup_exit)},
            )
            files = sorted(str(path.relative_to(backup)) for path in backup.rglob("*"))
            return result, trace.read_text(encoding="utf-8").splitlines(), files

    def test_backup_failure_aborts_before_mutation(self):
        result, commands, files = self.run_upgrade(backup_exit=1)
        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertEqual(1, len(commands), commands)
        self.assertTrue(commands[0].startswith("mkdir "), commands)
        self.assertEqual([], files)

    def test_existing_backup_is_preserved_and_aborts_upgrade(self):
        result, commands, files = self.run_upgrade(existing_backup=True)
        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertEqual(1, len(commands), commands)
        self.assertEqual(["previous.jar"], files)

    def test_copy_failure_after_creating_backup_aborts_upgrade(self):
        result, commands, files = self.run_upgrade(missing_library=True)
        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertIn("aborting before stopping the node", result.stdout)
        self.assertEqual(1, len(commands), commands)
        self.assertEqual([], files)

    def test_successful_backup_precedes_stop_and_deploy(self):
        result, commands, files = self.run_upgrade()
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertTrue(commands[0].startswith("mkdir "), commands)
        self.assertEqual(["current.jar"], files)
        self.assertIn("pangrova-stop.sh --worker", commands[1])
        self.assertTrue(commands[2].startswith("rsync "), commands)

    def test_quoted_paths_reach_backup_deploy_and_cleanup_as_single_arguments(self):
        result, commands, files = self.run_upgrade(quoted_paths=True)
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual(["current.jar"], files)
        stop, deploy, start, cleanup = map(shlex.split, commands[1:])
        self.assertTrue(stop[0].endswith("/current's install/bin/pangrova-stop.sh"))
        self.assertTrue(deploy[-2].endswith("/release's stage/lib/"))
        self.assertTrue(deploy[-1].endswith("/current's install/lib/"))
        self.assertTrue(start[0].endswith("/current's install/bin/pangrova-worker.sh"))
        self.assertTrue(cleanup[-1].endswith("/current's install/lib.backup"))


if __name__ == "__main__":
    unittest.main()
