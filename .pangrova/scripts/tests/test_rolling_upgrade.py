"""Check rolling-upgrade command ordering without SSH or a live cluster."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / "bin" / "pangrova-rolling-upgrade.sh"


class RollingUpgradeTest(unittest.TestCase):
    def run_upgrade(self, backup_exit=0, *, existing_backup=False, role="worker"):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "release" / "lib").mkdir(parents=True)
            stubs = root / "stubs"
            stubs.mkdir()
            commands = {
                "ssh": """#!/usr/bin/env bash
printf '%s\\n' "${@: -1}" >> "$SSH_TRACE"
case "${@: -1}" in
  mkdir\\ *) if [[ "$BACKUP_EXIT" != 0 ]]; then exit "$BACKUP_EXIT"; fi
             exec bash -c "${@: -1}";;
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
            install = root / "current"
            (install / "lib").mkdir(parents=True)
            (install / "lib" / "runtime.jar").write_text("current", encoding="utf-8")
            if existing_backup:
                (install / "lib.backup").mkdir()
                (install / "lib.backup" / "runtime.jar").write_text("previous", encoding="utf-8")
            result = subprocess.run(
                ["bash", str(SCRIPT), "--new-version-dir", str(root / "release"),
                 f"--{role}s", f"{role}.invalid:8081"],
                capture_output=True, text=True, timeout=10, check=False,
                env={**os.environ, "PATH": f"{stubs}:{os.environ['PATH']}",
                     "PANGROVA_HOME": str(install), "SSH_TRACE": str(trace),
                     "BACKUP_EXIT": str(backup_exit)},
            )
            backup = install / "lib.backup" / "runtime.jar"
            contents = backup.read_text(encoding="utf-8") if backup.exists() else None
            return result, trace.read_text(encoding="utf-8").splitlines(), contents

    def test_backup_failure_aborts_before_mutation(self):
        for role in ("worker", "coordinator"):
            with self.subTest(role=role):
                result, commands, backup = self.run_upgrade(backup_exit=1, role=role)
                self.assertNotEqual(0, result.returncode, result.stdout)
                self.assertEqual(1, len(commands), commands)
                self.assertTrue(commands[0].startswith("mkdir "), commands)
                self.assertIsNone(backup)

    def test_existing_backup_stops_upgrade_and_is_preserved(self):
        result, commands, backup = self.run_upgrade(existing_backup=True)
        self.assertNotEqual(0, result.returncode, result.stdout)
        self.assertEqual(1, len(commands), commands)
        self.assertEqual("previous", backup)

    def test_successful_backup_precedes_stop_and_deploy(self):
        result, commands, backup = self.run_upgrade()
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertEqual("current", backup)
        self.assertTrue(commands[0].startswith("mkdir "), commands)
        self.assertIn("pangrova-stop.sh --worker", commands[1])
        self.assertTrue(commands[2].startswith("rsync "), commands)
        self.assertIn("pangrova-worker.sh --daemon", commands[3])
        self.assertTrue(commands[4].startswith("rm -rf "), commands)


if __name__ == "__main__":
    unittest.main()
