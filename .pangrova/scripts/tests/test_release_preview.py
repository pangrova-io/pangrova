"""Preview host ordering and resource ownership with command stubs."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import unittest


ROOT = Path(__file__).resolve().parents[3]
LAUNCHER = ROOT / "deploy/release/pangrova-preview.sh"
DOCKER = """
import json, os, sys
args = sys.argv[1:]
with open(os.environ["TRACE"], "a") as trace:
    trace.write(json.dumps(args) + "\\n")
if args[0] == "info":
    print("linux/x86_64")
elif args[0] in ("container", "network", "image") and args[1] == "ls":
    if os.environ["PREVIEW_CREATED"] == "yes":
        print("preview-resource")
elif args[0] == "run" and "--init" in args:
    raise SystemExit(int(os.environ["RUNTIME_EXIT"]))
"""


class ReleasePreviewTest(unittest.TestCase):
    def setUp(self):
        work = tempfile.TemporaryDirectory()
        self.addCleanup(work.cleanup)
        self.root = Path(work.name)
        name = "pangrova-1.1-linux-x86_64"
        bundle = self.root / name
        (bundle / "preview").mkdir(parents=True)
        (bundle / "manifest.json").write_text("{}")
        self.archive = self.root / f"{name}.tar.gz"
        with tarfile.open(self.archive, "w:gz") as tar:
            tar.add(bundle, arcname=name)
        self.digest = hashlib.sha256(self.archive.read_bytes()).hexdigest()
        self.trace = self.root / "trace"
        stubs = self.root / "stubs"
        stubs.mkdir()
        for command, content in {"docker": f"#!{sys.executable}\n{DOCKER}",
                                 "uname": "#!/bin/sh\ncase \"$1\" in -s) echo Linux;; -m) echo x86_64;; esac\n",
                                 "sha256sum": f"#!{sys.executable}\n"
                                 "import hashlib, pathlib, sys\n"
                                 "digest, name = sys.stdin.readline().rstrip().split('  ', 1)\n"
                                 "sys.exit(hashlib.sha256(pathlib.Path(name).read_bytes()).hexdigest() != digest)\n"
                                 }.items():
            script = stubs / command
            script.write_text(content)
            script.chmod(0o755)
        self.env = {**os.environ, "PATH": str(stubs) + os.pathsep + os.environ["PATH"],
                    "TRACE": str(self.trace), "PREVIEW_CREATED": "no", "RUNTIME_EXIT": "0"}

    def run_preview(self, digest, **environment):
        return subprocess.run(
            ["bash", str(LAUNCHER), "verify", "1.1", digest, str(self.archive), "split"],
            env={**self.env, **environment}, text=True, capture_output=True, timeout=10)

    def calls(self):
        return [json.loads(line) for line in self.trace.read_text().splitlines()]

    def test_digest_failure_precedes_docker_mutations(self):
        result = self.run_preview("0" * 64)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(any(args[0] in ("build", "run", "rm") for args in self.calls()))

    def test_runtime_failure_keeps_failure_and_cleans_only_labelled_trial(self):
        result = self.run_preview(self.digest, PREVIEW_CREATED="yes", RUNTIME_EXIT="9")
        self.assertEqual(result.returncode, 9, result.stdout + result.stderr)
        identifier = result.stdout.splitlines()[0].removeprefix("Preview ID: ")
        calls = self.calls()
        foreground = next(args for args in calls if args[0] == "run" and "--init" in args)
        self.assertIn("PANGROVA_PREVIEW_TOPOLOGY=split", foreground)
        self.assertIn(["rm", "-fv", f"{identifier}-runtime"], calls)
        self.assertIn(["rm", "-fv", f"{identifier}-db"], calls)
        self.assertIn(["network", "rm", identifier], calls)
        self.assertIn(["image", "rm", f"{identifier}:preview"], calls)
        for args in calls:
            if args[0] in ("container", "image", "network") and args[1] == "ls":
                self.assertIn(f"label=io.pangrova.preview={identifier}", args)
