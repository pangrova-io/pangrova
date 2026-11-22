"""Exercise the read-only workflow with deterministic GitHub event and job fixtures."""

from io import BytesIO
import json
from pathlib import Path
import tempfile
import textwrap
from types import SimpleNamespace
import unittest
from urllib.request import Request


WORKFLOW = Path(__file__).resolve().parents[3] / ".github/workflows/diagnose-main-ci.yml"


def job(job_id, name, conclusion, *failed_steps):
    return {
        "id": job_id, "name": name, "conclusion": conclusion,
        "steps": [{"number": index, "name": name, "conclusion": "failure"}
                  for index, name in enumerate(failed_steps, 1)],
    }


class MainCiDiagnosticsTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        directory = Path(temp.name)
        self.event_path = directory / "event.json"
        self.summary = directory / "summary.md"
        repo = {"id": 11, "full_name": "owner/project"}
        self.event = {"action": "completed", "repository": repo, "workflow_run": {
            "id": 22, "run_attempt": 3, "head_sha": "a" * 40,
            "status": "completed", "conclusion": "failure", "name": "CI",
            "path": ".github/workflows/ci.yml", "event": "push", "head_branch": "main",
            "repository": repo, "head_repository": repo,
        }}
        self.calls = []
        self.payload = {"jobs": [], "total_count": 0}
        workflow = WORKFLOW.read_text(encoding="utf-8")
        inline = workflow.split("python3 - <<'PY'\n", 1)[1].rsplit("\n          PY", 1)[0]
        self.script = {}
        exec(compile(textwrap.dedent(inline), str(WORKFLOW), "exec"), self.script)
        self.script["os"] = SimpleNamespace(environ={
            "GITHUB_EVENT_PATH": str(self.event_path), "GITHUB_STEP_SUMMARY": str(self.summary),
            "GITHUB_REPOSITORY": "owner/project", "GITHUB_API_URL": "https://api.example.test",
            "GITHUB_SERVER_URL": "https://example.test", "GH_TOKEN": "fixture-token",
        })
        self.script["urllib"] = SimpleNamespace(request=SimpleNamespace(Request=Request, urlopen=self.urlopen))

    def urlopen(self, request, timeout):
        self.calls.append((request.full_url, request.get_header("Authorization"), timeout))
        return BytesIO(json.dumps(self.payload).encode())

    def run_event(self):
        self.event_path.write_text(json.dumps(self.event), encoding="utf-8")
        self.script["main"]()

    def test_reports_failed_and_incomplete_jobs_from_exact_attempt(self):
        jobs = [job(1, "Build", "failure", "Configure", "Publish native test report"),
                job(2, "Worker [arm]", "cancelled"),
                job(3, "Test", "timed_out"),
                job(4, "Validation result", "failure", "Check dependencies"),
                job(5, "Skipped job", "skipped")]
        self.payload = {"jobs": jobs, "total_count": len(jobs)}
        self.run_event()
        self.assertEqual(self.calls, [(
            "https://api.example.test/repos/owner/project/actions/runs/22/attempts/3/jobs?per_page=100",
            "Bearer fixture-token", 25,
        )])
        report = self.summary.read_text(encoding="utf-8")
        self.assertIn("## Incomplete jobs", report)
        self.assertIn("/job/2): cancelled", report)
        self.assertIn("/job/3): timed out", report)
        self.assertIn("/job/1): step 1: Configure", report)
        self.assertIn("/job/1): step 2: Publish native test report", report)
        self.assertIn("/job/4): step 1: Check dependencies", report)
        self.assertNotIn("Skipped job", report)
        self.assertNotIn("fixture-token", report)

    def test_rejects_pull_request_and_cross_repository_events(self):
        run = self.event["workflow_run"]
        for key, value in (("event", "pull_request"), ("head_branch", "topic"),
                           ("path", ".github/workflows/other.yml"), ("conclusion", "success"),
                           ("head_repository", {"id": 12}), ("repository", {"id": 12})):
            with self.subTest(key=key):
                original = run[key]
                run[key] = value
                self.run_event()
                self.assertEqual(self.calls, [])
                self.assertFalse(self.summary.exists())
                run[key] = original
