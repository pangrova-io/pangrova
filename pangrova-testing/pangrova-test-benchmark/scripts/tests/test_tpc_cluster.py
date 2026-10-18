"""Lifecycle ownership tests using a small Docker command collaborator; no engine claims."""

from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tpc_cluster import start, stop
from tpc_common import read_json, write_json
import test_tpc_suite


class DockerCommands:
    def __init__(self, fail_wait=False):
        self.calls, self.removed, self.project = [], set(), None
        self.fail_wait, self.foreign = fail_wait, False
    def __call__(self, args, json_output=False):
        self.calls.append(args)
        if '-p' in args:self.project = args[args.index('-p') + 1]
        if 'info' in args:
            return {'{{.Name}}': 'fixture-host', '{{.ID}}': 'fixture-daemon',
                    '{{.MemTotal}}': '1000', '{{.NCPU}}': '8'}[args[-1]]
        definitions = {name: {'image': 'fixture-image', 'command': ['start', role],
                              'mem_limit': memory, 'cpus': cpu}
                       for name, role, memory, cpu in [('coordinator', 'coordinator', 100, 1), ('worker', 'worker', 400, 3)]}
        if 'config' in args:return {'services': definitions}
        if 'image' in args:return [{'Id': 'sha256:' + 'f' * 64,
                                   'Config': {'Labels': {'org.opencontainers.image.revision': 'fixture-revision'}}}]
        if 'up' in args:
            if self.fail_wait and args[-1] == 'worker':raise subprocess.CalledProcessError(1, args)
            return ''
        if 'compose' in args and 'ps' in args:return 'cid-' + args[-1]
        if 'inspect' in args:
            worker = args[-1] == 'cid-worker'
            return [{'Image': 'sha256:' + 'f' * 64, 'State': {'Running': True, 'Pid': 2 if worker else 1,
                        'Health': {'Status': 'healthy'}},
                     'Config': {'Labels': {'com.docker.compose.project': 'foreign' if self.foreign else self.project}},
                     'HostConfig': {'Memory': 400 if worker else 100, 'NanoCpus': 3000000000 if worker else 1000000000}}]
        if 'cp' in args:
            Path(args[-1]).write_bytes(b'fixture-client-not-a-runtime')
            return ''
        if 'ps' in args:
            cid = args[-1].removeprefix('id=')
            return '' if cid in self.removed else cid
        if 'network' in args:return ''
        if 'rm' in args:
            self.removed.add(args[-1])
            return ''
        raise AssertionError(args)


class ClusterTest(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        compose = self.root / 'compose.yml';compose.write_text('fixture; commands supplied by collaborator')
        self.plan = self.root / 'deployment.json'
        write_json(self.plan, {'schemaVersion': 1, 'sourceRevision': 'fixture-revision',
                              'clientJarInImage': '/fixture/client.jar', 'nodes': [{'context': 'fixture',
                              'composeFile': str(compose), 'services': {
                                  'coordinator': {'role': 'coordinator', 'endpoint': 'http://fixture.invalid:28080'},
                                  'worker': {'role': 'worker', 'endpoint': 'http://fixture.invalid:28081'}}}]})
        self.template = self.root / 'template.json'
        write_json(self.template, test_tpc_suite.EnvironmentAndSnapshotTest().environment_fixture())
        self.state = self.root / 'state.json'
        self.env = self.root / 'environment.json'

    def test_process_identity_limits_and_idempotent_owned_cleanup(self):
        docker = DockerCommands()
        state = start(self.plan, self.state, self.template, self.env, self.root / 'client.jar', docker)
        self.assertEqual(state['status'], 'running')
        self.assertEqual(read_json(self.env)['workers'][0]['process'], 2)
        stop(self.state, docker)
        count = len(docker.calls)
        stop(self.state, docker)
        self.assertEqual(len(docker.calls), count)
        self.assertEqual(docker.removed, {'cid-worker', 'cid-coordinator'})
        self.assertTrue(all('--volumes' not in call and '-v' not in call for call in docker.calls))

    def test_failed_readiness_still_records_and_cleans_created_container(self):
        docker = DockerCommands(fail_wait=True)
        with self.assertRaises(subprocess.CalledProcessError):
            start(self.plan, self.state, self.template, self.env, self.root / 'client.jar', docker)
        self.assertEqual(read_json(self.state)['status'], 'failed')
        self.assertEqual(len(read_json(self.state)['containers']), 2)
        stop(self.state, docker)
        self.assertEqual(docker.removed, {'cid-worker', 'cid-coordinator'})

    def test_foreign_container_labels_prevent_removal_and_fail_cleanup(self):
        docker = DockerCommands()
        start(self.plan, self.state, self.template, self.env, self.root / 'client.jar', docker)
        docker.foreign = True
        with self.assertRaisesRegex(ValueError, 'cleanup failed'):
            stop(self.state, docker)
        self.assertEqual(docker.removed, set())
        self.assertEqual(read_json(self.state)['status'], 'cleanup-failed')
