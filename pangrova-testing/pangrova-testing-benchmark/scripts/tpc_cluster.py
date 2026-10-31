#!/usr/bin/env python3
"""Own benchmark containers on explicit Docker contexts; never infer a multi-host cluster."""

import argparse
import json
from pathlib import Path
import subprocess
import sys
import uuid
from urllib.parse import urlsplit

from tpc_common import file_digest, read_json, write_json


def command(arguments, json_output=False):
    completed = subprocess.run(arguments, check=True, stdout=subprocess.PIPE, text=True)
    return json.loads(completed.stdout) if json_output else completed.stdout.strip()


def docker(context):
    return ['docker', '--context', context]


def deployment(path):
    document = read_json(path)
    if document.get('schemaVersion') != 1 or not document.get('nodes') or not document.get('sourceRevision'):
        raise ValueError('deployment requires version, source revision and explicit nodes')
    services = set()
    for node in document['nodes']:
        if not Path(node['composeFile']).is_file() or not node.get('context'):
            raise ValueError('each deployment node requires a Docker context and existing compose file')
        for service, info in node['services'].items():
            key = node['context'], service
            if key in services or info.get('role') not in ('coordinator', 'worker') or not info.get('endpoint'):
                raise ValueError('invalid/duplicate deployment service, role or endpoint')
            if info['endpoint'] == 'AUTO' and not node.get('clientHost'):
                raise ValueError('AUTO endpoints require a client-routable Docker host address')
            services.add(key)
    roles = [info['role'] for n in document['nodes'] for info in n['services'].values()]
    if roles.count('coordinator') != 1 or 'worker' not in roles:
        raise ValueError('benchmark deployment requires one coordinator and at least one worker')
    return document


def validate_capacity(plan, template, run=command):
    requested, capacity = {}, {}
    for node in plan['nodes']:
        base = docker(node['context'])
        host = run(base + ['info', '--format', '{{.Name}}'])
        available = {'memory': int(run(base + ['info', '--format', '{{.MemTotal}}'])),
                     'cpu': int(run(base + ['info', '--format', '{{.NCPU}}']))}
        capacity[host] = available
        request = requested.setdefault(host, {'memory': 0, 'cpu': 0})
        resolved = run(base + ['compose', '-f', node['composeFile'], 'config', '--format', 'json'], True)['services']
        for name, service in node['services'].items():
            definition = resolved[name]
            if definition.get('container_name'):
                raise ValueError('benchmark services must use project-owned names, never fixed container_name')
            memory, cpu = int(definition.get('mem_limit', 0)), float(definition.get('cpus', 0))
            if memory <= 0 or cpu <= 0:
                raise ValueError('every benchmark service requires explicit mem_limit and cpus')
            request['memory'] += memory
            request['cpu'] += cpu
            if service['role'] == 'coordinator':
                request['memory'] += template['resources']['backgroundMemoryBytes']
    for host, request in requested.items():
        if any(request[key] > capacity[host][key] for key in ('memory', 'cpu')):
            raise ValueError('requested containers/background services exceed actual Docker host capacity')


def start(deployment_path, state_path, template_path, environment_path, client_jar, run=command):
    plan = deployment(deployment_path)
    if Path(state_path).exists() or Path(environment_path).exists() or Path(client_jar).exists():
        raise ValueError('state, environment and client JAR outputs must be new')
    template = read_json(template_path)
    validate_capacity(plan, template, run)
    project = 'pangrova-tpc-' + uuid.uuid4().hex[:12]
    state = {'schemaVersion': 1, 'project': project, 'containers': [],
             'contexts': sorted({n['context'] for n in plan['nodes']}), 'status': 'starting'}
    write_json(state_path, state)
    hosts, image_ids, workers = set(), set(), []
    cpu_total, memory_total = 0, 0
    try:
        # Start the coordinator first, then remote worker processes on their configured contexts.
        jobs = [(node, name, info) for node in plan['nodes'] for name, info in node['services'].items()]
        jobs.sort(key=lambda j: j[2]['role'] != 'coordinator')
        coordinator = None
        for node, service, info in jobs:
            context = node['context']
            base = docker(context)
            daemon = run(base + ['info', '--format', '{{.ID}}'])
            host = run(base + ['info', '--format', '{{.Name}}'])
            hosts.add(host)
            compose = base + ['compose', '-f', node['composeFile'], '-p', project]
            resolved = run(compose + ['config', '--format', 'json'], True)['services'][service]
            image = run(base + ['image', 'inspect', resolved['image']], True)[0]
            if image['Config'].get('Labels', {}).get('org.opencontainers.image.revision') != plan['sourceRevision']:
                raise ValueError('container image does not match requested source revision')
            image_ids.add(image['Id'])
            if len(image_ids) > 1:
                raise ValueError('coordinator and workers must use the identical immutable image')
            expected_role = ['start', info['role']]
            if resolved.get('command', [])[:2] != expected_role:
                raise ValueError('compose service command does not match its declared process role')
            # Explicit services only; do not create implicit dependencies or touch existing projects.
            try:
                run(compose + ['up', '-d', '--no-deps', '--wait', '--wait-timeout', '180', service])
            finally:
                # A failed readiness wait can still have created a live container.
                container = run(compose + ['ps', '--all', '-q', service])
                if container:
                    record = {'context': context, 'containerId': container, 'service': service}
                    state['containers'].append(record)
                    write_json(state_path, state)
            if not container:
                raise ValueError('compose did not create the requested service')
            inspected = run(base + ['inspect', container], True)[0]
            if inspected['Image'] != image['Id'] or not inspected['State']['Running'] or inspected['State']['Pid'] <= 0:
                raise ValueError('container image/process identity mismatch')
            if inspected['State'].get('Health', {}).get('Status') != 'healthy':
                raise ValueError('every benchmark process requires a passing application health check')
            limits = inspected['HostConfig']
            if limits['Memory'] <= 0 or limits.get('NanoCpus', 0) <= 0:
                raise ValueError('benchmark containers require explicit memory and CPU limits')
            memory_total += limits['Memory']
            cpu_total += limits['NanoCpus'] / 1e9
            record.update(host=host, dockerDaemonId=daemon, process=inspected['State']['Pid'],
                          imageId=image['Id'], role=info['role'])
            endpoint = info['endpoint']
            if endpoint == 'AUTO':
                port = '8080/tcp' if info['role'] == 'coordinator' else '8081/tcp'
                mappings = inspected['NetworkSettings']['Ports'].get(port, [])
                if len(mappings) != 1:
                    raise ValueError('AUTO endpoint requires one dynamically mapped HTTP port')
                endpoint = f"http://{node['clientHost']}:{mappings[0]['HostPort']}"
            write_json(state_path, state)
            if info['role'] == 'worker':
                workers.append({'id': container, 'host': host, 'process': inspected['State']['Pid'],
                                'endpoint': endpoint})
                required = sum(template['resources'][k] for k in ('workerHeapBytes', 'workerNativeBytes', 'workerDirectBytes'))
                if required > limits['Memory']:
                    raise ValueError('worker component memory budget exceeds its enforced container limit')
            else:
                coordinator = (base, container)
                address = urlsplit(endpoint)
                if address.scheme not in ('http', 'https') or not address.hostname or not address.port:
                    raise ValueError('coordinator endpoint must be an explicit HTTP(S) address and port')
                template['client'].update(host=address.hostname, port=address.port, tls=address.scheme == 'https')
                if template['resources']['coordinatorMemoryBytes'] > limits['Memory']:
                    raise ValueError('coordinator memory budget exceeds container limit')
        if template['resources']['totalCpu'] != cpu_total or template['resources']['totalMemoryBytes'] < memory_total:
            raise ValueError('declared CPU/memory budget disagrees with enforced containers')
        Path(client_jar).parent.mkdir(parents=True, exist_ok=True)
        jar_path = plan['clientJarInImage']
        run(coordinator[0] + ['cp', f'{coordinator[1]}:{jar_path}', str(client_jar)])
        template['engine']['artifactSha256'] = next(iter(image_ids)).removeprefix('sha256:')
        template['sourceRevision'] = plan['sourceRevision']
        template['workers'] = workers
        template['multiHost'] = len(hosts) > 1
        if template['multiHost'] and len({w['host'] for w in workers}) != len(workers):
            raise ValueError('multi-host matrix requires one worker per independently identified Docker host')
        template['client'].update(classpath=str(Path(client_jar).resolve()), classpathSha256=file_digest(client_jar))
        from tpc_contract import environment
        write_json(environment_path, template)
        environment(environment_path)
        state['status'] = 'running'
        write_json(state_path, state)
        return state
    except Exception:
        state['status'] = 'failed'
        write_json(state_path, state)
        # Preserve state so the caller's always/finally cleanup owns every created container.
        raise


def stop(path, run=command):
    state = read_json(path)
    if state.get('status') == 'stopped':
        return
    failures = []
    for record in reversed(state['containers']):
        base = docker(record['context'])
        try:
            if not run(base + ['ps', '--all', '--quiet', '--filter', f"id={record['containerId']}"]):
                continue
            inspected = run(base + ['inspect', record['containerId']], True)[0]
            if inspected['Config']['Labels'].get('com.docker.compose.project') != state['project']:
                raise ValueError('refusing to remove a container not owned by this benchmark run')
            run(base + ['rm', '--force', record['containerId']])
        except (ValueError, subprocess.CalledProcessError) as error:
            failures.append(type(error).__name__)
    for context in state.get('contexts', {r['context'] for r in state['containers']}):
        base = docker(context)
        networks = run(base + ['network', 'ls', '--quiet', '--filter',
                                   f"label=com.docker.compose.project={state['project']}"])
        for network in networks.splitlines():
            try:
                run(base + ['network', 'rm', network])
            except subprocess.CalledProcessError as error:
                failures.append(type(error).__name__)
    state['status'] = 'cleanup-failed' if failures else 'stopped'
    write_json(path, state)
    if failures:
        raise ValueError('benchmark container cleanup failed; inspect retained state')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('start', 'stop'))
    parser.add_argument('--state', type=Path, required=True)
    parser.add_argument('--deployment', type=Path)
    parser.add_argument('--environment-template', type=Path)
    parser.add_argument('--environment-output', type=Path)
    parser.add_argument('--client-jar', type=Path)
    args = parser.parse_args()
    try:
        if args.action == 'start':
            if not all((args.deployment, args.environment_template, args.environment_output, args.client_jar)):
                raise ValueError('start requires deployment, environment template/output and client JAR destination')
            start(args.deployment, args.state, args.environment_template, args.environment_output, args.client_jar)
        else:
            stop(args.state)
        return 0
    except (OSError, ValueError, KeyError, subprocess.CalledProcessError) as error:
        print(f'Cluster {args.action} failed: {type(error).__name__}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main())
