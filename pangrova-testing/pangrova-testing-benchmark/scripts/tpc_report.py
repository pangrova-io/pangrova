#!/usr/bin/env python3
"""Summarize real windows, cross-engine ratios and fixed-scale expansion efficiency."""

import argparse
from collections import defaultdict
from pathlib import Path
import statistics
import sys

from tpc_common import digest, positive, read_json, write_json
from tpc_suite_compare import experiment_identity, windows


def homogeneous_worker_hosts(left, right):
    """Scaling ratios require the same per-host hardware and per-worker budgets."""
    fields = ('cpuModel', 'cpus', 'memoryBytes', 'storage', 'network')
    profiles = []
    for env in (left, right):
        inventory = {host['host']: host for host in env['hardware']}
        for worker in env['workers']:
            host = inventory.get(worker['host'], {})
            if any(field not in host for field in fields):
                return False
            profiles.append({field: host[field] for field in fields})
    budgets = ('coordinatorMemoryBytes', 'workerHeapBytes', 'workerNativeBytes',
               'workerDirectBytes', 'workerOverheadBytes', 'backgroundMemoryBytes')
    return (all(profile == profiles[0] for profile in profiles)
            and all(key in left['resources'] and key in right['resources']
                    and left['resources'][key] == right['resources'][key] for key in budgets))


def report(documents):
    groups, failures = defaultdict(list), []
    for document in documents:
        if document.get('status') != 'passed':
            failures.append({'runId': document.get('runId'), 'status': document.get('status'),
                             'error': document.get('error'), 'summary': document.get('summary')})
        else:
            groups[digest(experiment_identity(document['identity']))].append(document)
    experiments = []
    for group in groups.values():
        summaries, _ = windows(group)
        first = group[0]
        identity, env = first['identity'], first['identity']['environment']
        wall = statistics.median(s['suiteWallMs'] for s in summaries)
        cpu = positive(env['resources']['totalCpu'], 'total CPU')
        workers = len(env['workers'])
        hourly = env.get('costPerHour')
        if hourly is not None:
            positive(hourly, 'declared infrastructure cost/hour')
        samples = first['identity']['config']['streams'] * first['identity']['config']['iterations'] * len(first['identity']['querySteps'])
        experiments.append({'engine': env['engine'], 'sourceRevision': env['sourceRevision'],
                            'benchmark': first['benchmark'], 'scaleFactor': first['scaleFactor'],
                            'workers': workers, 'multiHost': env.get('multiHost', False),
                            'workerHosts': len({w.get('host') for w in env['workers']}),
                            'streams': identity['config']['streams'], 'suiteWallMs': wall,
                            'queryMediansMs': {q: statistics.median(s['queries'][q] for s in summaries)
                                               for q in summaries[0]['queries']},
                            'completedTemplatesPerHour': samples * 3600000 / wall,
                            'allocatedCoreSecondsPerMeasuredSuite': wall / 1000 * cpu / identity['config']['iterations'],
                            'declaredCostPerMeasuredSuite': wall / 3600000 * hourly / identity['config']['iterations'] if hourly else None,
                            'identity': identity, 'referenceId': first['referenceId'],
                            'runIds': [d['runId'] for d in group]})
    comparisons = []
    for index, left in enumerate(experiments):
        for right in experiments[index + 1:]:
            li, ri = left['identity'], right['identity']
            same_workload = all(li[k] == ri[k] for k in ('workloadId', 'datasetId', 'bindingId', 'harnessId', 'config', 'timingScope'))
            if not same_workload or left['referenceId'] != right['referenceId']:
                continue
            le, re = li['environment'], ri['environment']
            if le['cachePolicy'] != re['cachePolicy']:
                continue
            if left['engine']['name'] != right['engine']['name']:
                equal_resources = (le['hardware'] == re['hardware'] and all(
                    le['resources'][k] == re['resources'][k] for k in ('totalCpu', 'totalMemoryBytes')))
                if not equal_resources:
                    continue
                comparisons.append({'kind': 'cross-engine', 'leftRunIds': left['runIds'], 'rightRunIds': right['runIds'],
                                    'leftOverRightSuiteTime': left['suiteWallMs'] / right['suiteWallMs'],
                                    'leftOverRightQueryTimes': {q: left['queryMediansMs'][q] / right['queryMediansMs'][q]
                                                               for q in left['queryMediansMs']}})
            elif (left['workerHosts'] == left['workers'] and right['workerHosts'] == right['workers']
                  and left['workers'] != right['workers']
                  and left['engine'] == right['engine'] and le['settings'] == re['settings']
                  and homogeneous_worker_hosts(le, re)):
                small, large = sorted((left, right), key=lambda e: e['workers'])
                speedup = small['suiteWallMs'] / large['suiteWallMs']
                cpu_ratio = large['identity']['environment']['resources']['totalCpu'] / small['identity']['environment']['resources']['totalCpu']
                comparisons.append({'kind': 'strong-scaling', 'referenceWorkers': small['workers'],
                                    'workers': large['workers'], 'speedup': speedup,
                                    'cpuEfficiency': speedup / cpu_ratio,
                                    'referenceRunIds': small['runIds'], 'runIds': large['runIds']})
    return {'schemaVersion': 1, 'kind': 'tpc-engineering-report', 'experiments': experiments,
            'comparisons': comparisons, 'failedOrIncompleteWindows': failures,
            'interpretation': 'TPC-derived engineering results; costs are declared infrastructure costs, not official TPC pricing'}


def markdown(document):
    lines = ['# TPC-derived engineering results', '', document['interpretation'], '',
             '| Engine | SF | Workers | Streams | Median window ms | Templates/hour |',
             '|---|---:|---:|---:|---:|---:|']
    for experiment in document['experiments']:
        lines.append(f"| {experiment['engine']['name']} {experiment['engine']['version']} | {experiment['scaleFactor']} | "
                     f"{experiment['workers']} | {experiment['streams']} | {experiment['suiteWallMs']:.3f} | "
                     f"{experiment['completedTemplatesPerHour']:.3f} |")
    lines += ['', f"Failed/incomplete windows retained: {len(document['failedOrIncompleteWindows'])}", '',
              'Comparisons are emitted only for compatible experiments; see the JSON for fingerprints, query ratios and scaling reference sizes.']
    return '\n'.join(lines) + '\n'


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--windows', type=Path, nargs='+', required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--markdown', type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.output.exists() or args.markdown.exists() or args.output.resolve() == args.markdown.resolve():
            raise ValueError('report destinations must be distinct new files')
        document = report([read_json(p) for p in args.windows])
        write_json(args.output, document)
        args.markdown.parent.mkdir(parents=True, exist_ok=True)
        args.markdown.write_text(markdown(document))
        return 1 if document['failedOrIncompleteWindows'] else 0
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f'INVALID_REPORT_INPUT: {error}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main())
