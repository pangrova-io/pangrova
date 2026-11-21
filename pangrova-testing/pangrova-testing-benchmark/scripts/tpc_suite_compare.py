#!/usr/bin/env python3
"""Compare three independent full-suite windows; preserve strict tail gates separately."""

import argparse
from decimal import Decimal
import copy
from pathlib import Path
import statistics
import sys

from tpc_common import positive, read_json, write_json
from tpc_common import table_definitions


class Incompatible(ValueError):
    pass


def comparable_identity(identity):
    """The engine build is the treatment, not a reason to reject a regression run."""
    controlled = copy.deepcopy(identity)
    env = controlled['environment']
    env.pop('sourceRevision', None)
    env['engine'] = {'name': env['engine']['name']}
    if 'workers' in env:
        # Container IDs, PIDs and AUTO ports are observations, not stable topology keys.
        env['workers'] = sorted([{'host': worker['host']} for worker in env['workers']], key=lambda w: w['host'])
    return controlled


def experiment_identity(identity):
    controlled = comparable_identity(identity)
    for field in ('engine', 'sourceRevision'):
        controlled['environment'][field] = identity['environment'][field]
    return controlled


def validate_window(document, purpose='measurement'):
    if (document.get('schemaVersion') != 1 or document.get('kind') != 'tpc-suite-window'
            or document.get('purpose') != purpose or document.get('status') != 'passed'
            or document.get('snapshotVerifiedBeforeAndAfter') is not True
            or document.get('clientSnapshotsVerified') is not True or not document.get('runId')):
        raise ValueError('failed, incomplete or unverified window')
    identity = document['identity']
    if identity.get('timingScope') != 'persistent-client-end-to-end':
        raise ValueError('incompatible timing scope')
    config = identity['config']
    if not identity.get('harnessId'):
        raise ValueError('measurement harness identity is required')
    for key in ('iterations', 'streams', 'queryTimeout'):
        positive(config.get(key), f'config {key}', integer=True)
    runtimes = document.get('clientRuntimes')
    if not isinstance(runtimes, list) or len(runtimes) != config['streams'] or any(not r for r in runtimes):
        raise ValueError('every stream requires an observed client runtime')
    if type(config.get('warmup')) is not int or config['warmup'] < (1 if purpose == 'measurement' else 0):
        raise ValueError('window warmup is insufficient')
    if purpose == 'measurement' and (config['iterations'] < 5 or not document.get('referenceId')):
        raise ValueError('ordinary calibration requires at least five complete measured rounds and a reference')
    expected_queries = {f'q{i:02d}' for i in range(1, (22 if document['benchmark'] == 'tpch' else 99) + 1)}
    steps = identity.get('querySteps', {})
    if set(steps) != expected_queries or any(not s for s in steps.values()):
        raise ValueError('incomplete template/step inventory')
    seen, query_ids = set(), set()
    snapshots = identity.get('snapshots', {})
    if set(snapshots) != set(table_definitions(document['benchmark'])):
        raise ValueError('complete table snapshot identities are required')
    observations = document.get('snapshotObservations', [])
    expected_observations = {(phase, stream) for phase in ('before', 'after') for stream in range(config['streams'])}
    if (len(observations) != len(expected_observations)
            or {(o['phase'], o['stream']) for o in observations} != expected_observations):
        raise ValueError('SUT before/after snapshot receipts are incomplete')
    for observation in observations:
        if len(observation['tables']) != len(snapshots) or {o['table'] for o in observation['tables']} != set(snapshots):
            raise ValueError('SUT snapshot table coverage differs from binding')
        for observed in observation['tables']:
            if (observed['snapshotId'] != snapshots[observed['table']]['snapshotId']
                    or not observed.get('queryId') or observed['queryId'] in query_ids):
                raise ValueError('SUT snapshot changed or reused a metadata execution receipt')
            query_ids.add(observed['queryId'])
    measured = {query: [] for query in expected_queries}
    total_latency = 0
    for record in document.get('records', []):
        phase, query = record['phase'], record['query']
        if phase not in ('warmup', 'measurement') or query not in steps or record.get('status') != 'passed':
            raise ValueError('failed or unrecognized execution')
        rounds = config['warmup'] if phase == 'warmup' else config['iterations']
        if (type(record['stream']) is not int or not 0 <= record['stream'] < config['streams']
                or type(record['round']) is not int or not 0 <= record['round'] < rounds):
            raise ValueError('invalid stream/round identity')
        execution = (phase, query, record['stream'], record['round'])
        if execution in seen:
            raise ValueError('duplicate template execution')
        seen.add(execution)
        outcomes = record.get('steps', [])
        if len(outcomes) != len(steps[query]):
            raise ValueError('missing query step')
        latency = 0
        for outcome, sql_hash in zip(outcomes, steps[query]):
            query_id = outcome.get('queryId')
            if (not isinstance(query_id, str) or not query_id.strip() or query_id in query_ids
                    or outcome.get('state') != 'FINISHED' or outcome.get('resultConsumed') is not True
                    or outcome.get('sqlSha256') != sql_hash):
                raise ValueError('invalid/replayed execution receipt or SQL digest')
            query_ids.add(query_id)
            latency += positive(outcome.get('elapsedMs'), 'step elapsed time')
        if phase == 'measurement':
            measured[query].append(latency)
            total_latency += latency
    expected_count = len(steps) * config['streams'] * (config['warmup'] + config['iterations'])
    if len(seen) != expected_count:
        raise ValueError('incomplete measured/warmup query coverage')
    wall = positive(document.get('summary', {}).get('driverMeasuredWallMs'), 'suite wall time')
    if max(max(v) for v in measured.values()) > wall:
        raise ValueError('suite wall time is shorter than a completed query')
    return {'queries': {q: statistics.median(v) for q, v in measured.items()},
            'suiteWallMs': wall, 'queryIds': query_ids, 'totalClientMs': total_latency}


def windows(documents):
    if len(documents) != 3:
        raise ValueError('calibration/comparison requires exactly three independent windows')
    identities = [experiment_identity(d['identity']) for d in documents]
    if any(i != identities[0] for i in identities[1:]):
        raise Incompatible('window workload/data/snapshot/environment/config identities differ')
    if len({d['runId'] for d in documents}) != 3:
        raise ValueError('independent windows require distinct run IDs')
    if len({d['referenceId'] for d in documents}) != 1:
        raise Incompatible('windows used different correctness references')
    summaries = [validate_window(d) for d in documents]
    if any(d.get('clientRuntimes') != documents[0].get('clientRuntimes') for d in documents[1:]):
        raise Incompatible('client runtime versions differ across windows')
    all_ids = set()
    for summary in summaries:
        if all_ids & summary['queryIds']:
            raise ValueError('execution IDs replayed across windows')
        all_ids.update(summary['queryIds'])
    return summaries, all_ids


def compare(baseline, current, threshold=0.15):
    if type(threshold) not in (int, float) or not 0 <= threshold < 1:
        raise ValueError('regression threshold must be in [0,1)')
    base, base_ids = windows(baseline)
    now, now_ids = windows(current)
    if (comparable_identity(baseline[0]['identity']) != comparable_identity(current[0]['identity'])
            or baseline[0]['referenceId'] != current[0]['referenceId']
            or baseline[0].get('clientRuntimes') != current[0].get('clientRuntimes')):
        raise Incompatible('baseline and current identities differ; do not compare different experiments')
    if base_ids & now_ids or {d['runId'] for d in baseline} & {d['runId'] for d in current}:
        raise ValueError('current measurements reuse baseline executions')
    comparisons = []
    keys = list(base[0]['queries']) + ['suiteWallMs']
    for key in sorted(keys):
        old = [s['suiteWallMs'] if key == 'suiteWallMs' else s['queries'][key] for s in base]
        new = [s['suiteWallMs'] if key == 'suiteWallMs' else s['queries'][key] for s in now]
        center = statistics.median(old)
        ratios = [(Decimal(str(value)) - Decimal(str(center))) / Decimal(str(center)) for value in new]
        limit = Decimal(str(threshold))
        baseline_spread = (max(old) - min(old)) / center
        if baseline_spread > threshold:
            status = 'needs-review'
        elif all(change > limit for change in ratios):
            status = 'regression'
        elif any(change > limit for change in ratios):
            status = 'needs-review'
        else:
            status = 'within-threshold'
        comparisons.append({'metric': key, 'baselineWindowValuesMs': old, 'currentWindowValuesMs': new,
                            'baselineMedianMs': center, 'changes': [float(c) for c in ratios], 'status': status})
    statuses = {c['status'] for c in comparisons}
    status = 'regression' if 'regression' in statuses else 'needs-review' if 'needs-review' in statuses else 'within-threshold'
    return {'schemaVersion': 1, 'kind': 'tpc-suite-comparison', 'status': status,
            'threshold': threshold, 'interpretation': 'descriptive repeated-window gate; not a significance test or TPC score',
            'comparisons': comparisons}


def load_windows(paths):
    documents = [read_json(p) for p in paths]
    if len(documents) == 1 and documents[0].get('kind') == 'tpc-suite-baseline':
        return documents[0]['windows']
    return documents


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline', type=Path, nargs='+')
    parser.add_argument('--current', type=Path, nargs='+', required=True)
    parser.add_argument('--calibrate', action='store_true')
    parser.add_argument('--threshold', type=float, default=0.15)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    inputs = (args.baseline or []) + args.current
    if args.output.resolve() in {p.resolve() for p in inputs} or args.output.exists():
        parser.error('comparison output must be new and must not overwrite evidence')
    try:
        current = load_windows(args.current)
        if args.calibrate:
            windows(current)
            report = {'schemaVersion': 1, 'kind': 'tpc-suite-baseline', 'windows': current}
            code = 0
        elif not args.baseline or any(not p.is_file() for p in args.baseline):
            report, code = {'status': 'uncalibrated', 'error': 'supply three real matching windows; historical numbers are not calibration'}, 4
        else:
            report = compare(load_windows(args.baseline), current, args.threshold)
            code = {'within-threshold': 0, 'regression': 1, 'needs-review': 3}[report['status']]
    except Incompatible as error:
        report, code = {'status': 'incompatible', 'error': str(error)}, 5
    except (OSError, ValueError, KeyError, TypeError) as error:
        report, code = {'status': 'invalid-evidence', 'error': str(error)}, 2
    write_json(args.output, report)
    print(report.get('status', 'calibrated'))
    return code


if __name__ == '__main__':
    sys.exit(main())
