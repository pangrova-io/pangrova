#!/usr/bin/env python3
"""Execute complete, explicitly stepped TPC-derived suites through persistent clients."""

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
import json
import statistics
import subprocess
import sys
import tempfile
import threading
import time
import uuid

from tpc_common import ROOT, digest, positive, read_json, write_json
from tpc_contract import catalog, dataset, environment, identity, qualify_generated_dataset, workload
from tpc_clients import QueryFailure, connect
from tpc_results import compare_result, normalize_projections


def reference(path, work, data, binding):
    document = read_json(path)
    if (document.get('kind') != 'tpc-reference' or document.get('schemaVersion') != 1
            or document.get('status') != 'passed' or document.get('engines') != ['spark', 'trino']):
        raise ValueError('measurement requires an agreed Spark + Trino reference')
    for field, expected in (('workloadId', work['workloadId']), ('datasetId', data['datasetId']),
                            ('bindingId', binding['bindingId'])):
        if document.get(field) != expected:
            raise ValueError('reference workload/dataset/snapshot mismatch')
    expected = {q['id']: len(q['steps']) for q in work['queries']}
    if {k: len(v) for k, v in document.get('queries', {}).items()} != expected:
        raise ValueError('reference must cover every step of every template')
    if document.get('referenceId') != digest({k: v for k, v in document.items() if k != 'referenceId'}):
        raise ValueError('reference digest mismatch')
    return document


def verify_catalog_process(args):
    # A verifier SparkContext must never be reused as the benchmark SparkContext.
    with tempfile.TemporaryFile() as diagnostics:
        completed = subprocess.run([sys.executable, str(ROOT / 'scripts/tpc_catalog.py'), 'verify',
                                    '--dataset', str(args.dataset), '--binding', str(args.binding),
                                    '--spark-config', str(args.catalog_verifier)], stdout=diagnostics,
                                   stderr=diagnostics, timeout=600)
    if completed.returncode:
        raise ValueError('shared catalog verification failed; run tpc_catalog.py verify for diagnostics')


def run_window(work, sql_root, env, config, expected=None, factory=connect, binding=None, on_record=None):
    """One window; all streams finish warmup before measurement starts."""
    streams = config['streams']
    clients = []
    records, used_ids, lock = [], set(), threading.Lock()
    warmup_failed = threading.Event()
    snapshot_observations = []
    snapshot_error = None
    cleanup_errors = []
    try:
        for _ in range(streams):
            clients.append(factory(env))
        if binding is not None:
            from tpc_catalog import verify_client
            for stream, client in enumerate(clients):
                snapshot_observations.append({'phase': 'before', 'stream': stream,
                                             'tables': verify_client(client, env['engine']['name'], env['client'],
                                                                     binding, config['queryTimeout'])})

        def run_stream(stream, phase, iterations):
            queries = work['queries'][stream % len(work['queries']):] + work['queries'][:stream % len(work['queries'])]
            for iteration in range(iterations):
                for query in queries:
                    started = time.perf_counter_ns()
                    record = {'query': query['id'], 'stream': stream, 'round': iteration,
                              'phase': phase, 'status': 'passed', 'steps': []}
                    try:
                        for index, step in enumerate(query['steps']):
                            sql = (Path(sql_root) / step['file']).read_text(encoding='utf-8')
                            outcome = clients[stream].execute(sql, config['queryTimeout'])
                            if (outcome.get('state') != 'FINISHED' or outcome.get('resultConsumed') is not True
                                    or not outcome.get('queryId')):
                                raise QueryFailure('incomplete-execution', outcome.get('queryId'))
                            positive(outcome.get('elapsedMs'), 'elapsed query time')
                            with lock:
                                if outcome['queryId'] in used_ids:
                                    raise QueryFailure('replayed-query-identity', outcome['queryId'])
                                used_ids.add(outcome['queryId'])
                            outcome['sqlSha256'] = step['sqlSha256']
                            if 'projectionNames' in step:
                                normalize_projections(outcome['result'], step['projectionNames'])
                            record['steps'].append(outcome)
                            if expected:
                                compare_result(expected['queries'][query['id']][index], outcome['result'], step['order'])
                        record['elapsedMs'] = sum(s['elapsedMs'] for s in record['steps'])
                    except Exception as error:
                        record.update(status='failed', failurePhase=phase,
                                      error=str(error) if isinstance(error, (ValueError, QueryFailure)) else type(error).__name__,
                                      failedQueryId=getattr(error, 'query_id', None),
                                      cancellation=getattr(error, 'cancellation', 'not-requested'),
                                      resourceDrain='unverified',
                                      elapsedMs=(time.perf_counter_ns() - started) / 1000000)
                    with lock:
                        records.append(record)
                        if on_record is not None:
                            on_record(record)
                    if record['status'] == 'failed':
                        if phase == 'warmup':
                            warmup_failed.set()
                        return

        with ThreadPoolExecutor(max_workers=streams, thread_name_prefix='tpc-stream') as executor:
            for task in [executor.submit(run_stream, s, 'warmup', config['warmup']) for s in range(streams)]:
                task.result()
            if not warmup_failed.is_set():
                started = time.perf_counter_ns()
                for task in [executor.submit(run_stream, s, 'measurement', config['iterations']) for s in range(streams)]:
                    task.result()
                wall_ms = (time.perf_counter_ns() - started) / 1000000
            else:
                wall_ms = None
        if binding is not None and not any(r['status'] == 'failed' for r in records):
            try:
                for stream, client in enumerate(clients):
                    snapshot_observations.append({'phase': 'after', 'stream': stream,
                                                 'tables': verify_client(client, env['engine']['name'], env['client'],
                                                                         binding, config['queryTimeout'])})
            except Exception as error:
                snapshot_error = str(error) if isinstance(error, ValueError) else type(error).__name__
    finally:
        for client in clients:
            try:
                client.close()
            except Exception as error:
                cleanup_errors.append(type(error).__name__)
    measured = [r for r in records if r['phase'] == 'measurement']
    expected_count = streams * config['iterations'] * len(work['queries'])
    passed = (len(measured) == expected_count and all(r['status'] == 'passed' for r in records)
              and snapshot_error is None and not cleanup_errors)
    latencies = {}
    for query in work['queries']:
        samples = [r['elapsedMs'] for r in measured if r['query'] == query['id'] and r['status'] == 'passed']
        latencies[query['id']] = {'samples': samples, 'sampleCount': len(samples),
                                 'medianMs': statistics.median(samples) if samples else None}
    completed_count = sum(r['status'] == 'passed' for r in measured)
    summary = {'expected': expected_count, 'attempted': len(measured), 'completed': completed_count,
               'failed': sum(r['status'] == 'failed' for r in records),
               'measurementFailureRate': (len(measured) - completed_count) / len(measured) if measured else None,
               'driverMeasuredWallMs': wall_ms,
               'completedTemplatesPerHour': completed_count * 3600000 / wall_ms if wall_ms else None,
               'timingNote': 'wall time includes driver validation and inter-query overhead; not a TPC metric'}
    return {'status': 'passed' if passed else 'failed', 'records': records,
            'clientRuntimes': [getattr(c, 'runtime_info', {}) for c in clients],
            'clientSnapshotsVerified': binding is not None and snapshot_error is None and passed,
            'snapshotObservations': snapshot_observations, 'snapshotError': snapshot_error,
            'cleanupErrors': cleanup_errors,
            'queries': latencies, 'summary': summary}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workload', required=True, type=Path)
    parser.add_argument('--dataset', required=True, type=Path)
    parser.add_argument('--binding', required=True, type=Path)
    parser.add_argument('--environment', required=True, type=Path)
    parser.add_argument('--catalog-verifier', required=True, type=Path, help='Spark configuration to verify shared snapshots')
    parser.add_argument('--reference', type=Path)
    parser.add_argument('--purpose', choices=('reference', 'measurement'), default='measurement')
    parser.add_argument('--streams', type=int, default=1)
    parser.add_argument('--warmup', type=int, default=1)
    parser.add_argument('--iterations', type=int, default=5)
    parser.add_argument('--query-timeout', type=int, default=600)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--preflight-only', action='store_true')
    args = parser.parse_args()
    journal_path = args.output.with_suffix('.events.jsonl')
    if args.output.exists() or journal_path.exists():
        parser.error('output exists; every independent window requires a new result file')
    document = {'schemaVersion': 1, 'kind': 'tpc-suite-window', 'runId': str(uuid.uuid4()),
                'timestamp': datetime.now(timezone.utc).isoformat(), 'purpose': args.purpose, 'status': 'failed'}
    try:
        for value in (args.streams, args.iterations, args.query_timeout):
            positive(value, 'run setting', integer=True)
        if args.warmup < 0:
            raise ValueError('warmup must be nonnegative')
        work, data, env = workload(args.workload), dataset(args.dataset), environment(args.environment)
        binding = catalog(args.binding, data)
        qualify_generated_dataset(data)
        if work['benchmark'] != data['benchmark'] or work['scaleFactor'] != data['scaleFactor']:
            raise ValueError('workload and actual dataset benchmark/scale differ')
        if data.get('generator', {}).get('provenance') == 'unverified-external-input':
            raise ValueError('scale measurements require verified generator provenance')
        if env['engine']['name'] == 'spark' and args.streams > 1:
            if env['client']['sparkConf'].get('spark.scheduler.mode') != 'FAIR':
                raise ValueError('concurrent Spark streams require explicit FAIR scheduling')
        expected = reference(args.reference, work, data, binding) if args.reference else None
        if args.purpose == 'measurement' and expected is None:
            raise ValueError('measurement requires --reference')
        if args.purpose == 'reference' and (args.streams != 1 or args.iterations != 1):
            raise ValueError('reference capture requires one stream and one measured iteration')
        config = {'streams': args.streams, 'warmup': args.warmup, 'iterations': args.iterations,
                  'queryTimeout': args.query_timeout, 'ordering': 'ascending-template-with-stream-rotation'}
        document.update(benchmark=work['benchmark'], scaleFactor=work['scaleFactor'],
                        identity=identity(work, data, binding, env, config),
                        referenceId=expected['referenceId'] if expected else None)
        if args.preflight_only:
            document.update(status='preflight-only', measurementExecuted=False)
        else:
            document.update(status='in-progress', eventJournal=journal_path.name)
            write_json(args.output, document)
            verify_catalog_process(args)
            with journal_path.open('x', encoding='utf-8') as journal:
                def checkpoint(record):
                    receipt = {k: v for k, v in record.items() if k != 'steps'}
                    receipt['steps'] = [{k: v for k, v in step.items() if k not in ('result', 'serverStats', 'profile')}
                                        | {'resultSha256': digest(step['result'])} for step in record['steps']]
                    journal.write(json.dumps(receipt, allow_nan=False) + '\n')
                    journal.flush()
                document.update(run_window(work, args.workload.parent, env, config, expected,
                                           binding=binding, on_record=checkpoint))
            # Snapshot change invalidates the entire window, even if all queries returned values.
            verify_catalog_process(args)
            document['snapshotVerifiedBeforeAndAfter'] = True
    except Exception as error:
        document.update(status='failed', error=str(error) if isinstance(error, ValueError) else type(error).__name__)
        if isinstance(error, QueryFailure):
            document.update(error=str(error), failedQueryId=error.query_id, cancellation=error.cancellation,
                            resourceDrain='unverified')
    write_json(args.output, document)
    print(f"{document['status']}: {args.output}")
    return 0 if document['status'] in ('passed', 'preflight-only') else 2


if __name__ == '__main__':
    sys.exit(main())
