#!/usr/bin/env python3
"""Publish a reference only after independent Spark and Trino results agree completely."""

import argparse
from pathlib import Path
import sys

from tpc_common import digest, read_json, write_json
from tpc_contract import workload
from tpc_results import compare_result
from tpc_suite_compare import validate_window


def agree(work, spark, trino):
    for engine, document in (('spark', spark), ('trino', trino)):
        validate_window(document, purpose='reference')
        identity = document['identity']
        if identity['environment']['engine']['name'] != engine or identity['workloadId'] != work['workloadId']:
            raise ValueError('reference engine/workload identity mismatch')
        if identity['config']['streams'] != 1 or identity['config']['iterations'] != 1:
            raise ValueError('reference capture must have one stream and one measured iteration')
    for key in ('workloadId', 'datasetId', 'bindingId'):
        if spark['identity'][key] != trino['identity'][key]:
            raise ValueError('reference sources must use the same data and snapshot')
    sources = []
    all_ids = set()
    for document in (spark, trino):
        ids = {s['queryId'] for r in document['records'] for s in r['steps']}
        if all_ids & ids:
            raise ValueError('independent reference engines reused executions')
        all_ids.update(ids)
        sources.append({r['query']: r for r in document['records'] if r['phase'] == 'measurement'})
    results = {}
    for query in work['queries']:
        results[query['id']] = []
        for index, step in enumerate(query['steps']):
            left, right = (s[query['id']]['steps'][index]['result'] for s in sources)
            compare_result(left, right, step['order'])
            results[query['id']].append(left)
    document = {'schemaVersion': 1, 'kind': 'tpc-reference', 'status': 'passed',
                'engines': ['spark', 'trino'], 'workloadId': work['workloadId'],
                'datasetId': spark['identity']['datasetId'], 'bindingId': spark['identity']['bindingId'],
                'sourceRuns': [{'runId': d['runId'], 'engine': d['identity']['environment']['engine'],
                                'snapshotObservations': d['snapshotObservations'],
                                'executionIds': [s['queryId'] for r in d['records'] for s in r['steps']]}
                               for d in (spark, trino)], 'queries': results}
    document['referenceId'] = digest(document)
    return document


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--workload', type=Path, required=True)
    parser.add_argument('--spark', type=Path, required=True)
    parser.add_argument('--trino', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.output.exists():
            raise ValueError('reference already exists; never overwrite an accepted reference')
        document = agree(workload(args.workload), read_json(args.spark), read_json(args.trino))
        write_json(args.output, document)
        print('Spark and Trino full results agree')
        return 0
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f'INVALID_REFERENCE: {error}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main())
