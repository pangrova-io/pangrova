#!/usr/bin/env python3
"""Explicitly calibrate genuine complete CLI tail evidence and its controlled context."""

import argparse
import copy
from pathlib import Path
import sys

from tpc_common import read_json, write_json
from tpch_regression_validation import validate_results


def calibrate(results, context):
    if (context.get('kind') != 'tpc-tail-context' or context.get('schemaVersion') != 1
            or context.get('status') != 'passed' or context.get('scaleFactor') != results.get('scaleFactor')
            or context.get('identity', {}).get('timingScope') != 'cli-end-to-end'
            or not context.get('referenceId') or not context.get('clientRuntimes')
            or not context.get('snapshotsVerifiedAfterTail')):
        raise ValueError('complete successful tail context and correctness/snapshot checks required')
    baseline = copy.deepcopy(results)
    baseline['suiteContext'] = context
    baseline['regressionThreshold'] = 0.15
    for query, record in baseline['queries'].items():
        record['baselineP99Ms'] = record.pop('p99Ms')
        record['baselineP50Ms'] = record.pop('p50Ms')
        key = f'q{int(query[1:]):02d}'
        if context['identity']['querySteps'].get(key) != [record['sqlSha256']]:
            raise ValueError('tail SQL digests differ from the verified workload context')
    validate_results(baseline, results)
    return baseline


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--results', type=Path, required=True)
    parser.add_argument('--context', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.output.exists():
            raise ValueError('baseline destination exists; calibration never overwrites history')
        baseline = calibrate(read_json(args.results), read_json(args.context))
        write_json(args.output, baseline)
        return 0
    except (OSError, ValueError, KeyError, TypeError) as error:
        print(f'INVALID_TAIL_CALIBRATION: {error}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main())
