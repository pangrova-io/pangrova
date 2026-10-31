#!/usr/bin/env python3
"""Dedicated-host job: verified datasets, owned cluster, complete windows and comparison."""

import argparse
import os
import shlex
from pathlib import Path
import shutil
import subprocess
import sys

from tpc_common import ROOT, read_json, write_json
from tpc_contract import catalog, dataset, environment, identity, qualify_generated_dataset, workload
from generate_tpc_data import bootstrap, generate
from prepare_tpc_workload import generate as generate_workload
from tpc_cluster import start, stop
from tpc_suite_compare import comparable_identity, compare


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--benchmark', choices=('tpch', 'tpcds'), required=True)
    parser.add_argument('--scale-factor', type=int, default=100)
    parser.add_argument('--config-dir', type=Path, required=True)
    parser.add_argument('--data-dir', type=Path, default=Path('/data/tpc'))
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--source-revision', required=True)
    parser.add_argument('--streams', type=int, default=1)
    parser.add_argument('--profile', choices=('ordinary', 'tail'), default='ordinary')
    args = parser.parse_args()
    if args.output_dir.exists():
        parser.error('output directory must be new')
    args.output_dir.mkdir(parents=True)
    status, code, state_path = {'status': 'failed'}, 2, args.output_dir / 'cluster-state.json'
    try:
        if args.scale_factor < 1 or args.streams < 1:
            raise ValueError('scale and streams must be positive')
        config = args.config_dir.resolve()
        for name in ('deployment.json', 'environment-template.json', 'catalog-verifier.json'):
            if not (config / name).is_file():
                raise ValueError(f'missing dedicated-host configuration: {name}')
        binding = config / f'{args.benchmark}-sf{args.scale_factor}-binding.json'
        reference = config / f'{args.benchmark}-sf{args.scale_factor}-reference.json'
        if not binding.is_file() or not reference.is_file():
            raise ValueError('prepare the shared Iceberg binding and agreed Spark/Trino reference before measurement')
        data_path = args.data_dir / args.benchmark / f'sf{args.scale_factor}' / 'parquet/dataset.json'
        if not data_path.exists():
            args.data_dir.mkdir(parents=True, exist_ok=True)
            # Raw generator files, Parquet, and staging need independent capacity.
            if shutil.disk_usage(args.data_dir).free < args.scale_factor * 3 * 1000 ** 3:
                raise ValueError('insufficient free space: reserve at least 3x logical dataset size for generation')
            source = bootstrap(args.benchmark, args.data_dir / '.build', 8)
            generate(args.benchmark, args.scale_factor, args.data_dir, 8, 8, source)
        data = qualify_generated_dataset(dataset(data_path))
        if args.scale_factor == 100:
            work = ROOT / 'queries' / args.benchmark / 'workload.json'
        else:
            source = bootstrap(args.benchmark, args.data_dir / '.build', 8)
            prepared = args.output_dir / 'queries' / args.benchmark
            generate_workload(args.benchmark, source, prepared, args.scale_factor)
            work = prepared / 'workload.json'
        deployment = read_json(config / 'deployment.json')
        deployment['sourceRevision'] = args.source_revision
        write_json(args.output_dir / 'deployment.json', deployment)
        # Compose files select the same-source immutable image through the job's environment.
        env_path = args.output_dir / 'environment.json'
        start(args.output_dir / 'deployment.json', state_path, config / 'environment-template.json',
              env_path, args.output_dir / 'pangrova-client.jar')
        if args.profile == 'tail':
            if args.benchmark != 'tpch' or args.streams != 1:
                raise ValueError('strict tail profile supports the existing complete TPC-H serial contract only')
            env_doc = environment(env_path)
            env = env_doc['client']
            sanity_output = args.output_dir / 'tail-correctness.json'
            subprocess.run([sys.executable, str(ROOT / 'scripts/run_tpc_suite.py'), '--workload', str(work),
                            '--dataset', str(data_path), '--binding', str(binding), '--environment', str(env_path),
                            '--catalog-verifier', str(config / 'catalog-verifier.json'), '--reference', str(reference),
                            '--iterations', '1', '--streams', '1', '--output', str(sanity_output)], check=True)
            sanity = read_json(sanity_output)
            launcher = args.output_dir / 'tail-cli.sh'
            java = str(Path(os.environ['JAVA_HOME']) / 'bin/java') if os.environ.get('JAVA_HOME') else 'java'
            extra = ['--ssl', 'true'] if env.get('tls', True) else []
            launcher.write_text('#!/bin/sh\nexec ' + ' '.join(shlex.quote(v) for v in
                [java, '--add-opens=java.base/java.nio=ALL-UNNAMED', '-jar', str((args.output_dir / 'pangrova-client.jar').resolve()),
                 *extra]) + ' "$@"\n')
            launcher.chmod(0o700)
            output = args.output_dir / 'tail.json'
            command = [sys.executable, str(ROOT / 'scripts/run_tpc_benchmark.py'), '--benchmark', 'tpch',
                       '--scale-factor', str(args.scale_factor), '--iterations', '1000', '--warmup', '1',
                       '--host', env['host'], '--port', str(env.get('port', 8080)), '--catalog', env['catalog'],
                       '--schema', env['schema'], '--cli-path', str(launcher),
                       '--queries-dir', str(work.parent.parent), '--output', str(output)]
            if env.get('tokenEnv'):
                command += ['--token-env', env['tokenEnv']]
            subprocess.run(command, check=True)
            from tpc_catalog import verify_client
            from tpc_clients import connect
            observer = connect(env_doc)
            try:
                observations = verify_client(observer, 'pangrova', env, catalog(binding, data), 600)
            finally:
                observer.close()
            context_identity = identity(workload(work), data, catalog(binding, data), env_doc,
                                        {'warmup': 1, 'iterations': 1000, 'queryTimeout': 300, 'timeout': 600})
            context_identity['timingScope'] = 'cli-end-to-end'
            context = {'schemaVersion': 1, 'kind': 'tpc-tail-context', 'status': 'passed',
                       'scaleFactor': args.scale_factor, 'identity': context_identity,
                       'referenceId': sanity['referenceId'], 'correctnessRunId': sanity['runId'],
                       'clientRuntimes': sanity['clientRuntimes'], 'snapshotsVerifiedAfterTail': True,
                       'snapshotObservations': observations}
            write_json(args.output_dir / 'tail-context.json', context)
            baseline = config / f'tpch-sf{args.scale_factor}-tail-baseline.json'
            if not baseline.is_file():
                status, code = {'status': 'uncalibrated', 'error': 'strict tail baseline missing'}, 4
            else:
                previous = read_json(baseline).get('suiteContext', {})
                if (previous.get('kind') != 'tpc-tail-context' or previous.get('status') != 'passed'
                        or previous.get('snapshotsVerifiedAfterTail') is not True
                        or comparable_identity(previous['identity']) != comparable_identity(context_identity)
                        or previous.get('referenceId') != context['referenceId']
                        or previous.get('clientRuntimes') != context['clientRuntimes']):
                    raise ValueError('tail baseline is missing or incompatible with controlled context')
                # Preserve the original strict tail comparator and exit semantics.
                completed = subprocess.run(['bash', str(ROOT / 'scripts/tpch-regression-check.sh'),
                                            str(output), str(baseline)])
                code = completed.returncode
                status = {'status': 'within-threshold' if code == 0 else 'tail-comparison-failed'}
        else:
            documents = []
            for window in range(1, 4):
                output = args.output_dir / f'window-{window}.json'
                completed = subprocess.run([
                    sys.executable, str(ROOT / 'scripts/run_tpc_suite.py'), '--workload', str(work),
                    '--dataset', str(data_path), '--binding', str(binding), '--environment', str(env_path),
                    '--catalog-verifier', str(config / 'catalog-verifier.json'), '--reference', str(reference),
                    '--streams', str(args.streams), '--warmup', '1', '--iterations', '5', '--output', str(output)])
                if completed.returncode:
                    raise ValueError(f'window {window} failed; retained failure evidence must be investigated')
                documents.append(read_json(output))
            baseline = config / f'{args.benchmark}-sf{args.scale_factor}-streams{args.streams}-baseline.json'
            if not baseline.is_file():
                status, code = {'status': 'uncalibrated', 'error': 'three real windows collected; explicitly calibrate before comparison'}, 4
            else:
                status = compare(read_json(baseline)['windows'], documents)
                code = {'within-threshold': 0, 'regression': 1, 'needs-review': 3}[status['status']]
    except Exception as error:
        status = {'status': 'failed', 'error': str(error) if isinstance(error, ValueError) else type(error).__name__}
        code = 2
    finally:
        if state_path.exists():
            try:
                stop(state_path)
            except Exception as error:
                status['cleanup'] = type(error).__name__
                code = 2
        write_json(args.output_dir / 'comparison.json', status)
    print(status['status'])
    return code


if __name__ == '__main__':
    sys.exit(main())
