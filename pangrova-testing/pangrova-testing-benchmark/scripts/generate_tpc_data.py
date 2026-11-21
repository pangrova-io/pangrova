#!/usr/bin/env python3
"""Generate TPC-derived engineering data with pinned tools and explicit failure ownership."""

import argparse
from concurrent.futures import ThreadPoolExecutor
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.request

from tpc_common import ROOT, file_digest, read_json, write_json


def bootstrap(benchmark, directory, jobs):
    lock = read_json(ROOT / 'tools.lock.json')['tools'][benchmark]
    destination = Path(directory) / f"{benchmark}-{lock['commit']}"
    marker = destination / 'source.json'
    if destination.exists():
        if not marker.is_file() or read_json(marker) != lock:
            raise ValueError('unverified cached generator source; use a clean build directory')
        return destination
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix='.tpc-source-', dir=destination.parent))
    try:
        archive = temporary / 'source.tar.gz'
        with urllib.request.urlopen(lock['url'], timeout=60) as source, archive.open('wb') as output:
            shutil.copyfileobj(source, output)
        if file_digest(archive) != lock['sha256']:
            raise ValueError('generator source archive checksum mismatch')
        with tarfile.open(archive) as source:
            for member in source.getmembers():
                target = (temporary / member.name).resolve()
                if not target.is_relative_to(temporary.resolve()):
                    raise ValueError('unsafe generator archive path')
                link = target.parent / member.linkname if member.issym() else temporary / member.linkname
                if (member.issym() or member.islnk()) and not link.resolve().is_relative_to(temporary.resolve()):
                    raise ValueError('unsafe generator archive link')
                if not (member.isfile() or member.isdir() or member.issym() or member.islnk()):
                    raise ValueError('unsupported generator archive member')
            source.extractall(temporary)
        folders = [p for p in temporary.iterdir() if p.is_dir()]
        if len(folders) != 1:
            raise ValueError('unexpected generator archive layout')
        folder = folders[0]
        if platform.system() != 'Linux':
            raise ValueError('automatic generator builds require Linux; supply verified prebuilt tools')
        command = (['make', '-f', 'makefile.suite', f'-j{jobs}', 'MACHINE=LINUX',
                    'DATABASE=ORACLE', 'WORKLOAD=TPCH', 'CC=gcc -std=gnu89 -fcommon', 'dbgen', 'qgen']
                   if benchmark == 'tpch' else ['make', f'-j{jobs}', 'OS=LINUX',
                                               'CC=gcc -std=gnu89 -fcommon', 'dsdgen', 'dsqgen'])
        # These legacy kits use tentative common definitions and pre-C99 source.
        # Select their language/linkage semantics explicitly on modern GCC.
        subprocess.run(command, cwd=folder if benchmark == 'tpch' else folder / 'tools', check=True)
        write_json(folder / 'build.json', {'command': command,
                   'compilerVersion': subprocess.run(['gcc', '--version'], check=True,
                                                     stdout=subprocess.PIPE, text=True).stdout.splitlines()[0],
                   'system': platform.system(), 'machine': platform.machine()})
        write_json(folder / 'source.json', lock)
        folder.rename(destination)
        return destination
    finally:
        shutil.rmtree(temporary)


def generator_commands(benchmark, binary, output, scale, chunks):
    if benchmark == 'tpch':
        for child in range(1, chunks + 1):
            for table in ('L', 'O'):
                args = ['-s', str(scale), '-f', '-T', table]
                if chunks > 1:
                    args += ['-C', str(chunks), '-S', str(child)]
                yield [str(binary), *args]
        for table in ('c', 's', 'n', 'r', 'P', 'S'):
            yield [str(binary), '-s', str(scale), '-f', '-T', table]
    else:
        # The pinned kit omits child>1 for tables whose generation key count is
        # below one million. Generate small-scale suites without chunk suffixes;
        # at SF100+ explicitly chunk the four fact parents and generate dimensions
        # once. Returns are emitted by their sales parent, never independently.
        if scale < 100:
            yield [str(binary), '-SCALE', str(scale), '-DIR', str(output), '-FORCE', '-QUIET']
            return
        parents = ('catalog_sales', 'store_sales', 'web_sales', 'inventory')
        for child in range(1, chunks + 1):
            for table in parents:
                args = ['-SCALE', str(scale), '-DIR', str(output), '-TABLE', table, '-FORCE', '-QUIET']
                if chunks > 1:
                    args += ['-PARALLEL', str(chunks), '-CHILD', str(child)]
                yield [str(binary), *args]
        from tpc_common import table_definitions
        for table in table_definitions('tpcds'):
            if table not in parents and not table.endswith('_returns'):
                yield [str(binary), '-SCALE', str(scale), '-DIR', str(output), '-TABLE', table, '-FORCE', '-QUIET']


def generate(benchmark, scale, output_root, chunks, jobs, source, external_provenance=None):
    source = Path(source).resolve()
    binary = source / ('dbgen' if benchmark == 'tpch' else 'tools/dsdgen')
    if not binary.is_file() or not os.access(binary, os.X_OK):
        raise ValueError(f'missing executable generator: {binary.name}')
    if external_provenance:
        provenance = read_json(external_provenance)
        if provenance.get('binarySha256') != file_digest(binary) or not provenance.get('version'):
            raise ValueError('prebuilt generator requires matching binarySha256 and version')
    else:
        provenance = read_json(source / 'source.json')
        if provenance != read_json(ROOT / 'tools.lock.json')['tools'][benchmark]:
            raise ValueError('generator source identity mismatch')
    output = Path(output_root) / benchmark / f'sf{scale}'
    if output.exists():
        raise ValueError(f'dataset destination exists: {output}; inspect it, do not reuse incomplete data')
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f'.sf{scale}.', dir=output.parent))
    try:
        csv = staging / 'csv'
        csv.mkdir()
        environment = os.environ.copy()
        environment['DSS_PATH'] = str(csv)
        environment['DSS_DIST'] = 'dists.dss'
        commands = list(generator_commands(benchmark, binary, csv, scale, chunks))
        logs = staging / 'logs'
        logs.mkdir()

        def execute(index_command):
            index, command = index_command
            with (logs / f'{index:03d}.log').open('wb') as log:
                completed = subprocess.run(command, cwd=binary.parent, env=environment,
                                           stdout=log, stderr=subprocess.STDOUT)
            return completed.returncode

        with ThreadPoolExecutor(max_workers=jobs) as executor:
            codes = list(executor.map(execute, enumerate(commands)))
        if any(codes):
            # Preserve concrete diagnostics outside the uncommitted dataset.
            failure = output.parent / f'{staging.name}.failed-logs'
            logs.rename(failure)
            raise ValueError(f'generator child failures {codes}; diagnostics: {failure}')
        provenance = {**provenance, 'binarySha256': file_digest(binary), 'scaleFactor': scale,
                      'chunks': chunks, 'commands': [[binary.name, *c[1:]] for c in commands],
                      'distributionSha256': file_digest(source / ('dists.dss' if benchmark == 'tpch'
                                                                else 'tools/tpcds.idx'))}
        if (source / 'build.json').is_file():
            provenance['build'] = read_json(source / 'build.json')
        # Output paths are runtime locations, not part of reproducible command identity.
        for command in provenance['commands']:
            if '-DIR' in command:
                command[command.index('-DIR') + 1] = '<csv-output>'
        write_json(staging / 'generator.json', provenance)
        subprocess.run([sys.executable, str(ROOT / 'scripts/csv-to-parquet.py'),
                        '--benchmark', benchmark, '--input-dir', str(csv),
                        '--output-dir', str(staging / 'parquet'), '--chunks', str(chunks),
                        '--parallel', str(jobs), '--scale-factor', str(scale),
                        '--generator-manifest', str(staging / 'generator.json')], check=True)
        staging.rename(output)
        return output
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--benchmark', choices=('tpch', 'tpcds', 'both'), default='both')
    parser.add_argument('--scale-factor', type=int, default=100)
    parser.add_argument('--output-dir', type=Path, default=Path('/data/tpc'))
    parser.add_argument('--parallel', type=int, default=8)
    parser.add_argument('--build-dir', type=Path)
    parser.add_argument('--tool-source', type=Path, help='prebuilt source root, one benchmark at a time')
    parser.add_argument('--generator-provenance', type=Path)
    args = parser.parse_args()
    try:
        if min(args.scale_factor, args.parallel) < 1:
            raise ValueError('scale factor and parallel count must be positive integers')
        if args.tool_source and (args.benchmark == 'both' or not args.generator_provenance):
            raise ValueError('prebuilt tools require one benchmark and --generator-provenance')
        for benchmark in (('tpch', 'tpcds') if args.benchmark == 'both' else (args.benchmark,)):
            source = args.tool_source or bootstrap(benchmark, args.build_dir or args.output_dir / '.build',
                                                  args.parallel)
            print(generate(benchmark, args.scale_factor, args.output_dir, args.parallel,
                           args.parallel, source, args.generator_provenance))
        return 0
    except (OSError, ValueError, ImportError, subprocess.CalledProcessError) as error:
        print(f'ERROR: {error}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main())
