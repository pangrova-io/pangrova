#!/usr/bin/env python3
"""Generate explicit query steps from checksum-pinned community TPC templates.

The generated suite is a TPC-derived engineering workload, not current official
qualification evidence. Statement boundaries are parsed once during preparation;
the executor always sends each immutable step intact.
"""

import argparse
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile

from tpc_common import ROOT, digest, file_digest, read_json, write_json
from tpc_results import projection_names


def ansi_day_intervals(sql):
    """Translate Netezza numeric DAYS tokens without touching strings or comments."""
    import sqlparse
    from sqlparse.tokens import Literal
    tokens = list(sqlparse.parse(sql)[0].flatten())
    output, index = [], 0
    while index < len(tokens):
        token = tokens[index]
        following = index + 1
        while following < len(tokens) and tokens[following].is_whitespace:
            following += 1
        if (token.ttype in Literal.Number.Integer and following < len(tokens)
                and tokens[following].value.lower() == 'days'):
            output.append(f"INTERVAL '{token.value}' DAY")
            index = following + 1
        else:
            output.append(token.value)
            index += 1
    return ''.join(output)


def adapt_ds_statement(number, sql):
    sql = ansi_day_intervals(sql)
    if number == 58:
        sql = re.sub(r'order by item_id\b', 'order by ss_items.item_id', sql, flags=re.I)
    if number == 72:
        sql = re.sub(r'(order by[^;]*)\bd_week_seq\b', r'\1d1.d_week_seq', sql, flags=re.I)
        sql = re.sub(r'd1\.d_date\s*\+\s*5\b', "d1.d_date + INTERVAL '5' DAY", sql, flags=re.I)
    if number == 90:
        sql = re.sub(r'\) at,', ') morning_counts,', sql, flags=re.I)
    if number == 77:
        import sqlparse
        from sqlparse.tokens import Name, Keyword
        sql = ''.join('return_amount' if token.value.lower() == 'returns'
                      and token.ttype in (Name, Keyword) else token.value
                      for token in sqlparse.parse(sql)[0].flatten())
    return sql


def normalize_sql(sql):
    import sqlparse
    normalized = '\n'.join(line.rstrip() for line in sql.splitlines()).strip() + '\n'
    def significant(text):
        return [(token.ttype, token.value) for statement in sqlparse.parse(text)
                for token in statement.flatten()
                if not token.is_whitespace and token.ttype not in sqlparse.tokens.Comment]
    if significant(normalized) != significant(sql):
        raise ValueError('whitespace normalization would change SQL tokens')
    return normalized


def generate(benchmark, source, output, scale=100, seed=19620718):
    output = Path(output)
    if output.exists():
        raise ValueError('workload destination already exists; use a new destination')
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=f'.{output.name}.', dir=output.parent))
    try:
        result = generate_staged(benchmark, source, staging, scale, seed)
        staging.rename(output)
        return result
    finally:
        if staging.exists():
            shutil.rmtree(staging)


def generate_staged(benchmark, source, output, scale, seed):
    import sqlparse
    source, output = Path(source).resolve(), Path(output)
    maximum = 22 if benchmark == 'tpch' else 99
    lock = read_json(ROOT / 'tools.lock.json')['tools'][benchmark]
    if read_json(source / 'source.json') != lock:
        raise ValueError('query generator source must match tools.lock.json')
    if output.exists() and any(output.iterdir()):
        raise ValueError('workload output must be new or empty')
    output.mkdir(parents=True, exist_ok=True)
    queries = []
    for number in range(1, maximum + 1):
        adaptations = []
        if benchmark == 'tpch':
            binary = source / 'qgen'
            template = source / 'queries' / f'{number}.sql'
            environment = {**os.environ, 'DSS_QUERY': 'queries', 'DSS_DIST': 'dists.dss'}
            command = [str(binary), '-d', '-s', str(scale), '-r', str(seed), '-a', '-N', str(number)]
            raw = subprocess.run(command, cwd=source, env=environment, check=True,
                                 stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout.decode()
            statements = [s for s in sqlparse.split(raw) if not re.fullmatch(
                r'where\s+rownum\s*<=\s*-?\d+\s*;', sqlparse.format(s, strip_comments=True).strip(), re.I)]
            statements = [re.sub(r"(interval\s+'[^']+'\s+day)\s*\(3\)", r'\1', s,
                                 flags=re.I) for s in statements]
            adaptations.append('Oracle ROWNUM directives removed; ANSI interval day precision omitted')
            if number == 15:
                # The same relational expression with query-local lifecycle, across all engines.
                match = re.search(r'create\s+view\s+(\w+)\s*(\([^;]*?\))\s+as\s+(.*)',
                                  statements[0], re.I | re.S)
                if len(statements) != 3 or match is None:
                    raise ValueError('unexpected TPC-H Q15 lifecycle')
                statements = [f'WITH {match[1]} {match[2]} AS (\n{match[3].rstrip().rstrip(";")}\n)\n'
                              + statements[1]]
                adaptations.append('Q15 view lifecycle expressed as equivalent query-local CTE; review required')
            limit = re.search(r'^:n\s+(-?\d+)', template.read_text(), re.M)
            if limit and int(limit[1]) > 0:
                if len(statements) != 1:
                    raise ValueError('row limit requires a single relational query')
                statements[0] = statements[0].rstrip().rstrip(';') + f'\nLIMIT {limit[1]};'
                adaptations.append('TPC row-count directive expressed as LIMIT after ORDER BY')
        else:
            binary = source / 'tools/dsqgen'
            template = source / 'query_templates' / f'query{number}.tpl'
            with tempfile.TemporaryDirectory(prefix='pangrova-query-') as temporary:
                command = [str(binary), '-directory', '../query_templates',
                           '-template', template.name, '-dialect', 'netezza', '-scale', str(scale),
                           '-rngseed', str(seed), '-qualify', 'Y', '-output_dir', temporary]
                subprocess.run(command, cwd=binary.parent, check=True, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE)
                raw = (Path(temporary) / 'query_0.sql').read_text()
            statements = [adapt_ds_statement(number, s) for s in sqlparse.split(raw)
                          if sqlparse.format(s, strip_comments=True).strip()]
            adaptations.append('Netezza template dialect (LIMIT); qualification parameters from pinned 2.10.0 kit')
            adaptations.append('Netezza numeric DAYS arithmetic translated to ANSI INTERVAL day syntax')
            if number in (58, 72):
                adaptations.append('ORDER BY projected join column qualified to remove ambiguous source names')
            if number == 72:
                adaptations.append('Date plus integer rewritten as date plus INTERVAL day')
            if number == 90:
                adaptations.append('Unreferenced subquery alias AT renamed morning_counts to avoid reserved keyword')
            if number == 77:
                adaptations.append('RETURNS expression alias and its references renamed return_amount to avoid reserved keyword')
        # sqlparse retains standalone end comments; only executable statements are steps.
        statements = [s for s in statements if sqlparse.format(s, strip_comments=True).strip()]
        if not statements:
            raise ValueError(f'empty generated template {number}')
        statements = [normalize_sql(sql) for sql in statements]
        adaptations.append('Trailing line whitespace normalized without changing SQL tokens')
        steps = []
        for index, sql in enumerate(statements, 1):
            path = output / (f'q{number:02d}.sql' if len(statements) == 1 else f'q{number:02d}-{index:02d}.sql')
            path.write_text(sql, encoding='utf-8')
            steps.append({'file': path.name, 'sqlSha256': file_digest(path), 'result': True,
                          'projectionNames': projection_names(sql),
                          'order': 'ordered' if re.search(r'\border\s+by\b', sql, re.I) else 'unordered'})
        # The standalone legacy CLI cannot execute a multi-statement file; the suite uses steps.
        if len(steps) > 1:
            (output / f'q{number:02d}.sql').write_text('\n'.join(statements), encoding='utf-8')
        queries.append({'id': f'q{number:02d}', 'template': template.name,
                        'templateSha256': file_digest(template), 'steps': steps,
                        'adaptations': adaptations, 'order': 'ordered' if re.search(r'\border\s+by\b', raw, re.I)
                        else 'unordered', 'semanticReview': 'requires-reference-validation'})
    manifest = {'schemaVersion': 1, 'kind': 'tpc-workload', 'benchmark': benchmark,
                'scaleFactor': scale, 'classification': 'tpc-derived-engineering',
                'targetSpecification': read_json(ROOT / 'tools.lock.json')['specifications'][benchmark],
                'generator': {**lock, 'binarySha256': file_digest(binary),
                              'version': '2.14.0' if benchmark == 'tpch' else '2.10.0',
                              'parameters': {'scaleFactor': scale, 'seed': seed, 'qualification': True}},
                'statementParser': {'name': 'sqlparse', 'version': sqlparse.__version__},
                'queries': queries}
    if (source / 'build.json').is_file():
        manifest['generator']['build'] = read_json(source / 'build.json')
    manifest['workloadId'] = digest(manifest)
    write_json(output / 'workload.json', manifest)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--benchmark', choices=('tpch', 'tpcds'), required=True)
    parser.add_argument('--tool-source', type=Path, required=True)
    parser.add_argument('--output-dir', type=Path, required=True)
    parser.add_argument('--scale-factor', type=int, default=100)
    parser.add_argument('--seed', type=int, default=19620718)
    args = parser.parse_args()
    try:
        if args.scale_factor < 1:
            raise ValueError('scale factor must be positive')
        result = generate(args.benchmark, args.tool_source, args.output_dir, args.scale_factor, args.seed)
        print(f"Generated {len(result['queries'])} templates; workloadId={result['workloadId']}")
        return 0
    except (OSError, ValueError, ImportError, subprocess.CalledProcessError) as error:
        print(f'ERROR: {error}', file=sys.stderr)
        return 2


if __name__ == '__main__':
    sys.exit(main())
