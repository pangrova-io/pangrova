"""Fail-closed workload, dataset, catalog and controlled-environment contracts."""

from pathlib import Path
import math
import re

from tpc_common import ROOT, digest, file_digest, positive, read_json, table_definitions


def bound_digest(document, field):
    if document.get(field) != digest({k: v for k, v in document.items() if k != field}):
        raise ValueError(f'{field} digest mismatch')


def workload(path):
    path = Path(path)
    document = read_json(path)
    if document.get('schemaVersion') != 1 or document.get('kind') != 'tpc-workload':
        raise ValueError('unsupported workload contract')
    bound_digest(document, 'workloadId')
    benchmark = document.get('benchmark')
    expected = {f'q{i:02d}' for i in range(1, (22 if benchmark == 'tpch' else 99) + 1)}
    if benchmark not in ('tpch', 'tpcds'):
        raise ValueError('invalid workload benchmark')
    positive(document.get('scaleFactor'), 'workload scale', integer=True)
    pinned = read_json(ROOT / 'tools.lock.json')['tools'][benchmark]
    if any(document.get('generator', {}).get(key) != value for key, value in pinned.items()):
        raise ValueError('workload generator source/version differs from the locked kit')
    records = document.get('queries', [])
    if len(records) != len(expected) or {q['id'] for q in records} != expected:
        raise ValueError('workload must contain every template exactly once')
    files = set()
    for query in records:
        expected_steps = 2 if benchmark == 'tpcds' and query['id'] in ('q14', 'q23', 'q24', 'q39') else 1
        if len(query.get('steps', [])) != expected_steps:
            raise ValueError('locked query requires every explicit executable step')
        for step in query['steps']:
            source = (path.parent / step['file']).resolve()
            if not source.is_relative_to(path.parent.resolve()) or source in files:
                raise ValueError('unsafe or duplicate workload step')
            files.add(source)
            if file_digest(source) != step['sqlSha256'] or not source.read_text().strip():
                raise ValueError('SQL digest mismatch or empty step')
            import sqlparse
            statements = [s for s in sqlparse.parse(source.read_text())
                          if sqlparse.format(str(s), strip_comments=True).strip().rstrip(';')]
            if len(statements) != 1:
                raise ValueError('each workload step must contain exactly one parsed SQL statement')
            if step.get('order') not in ('ordered', 'unordered'):
                raise ValueError('every step requires an explicit result order')
    return document


def dataset(path, verify_files=True):
    path = Path(path)
    document = read_json(path)
    if document.get('schemaVersion') != 1 or document.get('kind') != 'tpc-parquet-dataset':
        raise ValueError('unsupported dataset contract')
    bound_digest(document, 'datasetId')
    if (path.parent / '_SUCCESS').read_text().strip() != file_digest(path):
        raise ValueError('dataset is incomplete: completion digest mismatch')
    tables = table_definitions(document['benchmark'])
    if set(document.get('tables', {})) != set(tables):
        raise ValueError('dataset table coverage mismatch')
    if document.get('schemaSha256') != file_digest(ROOT / 'schemas' / f"{document['benchmark']}.json"):
        raise ValueError('dataset schema version mismatch')
    positive(document.get('scaleFactor'), 'dataset scale', integer=True)
    seen = set()
    for name, record in document['tables'].items():
        if record.get('columns') != tables[name] or not record.get('files'):
            raise ValueError(f'missing or incompatible dataset table: {name}')
        if sum(positive(f['rows'], 'file rows', integer=True) for f in record['files']) != record.get('rows'):
            raise ValueError('dataset row-count metadata mismatch')
        for item in record['files']:
            source = (path.parent / item['path']).resolve()
            if not source.is_relative_to(path.parent.resolve()) or source in seen:
                raise ValueError('unsafe/duplicate dataset file')
            seen.add(source)
            if verify_files:
                import pyarrow.parquet as pq
                # Full bytes and footer validation occurs before any measured query.
                if source.stat().st_size != item['bytes'] or file_digest(source) != item['sha256']:
                    raise ValueError('dataset file checksum/size mismatch')
                parquet = pq.ParquetFile(source)
                if parquet.metadata.num_rows != item['rows']:
                    raise ValueError('dataset footer row count mismatch')
                from tpc_common import arrow_schema
                if not parquet.schema_arrow.equals(arrow_schema(tables[name])):
                    raise ValueError('dataset Parquet schema differs from explicit logical schema')
    return document


def catalog(path, data):
    document = read_json(path)
    if document.get('schemaVersion') != 1 or document.get('datasetId') != data['datasetId']:
        raise ValueError('catalog binding must identify this dataset')
    bound_digest(document, 'bindingId')
    if set(document.get('tables', {})) != set(data['tables']):
        raise ValueError('catalog snapshot coverage mismatch')
    for name, table in document['tables'].items():
        positive(table.get('snapshotId'), 'Iceberg snapshot ID', integer=True)
        if not table.get('tableUuid') or table.get('rows') != data['tables'][name]['rows']:
            raise ValueError('catalog UUID/count mismatch')
    return document


def qualify_generated_dataset(document):
    """Structural fixtures cannot acquire an official scale merely by setting SF."""
    generator = document.get('generator', {})
    benchmark, scale = document['benchmark'], document['scaleFactor']
    pinned = read_json(ROOT / 'tools.lock.json')['tools'][benchmark]
    if (generator.get('scaleFactor') != scale or generator.get('commit') != pinned['commit']
            or generator.get('sha256') != pinned['sha256'] or generator.get('version') != pinned['version']
            or not re.fullmatch('[0-9a-f]{64}', generator.get('binarySha256', ''))
            or not generator.get('commands') or not generator.get('distributionSha256')):
        raise ValueError('scale qualification requires pinned generator provenance and parameters, not fixture multipliers')
    flag = '-s' if benchmark == 'tpch' else '-SCALE'
    for command in generator['commands']:
        if flag not in command or command[command.index(flag) + 1] != str(scale):
            raise ValueError('recorded generator commands do not match declared scale')
    counts = {name: table['rows'] for name, table in document['tables'].items()}
    if benchmark == 'tpch':
        expected = {'nation': 25, 'region': 5, 'part': 200000 * scale, 'partsupp': 800000 * scale,
                    'supplier': 10000 * scale, 'customer': 150000 * scale, 'orders': 1500000 * scale}
        lineitem = {1: 6001215, 10: 59986052, 30: 179998372, 100: 600037902,
                    300: 1799989091, 1000: 5999989709}
        if scale in lineitem:
            expected['lineitem'] = lineitem[scale]
        if any(counts[name] != rows for name, rows in expected.items()):
            raise ValueError('TPC-H generated row cardinalities do not match declared scale')
    elif counts['customer_demographics'] != 1920800 or counts['date_dim'] != 73049 or counts['time_dim'] != 86400:
        raise ValueError('TPC-DS fixed dimensions do not match the pinned generation model')
    return document


def environment(path):
    document = read_json(path)
    def reject_secrets(value):
        if isinstance(value, dict):
            for key, child in value.items():
                normalized = key.lower().replace('_', '.').replace('-', '.')
                if key != 'tokenEnv' and any(part in normalized for part in ('password', 'secret', 'access.key', 'api.key', 'token')):
                    raise ValueError('credentials must be supplied through environment/providers, never manifest values')
                reject_secrets(child)
        elif isinstance(value, list):
            for child in value:
                reject_secrets(child)
    reject_secrets(document)
    if document.get('schemaVersion') != 1 or document.get('kind') != 'tpc-environment':
        raise ValueError('unsupported environment contract')
    if document.get('exclusive') is not True:
        raise ValueError('benchmark requires exclusive resources')
    engine = document.get('engine', {})
    if engine.get('name') not in ('pangrova', 'trino', 'spark') or not engine.get('version'):
        raise ValueError('engine identity/version required')
    if not re.fullmatch('[0-9a-f]{64}', engine.get('artifactSha256', '')):
        raise ValueError('pinned engine artifact SHA256 required')
    for field in ('sourceRevision', 'hardware', 'settings', 'resources', 'cachePolicy', 'client'):
        if not document.get(field):
            raise ValueError(f'environment requires {field}')
    if not re.fullmatch('[0-9a-f]{64}', document['settings'].get('configurationSha256', '')):
        raise ValueError('rendered runtime configuration SHA256 is required')
    cache = document['cachePolicy']
    if cache.get('mode') not in ('warm', 'cold', 'uncontrolled') or not cache.get('procedure'):
        raise ValueError('explicit cache mode/procedure required')
    if cache['mode'] != 'warm':
        raise ValueError('ordinary baseline requires warm-cache protocol; cold/reset experiments need a separate driver')
    workers = document.get('workers', [])
    if not workers or any(not all(w.get(k) for k in ('id', 'host', 'process', 'endpoint')) for w in workers):
        raise ValueError('worker process/host/endpoint inventory required')
    for key in ('id', 'endpoint'):
        if len({w[key] for w in workers}) != len(workers):
            raise ValueError('workers must have distinct identities and endpoints')
    if len({(w['host'], w['process']) for w in workers}) != len(workers):
        raise ValueError('multiple worker IDs cannot share one process')
    if document.get('multiHost') and len({w['host'] for w in workers}) != len(workers):
        raise ValueError('multi-host results require different worker hosts')
    resource = document['resources']
    for field in ('totalCpu', 'totalMemoryBytes', 'coordinatorMemoryBytes', 'workerHeapBytes',
                  'workerNativeBytes', 'workerDirectBytes', 'backgroundMemoryBytes'):
        value = resource.get(field)
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
            raise ValueError(f'invalid resource budget: {field}')
    positive(resource['totalCpu'], 'total CPU')
    positive(resource['totalMemoryBytes'], 'total memory')
    overhead = resource.get('workerOverheadBytes', 0)
    if type(overhead) is not int or overhead < 0:
        raise ValueError('worker process overhead must be nonnegative integer bytes')
    allocated = (resource['coordinatorMemoryBytes'] + resource['backgroundMemoryBytes'] + len(workers) *
                 (overhead + sum(resource[k] for k in ('workerHeapBytes', 'workerNativeBytes', 'workerDirectBytes'))))
    if allocated > resource['totalMemoryBytes']:
        raise ValueError('resource budgets exceed total memory')
    hardware = document['hardware']
    if not isinstance(hardware, list) or {w['host'] for w in workers} - {h.get('host') for h in hardware}:
        raise ValueError('hardware inventory must identify every worker host')
    for host in hardware:
        for field in ('cpuModel', 'storage', 'network'):
            if not host.get(field) or 'REPLACE' in str(host[field]):
                raise ValueError(f'controlled host inventory requires measured {field}')
        positive(host.get('memoryBytes'), 'host physical memory', integer=True)
        positive(host.get('cpus'), 'host CPUs')
    if (sum(h['memoryBytes'] for h in hardware) < resource['totalMemoryBytes']
            or sum(h['cpus'] for h in hardware) < resource['totalCpu']):
        raise ValueError('resource budget exceeds inventoried host capacity')
    return document


def identity(work, data, binding, env, config):
    # Client location/authentication are operational inputs, not disclosed secrets.
    controlled = {k: v for k, v in env.items() if k != 'client'}
    harness = [ROOT / 'scripts' / name for name in ('tpc_clients.py', 'run_tpc_suite.py', 'tpc_results.py',
                                                   'tpc_common.py', 'tpc_contract.py', 'tpc_catalog.py',
                                                   'run_tpc_benchmark.py', 'tpc_statistics.py', 'java/TpcClient.java')]
    client_keys = ('catalog', 'schema', 'sessionProperties', 'sparkConf', 'requestTimeout',
                   'tls', 'allowPlaintextResults', 'tokenEnv', 'user')
    return {'workloadId': work['workloadId'], 'datasetId': data['datasetId'],
            'bindingId': binding['bindingId'], 'environment': controlled,
            'snapshots': {name: {'snapshotId': table['snapshotId'], 'tableUuid': table['tableUuid']}
                          for name, table in binding['tables'].items()},
            'querySteps': {q['id']: [s['sqlSha256'] for s in q['steps']] for q in work['queries']},
            'harnessId': digest({p.name: file_digest(p) for p in harness}),
            'clientSettings': {k: env['client'][k] for k in client_keys if k in env['client']},
            'timingScope': 'persistent-client-end-to-end', 'config': config}
