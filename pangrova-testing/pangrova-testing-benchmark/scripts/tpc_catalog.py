#!/usr/bin/env python3
"""Prepare a shared Iceberg snapshot through Spark, or verify an existing binding."""

import argparse
import json
from pathlib import Path
import re
import sys

from tpc_common import digest, read_json, write_json
from tpc_contract import catalog, dataset


def session(config):
    from pyspark.sql import SparkSession
    builder = SparkSession.builder.appName('pangrova-tpc-catalog')
    for key, value in config['sparkConf'].items():
        builder = builder.config(key, value)
    return builder.getOrCreate().newSession()


def identifier(value):
    if not re.fullmatch(r'[A-Za-z_][A-Za-z_0-9]*', value):
        raise ValueError('catalog/schema/table identifiers must be simple identifiers')
    return '`' + value + '`'


def snapshot(spark, table, scan_rows=True):
    current = spark.sql(f"SELECT snapshot_id FROM {table}.refs WHERE name = 'main'").collect()
    if len(current) != 1:
        raise ValueError('Iceberg table must expose exactly one main snapshot')
    metadata = spark.sql(f'SELECT file FROM {table}.metadata_log_entries ORDER BY timestamp DESC LIMIT 1').first()
    if metadata is None:
        raise ValueError('missing Iceberg metadata identity')
    document = json.loads('\n'.join(r.value for r in spark.read.text(metadata.file).collect()))
    count_query = (f'SELECT COUNT(*) AS n FROM {table}' if scan_rows else
                   f'SELECT SUM(record_count) AS n FROM {table}.files WHERE content = 0')
    rows = spark.sql(count_query).first().n
    return {'snapshotId': int(current[0].snapshot_id), 'tableUuid': document['table-uuid'],
            'rows': int(rows), 'metadataFile': metadata.file}


def verify(config, data, binding):
    spark = session(config)
    prefix = identifier(config['catalog']) + '.' + identifier(config['schema'])
    for name, expected in binding['tables'].items():
        if snapshot(spark, prefix + '.' + identifier(name), scan_rows=False) != expected:
            raise ValueError(f'catalog changed: {name}; no comparable run can be accepted')
    return True


def verify_client(client, engine, config, binding, timeout):
    """Verify the SUT's own main references, not only an external catalog observer."""
    quote = '`' if engine == 'spark' else '"'
    def quoted(value):
        identifier(value)
        return quote + value + quote
    prefix = quoted(config['catalog']) + '.' + quoted(config['schema']) + '.'
    observations = []
    for name, expected in binding['tables'].items():
        table = (prefix + quoted(name) + '.refs' if engine == 'spark'
                 else prefix + quote + name + '$refs' + quote)
        outcome = client.execute(f"SELECT snapshot_id FROM {table} WHERE name = 'main'", timeout)
        if (outcome.get('state') != 'FINISHED' or outcome.get('resultConsumed') is not True
                or outcome['result']['rows'] != [[expected['snapshotId']]]):
            raise ValueError(f'SUT main snapshot does not match shared binding: {name}')
        observations.append({'table': name, 'snapshotId': expected['snapshotId'], 'queryId': outcome['queryId']})
    return observations


def prepare(config, data_path, binding_path):
    data = dataset(data_path)
    if Path(binding_path).exists():
        raise ValueError('binding already exists; verify it instead of overwriting')
    spark = session(config)
    prefix = identifier(config['catalog']) + '.' + identifier(config['schema'])
    spark.sql(f'CREATE NAMESPACE IF NOT EXISTS {prefix}')
    # Never overwrite an existing benchmark table, including partially prepared ones.
    if any(spark.catalog.tableExists(prefix + '.' + identifier(n)) for n in data['tables']):
        raise ValueError('benchmark schema contains tables; use a fresh dedicated namespace')
    binding = {'schemaVersion': 1, 'kind': 'tpc-catalog-binding', 'datasetId': data['datasetId'],
               'catalogIdentity': config['catalogIdentity'], 'tables': {}}
    for name, record in data['tables'].items():
        sources = [str(Path(data_path).parent / item['path']) for item in record['files']]
        table = prefix + '.' + identifier(name)
        frame = spark.read.parquet(*sources)
        frame.writeTo(table).using('iceberg').tableProperty('write.parquet.compression-codec', 'snappy').create()
        actual = snapshot(spark, table)
        if actual['rows'] != record['rows']:
            raise ValueError(f'loaded table count mismatch: {name}')
        binding['tables'][name] = actual
    binding['bindingId'] = digest(binding)
    write_json(binding_path, binding)
    return binding


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=('prepare', 'verify'))
    parser.add_argument('--dataset', type=Path, required=True)
    parser.add_argument('--binding', type=Path, required=True)
    parser.add_argument('--spark-config', type=Path, required=True)
    args = parser.parse_args()
    try:
        config = read_json(args.spark_config)
        if args.action == 'prepare':
            prepare(config, args.dataset, args.binding)
        else:
            data = dataset(args.dataset)
            verify(config, data, catalog(args.binding, data))
        print('Iceberg snapshot binding validated')
        return 0
    except Exception as error:
        # Spark/provider diagnostics may contain credentials; do not publish arbitrary exceptions.
        print(f'Catalog operation failed: {type(error).__name__}', file=sys.stderr)
        return 2
    finally:
        if 'pyspark.sql' in sys.modules:
            from pyspark.sql import SparkSession
            active = SparkSession.getActiveSession()
            if active is not None:
                active.stop()


if __name__ == '__main__':
    sys.exit(main())
