"""Independent runner and evidence failure tests; fixtures are not engine measurements."""

import copy
from decimal import Decimal
from pathlib import Path
import sys
import tempfile
import unittest
import uuid

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))
from tpc_common import ROOT, digest, read_json, table_definitions, write_json
from tpc_contract import environment, workload
from tpc_clients import QueryFailure
from tpc_reference import agree
from tpc_results import compare_result, typed_result
from tpc_suite_compare import Incompatible, compare, validate_window, windows
from run_tpc_suite import run_window


def result(rows=None):
    return {'columns': [{'name': 'value', 'type': 'decimal'}],
            'rows': [[{'decimal': '1.25'}]] if rows is None else rows}


def fixture(label, multiplier=1, engine='pangrova', purpose='measurement'):
    """Synthetic time/IDs solely for validation arithmetic; never written to baselines."""
    ids = {f'q{i:02d}': ['a' * 64] for i in range(1, 23)}
    iterations = 5 if purpose == 'measurement' else 1
    records = []
    for phase, count in (('warmup', 1), ('measurement', iterations)):
        for n in range(count):
            for query in ids:
                records.append({'phase': phase, 'query': query, 'stream': 0, 'round': n, 'status': 'passed',
                                'steps': [{'queryId': f'{label}-{phase}-{n}-{query}', 'state': 'FINISHED',
                                           'resultConsumed': True, 'sqlSha256': 'a' * 64,
                                           'elapsedMs': 100 * multiplier, 'result': result()}]})
    return {'schemaVersion': 1, 'kind': 'tpc-suite-window', 'runId': label, 'benchmark': 'tpch',
            'scaleFactor': 100, 'purpose': purpose, 'status': 'passed', 'referenceId': 'reference',
            'snapshotVerifiedBeforeAndAfter': True,
            'clientSnapshotsVerified': True,
            'clientRuntimes': [{'javaVersion': 'synthetic-test-only'}],
            'snapshotObservations': [{'phase': phase, 'stream': 0, 'tables': [
                {'table': name, 'snapshotId': 123, 'queryId': f'{label}-metadata-{phase}-{name}'}
                for name in table_definitions('tpch')]} for phase in ('before', 'after')],
            'identity': {'workloadId': 'work', 'datasetId': 'data', 'bindingId': 'binding',
                         'harnessId': 'synthetic-test-only',
                         'snapshots': {name: {'snapshotId': 123, 'tableUuid': f'fixture-{name}'}
                                       for name in table_definitions('tpch')},
                         'environment': {'engine': {'name': engine, 'version': '1', 'artifactSha256': 'b' * 64},
                                         'sourceRevision': 'original', 'hardware': 'controlled'},
                         'timingScope': 'persistent-client-end-to-end', 'querySteps': ids,
                         'config': {'warmup': 1, 'iterations': iterations, 'streams': 1, 'queryTimeout': 600}},
            'records': records, 'summary': {'driverMeasuredWallMs': 2200 * iterations * multiplier}}


class ResultTest(unittest.TestCase):
    def test_decimal_exact_float_tolerance_null_and_full_strings(self):
        left = typed_result(['d', 'f', 'n', 's'], ['DECIMAL(15,2)', 'DOUBLE', 'BIGINT', 'VARCHAR'],
                            [[Decimal('1.25'), 1.0, None, 'x' * 200]])
        right = copy.deepcopy(left)
        right['rows'][0][1] += 1e-7
        compare_result(left, right)
        for index, value in ((0, {'decimal': '1.26'}), (2, 0), (3, 'x' * 199)):
            wrong = copy.deepcopy(right)
            wrong['rows'][0][index] = value
            with self.assertRaises(ValueError):
                compare_result(left, wrong)

    def test_empty_reference_is_valid_but_missing_reference_is_not(self):
        compare_result(result([]), result([]))
        with self.assertRaisesRegex(ValueError, 'row counts'):
            compare_result(result([]), result())
        with self.assertRaisesRegex(ValueError, 'missing complete'):
            compare_result({}, result())

    def test_unordered_perfect_matching_preserves_duplicates(self):
        a = {'columns': [{'name': 'x', 'type': 'float'}], 'rows': [[1.0], [1.0000015]]}
        b = {'columns': a['columns'], 'rows': [[1.0000008], [0.9999995]]}
        compare_result(a, b, 'unordered')
        with self.assertRaisesRegex(ValueError, 'multiplicities'):
            compare_result(result([[1], [1], [2]]), result([[1], [2], [2]]), 'unordered')

    def test_order_schema_and_width_are_checked(self):
        with self.assertRaisesRegex(ValueError, 'ordered'):
            compare_result(result([[1], [2]]), result([[2], [1]]))
        with self.assertRaisesRegex(ValueError, 'column'):
            compare_result(result(), {'columns': [{'name': 'wrong', 'type': 'decimal'}], 'rows': [[1]]})
        with self.assertRaisesRegex(ValueError, 'width'):
            typed_result(['x'], ['INTEGER'], [[1, 2]])


class EvidenceTest(unittest.TestCase):
    def test_all_shipped_templates_and_every_multistatement_step_are_bound(self):
        for benchmark, templates, steps in (('tpch', 22, 22), ('tpcds', 99, 103)):
            document = workload(ROOT / 'queries' / benchmark / 'workload.json')
            self.assertEqual(len(document['queries']), templates)
            self.assertEqual(sum(len(q['steps']) for q in document['queries']), steps)
            self.assertTrue(all(q['semanticReview'] == 'requires-reference-validation' for q in document['queries']))

    def test_dropped_variant_or_concatenated_statements_cannot_qualify(self):
        import shutil
        from tpc_common import file_digest
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary) / 'tpcds'
            shutil.copytree(ROOT / 'queries/tpcds', root)
            original = read_json(root / 'workload.json')
            for mutation in ('variant', 'statements'):
                document = copy.deepcopy(original)
                if mutation == 'variant':
                    next(q for q in document['queries'] if q['id'] == 'q14')['steps'].pop()
                else:
                    step = document['queries'][0]['steps'][0]
                    source = root / step['file']
                    source.write_text(source.read_text() + '\nSELECT 42;\n')
                    step['sqlSha256'] = file_digest(source)
                document['workloadId'] = digest({k: v for k, v in document.items() if k != 'workloadId'})
                write_json(root / 'workload.json', document)
                with self.subTest(mutation=mutation), self.assertRaisesRegex(ValueError, 'every explicit|exactly one'):
                    workload(root / 'workload.json')

    def test_legacy_single_statement_runner_rejects_multistep_templates_before_execution(self):
        from types import SimpleNamespace
        from run_tpc_benchmark import query_files
        args = SimpleNamespace(benchmark='tpcds', queries='q14', queries_dir=ROOT / 'queries')
        with self.assertRaisesRegex(ValueError, 'multiple execution steps'):
            query_files(args)
        args.queries = 'q01'
        self.assertEqual(set(query_files(args)), {'q01'})

    def test_three_windows_required_and_threshold_equality_permitted(self):
        baseline = [fixture(f'b{i}') for i in range(3)]
        for multiplier, expected in ((1.15, 'within-threshold'), (1.151, 'regression')):
            current = [fixture(f'c{i}', multiplier) for i in range(3)]
            self.assertEqual(compare(baseline, current)['status'], expected)
        with self.assertRaisesRegex(ValueError, 'three'):
            windows(baseline[:2])

    def test_new_engine_build_is_allowed_but_hardware_sql_and_sampling_changes_are_not(self):
        baseline = [fixture(f'b{i}') for i in range(3)]
        current = [fixture(f'c{i}') for i in range(3)]
        for doc in current:
            doc['identity']['environment']['engine'].update(version='new', artifactSha256='c' * 64)
            doc['identity']['environment']['sourceRevision'] = 'new-commit'
        self.assertEqual(compare(baseline, current)['status'], 'within-threshold')
        for field in ('hardware',):
            changed = copy.deepcopy(current)
            for doc in changed:
                doc['identity']['environment'][field] = 'different'
            with self.assertRaises(Incompatible):
                compare(baseline, changed)
        current[0]['identity']['querySteps']['q01'][0] = 'd' * 64
        with self.assertRaises(Incompatible):
            compare(baseline, current)

    def test_failure_partial_results_replayed_ids_and_insufficient_samples_rejected(self):
        base = fixture('b')
        for mutation in ('failed', 'partial', 'replay', 'sql', 'samples', 'snapshot'):
            doc = copy.deepcopy(base)
            if mutation == 'failed': doc['records'][0]['status'] = 'failed'
            if mutation == 'partial': doc['records'].pop()
            if mutation == 'replay': doc['records'][1]['steps'][0]['queryId'] = doc['records'][0]['steps'][0]['queryId']
            if mutation == 'sql': doc['records'][0]['steps'][0]['sqlSha256'] = 'x'
            if mutation == 'samples': doc['identity']['config']['iterations'] = 3
            if mutation == 'snapshot': doc['snapshotVerifiedBeforeAndAfter'] = False
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                validate_window(doc)

    def test_mixed_regression_windows_require_review_and_replays_across_runs_fail(self):
        baseline = [fixture(f'b{i}') for i in range(3)]
        current = [fixture(f'c{i}', factor) for i, factor in enumerate((1.2, 1, 1))]
        self.assertEqual(compare(baseline, current)['status'], 'needs-review')
        current = [fixture(f'c{i}') for i in range(3)]
        current[1]['records'][0]['steps'][0]['queryId'] = current[0]['records'][0]['steps'][0]['queryId']
        with self.assertRaisesRegex(ValueError, 'replayed across'):
            compare(baseline, current)

    def test_reference_agreement_rejects_disagreement_and_accepts_empty_results(self):
        work = {'workloadId': 'work', 'queries': [{'id': f'q{i:02d}', 'steps': [{'order': 'ordered'}]} for i in range(1, 23)]}
        spark, trino = fixture('spark', engine='spark', purpose='reference'), fixture('trino', engine='trino', purpose='reference')
        for doc in (spark, trino):
            for r in doc['records']: r['steps'][0]['result'] = result([])
        reference = agree(work, spark, trino)
        self.assertEqual(reference['queries']['q01'], [result([])])
        trino['records'][-1]['steps'][0]['result'] = result()
        with self.assertRaisesRegex(ValueError, 'row counts'):
            agree(work, spark, trino)


class RunnerTest(unittest.TestCase):
    def run_fixture(self, behavior, streams=1):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        (root / 'step.sql').write_text("SELECT ';not a separator'\n")
        work = {'queries': [{'id': 'q01', 'steps': [{'file': 'step.sql', 'sqlSha256': 'digest', 'order': 'ordered'},
                                                  {'file': 'step.sql', 'sqlSha256': 'digest', 'order': 'ordered'}]}]}
        clients = []
        class Client:
            closed = False
            calls = 0
            def execute(self, sql, timeout):
                self.calls += 1
                self.sql = sql
                if behavior == 'warmup-failure':
                    raise QueryFailure('query-timeout', 'server-failed', 'requested')
                query_id = 'replayed' if behavior == 'replay' else str(uuid.uuid4())
                return {'queryId': query_id, 'state': 'FINISHED', 'resultConsumed': True,
                        'elapsedMs': 1, 'result': result()}
            def close(self): self.closed = True
        def factory(env):
            client = Client()
            clients.append(client)
            return client
        run = run_window(work, root, {}, {'streams': streams, 'warmup': 1, 'iterations': 5, 'queryTimeout': 1}, factory=factory)
        return run, clients

    def test_persistent_clients_execute_steps_without_splitting_and_close(self):
        run, clients = self.run_fixture('passed', streams=2)
        self.assertEqual(run['status'], 'passed')
        self.assertEqual(len(clients), 2)
        self.assertTrue(all(c.calls == 12 and c.closed for c in clients))
        self.assertTrue(all("';not a separator'" in c.sql for c in clients))
        self.assertNotIn('p99Ms', run['queries']['q01'])

    def test_warmup_failure_blocks_measurements_and_retains_query_cancellation(self):
        run, clients = self.run_fixture('warmup-failure')
        self.assertEqual(run['status'], 'failed')
        self.assertEqual(run['summary']['completed'], 0)
        self.assertEqual(run['records'][0]['failedQueryId'], 'server-failed')
        self.assertEqual(run['records'][0]['resourceDrain'], 'unverified')
        self.assertTrue(clients[0].closed)

    def test_duplicate_execution_id_cannot_be_counted_as_another_step(self):
        run, _ = self.run_fixture('replay')
        self.assertEqual(run['status'], 'failed')
        self.assertIn('replayed', run['records'][0]['error'])

class QueryPreparationTest(unittest.TestCase):
    def test_projection_aliases_functions_with_spaces_literals_and_wildcards(self):
        from tpc_results import projection_names, normalize_projections
        self.assertEqual(projection_names("SELECT sum (x), 'literal,comma' AS text, t.y FROM t"), [None, 'text', 'y'])
        self.assertIsNone(projection_names('SELECT t.* FROM t'))
        a = {'columns': [{'name': '_col0', 'type': 'float'}], 'rows': [[1.0]]}
        b = {'columns': [{'name': 'sum(x)', 'type': 'float'}], 'rows': [[1.0]]}
        compare_result(normalize_projections(a, [None]), normalize_projections(b, [None]))
        with self.assertRaisesRegex(ValueError, 'explicit projection'):
            normalize_projections({'columns': [{'name': 'wrong', 'type': 'integer'}]}, ['right'])

    def test_day_interval_adaptation_never_modifies_quoted_text(self):
        from prepare_tpc_workload import ansi_day_intervals
        sql = "SELECT '14 days', d + 14 days FROM t -- 15 days\n"
        converted = ansi_day_intervals(sql)
        self.assertIn("'14 days'", converted)
        self.assertIn("d + INTERVAL '14' DAY", converted)
        self.assertIn('-- 15 days', converted)

    def test_harness_and_observed_runtime_are_required_and_frozen(self):
        baseline = [fixture(f'b{i}') for i in range(3)]
        for key in ('harnessId',):
            document = copy.deepcopy(baseline[0]);document['identity'].pop(key)
            with self.assertRaises(ValueError): validate_window(document)
        current = [fixture(f'c{i}') for i in range(3)]
        for d in current:d['clientRuntimes'] = [{'javaVersion': 'different-test-only'}]
        with self.assertRaises(Incompatible): compare(baseline, current)

class EnvironmentAndSnapshotTest(unittest.TestCase):
    def environment_fixture(self):
        return {'schemaVersion': 1, 'kind': 'tpc-environment', 'exclusive': True,
                'engine': {'name': 'pangrova', 'version': 'unit-fixture', 'artifactSha256': 'f' * 64},
                'sourceRevision': 'unit-fixture', 'hardware': [{'host': 'fixture-host', 'cpuModel': 'test CPU',
                    'storage': 'test storage', 'network': 'test network', 'memoryBytes': 1000, 'cpus': 8}],
                'resources': {'totalCpu': 4, 'totalMemoryBytes': 900, 'coordinatorMemoryBytes': 100,
                    'workerHeapBytes': 200, 'workerNativeBytes': 100, 'workerDirectBytes': 100, 'backgroundMemoryBytes': 100},
                'settings': {'configurationSha256': 'e' * 64}, 'cachePolicy': {'mode': 'warm', 'procedure': 'test full warmup'},
                'client': {'fixture': True}, 'workers': [{'id': 'worker', 'host': 'fixture-host',
                    'process': 100, 'endpoint': 'http://fixture.invalid'}]}

    def test_resource_overcommit_duplicate_processes_cold_and_placeholder_hardware_rejected(self):
        from tpc_common import write_json
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'environment.json'
            original = self.environment_fixture();write_json(path, original);environment(path)
            for field in ('overcommit', 'duplicate', 'cold', 'placeholder'):
                doc = copy.deepcopy(original)
                if field == 'overcommit':doc['resources']['workerNativeBytes'] = 800
                if field == 'duplicate':doc['workers'].append({**doc['workers'][0], 'id': 'second', 'endpoint': 'http://other.invalid'})
                if field == 'cold':doc['cachePolicy']['mode'] = 'cold'
                if field == 'placeholder':doc['hardware'][0]['cpuModel'] = 'REPLACE'
                write_json(path, doc)
                with self.subTest(field=field), self.assertRaises(ValueError):environment(path)

    def test_snapshot_is_checked_through_each_sut_and_mismatch_fails(self):
        from tpc_catalog import verify_client
        class Client:
            def __init__(self, value):self.value = value;self.sql = None
            def execute(self, sql, timeout):
                self.sql = sql
                return {'state': 'FINISHED', 'resultConsumed': True, 'queryId': 'metadata-fixture',
                        'result': {'rows': [[self.value]]}}
        binding = {'tables': {'lineitem': {'snapshotId': 123}}}
        config = {'catalog': 'tpc', 'schema': 'sf100'}
        client = Client(123)
        verify_client(client, 'pangrova', config, binding, 1)
        self.assertIn('"lineitem$refs"', client.sql)
        verify_client(client, 'spark', config, binding, 1)
        self.assertIn('`lineitem`.refs', client.sql)
        with self.assertRaisesRegex(ValueError, 'snapshot'):
            verify_client(Client(124), 'trino', config, binding, 1)

    def test_docker_capacity_preflight_rejects_oversubscription_before_launch(self):
        from tpc_cluster import validate_capacity
        plan = {'nodes': [{'context': 'unit-fixture', 'composeFile': 'fixture.yml',
                          'services': {'worker': {'role': 'worker'}}}]}
        def run(command, json_output=False):
            if '{{.Name}}' in command:return 'host'
            if '{{.MemTotal}}' in command:return '100'
            if '{{.NCPU}}' in command:return '8'
            return {'services': {'worker': {'mem_limit': 101, 'cpus': 2}}}
        with self.assertRaisesRegex(ValueError, 'actual Docker host capacity'):
            validate_capacity(plan, {'resources': {'backgroundMemoryBytes': 0}}, run)

class RecreatedProcessEvidenceTest(unittest.TestCase):
    def test_recreated_container_ids_pids_and_ports_do_not_change_hardware_identity(self):
        baseline = [fixture(f'b{i}') for i in range(3)]
        current = [fixture(f'c{i}') for i in range(3)]
        for n, d in enumerate(baseline + current):
            d['identity']['environment']['workers'] = [{'id': f'container-{n}', 'process': n + 1,
                                                        'host': 'fixed-test-host', 'endpoint': f'http://fixed-test-host:{10000+n}'}]
        self.assertEqual(compare(baseline, current)['status'], 'within-threshold')
        for d in current:d['identity']['environment']['workers'][0]['host'] = 'different-test-host'
        with self.assertRaises(Incompatible):compare(baseline, current)


if __name__ == '__main__':
    unittest.main()
