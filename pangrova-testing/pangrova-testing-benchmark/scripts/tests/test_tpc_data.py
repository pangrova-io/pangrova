"""Real Parquet conversion tests; generated fixtures are never performance evidence."""

import importlib.util
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
from decimal import Decimal

SCRIPTS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SCRIPTS))
from tpc_common import arrow_schema, file_digest, read_json, table_definitions
from generate_tpc_data import generate, generator_commands
from tpc_common import write_json

SPEC = importlib.util.spec_from_file_location('tpc_conversion', SCRIPTS / 'csv-to-parquet.py')
conversion = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(conversion)


class DataTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.source = self.root / 'csv'
        self.source.mkdir()
        self.output = self.root / 'parquet'

    def fixtures(self, benchmark, chunks=1):
        for name, columns in table_definitions(benchmark).items():
            for child in range(1, chunks + 1):
                values = []
                for col in columns:
                    typ = col['type']
                    values.append('2001-01-01' if typ == 'date' else
                                  '1.25' if typ.startswith('decimal') else
                                  'first row' if typ.startswith(('char', 'varchar')) else '1')
                suffix = (f'.tbl.{child}' if chunks > 1 else '.tbl') if benchmark == 'tpch' else (
                    f'_{child}_{chunks}.dat' if chunks > 1 else '.dat')
                (self.source / (name + suffix)).write_text('|'.join(values) + '|\n')

    def test_complete_schemas_and_64_bit_identifiers(self):
        self.assertEqual(len(table_definitions('tpch')), 8)
        self.assertEqual(len(table_definitions('tpcds')), 24)
        import pyarrow as pa
        self.assertEqual(arrow_schema(table_definitions('tpch')['orders']).field('o_orderkey').type, pa.int64())

    def test_preserves_first_row_and_types_and_splits_files(self):
        import pyarrow.parquet as pq
        self.fixtures('tpch')
        source = self.source / 'lineitem.tbl'
        source.write_text(source.read_text() * 3)
        result = conversion.convert('tpch', self.source, self.output, rows_per_file=2)
        files = result['tables']['lineitem']['files']
        self.assertEqual([f['rows'] for f in files], [2, 1])
        table = pq.read_table(self.output / files[0]['path'])
        self.assertEqual(table['l_quantity'][0].as_py(), Decimal('1.25'))
        self.assertEqual(table['l_comment'][0].as_py(), 'first row')
        self.assertEqual((self.output / '_SUCCESS').read_text().strip(), file_digest(self.output / 'dataset.json'))
        self.assertEqual(read_json(self.output / 'dataset.json')['datasetId'], result['datasetId'])

    def test_all_tpcds_chunks_use_named_typed_schema_and_nullable_strings(self):
        import pyarrow.parquet as pq
        self.fixtures('tpcds', 2)
        path = self.source / 'item_2_2.dat'
        parts = path.read_text().split('|')
        parts[4] = ''
        path.write_text('|'.join(parts))
        result = conversion.convert('tpcds', self.source, self.output, chunks=2)
        self.assertEqual(result['tables']['item']['rows'], 2)
        file = result['tables']['item']['files'][1]
        table = pq.read_table(self.output / file['path'])
        self.assertEqual(table['i_item_desc'].null_count, 1)
        self.assertEqual(table.column_names, [c['name'] for c in table_definitions('tpcds')['item']])

    def test_missing_table_chunk_empty_and_mixed_layout_fail_before_output(self):
        self.fixtures('tpcds', 2)
        missing = self.source / 'item_2_2.dat'
        original = missing.read_text()
        for action in ('missing', 'empty', 'mixed'):
            with self.subTest(action=action):
                missing.write_text(original)
                if action == 'missing':
                    missing.unlink()
                elif action == 'empty':
                    missing.write_text('')
                else:
                    (self.source / 'item.dat').write_text(original)
                with self.assertRaisesRegex(ValueError, 'missing|empty|mixed'):
                    conversion.convert('tpcds', self.source, self.output, chunks=2)
                self.assertFalse(self.output.exists())

    def test_empty_directory_never_reports_success(self):
        with self.assertRaisesRegex(ValueError, 'missing table'):
            conversion.convert('tpch', self.source, self.output)

    def test_invalid_decimal_and_trailing_data_leave_no_completion_marker(self):
        self.fixtures('tpch')
        source = self.source / 'lineitem.tbl'
        original = source.read_text()
        for value in (original.replace('1.25', 'bad', 1), original.rstrip('\n') + 'extra\n'):
            source.write_text(value)
            with self.assertRaises(ValueError):
                conversion.convert('tpch', self.source, self.output)
            self.assertFalse(self.output.exists())
            self.assertEqual(list(self.root.glob('.parquet.*')), [])

    def test_required_null_rejected_and_existing_output_not_overwritten(self):
        self.fixtures('tpch')
        path = self.source / 'orders.tbl'
        path.write_text(path.read_text().replace('1|', '|', 1))
        with self.assertRaisesRegex(ValueError, 'NULL in required'):
            conversion.convert('tpch', self.source, self.output)
        self.output.mkdir()
        with self.assertRaisesRegex(ValueError, 'already exists'):
            conversion.convert('tpch', self.source, self.output)

    def test_generator_never_uses_optional_action_as_output_and_no_step_for_one_chunk(self):
        commands = list(generator_commands('tpch', Path('/tool/dbgen'), Path('/output'), 100, 1))
        self.assertEqual(len(commands), 8)
        self.assertTrue(all('-O' not in c and '-S' not in c for c in commands))
        commands = list(generator_commands('tpcds', Path('/tool/dsdgen'), Path('/output'), 100, 2))
        self.assertEqual([c[c.index('-CHILD') + 1] for c in commands if '-CHILD' in c], ['1'] * 4 + ['2'] * 4)
        self.assertEqual(len(list(generator_commands('tpcds', Path('/tool/dsdgen'), Path('/output'), 1, 8))), 1)

    def test_generator_collects_every_child_failure_and_retains_diagnostics(self):
        tool = self.root / 'tool'
        tool.mkdir()
        binary = tool / 'dbgen'
        binary.write_text(f'#!{sys.executable}\nimport sys\nprint(" ".join(sys.argv[1:]))\n'
                          'sys.exit(7 if sys.argv[-1] == "n" else 0)\n')
        binary.chmod(0o700)
        provenance = self.root / 'provenance.json'
        write_json(provenance, {'binarySha256': file_digest(binary), 'version': 'test-only'})
        with self.assertRaisesRegex(ValueError, 'generator child failures.*7'):
            generate('tpch', 1, self.root / 'datasets', 1, 3, tool, provenance)
        self.assertFalse((self.root / 'datasets/tpch/sf1').exists())
        logs = list((self.root / 'datasets/tpch').glob('*.failed-logs/*.log'))
        self.assertEqual(len(logs), 8)
        self.assertEqual({p.read_text().strip().split()[-1] for p in logs},
                         {'L', 'O', 'c', 's', 'n', 'r', 'P', 'S'})

    def test_large_input_is_consumed_in_bounded_batches_with_all_rows_preserved(self):
        import pyarrow.csv as pcsv
        self.fixtures('tpch')
        source = self.source / 'lineitem.tbl'
        row = source.read_text().replace('first row', 'x' * 128)
        rows = 32003
        with source.open('w') as output:
            for _ in range(rows):
                output.write(row)
        sizes = []
        open_csv = pcsv.open_csv
        class ObservedReader:
            def __init__(self, reader): self.reader = reader
            def __iter__(self): return self
            def __next__(self):
                batch = next(self.reader)
                sizes.append(batch.nbytes)
                return batch
            def close(self): self.reader.close()
        with patch.object(pcsv, 'open_csv', side_effect=lambda *a, **k: ObservedReader(open_csv(*a, **k))):
            document = conversion.convert('tpch', self.source, self.output,
                                          block_size=8192, rows_per_file=1000)
        self.assertEqual(document['tables']['lineitem']['rows'], rows)
        self.assertGreater(source.stat().st_size, 8 * 1024 * 1024)
        self.assertGreater(len(sizes), 100)
        self.assertLess(max(sizes), 32 * 1024)

class DatasetEvidenceTest(unittest.TestCase):
    setUp = DataTest.setUp
    fixtures = DataTest.fixtures

    def test_structural_fixture_is_not_scale_qualified_and_corruption_is_rejected(self):
        from tpc_contract import dataset, qualify_generated_dataset
        self.fixtures('tpch')
        conversion.convert('tpch', self.source, self.output)
        document = dataset(self.output / 'dataset.json')
        with self.assertRaisesRegex(ValueError, 'scale qualification'):
            qualify_generated_dataset(document)
        file = self.output / document['tables']['orders']['files'][0]['path']
        with file.open('ab') as corrupt:
            corrupt.write(b'corrupt')
        with self.assertRaisesRegex(ValueError, 'checksum/size'):
            dataset(self.output / 'dataset.json')


if __name__ == '__main__':
    unittest.main()
