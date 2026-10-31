"""Tail calibration uses complete raw evidence; test numbers never enter baselines."""

from pathlib import Path
import sys
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from calibrate_tpc_tail import calibrate
from test_tpch_regression import results_fixture


class TailCalibrationTest(unittest.TestCase):
    def fixture_context(self, results):
        return {'schemaVersion': 1, 'kind': 'tpc-tail-context', 'status': 'passed', 'scaleFactor': 100,
                'snapshotsVerifiedAfterTail': True, 'referenceId': 'synthetic-reference',
                'clientRuntimes': [{'javaVersion': 'synthetic-runtime'}],
                'identity': {'timingScope': 'cli-end-to-end', 'querySteps': {
                    query: [record['sqlSha256']] for query, record in results['queries'].items()}}}

    def test_complete_genuine_format_can_be_explicitly_calibrated_without_changing_samples(self):
        results = results_fixture()
        baseline = calibrate(results, self.fixture_context(results))
        self.assertEqual(baseline['queries']['q01']['samples'], results['queries']['q01']['samples'])
        self.assertEqual(baseline['queries']['q01']['baselineP99Ms'], results['queries']['q01']['p99Ms'])
        self.assertIn('p99Ms', results['queries']['q01'])

    def test_short_runs_missing_context_and_wrong_sql_never_calibrate(self):
        for invalid in ('short', 'context', 'sql'):
            results = results_fixture();context = self.fixture_context(results)
            if invalid == 'short':results['queries']['q01']['samples'] = [100] * 3
            if invalid == 'context':context['snapshotsVerifiedAfterTail'] = False
            if invalid == 'sql':context['identity']['querySteps']['q01'] = ['wrong']
            with self.subTest(invalid=invalid), self.assertRaises(ValueError):
                calibrate(results, context)
