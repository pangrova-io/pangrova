"""Report only repeated qualified experiments; retain failures and refuse biased ratios."""

import copy
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tpc_report import report
from test_tpc_suite import fixture


class ReportTest(unittest.TestCase):
    def experiment(self, engine, factor=1):
        result = [fixture(f'{engine}-{n}', factor, engine=engine) for n in range(3)]
        for document in result:
            env = document['identity']['environment']
            env.update(hardware=[{'host': 'test-hardware'}],
                       resources={'totalCpu': 8, 'totalMemoryBytes': 1000},
                       workers=[{'id': 'fixture-worker', 'host': 'test-hardware'}], cachePolicy={'mode': 'warm'}, settings={'fixture': True})
        return result

    def test_cross_engine_ratios_require_identical_cpu_ram_cache_and_data(self):
        left, right = self.experiment('pangrova'), self.experiment('trino', 2)
        result = report(left + right)
        self.assertEqual(len(result['comparisons']), 1)
        self.assertEqual(result['comparisons'][0]['leftOverRightSuiteTime'], 0.5)
        changed = copy.deepcopy(right)
        for d in changed:d['identity']['environment']['resources']['totalCpu'] = 16
        self.assertEqual(report(left + changed)['comparisons'], [])

    def test_failures_remain_visible_and_cost_is_unavailable_without_declared_rate(self):
        runs = self.experiment('pangrova')
        failed = {'status': 'failed', 'runId': 'failed-fixture', 'error': 'query-timeout'}
        result = report(runs + [failed])
        self.assertEqual(len(result['failedOrIncompleteWindows']), 1)
        self.assertIsNone(result['experiments'][0]['declaredCostPerMeasuredSuite'])
        for d in runs:d['identity']['environment']['costPerHour'] = 2
        self.assertGreater(report(runs)['experiments'][0]['declaredCostPerMeasuredSuite'], 0)

    def test_scaling_rejects_different_worker_hardware_or_component_budgets(self):
        small, large = self.experiment('pangrova'), self.experiment('pangrova', 0.6)
        for count, documents in ((1, small), (2, large)):
            for index, document in enumerate(documents):
                # Each experiment uses independent execution and metadata receipts.
                label = f'scaling-{count}-{index}'
                replacement = fixture(label, 1 if count == 1 else 0.6)
                replacement['identity']['environment'] = document['identity']['environment']
                document.clear();document.update(replacement)
                env = document['identity']['environment']
                env['workers'] = [{'id': f'worker-{n}', 'host': f'host-{n}'} for n in range(count)]
                env['hardware'] = [{'host': f'host-{n}', 'cpuModel': 'same-model', 'cpus': 8,
                                    'memoryBytes': 1000, 'storage': 'same-storage', 'network': 'same-link'}
                                   for n in range(count)]
                env['resources'].update(totalCpu=8 * count, totalMemoryBytes=1000 * count,
                                        coordinatorMemoryBytes=10, workerHeapBytes=300,
                                        workerNativeBytes=300, workerDirectBytes=100,
                                        workerOverheadBytes=100, backgroundMemoryBytes=10)
        comparisons = report(small + large)['comparisons']
        self.assertEqual(len(comparisons), 1)
        self.assertEqual(comparisons[0]['kind'], 'strong-scaling')
        self.assertEqual(comparisons[0]['referenceWorkers'], 1)
        for mutation in ('hardware', 'budget'):
            changed = copy.deepcopy(large)
            for d in changed:
                env = d['identity']['environment']
                if mutation == 'hardware':env['hardware'][0]['cpuModel'] = 'different-model'
                else:env['resources']['workerHeapBytes'] += 1
            self.assertEqual(report(small + changed)['comparisons'], [])
