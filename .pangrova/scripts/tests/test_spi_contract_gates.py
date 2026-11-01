"""The central pangrova-spi must participate in both contract gates."""
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPTS = Path(__file__).resolve().parents[1]


class SpiContractGatesTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        scripts = self.root / '.pangrova/scripts'
        scripts.mkdir(parents=True)
        for name in ('check-spi-annotations.sh', 'check-spi-stability.sh'):
            (scripts / name).write_text((SCRIPTS / name).read_text())
        (scripts / '.spi-annotation-baseline.txt').write_text('')
        (self.root / '.pangrova/modules.yaml').write_text(
            'artifacts:\n  pangrova-spi:\n    path: pangrova-spi\n')
        self.source = self.root / 'pangrova-spi/src/main/java/example/Contract.java'
        self.source.parent.mkdir(parents=True)

    def tearDown(self):
        self.temp.cleanup()

    def gate(self, name):
        return subprocess.run(['bash', str(self.root / '.pangrova/scripts' / name)],
                              capture_output=True, text=True)

    def test_central_spi_rejects_missing_stability_annotation(self):
        self.source.write_text('package example;\npublic interface Contract {}\n')
        result = self.gate('check-spi-annotations.sh')
        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        self.assertIn('NEW unannotated public SPI', result.stderr)
        self.assertIn('pangrova-spi/src/main/java/example/Contract.java', result.stderr)
        self.source.write_text('package example;\n@Experimental\npublic interface Contract {}\n')
        result = self.gate('check-spi-annotations.sh')
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)

    def test_central_spi_stable_contract_requires_production_implementation(self):
        self.source.write_text('package example;\n@Stable(since = "0.1")\n'
                               'public interface Contract {}\n')
        result = self.gate('check-spi-stability.sh')
        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        self.assertIn('zero implementors', result.stderr)
        test = self.root / 'pangrova-provider/src/test/java/example/ImplementationTest.java'
        test.parent.mkdir(parents=True)
        test.write_text('package example;\nclass ImplementationTest implements Contract {}\n')
        result = self.gate('check-spi-stability.sh')
        self.assertEqual(1, result.returncode, result.stdout + result.stderr)
        main = self.root / 'pangrova-provider/src/main/java/example/Implementation.java'
        main.parent.mkdir(parents=True)
        main.write_text('package example;\nclass Implementation implements Contract {}\n')
        result = self.gate('check-spi-stability.sh')
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)


if __name__ == '__main__':
    unittest.main()
