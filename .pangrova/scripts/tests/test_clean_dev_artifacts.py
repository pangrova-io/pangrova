"""Run cleanup only in a disposable repository containing source-like build directories."""
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "clean-dev-artifacts.sh"


class CleanDevArtifactsTest(unittest.TestCase):
    def test_all_preserves_sources_including_untracked_packages(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            subprocess.run(["git", "init", "-q"], cwd=root, check=True)
            module = root / "new module"
            module.mkdir()
            (module / "pom.xml").touch()
            source = module / "src/main/java/org/example/build/Builder.java"
            source.parent.mkdir(parents=True)
            source.touch()
            resource = module / "src/main/resources/build-template/schema.sql"
            resource.parent.mkdir(parents=True)
            resource.touch()
            output = module / "target/classes"
            native = root / "pangrova-worker-native/build-asan"
            for path in (output, native):
                path.mkdir(parents=True)
            subprocess.run(["bash", str(SCRIPT), "--all", "--quiet"], cwd=root, check=True)
            self.assertTrue(source.is_file())
            self.assertTrue(resource.is_file())
            self.assertFalse(output.exists())
            self.assertFalse(native.exists())


if __name__ == "__main__":
    unittest.main()
