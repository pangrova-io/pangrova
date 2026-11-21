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

    def test_default_removes_only_output_of_deleted_modules(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            subprocess.run(["git", "init", "-q"], cwd=root, check=True)
            retired = root / "pangrova-family/pangrova-family-core"
            (retired / "target/classes").mkdir(parents=True)
            active = root / "pangrova-family/pangrova-family-api"
            (active / "target/classes").mkdir(parents=True)
            (active / "pom.xml").touch()
            cargo = root / "pangrova-worker-native/lance-ffi"
            (cargo / "target/release").mkdir(parents=True)
            (cargo / "Cargo.toml").touch()
            subprocess.run(["bash", str(SCRIPT), "--quiet"], cwd=root, check=True)
            self.assertFalse(retired.exists())
            self.assertTrue((active / "target/classes").is_dir())
            self.assertTrue((cargo / "target/release").is_dir())


if __name__ == "__main__":
    unittest.main()
