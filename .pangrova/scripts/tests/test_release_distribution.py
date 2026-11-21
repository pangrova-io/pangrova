"""Exercise the release staging contract without publishing assets."""
from pathlib import Path
import importlib.util
import json
import shutil
import struct
import subprocess
import sys
import tarfile
import tempfile
import unittest
from zipfile import ZipFile


ROOT = Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location("stage_release", ROOT / "deploy/release/stage-release.py")
release = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = release
spec.loader.exec_module(release)


def library_bytes(platform):
    header = bytearray(64)
    if platform.os == "linux":
        header[:7] = b"\x7fELF\x02\x01\x01"
        struct.pack_into("<HH", header, 16, 3, platform.machine)
    else:
        header[:4] = b"\xcf\xfa\xed\xfe"
        struct.pack_into("<I", header, 4, platform.machine)
        struct.pack_into("<I", header, 12, 6)
    return bytes(header)


class ReleaseDistributionTest(unittest.TestCase):
    def setUp(self):
        work = tempfile.TemporaryDirectory()
        self.addCleanup(work.cleanup)
        self.root = Path(work.name)
        self.artifacts = self.root / "artifacts"
        self.artifacts.mkdir()

    def artifact(self, classifier):
        platform = release.PLATFORMS[classifier]
        raw_dir = self.artifacts / f"release-native-lib-{classifier}"
        raw_dir.mkdir()
        raw = raw_dir / platform.library
        raw.write_bytes(library_bytes(platform))
        bundle_dir = self.artifacts / f"release-native-bundle-{classifier}"
        bundle_dir.mkdir()
        bundle = bundle_dir / f"pangrova-native-bundle-1.1-{classifier}.jar"
        with ZipFile(bundle, "w") as jar:
            jar.writestr(platform.resource, raw.read_bytes())
        return raw, bundle

    def test_four_variant_matrix_uses_exact_pairs(self):
        for classifier in release.PLATFORMS:
            self.artifact(classifier)
        self.assertEqual(set(release.validate_matrix(self.artifacts, release.PLATFORMS)), set(release.PLATFORMS))

    def test_rejects_missing_or_extra_artifact_directories(self):
        first = "linux-x86_64"
        self.artifact(first)
        with self.assertRaisesRegex(ValueError, "Missing"):
            release.validate_matrix(self.artifacts, release.PLATFORMS)
        self.artifact("linux-aarch_64")
        with self.assertRaisesRegex(ValueError, "Unexpected"):
            release.validate_matrix(self.artifacts, [first])

    def test_rejects_duplicate_artifacts_within_a_directory(self):
        classifier = "osx-x86_64"
        _, bundle = self.artifact(classifier)
        shutil.copy2(bundle, bundle.with_name(f"pangrova-native-bundle-2-{classifier}.jar"))
        with self.assertRaisesRegex(ValueError, "Expected exactly one"):
            release.validate_matrix(self.artifacts, [classifier])

    def test_rejects_mislabeled_machine_and_mismatched_bytes(self):
        classifier = "linux-aarch_64"
        raw, _ = self.artifact(classifier)
        raw.write_bytes(library_bytes(release.PLATFORMS["linux-x86_64"]))
        with self.assertRaisesRegex(ValueError, "shared library"):
            release.validate_matrix(self.artifacts, [classifier])
        raw.write_bytes(library_bytes(release.PLATFORMS[classifier]) + b"other")
        with self.assertRaisesRegex(ValueError, "differ"):
            release.validate_matrix(self.artifacts, [classifier])

    def test_archive_resolves_each_launcher_and_installs_only_matching_jni(self):
        generator = release.runpy.run_path(str(ROOT / "deploy/docker/stage-role-classpaths.py"))
        project = self.root / "project"
        for directory in ("bin", "deploy/docker", "deploy/release", "etc", "examples"):
            (project / directory).mkdir(parents=True)
        shutil.copy2(ROOT / "deploy/docker/stage-role-classpaths.py", project / "deploy/docker")
        shutil.copytree(ROOT / "deploy/release/preview", project / "deploy/release/preview",
                        ignore=shutil.ignore_patterns("__pycache__"))
        shutil.copy2(ROOT / "deploy/release/pangrova-preview.sh", project / "deploy/release")
        shutil.copy2(ROOT / "bin/pangrova-classpath.sh", project / "bin")
        for name in ("iceberg-smoke.py", "iceberg-smoke.sql"):
            (project / "examples" / name).write_text("sample\n")
        for role, module in generator["ROLES"].items():
            target = project / module / "target"
            (target / "maven-archiver").mkdir(parents=True)
            (target / "maven-archiver/pom.properties").write_text(f"artifactId={module.name}\nversion=1.1\n")
            with ZipFile(target / f"{module.name}-1.1.jar", "w") as jar:
                jar.writestr(generator["MAIN_CLASS"][role], b"fixture class")
                jar.writestr("META-INF/MANIFEST.MF", f"Automatic-Module-Name: pangrova.{role}\n")
            dependency = self.root / "dep.jar"
            with ZipFile(dependency, "w"):
                pass
            (target / "runtime-classpath.txt").write_text(str(dependency))
        cli = project / "pangrova-client/pangrova-client-cli/target"
        cli.mkdir(parents=True)
        (cli / "pangrova-client-cli-1.1-exec.jar").touch()
        for classifier in release.PLATFORMS:
            self.artifact(classifier)

        output = self.root / "output"
        release.stage_release(project, self.artifacts, output, "1.1", "a" * 40, release.PLATFORMS)
        names = [path.name for path in output.glob("libpangrova_native-*")]
        self.assertEqual(len(names), 4)
        self.assertEqual(len((output / "SHA256SUMS").read_text().splitlines()), 10)
        self.assertEqual(len(list(output.glob("*.tar.gz"))), 1)
        name = "pangrova-1.1-linux-x86_64"
        clean = self.root / "clean"
        clean.mkdir()
        with tarfile.open(output / f"{name}.tar.gz") as archive:
            archive.extractall(clean)
        installation = clean / name
        manifest = json.loads((installation / "manifest.json").read_text())
        for role in generator["ROLES"]:
            value = subprocess.check_output(
                ["bash", "-c", 'source bin/pangrova-classpath.sh; pangrova_runtime_classpath "$1"', "-", role],
                cwd=installation, env={"PANGROVA_HOME": str(installation)}, text=True).strip()
            entries = value.split(":")
            self.assertTrue(all(Path(entry).is_file() for entry in entries))
            self.assertEqual(any(entry.endswith(manifest["nativeBundle"]) for entry in entries),
                             role != "coordinator")
