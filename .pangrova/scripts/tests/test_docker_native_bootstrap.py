"""Check that the pinned container builder stages native bootstrap inputs."""

from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[3]
DOCKERFILE = ROOT / "deploy/docker/Dockerfile.native-build"
ATOMIC_PATCH = "pangrova-worker-native/patches/velox-darwin-atomic-link.patch"


class DockerNativeBootstrapTest(unittest.TestCase):
    def setUp(self):
        # Treat multi-line RUN instructions as one and ignore comments.
        self.instructions = re.sub(r"\\\n\s*", " ", DOCKERFILE.read_text()).splitlines()

    def test_ripgrep_is_installed_before_native_prerequisite_check(self):
        first_bootstrap = next(i for i, line in enumerate(self.instructions)
                               if line.startswith("RUN ") and "setup-native-build-env.sh" in line)
        self.assertTrue(any(line.startswith("RUN ") and "apt-get install" in line
                            and re.search(r"\bripgrep\b", line)
                            for line in self.instructions[:first_bootstrap]),
                        "the builder's first bootstrap calls check_prerequisites, which requires rg")

    def test_velox_atomic_patch_is_staged_before_velox_bootstrap(self):
        velox_bootstrap = next(i for i, line in enumerate(self.instructions)
                               if line.startswith("RUN ") and "PANGROVA_NATIVE_BUILD_PHASE=velox" in line)
        self.assertIn(f"COPY {ATOMIC_PATCH} /workspace/{ATOMIC_PATCH}",
                      self.instructions[:velox_bootstrap])


if __name__ == "__main__":
    unittest.main()
