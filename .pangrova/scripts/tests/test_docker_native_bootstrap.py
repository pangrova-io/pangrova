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

    def test_direct_and_container_builds_use_the_same_lz4_archive(self):
        container = " ".join(self.instructions)
        url = re.search(r"https://codeload\.github\.com/lz4/lz4/tar\.gz/refs/tags/v[^ ]+",
                        container)
        self.assertIsNotNone(url)
        for mode in ("build", "asan"):
            with self.subTest(mode=mode):
                script = ROOT / f"pangrova-worker-native/bin/setup-native-{mode}-env.sh"
                self.assertIn('export ARROW_LZ4_URL="${ARROW_LZ4_URL:-' + url[0] + '}"',
                              script.read_text())

    def test_release_and_asan_bootstrap_use_the_same_upstream_snappy_archive(self):
        url = "https://codeload.github.com/google/snappy/tar.gz/refs/tags/1.1.10"
        for mode in ("build", "asan"):
            with self.subTest(mode=mode):
                script = ROOT / f"pangrova-worker-native/bin/setup-native-{mode}-env.sh"
                self.assertIn('export ARROW_SNAPPY_URL="${ARROW_SNAPPY_URL:-' + url + '}"',
                              script.read_text())

    def test_libunwind_pic_check_rescans_the_same_archives_as_the_production_link(self):
        script = (ROOT / "deploy/docker/build-static-libunwind.sh").read_text()
        compiler_link = script.split('-fPIC -shared -Wl,-z,defs', 1)[1].split(' -o ', 1)[0]
        self.assertRegex(compiler_link, r'--start-group "\$prefix/lib/libunwind\.a" '
                                         r'"\$prefix/lib/libunwind-\$\{arch\}\.a"')
        self.assertLess(compiler_link.index('libunwind-${arch}.a'),
                        compiler_link.index('-Wl,--end-group'))

    def test_folly_source_and_bootstrap_patches_are_staged_before_velox(self):
        velox_bootstrap = next(i for i, line in enumerate(self.instructions)
                               if line.startswith("RUN ") and "PANGROVA_NATIVE_BUILD_PHASE=velox" in line)
        copies = " ".join(line for line in self.instructions[:velox_bootstrap]
                          if line.startswith("COPY "))
        for patch in ("velox-folly-common-build.patch", "velox-folly-alignment-build.patch",
                      "folly-2026.01.05-openssl-const.patch",
                      "folly-2026.01.05-spookyhash-alignment.patch"):
            with self.subTest(patch=patch):
                self.assertIn(f"pangrova-worker-native/patches/{patch}", copies)


if __name__ == "__main__":
    unittest.main()
