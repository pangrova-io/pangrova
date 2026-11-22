"""Actual model artifact integrity and CI prerequisite ordering contracts."""

import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "prepare-e2e-ai-image.py"
SPEC = importlib.util.spec_from_file_location("prepare_e2e_ai_image", SCRIPT)
FIXTURE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(FIXTURE)


class ModelIntegrityTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.blob = b"real file contents used only for artifact integrity unit tests"
        digest = hashlib.sha256(self.blob).hexdigest()
        (self.root / "blobs").mkdir()
        self.blob_path = self.root / "blobs" / ("sha256-" + digest)
        self.blob_path.write_bytes(self.blob)
        self.entry = {"digest": "sha256:" + digest, "size": len(self.blob),
                      "mediaType": "application/vnd.ollama.image.model"}
        self.manifests = []
        for model in FIXTURE.MODELS:
            name, tag = model.split(":")
            manifest = self.root / "manifests" / "registry.ollama.ai" / "library" / name / tag
            manifest.parent.mkdir(parents=True)
            manifest.write_text(json.dumps({"config": self.entry, "layers": [self.entry]}))
            self.manifests.append(manifest)

    def test_both_manifest_identities_are_computed_from_verified_files(self):
        expected = hashlib.sha256(self.manifests[0].read_bytes()).hexdigest()
        self.assertEqual(FIXTURE.verify_models(self.root), {
            "completion-digest": expected, "embedding-digest": expected})

    def test_same_size_blob_corruption_is_rejected(self):
        self.blob_path.write_bytes(b"X" * len(self.blob))
        with self.assertRaisesRegex(ValueError, "checksum"):
            FIXTURE.verify_models(self.root)

    def test_truncation_and_missing_model_are_rejected(self):
        self.blob_path.write_bytes(self.blob[:-1])
        with self.assertRaisesRegex(ValueError, "size"):
            FIXTURE.verify_models(self.root)
        self.blob_path.write_bytes(self.blob)
        self.manifests[1].unlink()
        with self.assertRaises(FileNotFoundError):
            FIXTURE.verify_models(self.root)

    def test_manifest_cannot_reference_a_path_outside_the_blob_store(self):
        self.manifests[0].write_text(json.dumps({"config": {
            "digest": "../../other-file", "size": 1}, "layers": []}))
        with self.assertRaisesRegex(ValueError, "Invalid blob identity"):
            FIXTURE.verify_models(self.root)

    def test_configured_fixture_requires_fixed_threads_and_preserves_model_layers(self):
        with tempfile.TemporaryDirectory() as temporary:
            configured = Path(temporary) / "models"
            shutil.copytree(self.root, configured)
            for threads in (8, FIXTURE.INFERENCE_THREADS):
                contents = json.dumps({"num_thread": threads}).encode()
                digest = hashlib.sha256(contents).hexdigest()
                (configured / "blobs" / ("sha256-" + digest)).write_bytes(contents)
                parameters = {"digest": "sha256:" + digest, "size": len(contents),
                              "mediaType": "application/vnd.ollama.image.params"}
                for model in FIXTURE.MODELS:
                    manifest = FIXTURE.model_manifest(configured, model)
                    carried_layer = {**self.entry, "from": model}
                    manifest.write_text(json.dumps({"config": self.entry, "layers": [carried_layer, parameters]}))
                if threads != FIXTURE.INFERENCE_THREADS:
                    with self.assertRaisesRegex(ValueError, "inference thread count"):
                        FIXTURE.verify_configured_models(self.root, configured)
            expected = FIXTURE.verify_models(configured)
            self.assertEqual(FIXTURE.verify_configured_models(self.root, configured), expected)
            manifest.write_text(json.dumps({"config": self.entry, "layers": [parameters]}))
            with self.assertRaisesRegex(ValueError, "non-parameter layer"):
                FIXTURE.verify_configured_models(self.root, configured)

    def test_production_regression_receives_the_built_immutable_artifact(self):
        root = SCRIPT.parents[2]
        workflow = (root / ".github/workflows/ci.yml").read_text()
        production = workflow.split("  production-regression:", 1)[1].split("  tpch-performance:", 1)[0]
        self.assertLess(production.index("prepare-e2e-ai-image.py"),
                        production.index("full-regression verify"))
        self.assertIn('-Dpangrova.e2e.ai.image-id="$PANGROVA_E2E_AI_IMAGE_ID"', production)


if __name__ == "__main__":
    unittest.main()
