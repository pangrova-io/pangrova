import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[3] / "pangrova-worker-native/bin/lance-ffi-provenance.py"
SPEC = importlib.util.spec_from_file_location("lance_provenance", SCRIPT)
PROVENANCE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PROVENANCE)


class LanceArchiveProvenanceTest(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name) / "source"
        self.native = self.root / "pangrova-worker-native"
        self.crate = self.native / "lance-ffi"
        files = ["Cargo.toml", "Cargo.lock", "build.rs", "rust-toolchain.toml", ".cargo/config.toml",
                 ".cargo/owned-lance-sdk.toml", "src/lib.rs", "include/lance_c_ffi.h"]
        for name in files:
            self.write(self.crate / name, name)
        self.write(self.native / "bin/build-lance-ffi.sh", "build the verified SDK")
        self.write(self.native / "bin/prepare-lance-sdk.sh", "prepare exact SDK")
        self.write(self.native / "patches/lance-writer-drain.patch", "owned patch")
        self.protocol = self.root / "pangrova-protocol/src/main/proto/common.proto"
        self.write(self.protocol, "message TaskAttemptId {}")
        self.write(self.root / "pangrova-protocol/src/main/proto-imports/imported.proto", "import")
        self.archive = Path(self.directory.name) / "liblance_c_ffi.a"
        self.write(self.archive, "compiled archive")
        self.header = self.crate / "include/lance_c_ffi.h"
        self.capture = Path(self.directory.name) / "capture.json"
        self.capture.write_text(json.dumps(PROVENANCE.inputs(self.root)))

    @staticmethod
    def write(path, value):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value)

    def seal(self):
        PROVENANCE.seal(self.root, self.archive, self.header, "test-target", self.capture)

    def verify(self):
        PROVENANCE.verify(self.root, self.archive, self.header, "test-target")

    def test_requires_receipt_and_checks_actual_archive(self):
        with self.assertRaises(FileNotFoundError):
            self.verify()
        self.seal()
        self.verify()
        self.archive.write_text("different archive at the same path")
        with self.assertRaisesRegex(ValueError, "provenance differs"):
            self.verify()

    def test_rejects_protocol_change_and_addition_after_build(self):
        self.seal()
        original = self.protocol.read_text()
        self.protocol.write_text("message TaskAttemptId { string worker_identity = 10; }")
        with self.assertRaises(ValueError):
            self.verify()
        self.protocol.write_text(original)
        self.write(self.protocol.parent / "new.proto", "new protocol input")
        with self.assertRaises(ValueError):
            self.verify()

    def test_refuses_to_seal_when_sources_changed_during_cargo(self):
        self.write(self.crate / "src/lib.rs", "changed during compilation")
        with self.assertRaisesRegex(ValueError, "changed during compilation"):
            self.seal()
        self.assertFalse(Path(str(self.archive) + ".provenance.json").exists())

    def test_rejects_changed_sdk_override_configuration(self):
        self.seal()
        self.write(self.crate / ".cargo/owned-lance-sdk.toml", "changed the SDK inputs")
        with self.assertRaisesRegex(ValueError, "provenance differs"):
            self.verify()

    def test_requires_exact_owned_cargo_resolution_for_each_patched_crate(self):
        sdk = self.native / "deps-download/lance-sdk-owned"
        packages = [
            {"name": name, "version": "0.20.0", "source": None,
             "manifest_path": str(sdk / f"{name}-0.20.0/Cargo.toml")}
            for name in ("lance", "lance-encoding", "lance-file", "lance-io")
        ]
        PROVENANCE.verify_sdk_resolution(self.root, {"packages": packages})
        for change in ({"source": "registry+https://github.com/rust-lang/crates.io-index"},
                       {"version": "0.20.1"}, {"manifest_path": str(self.crate / "Cargo.toml")}):
            with self.subTest(change=change), self.assertRaisesRegex(ValueError, "verified SDK"):
                PROVENANCE.verify_sdk_resolution(self.root, {"packages": [packages[0] | change, *packages[1:]]})
        with self.assertRaisesRegex(ValueError, "exactly one resolved lance crate"):
            PROVENANCE.verify_sdk_resolution(self.root, {"packages": packages[1:]})

    def test_packaged_receipt_is_relocatable_but_header_and_target_are_fenced(self):
        self.seal()
        relocated = Path(self.directory.name) / "relocated"
        shutil.copytree(self.root, relocated)
        installed_header = Path(self.directory.name) / "installed/include/lance_c_ffi.h"
        installed_header.parent.mkdir(parents=True)
        shutil.copy2(self.header, installed_header)
        PROVENANCE.verify(relocated, self.archive, installed_header, "test-target")
        with self.assertRaises(ValueError):
            PROVENANCE.verify(relocated, self.archive, installed_header, "other-target")
        installed_header.write_text("different header")
        with self.assertRaises(ValueError):
            PROVENANCE.verify(relocated, self.archive, installed_header, "test-target")


if __name__ == "__main__":
    unittest.main()
