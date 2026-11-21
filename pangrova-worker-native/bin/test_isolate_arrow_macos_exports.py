"""Tests for the Mach-O translation of Arrow's public symbol policy."""

import importlib.util
import unittest
from pathlib import Path


spec = importlib.util.spec_from_file_location(
    "isolate_arrow_exports", Path(__file__).with_name("isolate-arrow-macos-exports.py")
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class ArrowSymbolPolicyTest(unittest.TestCase):
    def test_public_cpp_namespaces_and_template_specializations(self):
        for symbol in (
            "__ZN5arrow6Status2OKE", "__ZTVN5arrow6BufferE",
            "__ZNSt3__16vectorIN5arrow6flight14FlightEndpointEEE",
            "__ZN14arrow_vendored7xxhash8XXH3_64bEv",
            "__ZN13opentelemetry3v115trace6TracerE",
        ):
            with self.subTest(symbol=symbol):
                self.assertTrue(module.is_public(symbol))

    def test_public_c_entry_points(self):
        for symbol in ("_arrow_malloc", "_ArrowArrayStream", "_descriptor_table_Flight_2eproto"):
            with self.subTest(symbol=symbol):
                self.assertTrue(module.is_public(symbol))

    def test_private_dependencies_and_misleading_identifiers(self):
        for symbol in (
            "_SSL_new", "_FLAGS_grpc_trace", "_grpc_call_start_batch", "_upb_alloc",
            "__ZN6google8protobuf7MessageE", "__ZN4absl12lts_202401168MutexE",
            "__ZNSt3__111char_traitsIcEE", "__Z38descriptor_table_Flight_2eproto_getterv",
            "__ZN15arrowNotANamespaceE", "_other_arrow_function",
        ):
            with self.subTest(symbol=symbol):
                self.assertFalse(module.is_public(symbol))

    def test_unresolved_static_dependencies_are_rejected_even_when_not_exported(self):
        for symbol in (
            "__ZN3re211StringPiece6setptrEPKc", "__ZN6google8protobuf7MessageE",
            "__ZN4absl12lts_202401168MutexE", "__ZN9grpc_core7ChannelE",
            "_grpc_call_start_batch", "_gpr_malloc", "_upb_alloc",
        ):
            line = f"                 (undefined) external {symbol} (from dependency)"
            with self.subTest(symbol=symbol):
                self.assertEqual(module.unresolved_static_imports(line), [line.strip()])

    def test_dynamic_lookup_is_rejected_but_actual_public_and_system_bindings_are_allowed(self):
        unresolved = "(undefined) external _missing (dynamically looked up)"
        allowed = (
            "(undefined) external _malloc (from libSystem)\n"
            "(undefined) external __ZN5arrow6Status2OKE (from libarrow)\n"
            "(__TEXT,__text) external __ZN3re211StringPiece6setptrEPKc\n"
        )
        self.assertEqual(module.unresolved_static_imports(allowed), [])
        self.assertEqual(module.unresolved_static_imports(allowed + unresolved), [unresolved])


if __name__ == "__main__":
    unittest.main()
