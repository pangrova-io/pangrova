from copy import deepcopy
import json
from pathlib import Path
import unittest

from binding_declarations import read_declarations, structural_contract_digest, unresolved_bindings
from generate_structural_contracts import render_header


RESOURCE = Path(__file__).resolve().parents[2] / "src/main/resources/function-contracts/sql-bindings.json"


class BindingDeclarationsTest(unittest.TestCase):
    def test_actual_declaration_preserves_complete_mapping_and_purpose(self):
        declaration = read_declarations(RESOURCE.read_text())
        self.assertEqual(94, len(declaration["bindings"]))
        embed = next(entry for entry in declaration["bindings"] if entry["name"] == "ai_embed")
        self.assertEqual("WORKER_AI_CALLBACK", embed["registrationDependency"])
        self.assertEqual({"ordinal": 2, "name": "purpose", "type": "VARCHAR", "constant": True,
                          "optional": True, "default": "RAW", "enumValues": ["RAW", "QUERY", "DOCUMENT"]},
                         embed["normalizedArguments"][0])

    def test_structural_contract_hash_is_canonical_not_field_order_dependent(self):
        contract = read_declarations(RESOURCE.read_text())["structuralContracts"][0]
        self.assertEqual("d3b04ee20b0ac364c040e3e9a775221cb5e6126e6391d39e749d51ba372cd1c6",
                         structural_contract_digest(contract))
        reordered = dict(reversed(list(contract.items())))
        self.assertEqual(structural_contract_digest(contract), structural_contract_digest(reordered))
        changed = deepcopy(contract)
        changed["output"]["ordinalityBase"] = 0
        self.assertNotEqual(structural_contract_digest(contract), structural_contract_digest(changed))

    def test_missing_real_registration_remains_a_gap_not_an_available_function(self):
        declaration = read_declarations(RESOURCE.read_text())
        empty = {"functions": [], "specialForms": ["and", "or", "coalesce", "row_constructor"]}
        missing = unresolved_bindings(declaration, empty)
        self.assertIn(("SCALAR", "arithmetic", "round", "round"), missing)
        self.assertIn(("WINDOW", "window", "sum0", "sum0"), missing)
        self.assertFalse(any(entry[2] == "ai_embed" for entry in missing))
        self.assertFalse(any(entry[2] == "and" for entry in missing))

    def test_duplicate_identity_enum_drift_or_rewrite_cycle_fails_closed(self):
        original = read_declarations(RESOURCE.read_text())
        for mutate in (lambda d: d["bindings"].append(deepcopy(d["bindings"][0])),
                       lambda d: d["bindings"][0].update(unknown=True),
                       lambda d: next(e for e in d["bindings"] if e["name"] == "ai_embed")
                       ["normalizedArguments"][0].update(default="AUTO"),
                       lambda d: next(e for e in d["bindings"] if e["name"] == "is_not_null")
                       ["rewrite"]["nodes"][1].update(nodes=[1])):
            changed = deepcopy(original)
            mutate(changed)
            with self.assertRaises(ValueError):
                read_declarations(json.dumps(changed))

    def test_structural_generation_is_strict_deterministic_and_not_a_native_registration(self):
        declaration = read_declarations(RESOURCE.read_text())
        self.assertEqual(render_header(declaration), render_header(deepcopy(declaration)))
        self.assertIn("std::array<uint8_t, 32> signature", render_header(declaration))
        self.assertNotIn("registerFunction", render_header(declaration))
        for mutate in (lambda d: d["structuralContracts"][0].update(id='bad";'),
                       lambda d: d["structuralContracts"][0]["output"].update(ordinalityBase=0),
                       lambda d: d["structuralContracts"][0]["argumentLayout"].update(types=["ANY"]),
                       lambda d: d["structuralContracts"][0]["options"][0].update(required=False)):
            changed = deepcopy(declaration)
            mutate(changed)
            with self.assertRaises(ValueError):
                read_declarations(json.dumps(changed))

    def test_purpose_callback_signature_cannot_remain_a_separate_two_argument_contract(self):
        declaration = read_declarations(RESOURCE.read_text())
        for signatures in ([["VARCHAR", "VARCHAR"]], [["VARCHAR", "VARCHAR", "BIGINT"]]):
            changed = deepcopy(declaration)
            embed = next(entry for entry in changed["bindings"] if entry["name"] == "ai_embed")
            embed["runtimeSignature"]["argumentSignatures"] = signatures
            with self.assertRaises(ValueError):
                read_declarations(json.dumps(changed))

    def test_governed_layout_keeps_repeated_message_names_and_exact_order(self):
        declaration = read_declarations(RESOURCE.read_text())
        arguments = declaration["governedLayouts"][0]["arguments"]
        self.assertEqual(["model", "model", "id"], arguments[1]["path"])
        self.assertEqual("EXACT_FLOAT", next(entry for entry in arguments
                                            if entry["name"] == "reuse_similarity_threshold")["encoding"])
        for mutate in (lambda d: d["governedLayouts"][0]["arguments"][1].update(path=[]),
                       lambda d: d["governedLayouts"][0]["arguments"][1].update(path=["model", "bad.name"]),
                       lambda d: d["governedLayouts"][0]["arguments"][0].update(allowAbsentMessage=False),
                       lambda d: d["governedLayouts"].append(deepcopy(d["governedLayouts"][0]))):
            changed = deepcopy(declaration)
            mutate(changed)
            with self.assertRaises(ValueError):
                read_declarations(json.dumps(changed))


if __name__ == "__main__":
    unittest.main()
