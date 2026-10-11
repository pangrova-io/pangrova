"""Actual declaration projection does not invent registration availability."""

from copy import deepcopy
from pathlib import Path
import unittest

from binding_declarations import read_declarations
from binding_manifest import render_binding_manifest


RESOURCE = Path(__file__).resolve().parents[2] / "src/main/resources/function-contracts/sql-bindings.json"


class BindingManifestTest(unittest.TestCase):
    def setUp(self):
        self.declaration = read_declarations(RESOURCE.read_text(encoding="utf-8"))

    def test_all_declared_bindings_have_exact_urn_and_name_without_inventory_claims(self):
        rendered = "\n".join(render_binding_manifest(self.declaration))
        self.assertEqual(107, rendered.count("manifest->add_definitions()"))
        self.assertEqual(107, rendered.count("FUNCTION_NULLABILITY_CANONICAL_DECLARED_OUTPUT"))
        for entry in self.declaration["bindings"]:
            self.assertIn(f'reference->set_urn("{entry["canonicalUrn"]}")', rendered)
            self.assertIn(f'reference->set_name("{entry["name"]}")', rendered)
        self.assertNotIn("add_registered_functions", rendered)
        self.assertNotIn("add_functions", rendered)
        self.assertNotIn("registerFunction", rendered)

    def test_normalized_purpose_and_governed_order_are_not_reconstructed_from_callback_arity(self):
        rendered = "\n".join(render_binding_manifest(self.declaration))
        embed = rendered[rendered.index('reference->set_name("ai_embed")'):]
        embed = embed[:embed.index('reference->set_name("ai_extract")')]
        self.assertIn("normalized_0->set_ordinal(2)", embed)
        self.assertIn('set_string_value("RAW")', embed)
        self.assertIn('governed_0->set_name("invocation_id")', embed)
        self.assertIn('governed_25->set_name("output_vector_dimension")', embed)
        self.assertLess(embed.index('add_enum_values("QUERY")'), embed.index('governed_0'))

    def test_actual_rewrite_dependencies_and_structural_lowering_are_preserved(self):
        rendered = "\n".join(render_binding_manifest(self.declaration))
        self.assertIn('rewrite_0->mutable_target()->set_name("is_distinct_from")', rendered)
        self.assertIn('rewrite_0->mutable_target()->set_urn("extension:io.substrait:functions_comparison")', rendered)
        self.assertIn("rewrite_1->add_arguments()->set_node_ordinal(0)", rendered)
        self.assertIn('definition->set_structural_lowering("GROUP_ID")', rendered)
        self.assertIn('definition->set_registration_dependency("WORKER_AI_CALLBACK")', rendered)

    def test_hash_and_order_are_stable_but_declaration_changes_are_visible(self):
        generated = render_binding_manifest(self.declaration)
        self.assertEqual(generated, render_binding_manifest(deepcopy(self.declaration)))
        changed = deepcopy(self.declaration)
        embed = next(entry for entry in changed["bindings"] if entry["name"] == "ai_embed")
        embed["normalizedArguments"][0]["default"] = "QUERY"
        self.assertNotEqual(generated, render_binding_manifest(changed))

    def test_callback_null_policy_changes_the_single_declaration_semantics_identity(self):
        original = "\n".join(render_binding_manifest(self.declaration))
        changed = deepcopy(self.declaration)
        callback = next(entry for entry in changed["bindings"] if entry["name"] == "file_from_url")
        callback["runtimeSignature"]["defaultNullBehavior"] = True
        rendered = "\n".join(render_binding_manifest(changed))
        self.assertNotEqual(original, rendered)
        # All output topology, ownership and ordered arguments remain in the original declaration.
        def without_hashes(lines):
            return [line for line in lines.splitlines() if "sha256" not in line]
        self.assertEqual(without_hashes(original), without_hashes(rendered))

    def test_rewrite_requires_a_unique_declared_direct_dependency(self):
        for ambiguous in (False, True):
            changed = deepcopy(self.declaration)
            direct = next(entry for entry in changed["bindings"] if entry["name"] == "is_distinct_from")
            if ambiguous:
                duplicate = deepcopy(direct)
                duplicate["name"] = "ambiguous_distinct"
                changed["bindings"].append(duplicate)
            else:
                changed["bindings"].remove(direct)
            with self.assertRaisesRegex(ValueError, "exact direct scalar declaration"):
                render_binding_manifest(changed)


if __name__ == "__main__":
    unittest.main()
