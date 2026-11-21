"""Control-contract tests do not claim actual Native execution coverage."""

from copy import deepcopy
from pathlib import Path
import unittest

from execution_selection import read_selection, select_bound, selection_digest
from generate_execution_selection import render_cpp, render_java
from registration_catalog import canonical_json
from contract_generation_inputs import generation_input_digest


DECLARATION = Path(__file__).resolve().parents[2] / "src/main/resources/function-contracts/execution-selection.json"


class ExecutionSelectionTest(unittest.TestCase):
    def setUp(self):
        self.value = read_selection(DECLARATION.read_text(encoding="utf-8"))

    def test_rendered_languages_share_the_exact_contract_identity_and_program(self):
        digest = selection_digest(self.value)
        input_digest = generation_input_digest(DECLARATION)
        cpp, java = render_cpp(self.value, input_digest), render_java(self.value)
        self.assertIn(digest, cpp)
        self.assertIn(digest, java)
        self.assertIn("{1, false}, {2, false}, {3, true}", cpp)
        self.assertIn("new Rule(1, false), new Rule(2, false), new Rule(3, true)", java)
        self.assertIn(input_digest, cpp)
        self.assertEqual(cpp, render_cpp(deepcopy(self.value), input_digest))
        self.assertEqual(java, render_java(deepcopy(self.value)))
        self.assertNotIn("factory(", cpp)
        self.assertNotIn("desiredOutput", java)

    def test_vector_entry_is_selected_before_lower_priority_scalar(self):
        entries = [{"family": "SCALAR", "priority": 0}, {"family": "VECTOR", "priority": 9}]
        self.assertIs(entries[1], select_bound(self.value, entries, lambda entry: entry))

    def test_only_bound_entries_compete_and_equal_priority_preserves_actual_traversal(self):
        entries = [{"family": "SCALAR", "priority": 5}, {"family": "SCALAR", "priority": 1},
                   {"family": "SCALAR", "priority": 1}]
        self.assertIs(entries[1], select_bound(self.value, entries, lambda entry: entry))
        self.assertIs(entries[2], select_bound(self.value, entries, lambda entry: None if entry is entries[1] else entry))

    def test_binding_error_does_not_restart_selection_at_another_family(self):
        entries = [{"family": "VECTOR", "priority": 0}, {"family": "SCALAR", "priority": 0}]

        def failed_binding(entry):
            raise ArithmeticError("registered output constraint is undefined")

        with self.assertRaisesRegex(ArithmeticError, "registered output constraint"):
            select_bound(self.value, entries, failed_binding)

    def test_unsupported_or_duplicate_contract_operations_fail_closed(self):
        mutations = (lambda v: v.update(schemaVersion=True),
                     lambda v: v.update(coercion="ALLOW"),
                     lambda v: v.update(factoryInvocation="PLANNING"),
                     lambda v: v.update(initializationFailure="TRY_SCALAR"),
                     lambda v: v.update(constantValidation="BEFORE_SELECTION"),
                     lambda v: v.update(matching=["SIGNATURE"]),
                     lambda v: v["families"][0].update(family="UNKNOWN"),
                     lambda v: v["families"][1].update(family="SPECIAL_FORM"),
                     lambda v: v["families"][0].update(selection="BEST_OUTPUT"))
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                value = deepcopy(self.value)
                mutation(value)
                with self.assertRaises(ValueError):
                    read_selection(canonical_json(value))

    def test_family_order_is_content_identity_not_a_hidden_runtime_default(self):
        value = deepcopy(self.value)
        value["families"][0], value["families"][1] = value["families"][1], value["families"][0]
        altered = read_selection(canonical_json(value))
        self.assertNotEqual(selection_digest(self.value), selection_digest(altered))
        self.assertIn("new Rule(2, false), new Rule(1, false)", render_java(altered))


if __name__ == "__main__":
    unittest.main()
