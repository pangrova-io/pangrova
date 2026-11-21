"""Structural contract generation checks; Native Tasks supply value oracles."""

from copy import deepcopy
from pathlib import Path
import unittest

from execution_selection import read_selection
from generate_special_type_programs import render_special_cpp, render_special_java
from registration_catalog import canonical_json
from special_type_programs import index_formula, render_index, validate_programs


DECLARATION = Path(__file__).resolve().parents[2] / "src/main/resources/function-contracts/execution-selection.json"


class SpecialTypeProgramsTest(unittest.TestCase):
    def setUp(self):
        self.value = read_selection(DECLARATION.read_text(encoding="utf-8"))["specialTypePrograms"]

    def test_same_declaration_generates_both_hosts_without_names_or_factories(self):
        declaration = {"specialTypePrograms": self.value}
        cpp, java = "\n".join(render_special_cpp(declaration)), render_special_java(declaration)
        for program in self.value["programs"]:
            self.assertIn(program["id"], cpp)
            self.assertIn(program["id"], java)
        self.assertNotIn("factory(", cpp)
        self.assertNotIn("factory(", java)
        self.assertNotIn("row_constructor", cpp)
        self.assertNotIn("coalesce", java)
        self.assertEqual(cpp, "\n".join(render_special_cpp(deepcopy(declaration))))
        self.assertEqual(java, render_special_java(deepcopy(declaration)))

    def test_indices_use_one_formula_ast_in_both_languages(self):
        source = "(argument_count / 2) * 2"
        self.assertIsNotNone(index_formula(source))
        self.assertEqual(render_index(source, "java"), render_index(source, "cpp"))
        self.assertIn("arity", render_index(source, "cpp"))

    def test_unbounded_arity_and_duplicate_identity_are_rejected(self):
        for mutate in (lambda v: v["programs"][0].update(maximumArity=257),
                       lambda v: v["programs"].append(deepcopy(v["programs"][0])),
                       lambda v: v.update(factoryEvaluation="INSTANTIATE")):
            value = deepcopy(self.value)
            mutate(value)
            with self.assertRaises(ValueError):
                validate_programs(value)

    def test_required_equality_operand_and_result_cannot_escape_actual_arity(self):
        exact = next(p for p in self.value["programs"] if p["id"] == "EXACT_SEQUENCE")
        for mutation in ({"equalToArgument": exact["minimumArity"]},):
            value = deepcopy(self.value)
            target = next(p for p in value["programs"] if p["id"] == "EXACT_SEQUENCE")
            target["constraints"][0].update(mutation)
            with self.assertRaises(ValueError):
                validate_programs(value)
        value = deepcopy(self.value)
        value["programs"][0]["result"] = {"argument": 256}
        with self.assertRaises(ValueError):
            validate_programs(value)

    def test_invalid_index_arithmetic_and_zero_stride_are_rejected(self):
        for source in ("argument_count / 0", "unknown", "argument_count % 2", "-1"):
            with self.subTest(source=source), self.assertRaises(ValueError):
                index_formula(source)
        value = deepcopy(self.value)
        value["programs"][0]["constraints"][0]["selection"]["step"] = 0
        with self.assertRaises(ValueError):
            validate_programs(value)

    def test_unhashable_native_kind_is_a_control_validation_error(self):
        value = deepcopy(self.value)
        value["programs"][0]["constraints"][0]["nativeKinds"] = [{"kind": "BOOLEAN"}]
        with self.assertRaises(ValueError):
            validate_programs(value)

    def test_checked_target_is_not_a_guess_from_expected_output(self):
        checked = next(p for p in self.value["programs"] if p["id"] == "DECLARED_TARGET_CHECKED")
        self.assertEqual({"declaredTarget": True}, checked["result"])
        self.assertEqual(["VARBINARY"], checked["constraints"][0]["targetKinds"])
        value = deepcopy(self.value)
        checked = next(p for p in value["programs"] if p["id"] == "DECLARED_TARGET_CHECKED")
        checked["result"] = {"declaredTarget": False}
        with self.assertRaises(ValueError):
            read_selection(canonical_json({
                **read_selection(DECLARATION.read_text(encoding="utf-8")), "specialTypePrograms": value}))


if __name__ == "__main__":
    unittest.main()
