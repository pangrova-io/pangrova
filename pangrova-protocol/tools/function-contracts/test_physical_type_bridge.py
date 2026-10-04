"""Bridge declaration/generator checks do not replace actual Native differential tests."""

from copy import deepcopy
from pathlib import Path
import json
import unittest

from physical_type_bridge import expression, render_bridge, validate_bridge


SOURCE = Path(__file__).resolve().parents[2] / "src/main/resources/function-contracts/sql-bindings.json"


class PhysicalTypeBridgeTest(unittest.TestCase):
    def setUp(self):
        self.bridge = json.loads(SOURCE.read_text(encoding="utf-8"))["physicalTypeBridge"]

    def test_one_program_preserves_existing_factories_and_erased_binding_parameters(self):
        validate_bridge(self.bridge)
        self.assertEqual(21, len(self.bridge["projections"]))
        rendered = render_bridge(self.bridge)
        self.assertEqual(rendered, render_bridge(deepcopy(self.bridge)))
        for projection in self.bridge["projections"]:
            for variant in projection["variants"]:
                self.assertIn('facebook::velox::getType("' + variant["nativeName"] + '"', rendered)
        by_kind = {entry["kind"]: entry for entry in self.bridge["projections"]}
        self.assertEqual("string", by_kind["varchar"]["variants"][0]["bindingKind"])
        self.assertEqual("string", by_kind["fixed_char"]["variants"][0]["bindingKind"])
        self.assertEqual("precision_timestamp", by_kind["precision_timestamp_tz"]["variants"][0]["bindingKind"])
        self.assertEqual("NEVER", by_kind["map"]["orderable"])
        self.assertEqual("ALL_CHILDREN", by_kind["map"]["comparable"])
        self.assertEqual("ALWAYS", by_kind["list"]["known"])

    def test_decimal_retains_exact_input_parameters_and_native_precision_split(self):
        decimal = next(entry for entry in self.bridge["projections"] if entry["kind"] == "decimal")
        self.assertEqual(["p", "s"], [entry["name"] for entry in decimal["parameters"]])
        self.assertTrue(all(entry["nativeArgument"] for entry in decimal["parameters"]))
        self.assertEqual(["BIGINT", "HUGEINT"], [entry["physicalTag"] for entry in decimal["variants"]])
        from type_formula import evaluate, load_formula
        for precision in range(1, 39):
            for scale in range(precision + 1):
                variables = {"p": precision, "s": scale}
                matches = [entry for entry in decimal["variants"] if all(
                    evaluate(load_formula(predicate, variables), variables) for predicate in entry["predicates"])]
                self.assertEqual(1, len(matches))
                self.assertEqual("BIGINT" if precision <= 18 else "HUGEINT", matches[0]["physicalTag"])
                self.assertEqual(precision, evaluate(load_formula(matches[0]["bindingParameters"]["precision"], variables), variables))
                self.assertEqual(scale, evaluate(load_formula(matches[0]["bindingParameters"]["scale"], variables), variables))

    def test_invalid_parameters_report_the_declared_kind_field_bounds_and_actual_value(self):
        rendered = render_bridge(self.bridge)
        self.assertIn('Canonical bridge decimal.precision must be in [1, 38]; actual=', rendered)
        self.assertIn('Canonical bridge decimal.scale must be in [0, 38]; actual=', rendered)
        self.assertIn('Canonical bridge precision_time.precision must be in [0, 6]; actual=', rendered)
        self.assertIn('Canonical bridge decimal parameter constraint failed: (s <= p)', rendered)
        self.assertIn('std::to_string(p)', rendered)
        self.assertIn('std::to_string(s)', rendered)
        self.assertIn('std::string("Canonical bridge decimal parameter constraint failed: (s <= p)") '
                      '+ "; precision=" + std::to_string(p)', rendered)

    def test_parameter_diagnostics_are_derived_from_the_same_declaration_not_a_type_table(self):
        bridge = deepcopy(self.bridge)
        decimal = next(entry for entry in bridge["projections"] if entry["kind"] == "decimal")
        decimal["parameters"][0]["maximum"] = 27
        rendered = render_bridge(bridge)
        self.assertIn('Canonical bridge decimal.precision must be in [1, 27]; actual=', rendered)
        self.assertNotIn('Canonical bridge decimal.precision must be in [1, 38]; actual=', rendered)

    def test_unknown_duplicate_inexact_and_undeclared_program_fields_are_rejected(self):
        mutations = (lambda b: b.update(ignored="anything"),
                     lambda b: b["projections"].append(deepcopy(b["projections"][0])),
                     lambda b: b["projections"][0].update(known="SOMETIMES"),
                     lambda b: b["projections"][0]["variants"][0].update(nativeName="BOOLEAN();"),
                     lambda b: b["projections"][8]["parameters"][0].update(minimum=True),
                     lambda b: b["projections"][8]["parameters"][0].update(maximum=1 << 31),
                     lambda b: b["projections"][8]["parameters"][0].update(nativeArgument=1),
                     lambda b: b["projections"][14]["variants"][0]["predicates"][0]["operands"][0].update(value="unbound"))
        for mutation in mutations:
            bridge = deepcopy(self.bridge)
            mutation(bridge)
            with self.subTest(mutation=mutation), self.assertRaises(ValueError):
                validate_bridge(bridge)

    def test_generated_formula_operations_use_checked_helpers_and_eager_conditional_call(self):
        formula = {"operation": "CONDITIONAL", "operands": [
            {"operation": "INTEGER", "value": 0},
            {"operation": "DIVIDE", "operands": [{"operation": "INTEGER", "value": 1},
                                                  {"operation": "INTEGER", "value": 0}]},
            {"operation": "INTEGER", "value": 2}]}
        self.assertIn("typeFormulaConditional(", expression(formula))
        self.assertIn("typeFormulaDivide(", expression(formula))
        self.assertNotIn("?", expression(formula))

    def test_manifest_and_actual_factories_are_lowered_from_every_same_variant(self):
        rendered = render_bridge(self.bridge)
        variants = [variant for projection in self.bridge["projections"] for variant in projection["variants"]]
        self.assertEqual(len(variants), rendered.count("manifest->add_physical_type_projections()"))
        for projection in self.bridge["projections"]:
            self.assertIn('bridgeSkeleton(logical, "' + projection["kind"] + '")', rendered)
            for child in projection["children"]:
                self.assertIn('bridgeField(*logical_kind, "' + child["field"] + '")', rendered)
        self.assertIn("projection->add_recursive_children()", rendered)
        self.assertNotIn("set_homogeneous_row", rendered)

    def test_manifest_formulas_preserve_signed_64_bit_exactness_without_json_number_rounding(self):
        bridge = deepcopy(self.bridge)
        bridge["projections"][0]["predicates"] = [{"operation": "EQUAL", "operands": [
            {"operation": "INTEGER", "value": (1 << 63) - 1},
            {"operation": "INTEGER", "value": -(1 << 63)}]}]
        rendered = render_bridge(bridge)
        self.assertIn("set_integer_value(int64_t{9223372036854775807})", rendered)
        self.assertIn("set_integer_value(std::numeric_limits<int64_t>::min())", rendered)

    def test_native_parameter_order_is_generated_only_from_actual_factory_arguments(self):
        from physical_type_manifest import manifest_variant
        by_kind = {entry["kind"]: entry for entry in self.bridge["projections"]}
        for kind in ("precision_timestamp", "precision_timestamp_tz", "precision_time", "varchar", "fixed_char"):
            for variant in by_kind[kind]["variants"]:
                rendered = "\n".join(manifest_variant(by_kind[kind], variant))
                self.assertNotIn("add_native_integer_parameter_paths", rendered)
        for variant in by_kind["decimal"]["variants"]:
            rendered = "\n".join(manifest_variant(by_kind["decimal"], variant))
            self.assertEqual(2, rendered.count("add_native_integer_parameter_paths"))
            first = rendered.index("native_parameter_0->add_path()->set_field_number(bridgeField(*binding_kind, \"precision\")")
            second = rendered.index("native_parameter_1->add_path()->set_field_number(bridgeField(*binding_kind, \"scale\")")
            self.assertLess(first, second)

    def test_native_parameter_cannot_be_guessed_from_erased_or_derived_metadata(self):
        bridge = deepcopy(self.bridge)
        decimal = next(entry for entry in bridge["projections"] if entry["kind"] == "decimal")
        decimal["variants"][0]["bindingParameters"]["precision"] = {"operation":"INTEGER", "value":38}
        with self.assertRaisesRegex(ValueError, "one exact canonical binding field"):
            render_bridge(bridge)


if __name__ == "__main__":
    unittest.main()
