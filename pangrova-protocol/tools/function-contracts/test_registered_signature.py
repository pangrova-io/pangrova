from copy import deepcopy
import unittest

from registered_signature import normalize_signature


def native_type(base, *parameters, **properties):
    return {"base": base, "parameters": list(parameters), "variableArity": False, **properties}


def integer(name, constraint=""):
    return {"name": name, "kind": "INTEGER", "constraint": constraint}


def signature():
    return {"arguments": [native_type("decimal", native_type("p"), native_type("s"))],
            "output": native_type("decimal", native_type("r"), native_type("s")),
            "variables": [integer("s"), integer("p"), integer("r", "min(38,p+1)")],
            "variableArity": False, "constantArguments": [False]}


class RegisteredSignatureTest(unittest.TestCase):
    def test_registered_formula_is_preserved_as_ast_not_replaced(self):
        normalized = normalize_signature(signature(), "SCALAR")
        self.assertEqual(["p", "r", "s"], [v["name"] for v in normalized["variables"]])
        formula = normalized["variables"][1]["formula"]
        self.assertEqual("MIN", formula["operation"])
        self.assertEqual("ADD", formula["operands"][1]["operation"])
        self.assertEqual("min(38,p+1)", normalized["variables"][1]["constraint"])

    def test_aggregate_requires_and_preserves_real_intermediate(self):
        source = signature()
        with self.assertRaises(ValueError):
            normalize_signature(source, "AGGREGATE")
        source["intermediate"] = native_type("row", native_type("bigint", rowFieldName="count"))
        self.assertEqual(source["intermediate"], normalize_signature(source, "AGGREGATE")["intermediate"])
        self.assertEqual(source["intermediate"], normalize_signature(source, "WINDOW")["intermediate"])

    def test_cycle_unknown_symbol_or_lexer_skip_is_rejected(self):
        for constraint in ("r+1", "unknown+1", "p@+1"):
            source = signature()
            source["variables"][2]["constraint"] = constraint
            with self.subTest(constraint=constraint), self.assertRaises(ValueError):
                normalize_signature(source, "SCALAR")
        source = signature()
        source["variables"] = [integer("p", "r+1"), integer("s"), integer("r", "p+1")]
        with self.assertRaises(ValueError):
            normalize_signature(source, "SCALAR")

    def test_only_digit_only_input_constraint_is_a_fixed_integer_parameter(self):
        for constraint, expected in (("3", 3), ("03", 3), ("(3)", None), ("min(3,3)", None)):
            source = signature()
            source["variables"][0]["constraint"] = constraint
            normalized = normalize_signature(source, "SCALAR")
            variable = next(v for v in normalized["variables"] if v["name"] == "s")
            self.assertEqual(expected, variable.get("inputIntegerConstant"))

    def test_constant_mask_and_variadic_bounds_are_not_defaulted(self):
        for constants in ([], [True, False], [1]):
            source = signature()
            source["constantArguments"] = constants
            with self.subTest(constants=constants), self.assertRaises(ValueError):
                normalize_signature(source, "SCALAR")
        source = signature()
        source.update(arguments=[], constantArguments=[], variableArity=True)
        with self.assertRaises(ValueError):
            normalize_signature(source, "SCALAR")

    def test_enum_named_row_and_function_properties_are_not_dropped(self):
        source = signature()
        source["variables"].append({"name": "E", "kind": "ENUM", "constraint": "enum-contract"})
        source["arguments"] = [native_type("function", native_type("row", native_type("bigint")),
                                           native_type("E"))]
        self.assertEqual(source["arguments"], normalize_signature(source, "SCALAR")["arguments"])

    def test_actual_simple_function_generated_type_variable_name_is_preserved(self):
        source = {"arguments": [native_type("__user_T1")], "output": native_type("__user_T1"),
                  "variables": [{"name": "__user_T1", "kind": "TYPE", "constraint": "",
                                 "knownTypesOnly": False, "orderableTypesOnly": True,
                                 "comparableTypesOnly": True}],
                  "variableArity": False, "constantArguments": [False]}
        result = normalize_signature(source, "SCALAR")
        self.assertEqual("__user_T1", result["variables"][0]["name"])
        self.assertTrue(result["variables"][0]["orderableTypesOnly"])

    def test_invalid_variable_flags_and_unknown_fields_fail_closed(self):
        for mutation in (lambda s: s.update(unrecognized=True),
                         lambda s: s["variables"][0].update(knownTypesOnly=True),
                         lambda s: s["variables"].append(deepcopy(s["variables"][0])),
                         lambda s: s["output"].update(base="r", parameters=[])):
            source = signature()
            mutation(source)
            with self.assertRaises(ValueError):
                normalize_signature(source, "SCALAR")


if __name__ == "__main__":
    unittest.main()
