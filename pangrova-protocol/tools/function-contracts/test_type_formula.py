"""Parser/evaluator checks; the registered Native binder is the separate oracle."""

import unittest

from type_formula import FormulaParser, evaluate_parameter


class TypeFormulaTest(unittest.TestCase):
    def evaluate(self, source, variables=None):
        variables = variables or {}
        return evaluate_parameter(FormulaParser(source, variables).parse(), variables)

    def test_precedence_and_left_associativity(self):
        self.assertEqual(5, self.evaluate("17 % 7 + 2 * 3 - 4"))
        self.assertEqual(1, self.evaluate("12 / 3 / 4"))
        self.assertEqual(-6, self.evaluate("-2 * (1+2)"))

    def test_comparison_and_right_associative_conditional(self):
        self.assertEqual(2, self.evaluate("0 ? 1 : 1 ? 2 : 3"))
        self.assertEqual(7, self.evaluate("a>=b ? max(a,b) : min(a,b)", {"a": 7, "b": 2}))
        for source in ("2==2", "2!=3", "2<3", "2<=2", "3>2", "3>=3"):
            self.assertEqual(1, self.evaluate(source))

    def test_remainder_and_division_truncate_toward_zero(self):
        self.assertEqual(-2, self.evaluate("-7 / 3"))
        self.assertEqual(-1, self.evaluate("-7 % 3"))
        self.assertEqual(1, self.evaluate("7 % -3"))

    def test_type_formula_has_no_implicit_defaults(self):
        for source in ("", "a+1", "p$+1", "1=1", "1&&2", "2 3", "min(1)",
                       "max(1,2,3)", "max(1,2", "1;2", "1 ? 2", "1e2", "+1"):
            with self.subTest(source=source), self.assertRaises(ValueError):
                self.evaluate(source)

    def test_parameter_overflow_and_invalid_arithmetic_are_rejected(self):
        for source in ("2147483648", "9223372036854775807+1", "1/0", "1%0",
                       "(-9223372036854775807-1)/-1", "-(-9223372036854775807-1)"):
            with self.subTest(source=source), self.assertRaises(ValueError):
                self.evaluate(source)

    def test_conditional_does_not_conceal_a_native_invalid_operand(self):
        with self.assertRaises(ValueError):
            self.evaluate("1 ? 2 : 1/0")

    def test_actual_constraint_shape_is_not_replaced_by_a_formula_default(self):
        source = "min(38, p1+s2+max(0,s2-s1))"
        node = FormulaParser(source, ("p1", "s1", "s2")).parse()
        self.assertEqual(24, evaluate_parameter(node, {"p1": 20, "s1": 2, "s2": 3}))
        self.assertEqual("MIN", node.canonical()["operation"])
        with self.assertRaises(ValueError):
            evaluate_parameter(node, {"p1": 20, "s1": 2})

    def test_non_integer_parameter_cannot_pass_as_an_integer(self):
        node = FormulaParser("p+1", ("p",)).parse()
        for value in (True, 1.5, "2"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                evaluate_parameter(node, {"p": value})

    def test_bounds(self):
        for source in ("(" * 66 + "1" + ")" * 66, "1+" * 2049 + "1", "1" * 65537):
            with self.subTest(length=len(source)), self.assertRaises(ValueError):
                self.evaluate(source)


if __name__ == "__main__":
    unittest.main()
