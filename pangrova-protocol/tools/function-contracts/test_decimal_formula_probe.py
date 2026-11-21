from copy import deepcopy
import unittest

from decimal_formula_probe import argument, decimal_domain, predict_unique_output, request_cases
from registration_catalog import normalize_catalog
from test_registration_catalog import catalog


class DecimalFormulaProbeTest(unittest.TestCase):
    def test_entire_valid_decimal_domain_includes_all_zero_and_max_scale_boundaries(self):
        domain = decimal_domain()
        self.assertEqual(779, len(domain))
        self.assertEqual(len(domain), len(set(domain)))
        self.assertEqual((1, 0), domain[0])
        self.assertEqual((38, 38), domain[-1])
        self.assertIn((38, 0), domain)
        self.assertTrue(all(1 <= precision <= 38 and 0 <= scale <= precision for precision, scale in domain))

    def test_formula_is_consumed_from_actual_registration_not_function_name(self):
        entry = normalize_catalog(catalog())["functions"][0]
        self.assertEqual(argument("decimal", 13, 3), predict_unique_output(entry, [argument("decimal", 12, 3)]))
        self.assertEqual(argument("decimal", 38, 38), predict_unique_output(entry, [argument("decimal", 38, 38)]))

    def test_ambiguous_or_unknown_signature_is_not_selected_by_desired_return_type(self):
        entry = normalize_catalog(catalog())["functions"][0]
        other = deepcopy(entry["overloads"][0])
        other["output"]["parameters"][0]["base"] = "p"
        entry["overloads"].append(other)
        with self.assertRaisesRegex(ValueError, "ambiguous"):
            predict_unique_output(entry, [argument("decimal", 12, 3)])
        with self.assertRaisesRegex(ValueError, "absent"):
            predict_unique_output(entry, [argument("varchar")])

    def test_generated_native_requests_keep_exact_scale_and_no_literal_value_dependency(self):
        cases = list(request_cases(["actual_registered_name"], False))
        self.assertEqual(779, len(cases))
        self.assertEqual(["decimal(38,38)"], cases[-1][0]["arguments"])
        self.assertEqual([argument("decimal", 38, 38)], cases[-1][1])
        binary = request_cases(["actual_registered_name"], True)
        self.assertEqual(["decimal(1,0)", "decimal(1,0)"], next(binary)[0]["arguments"])


if __name__ == "__main__":
    unittest.main()
