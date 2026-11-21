from copy import deepcopy
import json
import unittest

from registration_catalog import normalize_catalog, read_export, registration_digest
from test_registered_signature import signature


def catalog():
    return {"schemaVersion": 4, "functions": [
        {"kind": "SCALAR", "name": "actual_registered_name", "overloads": [signature()],
         "resolutionPolicy": "SCALAR_RUNTIME_PRIORITY",
         "simpleImplementations": [], "vectorImplementations": [vector_implementation()]}],
            "specialForms": ["if", "coalesce"]}


def vector_implementation():
    return {"signature": signature(), "deterministic": True, "defaultNullBehavior": True,
            "supportsFlattening": False, "companionFunction": False}


class RegistrationCatalogTest(unittest.TestCase):
    def test_deterministic_digest_is_order_independent_but_semantic_change_sensitive(self):
        source = catalog()
        other = deepcopy(source)
        other["specialForms"].reverse()
        other["functions"][0]["overloads"][0]["variables"].reverse()
        self.assertEqual(registration_digest(source), registration_digest(other))
        other["functions"][0]["overloads"][0]["constantArguments"][0] = True
        other["functions"][0]["vectorImplementations"][0]["signature"]["constantArguments"][0] = True
        self.assertNotEqual(registration_digest(source), registration_digest(other))

    def test_duplicate_signature_is_deduplicated_without_first_match_execution_policy(self):
        source = catalog()
        source["functions"][0]["overloads"].append(signature())
        self.assertEqual(1, len(normalize_catalog(source)["functions"][0]["overloads"]))

    def test_unknown_metadata_and_duplicate_identity_are_rejected(self):
        for mutation in (lambda s: s.update(schemaVersion=1),
                         lambda s: s["functions"].append(deepcopy(s["functions"][0])),
                         lambda s: s["specialForms"].append("if"),
                         lambda s: s["functions"][0].update(windowProcessMode="ROWS"),
                         lambda s: s["functions"][0].update(defaultNullBehavior=1)):
            source = catalog()
            mutation(source)
            with self.assertRaises(ValueError):
                normalize_catalog(source)

    def test_window_process_metadata_is_not_defaulted(self):
        source = catalog()
        source["functions"] = [{"kind": "WINDOW", "name": "rank", "overloads": [signature()],
                                "resolutionPolicy": "ORDERED_FIRST_MATCH"}]
        with self.assertRaises(ValueError):
            normalize_catalog(source)
        source["functions"][0].update(aggregateWindow=False, windowProcessMode="PARTITION")
        normalized = normalize_catalog(source)
        self.assertEqual("PARTITION", normalized["functions"][0]["windowProcessMode"])

    def test_actual_row_constructor_without_signatures_requires_registered_special_form(self):
        source = catalog()
        source["functions"] = [{"kind": "SCALAR", "name": "row_constructor", "overloads": [],
                                "resolutionPolicy": "SCALAR_RUNTIME_PRIORITY",
                                "simpleImplementations": [], "vectorImplementations": []}]
        with self.assertRaises(ValueError):
            normalize_catalog(source)
        source["specialForms"].append("row_constructor")
        self.assertEqual([], normalize_catalog(source)["functions"][0]["overloads"])

    def test_window_and_aggregate_overload_order_is_not_sorted_or_deduplicated(self):
        for kind in ("WINDOW", "AGGREGATE"):
            first, second = signature(), signature()
            second["output"]["base"] = "bigint"
            if kind == "AGGREGATE":
                first["intermediate"] = deepcopy(first["output"])
                second["intermediate"] = deepcopy(second["output"])
            entry = {"kind": kind, "name": "ordered", "overloads": [first, second, first],
                     "resolutionPolicy": "ORDERED_FIRST_MATCH"}
            if kind == "WINDOW":
                entry.update(aggregateWindow=False, windowProcessMode="PARTITION")
            source = catalog()
            source["functions"] = [entry]
            original = normalize_catalog(source)
            self.assertEqual(3, len(original["functions"][0]["overloads"]))
            original_digest = registration_digest(source)
            source["functions"][0]["overloads"][0:2] = [second, first]
            self.assertNotEqual(registration_digest(source), original_digest)

    def test_resolver_policy_cannot_be_implicitly_defaulted_or_swapped(self):
        for mutation in (lambda entry: entry.pop("resolutionPolicy"),
                         lambda entry: entry.update(resolutionPolicy="ORDERED_FIRST_MATCH")):
            source = catalog()
            mutation(source["functions"][0])
            with self.assertRaises(ValueError):
                normalize_catalog(source)

    def test_actual_json_export_duplicate_keys_and_non_finite_numbers_are_rejected(self):
        self.assertEqual(catalog(), read_export(json.dumps(catalog())))
        for source in ('{"schemaVersion":1,"schemaVersion":1}', '{"x":NaN}', '{"x":Infinity}'):
            with self.subTest(source=source), self.assertRaises(ValueError):
                read_export(source)

    def test_distinct_window_dependency_requires_the_actual_matching_registration(self):
        source = catalog()
        source["functions"] = [{"kind": "WINDOW", "name": "sum", "overloads": [signature()],
                                "resolutionPolicy": "ORDERED_FIRST_MATCH", "aggregateWindow": True,
                                "windowProcessMode": "ROWS", "distinctImplementation": "distinct_sum"}]
        with self.assertRaises(ValueError):
            normalize_catalog(source)
        target = deepcopy(source["functions"][0])
        target.update(name="distinct_sum", windowProcessMode="PARTITION")
        del target["distinctImplementation"]
        source["functions"].append(target)
        normalized = normalize_catalog(source)
        self.assertEqual("distinct_sum", normalized["functions"][1]["distinctImplementation"])
        for mutation in (lambda e: e.update(aggregateWindow=False),
                         lambda e: e.update(windowProcessMode="ROWS"),
                         lambda e: e["overloads"][0]["output"].update(base="bigint")):
            bad = deepcopy(source)
            mutation(bad["functions"][1])
            with self.assertRaises(ValueError):
                normalize_catalog(bad)


if __name__ == "__main__":
    unittest.main()
