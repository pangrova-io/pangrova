from copy import deepcopy
import unittest

from registration_catalog import normalize_catalog, registration_digest
from test_registration_catalog import catalog, vector_implementation
from test_registered_signature import signature


def implementation(priority=1, physical="BIGINT"):
    return {"signature": signature(), "priority": priority,
            "argumentsPhysical": [{"kind": physical, "children": []}],
            "outputPhysical": {"kind": physical, "children": []},
            "deterministic": True, "defaultNullBehavior": True}


class ScalarImplementationTest(unittest.TestCase):
    def test_actual_priority_physical_filters_and_equal_priority_traversal_are_preserved(self):
        source = catalog()
        entry = source["functions"][0]
        entry.update(simpleImplementations=[implementation(2), implementation(2, "HUGEINT")], vectorImplementations=[])
        normalized = normalize_catalog(source)["functions"][0]
        self.assertEqual(entry["simpleImplementations"][0]["priority"], normalized["simpleImplementations"][0]["priority"])
        self.assertEqual("HUGEINT", normalized["simpleImplementations"][1]["outputPhysical"]["kind"])
        changed = deepcopy(source)
        changed["functions"][0]["simpleImplementations"].reverse()
        self.assertNotEqual(registration_digest(source), registration_digest(changed))

    def test_missing_or_invented_implementation_signature_cannot_publish_as_inventory(self):
        for mutation in (lambda e: e.pop("simpleImplementations"),
                         lambda e: e.update(vectorImplementations=[]),
                         lambda e: e["vectorImplementations"][0]["signature"]["output"].update(base="bigint")):
            source = catalog()
            mutation(source["functions"][0])
            with self.assertRaises(ValueError):
                normalize_catalog(source)

    def test_priority_layout_flags_and_arity_are_exact_not_defaulted(self):
        for mutation in (lambda i: i.update(priority=True), lambda i: i.update(priority=-1),
                         lambda i: i.update(priority=1 << 32), lambda i: i.update(argumentsPhysical=[]),
                         lambda i: i.update(defaultNullBehavior=1),
                         lambda i: i["outputPhysical"].update(kind="decimal(12,3)"),
                         lambda i: i["outputPhysical"].update(children=None)):
            source = catalog()
            entry = implementation()
            mutation(entry)
            source["functions"][0].update(simpleImplementations=[entry], vectorImplementations=[])
            with self.assertRaises(ValueError):
                normalize_catalog(source)

    def test_vector_metadata_belongs_to_actual_entry_and_changes_identity(self):
        source = catalog()
        source["functions"][0]["simpleImplementations"] = [implementation()]
        before = registration_digest(source)
        source["functions"][0]["vectorImplementations"][0]["deterministic"] = False
        actual = normalize_catalog(source)["functions"][0]
        self.assertFalse(actual["vectorImplementations"][0]["deterministic"])
        self.assertTrue(actual["simpleImplementations"][0]["deterministic"])
        self.assertNotEqual(before, registration_digest(source))

    def test_vector_signature_order_is_not_sorted_or_deduplicated(self):
        source = catalog()
        first, second = vector_implementation(), vector_implementation()
        second["signature"]["constantArguments"][0] = True
        source["functions"][0]["overloads"].append(deepcopy(second["signature"]))
        source["functions"][0]["vectorImplementations"] = [first, second, first]
        normalized = normalize_catalog(source)["functions"][0]["vectorImplementations"]
        self.assertEqual(3, len(normalized))
        self.assertFalse(normalized[0]["signature"]["constantArguments"][0])
        self.assertTrue(normalized[1]["signature"]["constantArguments"][0])
        before = registration_digest(source)
        source["functions"][0]["vectorImplementations"][0:2] = [second, first]
        self.assertNotEqual(before, registration_digest(source))

    def test_vector_metadata_missing_non_boolean_or_name_level_is_rejected(self):
        fields = ("deterministic", "defaultNullBehavior", "supportsFlattening", "companionFunction")
        for field in fields:
            for value in (None, 0, "true"):
                source = catalog()
                if value is None:
                    del source["functions"][0]["vectorImplementations"][0][field]
                else:
                    source["functions"][0]["vectorImplementations"][0][field] = value
                with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                    normalize_catalog(source)
        source = catalog()
        source["functions"][0]["deterministic"] = True
        with self.assertRaises(ValueError):
            normalize_catalog(source)


if __name__ == "__main__":
    unittest.main()
