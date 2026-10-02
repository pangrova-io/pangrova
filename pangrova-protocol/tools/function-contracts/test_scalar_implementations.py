from copy import deepcopy
import unittest

from registration_catalog import normalize_catalog, registration_digest
from test_registration_catalog import catalog
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
        entry.update(simpleImplementations=[implementation(2), implementation(2, "HUGEINT")], vectorOverloads=[])
        normalized = normalize_catalog(source)["functions"][0]
        self.assertEqual(entry["simpleImplementations"][0]["priority"], normalized["simpleImplementations"][0]["priority"])
        self.assertEqual("HUGEINT", normalized["simpleImplementations"][1]["outputPhysical"]["kind"])
        changed = deepcopy(source)
        changed["functions"][0]["simpleImplementations"].reverse()
        self.assertNotEqual(registration_digest(source), registration_digest(changed))

    def test_missing_or_invented_implementation_signature_cannot_publish_as_inventory(self):
        for mutation in (lambda e: e.pop("simpleImplementations"),
                         lambda e: e.update(vectorOverloads=[]),
                         lambda e: e["vectorOverloads"][0]["output"].update(base="bigint")):
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
            source["functions"][0].update(simpleImplementations=[entry], vectorOverloads=[])
            with self.assertRaises(ValueError):
                normalize_catalog(source)


if __name__ == "__main__":
    unittest.main()
