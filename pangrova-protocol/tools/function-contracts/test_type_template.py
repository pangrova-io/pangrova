from copy import deepcopy
import unittest

from type_formula import FormulaParser
from type_template import CanonicalTypeDescriptor, validate_template


def field(name, number, kind, **properties):
    return {"name": name, "number": number, "type": kind, "repeated": False, **properties}


# A bounded schema fixture exercises descriptor mechanics only. The production
# graph comes from Type::descriptor() and is independently verified by Native.
DESCRIPTOR_FIXTURE = {"root": "substrait.Type", "messages": [
    {"name": "substrait.Type", "fields": [
        field("decimal", 24, "message", message="substrait.Type.Decimal", oneof="kind"),
        field("list", 27, "message", message="substrait.Type.List", oneof="kind"),
        field("struct", 25, "message", message="substrait.Type.Struct", oneof="kind")]},
    {"name": "substrait.Type.Decimal", "fields": [
        field("scale", 1, "int32"), field("precision", 2, "int32"),
        field("type_variation_reference", 3, "uint32"),
        field("nullability", 4, "enum", enumValues=[0, 1, 2])]},
    {"name": "substrait.Type.List", "fields": [
        field("type", 1, "message", message="substrait.Type")]},
    {"name": "substrait.Type.Struct", "fields": [
        field("types", 1, "message", message="substrait.Type", repeated=True)]}]}


def decimal_template():
    return {"skeleton": {"24": {"1": 0, "2": 0, "4": 1}}, "slots": [
        {"path": [{"field": 24}, {"field": 2}], "kind": "INTEGER",
         "value": FormulaParser("p", {"p"}).parse().canonical()}]}


class TypeTemplateTest(unittest.TestCase):
    def setUp(self):
        self.descriptor = CanonicalTypeDescriptor(DESCRIPTOR_FIXTURE)
        self.variables = {"p": "INTEGER", "T": "TYPE"}

    def test_decimal_slot_is_resolved_using_descriptor_field_numbers(self):
        validate_template(decimal_template(), self.descriptor, self.variables)

    def test_whole_type_and_nested_list_variable_are_preserved(self):
        template = {"skeleton": {"27": {"1": {"24": {"1": 0, "2": 1}}}}, "slots": [
            {"path": [{"field": 27}, {"field": 1}], "kind": "TYPE", "value": {"variable": "T"}}]}
        validate_template(template, self.descriptor, self.variables)

    def test_repeated_struct_path_requires_explicit_in_range_index(self):
        template = {"skeleton": {"25": {"1": [{"24": {"2": 1}}]}}, "slots": [
            {"path": [{"field": 25}, {"field": 1, "index": 0}],
             "kind": "TYPE", "value": {"variable": "T"}}]}
        validate_template(template, self.descriptor, self.variables)
        for index in (None, -1, 1, True):
            bad = deepcopy(template)
            if index is None:
                del bad["slots"][0]["path"][1]["index"]
            else:
                bad["slots"][0]["path"][1]["index"] = index
            with self.subTest(index=index), self.assertRaises(ValueError):
                validate_template(bad, self.descriptor, self.variables)

    def test_nullability_variation_unknown_field_or_wrong_kind_are_not_parameter_slots(self):
        for terminal in (3, 4, 99):
            bad = decimal_template()
            bad["slots"][0]["path"][1]["field"] = terminal
            with self.subTest(terminal=terminal), self.assertRaises(ValueError):
                validate_template(bad, self.descriptor, self.variables)
        bad = decimal_template()
        bad["slots"][0]["kind"] = "TYPE"
        with self.assertRaises(ValueError):
            validate_template(bad, self.descriptor, self.variables)

    def test_duplicate_overlap_malformed_formula_and_unknown_variable_reject(self):
        mutations = (lambda t: t["slots"].append(deepcopy(t["slots"][0])),
                     lambda t: t["slots"][0].update(value={"operation": "COERCE"}),
                     lambda t: t["slots"][0].update(value={"operation": "VARIABLE", "value": "x"}))
        for mutation in mutations:
            bad = decimal_template()
            mutation(bad)
            with self.assertRaises(ValueError):
                validate_template(bad, self.descriptor, self.variables)
        for template in ({"variable": "x"}, {"variable": "p"}, {"variable": "T", "slots": []}):
            with self.assertRaises(ValueError):
                validate_template(template, self.descriptor, self.variables)

    def test_noncanonical_skeleton_and_multiple_kind_reject(self):
        for skeleton in ({}, {"decimal": {}}, {"24": {}, "27": {}}, {"24": {"2": 1.5}},
                         {"24": {"4": 99}}, {"24": {"3": -1}}):
            with self.subTest(skeleton=skeleton), self.assertRaises(ValueError):
                validate_template({"skeleton": skeleton, "slots": []}, self.descriptor, self.variables)

    def test_named_empty_row_constraint_and_homogeneous_shape_remain_explicit(self):
        template = {"skeleton": {"25": {"1": [{"24": {"2": 1}}]}}, "slots": [],
                    "homogeneousRow": True,
                    "fieldNameConstraints": [{"path": [{"field": 25}, {"field": 1, "index": 0}],
                                              "name": ""}]}
        validate_template(template, self.descriptor, self.variables)
        for mutation in (lambda t: t["fieldNameConstraints"].append(deepcopy(t["fieldNameConstraints"][0])),
                         lambda t: t["fieldNameConstraints"][0].update(name=None),
                         lambda t: t["skeleton"]["25"]["1"].append({"24": {"2": 1}}),
                         lambda t: t.update(homogeneousRow=1)):
            bad = deepcopy(template)
            mutation(bad)
            with self.assertRaises(ValueError):
                validate_template(bad, self.descriptor, self.variables)
        bad = decimal_template()
        bad["homogeneousRow"] = True
        with self.assertRaises(ValueError):
            validate_template(bad, self.descriptor, self.variables)

    def test_named_constraint_cannot_attach_to_list_child_or_decimal_parameter(self):
        for skeleton, path in (({"27": {"1": {"24": {"2": 1}}}}, [{"field": 27}, {"field": 1}]),
                               ({"24": {"2": 1}}, [{"field": 24}, {"field": 2}])):
            template = {"skeleton": skeleton, "slots": [], "fieldNameConstraints": [{"path": path, "name": "x"}]}
            with self.assertRaises(ValueError):
                validate_template(template, self.descriptor, self.variables)


if __name__ == "__main__":
    unittest.main()
