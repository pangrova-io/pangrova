"""Descriptor-checked canonical Type skeletons and typed substitution paths."""

from copy import deepcopy

from registered_signature import exact_bool, exact_keys
from type_formula import MAX_DEPTH, load_formula


MAX_FIELDS = 4096


class CanonicalTypeDescriptor:
    def __init__(self, source):
        exact_keys(source, ("root", "messages"))
        if source["root"] != "substrait.Type" or not isinstance(source["messages"], list):
            raise ValueError("Type template must use the canonical Type descriptor")
        self.root = source["root"]
        self.messages = {}
        field_count = 0
        for message in source["messages"]:
            exact_keys(message, ("name", "fields"))
            name = message["name"]
            if not isinstance(name, str) or name in self.messages:
                raise ValueError("Duplicate or malformed descriptor message")
            fields = {}
            for field in message["fields"]:
                exact_keys(field, ("name", "number", "type", "repeated"),
                           ("message", "oneof", "enumValues"))
                number = field["number"]
                if type(number) is not int or number <= 0 or number in fields:
                    raise ValueError("Duplicate or malformed descriptor field number")
                exact_bool(field["repeated"])
                fields[number] = deepcopy(field)
                field_count += 1
            self.messages[name] = fields
        if field_count > MAX_FIELDS or self.root not in self.messages:
            raise ValueError("Type descriptor is missing its root or exceeds its bound")
        for fields in self.messages.values():
            for field in fields.values():
                if "message" in field and field["message"] not in self.messages:
                    raise ValueError("Type descriptor has an unresolved message reference")

    def validate_skeleton(self, skeleton, message=None, depth=0):
        if depth > MAX_DEPTH or not isinstance(skeleton, dict):
            raise ValueError("Type skeleton exceeds its bound or is not a message")
        message = message or self.root
        seen_oneofs = set()
        for number, value in skeleton.items():
            if not isinstance(number, str) or not number.isdigit():
                raise ValueError("Type skeleton uses names instead of canonical field numbers")
            field = self.messages[message].get(int(number))
            if field is None:
                raise ValueError("Type skeleton field is absent from the canonical descriptor")
            if "oneof" in field:
                if field["oneof"] in seen_oneofs:
                    raise ValueError("Type skeleton sets multiple members of the same oneof")
                seen_oneofs.add(field["oneof"])
            self.validate_value(field, value, depth)
        if message == self.root and "kind" not in seen_oneofs:
            raise ValueError("Canonical Type skeleton has no kind")

    def validate_value(self, field, value, depth):
        if field["repeated"]:
            if not isinstance(value, list) or len(value) > MAX_FIELDS:
                raise ValueError("Repeated skeleton field must be a bounded array")
            for element in value:
                self.validate_scalar(field, element, depth)
        else:
            self.validate_scalar(field, value, depth)

    def validate_scalar(self, field, value, depth):
        if "message" in field:
            self.validate_skeleton(value, field["message"], depth + 1)
        elif field["type"] in ("int32", "uint32", "int64", "uint64", "enum"):
            if type(value) is not int:
                raise ValueError("Numeric skeleton field requires an exact integer")
            if field["type"] == "enum" and value not in field["enumValues"]:
                raise ValueError("Unknown canonical enum value")
            if field["type"].startswith("uint") and value < 0:
                raise ValueError("Unsigned skeleton field is negative")
            bits = 32 if "32" in field["type"] or field["type"] == "enum" else 64
            lower = 0 if field["type"].startswith("uint") else -(1 << (bits - 1))
            upper = (1 << bits) - 1 if lower == 0 else (1 << (bits - 1)) - 1
            if not lower <= value <= upper:
                raise ValueError("Numeric skeleton field exceeds its descriptor range")
        elif field["type"] in ("string", "bytes"):
            if not isinstance(value, str):
                raise ValueError("String/encoded bytes skeleton field requires a string")
        elif field["type"] == "bool":
            exact_bool(value)
        else:
            raise ValueError("Unsupported canonical skeleton scalar field")

    def resolve_slot(self, skeleton, path):
        if not isinstance(path, list) or not path or len(path) > MAX_DEPTH:
            raise ValueError("Type substitution path must be bounded and nonempty")
        message, node = self.root, skeleton
        for position, step in enumerate(path):
            exact_keys(step, ("field",), ("index",))
            number = step["field"]
            if type(number) is not int or number not in self.messages[message]:
                raise ValueError("Substitution field is absent from the canonical descriptor")
            field = self.messages[message][number]
            value = node.get(str(number))
            if field["repeated"]:
                index = step.get("index")
                if type(index) is not int or not isinstance(value, list) or not 0 <= index < len(value):
                    raise ValueError("Repeated type path requires an in-range explicit index")
                value = value[index]
            elif "index" in step:
                raise ValueError("Singular type path cannot have a repeated index")
            if position == len(path) - 1:
                if field.get("message") == self.root:
                    return "TYPE"
                if field["type"] == "int32" and "type_variation" not in field["name"]:
                    return "INTEGER"
                raise ValueError("Substitution must end at Type or an integer type parameter")
            if "message" not in field or not isinstance(value, dict):
                raise ValueError("Substitution path traverses an absent or scalar skeleton field")
            message, node = field["message"], value
        raise ValueError("Unreachable empty type path")


def validate_template(template, descriptor, variables, depth=0):
    if depth > MAX_DEPTH:
        raise ValueError("Nested type templates exceed their depth bound")
    exact_keys(template, (), ("variable", "skeleton", "slots", "fieldNameConstraints", "homogeneousRow"))
    if "variable" in template:
        if (set(template) != {"variable"} or not isinstance(template["variable"], str)
                or variables.get(template["variable"]) != "TYPE"):
            raise ValueError("Whole-type variable cannot also carry a skeleton")
        return
    if not {"skeleton", "slots"} <= template.keys() or not isinstance(template["slots"], list):
        raise ValueError("Type template needs its skeleton and substitution slots")
    descriptor.validate_skeleton(template["skeleton"])
    paths = []
    for slot in template["slots"]:
        exact_keys(slot, ("path", "kind", "value"))
        if descriptor.resolve_slot(template["skeleton"], slot["path"]) != slot["kind"]:
            raise ValueError("Type substitution kind does not match its descriptor field")
        if slot["kind"] == "TYPE":
            validate_template(slot["value"], descriptor, variables, depth + 1)
        else:
            load_formula(slot["value"], {name for name, kind in variables.items() if kind == "INTEGER"})
        path = tuple((step["field"], step.get("index")) for step in slot["path"])
        if any(path[:len(previous)] == previous or previous[:len(path)] == path for previous in paths):
            raise ValueError("Type substitution paths overlap or duplicate")
        paths.append(path)
    validate_row_metadata(template, descriptor)


def validate_row_metadata(template, descriptor):
    if "homogeneousRow" in template:
        exact_bool(template["homogeneousRow"])
        fields = descriptor.messages[descriptor.root]
        struct_field = next((number for number, field in fields.items() if field["name"] == "struct"), None)
        if struct_field is None or str(struct_field) not in template["skeleton"]:
            raise ValueError("Homogeneous row metadata cannot be assigned to another canonical kind")
        struct_descriptor = fields[struct_field]["message"]
        types_field = next((number for number, field in descriptor.messages[struct_descriptor].items()
                            if field["name"] == "types"), None)
        children = template["skeleton"][str(struct_field)].get(str(types_field))
        if template["homogeneousRow"] and (not isinstance(children, list) or len(children) != 1):
            raise ValueError("A homogeneous row template must declare its one repeated type pattern")
    names = template.get("fieldNameConstraints", [])
    if not isinstance(names, list) or len(names) > MAX_FIELDS:
        raise ValueError("Named row constraints exceed their bound")
    paths = set()
    for constraint in names:
        exact_keys(constraint, ("path", "name"))
        if not isinstance(constraint["name"], str):
            raise ValueError("Explicit row-field name constraints require a string, including empty")
        path = constraint["path"]
        if descriptor.resolve_slot(template["skeleton"], path) != "TYPE":
            raise ValueError("Row-field name constraint must target a canonical Type child")
        message = descriptor.root
        for position, step in enumerate(path):
            field = descriptor.messages[message][step["field"]]
            if position == len(path) - 1:
                if (field["name"] != "types" or message != descriptor.root + ".Struct"
                        or not field["repeated"]):
                    raise ValueError("Name constraint is not a ROW field ordinal")
            else:
                message = field["message"]
        identity = tuple((step["field"], step.get("index")) for step in path)
        if identity in paths:
            raise ValueError("Duplicate ROW field-name constraint")
        paths.add(identity)
