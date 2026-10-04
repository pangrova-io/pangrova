"""One SQL/structural declaration; actual registration remains a prerequisite."""

import hashlib
import re

from registration_catalog import canonical_json, read_export
from registered_signature import exact_bool, exact_keys
from physical_type_bridge import validate_bridge


KINDS = {"SCALAR", "AGGREGATE", "WINDOW"}
MAX_DECLARATIONS = 4096


def strings(value):
    if (not isinstance(value, list) or len(value) > MAX_DECLARATIONS
            or any(not isinstance(item, str) or not item or not item.isascii() for item in value)
            or len(set(value)) != len(value)):
        raise ValueError("Expected unique bounded ASCII names")
    return value


def validate_binding(value):
    exact_keys(value, ("namespace", "canonicalUrn", "name", "kind"),
               ("implementation", "sqlNames", "operators", "rewrite", "structuralLowering",
                "registrationDependency", "governedLayout", "normalizedArguments", "runtimeSignature", "options"))
    strings([value["namespace"], value["name"]])
    if (not isinstance(value["canonicalUrn"], str)
            or not re.fullmatch(r"extension:[a-z0-9.]+:[a-z][a-z0-9_]*", value["canonicalUrn"])):
        raise ValueError("A binding requires its exact canonical extension URN")
    if value["kind"] not in KINDS:
        raise ValueError("Unknown binding kind")
    choices = {"implementation", "rewrite", "structuralLowering"} & value.keys()
    if len(choices) != 1:
        raise ValueError("A binding must select exactly one implementation path")
    for field in ("sqlNames", "operators"):
        if field in value:
            strings(value[field])
    if "registrationDependency" in value:
        if (value["registrationDependency"] not in ("WORKER_AI_CALLBACK", "WORKER_FILE_CALLBACK")
                or "implementation" not in value):
            raise ValueError("Unknown or non-runtime registration dependency")
    if "governedLayout" in value and not isinstance(value["governedLayout"], str):
        raise ValueError("Governed argument layout requires an exact identity")
    if "runtimeSignature" in value:
        if "registrationDependency" not in value:
            raise ValueError("Runtime signature cannot invent a pure Native registration")
        runtime = value["runtimeSignature"]
        exact_keys(runtime, ("returnType", "deterministic", "description"), ("argumentSignatures", "argumentLayout"))
        if len({"argumentSignatures", "argumentLayout"} & runtime.keys()) != 1:
            raise ValueError("Callback must select exactly one declared argument source")
        strings([runtime["returnType"]])
        exact_bool(runtime["deterministic"])
        if not isinstance(runtime["description"], str):
            raise ValueError("Runtime signature description must be a string")
        signatures = runtime.get("argumentSignatures", [])
        if "argumentLayout" in runtime:
            strings([runtime["argumentLayout"]])
        if (not isinstance(signatures, list) or len(signatures) > 256
                or ("argumentSignatures" in runtime and not signatures)):
            raise ValueError("Runtime signature overloads must be explicitly bounded")
        for signature in signatures:
            if not isinstance(signature, list) or not signature or len(signature) > 256:
                raise ValueError("Runtime argument list is empty or exceeds its bound")
            for type_name in signature:
                strings([type_name])
    if "rewrite" in value:
        validate_rewrite(value["rewrite"])
    for argument in value.get("normalizedArguments", []):
        validate_normalized_argument(argument)
        if "runtimeSignature" in value:
            for signature in value["runtimeSignature"]["argumentSignatures"]:
                ordinal = argument["ordinal"]
                if ordinal >= len(signature) or signature[ordinal] != argument["type"]:
                    raise ValueError("Normalized argument differs from the actual callback declaration")
    if "options" in value:
        options = value["options"]
        if (value["kind"] != "WINDOW" or "implementation" not in value
                or not isinstance(options, list) or len(options) > 64):
            raise ValueError("Executable window options require an implementation and bounded declarations")
        names, behaviors = set(), set()
        for option in options:
            exact_keys(option, ("name", "behavior", "enabledValue", "disabledValue"))
            strings([option["name"], option["behavior"], option["enabledValue"], option["disabledValue"]])
            if option["behavior"] != "NULL_TREATMENT" or option["name"] in names or option["behavior"] in behaviors:
                raise ValueError("Unknown or duplicate executable option behavior")
            names.add(option["name"])
            behaviors.add(option["behavior"])
    return value


def validate_rewrite(value):
    exact_keys(value, ("nodes", "result"))
    nodes = value["nodes"]
    if not isinstance(nodes, list) or not nodes or len(nodes) > 64:
        raise ValueError("Rewrite requires a bounded nonempty DAG")
    for ordinal, node in enumerate(nodes):
        exact_keys(node, ("implementation",), ("inputs", "nodes"))
        strings([node["implementation"]])
        if len({"inputs", "nodes"} & node.keys()) != 1:
            raise ValueError("Rewrite node needs one argument source")
        for source in node.get("nodes", []):
            if type(source) is not int or not 0 <= source < ordinal:
                raise ValueError("Rewrite node refers forward or forms a cycle")
        for source in node.get("inputs", []):
            if type(source) is not int or not 0 <= source < 65536:
                raise ValueError("Rewrite input ordinal is malformed")
    if type(value["result"]) is not int or not 0 <= value["result"] < len(nodes):
        raise ValueError("Rewrite output node is absent")


def validate_normalized_argument(value):
    exact_keys(value, ("ordinal", "name", "type", "constant", "optional", "default", "enumValues"))
    if type(value["ordinal"]) is not int or not 0 <= value["ordinal"] < 65536:
        raise ValueError("Normalized argument ordinal is malformed")
    strings([value["name"]])
    strings(value["enumValues"])
    if value["type"] != "VARCHAR" or value["default"] not in value["enumValues"]:
        raise ValueError("Normalized enum argument has an invalid type/default")
    if not exact_bool(value["constant"]) or not exact_bool(value["optional"]):
        raise ValueError("Defaulted enum arguments must be optional constants")


def read_declarations(source):
    value = read_export(source)
    exact_keys(value, ("schemaVersion", "bindings", "structuralContracts", "physicalTypeBridge", "governedLayouts"))
    if type(value["schemaVersion"]) is not int or value["schemaVersion"] != 1:
        raise ValueError("Unknown binding declaration schema")
    validate_bridge(value["physicalTypeBridge"])
    layout_ids = validate_governed_layouts(value["governedLayouts"])
    identities, names = set(), set()
    if not isinstance(value["bindings"], list) or len(value["bindings"]) > MAX_DECLARATIONS:
        raise ValueError("Binding declarations exceed their bound")
    for binding in value["bindings"]:
        validate_binding(binding)
        if "governedLayout" in binding and binding["governedLayout"] not in layout_ids:
            raise ValueError("Governed binding refers to an absent exact layout")
        runtime = binding.get("runtimeSignature", {})
        if "argumentLayout" in runtime and (runtime["argumentLayout"] not in layout_ids
                or runtime["argumentLayout"] != binding.get("governedLayout")):
            raise ValueError("Callback argument source differs from its governed layout")
        identity = (binding["kind"], binding["namespace"], binding["name"])
        if identity in identities:
            raise ValueError("Duplicate canonical binding identity")
        identities.add(identity)
        for field in ("sqlNames", "operators"):
            for name in binding.get(field, []):
                key = (binding["kind"], field, name)
                if key in names:
                    raise ValueError("Ambiguous SQL operator binding")
                names.add(key)
    contracts = value["structuralContracts"]
    if not isinstance(contracts, list) or len(contracts) > MAX_DECLARATIONS:
        raise ValueError("Structural declaration exceeds its bound")
    ids = [entry["id"] for entry in contracts]
    strings(ids)
    for contract in contracts:
        validate_structural_contract(contract)
    return value


def validate_governed_layouts(layouts):
    if not isinstance(layouts, list) or len(layouts) > 64:
        raise ValueError("Governed layouts exceed their bound")
    ids = set()
    for layout in layouts:
        exact_keys(layout, ("id", "arguments"))
        if not isinstance(layout["id"], str) or not re.fullmatch(r"[A-Z][A-Z0-9_]*", layout["id"]):
            raise ValueError("Governed layout ID is malformed")
        if layout["id"] in ids:
            raise ValueError("Duplicate governed layout ID")
        ids.add(layout["id"])
        arguments = layout["arguments"]
        if not isinstance(arguments, list) or not arguments or len(arguments) > 256:
            raise ValueError("Governed layout requires a bounded exact argument list")
        names = []
        for argument in arguments:
            exact_keys(argument, ("name", "type", "source", "encoding"), ("path", "allowAbsentMessage"))
            names.append(argument["name"])
            if (not re.fullmatch(r"[a-z][a-z0-9_]*", argument["name"]) or argument["type"] != "VARCHAR"
                    or argument["source"] not in ("OPERATOR_BINDING", "PLANNER_INVOCATION_ID", "OUTPUT_VECTOR_DIMENSION", "VALUE_EXPRESSION")
                    or argument["encoding"] not in ("STRING", "BASE64", "UNSIGNED_DECIMAL", "BOOLEAN", "EXACT_FLOAT")):
                raise ValueError("Unknown governed argument type/source/encoding")
            if argument["source"] == "OPERATOR_BINDING":
                path = argument.get("path")
                if (not isinstance(path, list) or not path or len(path) > 16
                        or any(not isinstance(component, str)
                               or not re.fullmatch(r"[a-z][a-z0-9_]*", component) for component in path)):
                    raise ValueError("Governed binding path is malformed")
            elif "path" in argument or "allowAbsentMessage" in argument:
                raise ValueError("Non-binding argument cannot carry a protobuf path")
            if "allowAbsentMessage" in argument:
                exact_bool(argument["allowAbsentMessage"])
        strings(names)
    return ids


def validate_structural_contract(value):
    exact_keys(value, ("id", "version", "kind", "placement", "implementation", "argumentLayout",
                       "options", "output", "deterministic", "retry"))
    if (not re.fullmatch(r"[a-z][a-z0-9_.]*", value["id"])
            or not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", value["version"])
            or value["kind"] != "TABLE" or value["placement"] != "NATIVE"
            or value["retry"] != "IDEMPOTENT"):
        raise ValueError("Malformed structural contract identity or execution semantics")
    exact_bool(value["deterministic"])
    strings([value["implementation"]])
    layout = value["argumentLayout"]
    exact_keys(layout, ("namePattern", "minArguments", "variadic", "types", "replicate"))
    if (type(layout["minArguments"]) is not int or not 0 < layout["minArguments"] <= 256
            or layout["namePattern"] != "input_i" or layout["replicate"] != "ALL_INPUT_COLUMNS"):
        raise ValueError("Unknown structural argument layout")
    exact_bool(layout["variadic"])
    strings(layout["types"])
    if not set(layout["types"]) <= {"ARRAY", "MAP"}:
        raise ValueError("Unknown structural argument type")
    options = value["options"]
    if not isinstance(options, list) or len(options) > 64:
        raise ValueError("Structural options exceed their bound")
    option_names = []
    for option in options:
        exact_keys(option, ("name", "type", "required"))
        option_names.append(option["name"])
        if option["type"] != "BOOLEAN" or not exact_bool(option["required"]):
            raise ValueError("Structural options require explicit boolean values")
    strings(option_names)
    output = value["output"]
    exact_keys(output, ("authority", "arrayColumns", "mapColumns", "zip", "ordinalityType",
                        "ordinalityBase", "outer"))
    if (output["authority"] != "TABLE_FUNCTION_DETAIL_OUTPUT_SCHEMA"
            or output["arrayColumns"] != 1 or output["mapColumns"] != 2
            or output["zip"] != "LONGEST_NULL_PAD" or output["ordinalityType"] != "BIGINT"
            or output["ordinalityBase"] != 1 or output["outer"] != "KEEP_NULL_OR_EMPTY_INPUT"):
        raise ValueError("Unknown structural output semantics require a reviewed lowering")


def structural_contract_digest(contract):
    """Build-independent compatibility identity, not proof or authentication."""
    return hashlib.sha256(canonical_json(contract).encode("ascii")).hexdigest()


def unresolved_bindings(declarations, actual_inventory):
    actual = {(entry["kind"], entry["name"]) for entry in actual_inventory["functions"]}
    actual.update(("SCALAR", name) for name in actual_inventory["specialForms"])
    missing = []
    for binding in declarations["bindings"]:
        if "registrationDependency" in binding or "structuralLowering" in binding:
            continue
        implementations = ([binding["implementation"]] if "implementation" in binding
                           else [node["implementation"] for node in binding["rewrite"]["nodes"]])
        for name in implementations:
            if (binding["kind"], name) not in actual:
                missing.append((binding["kind"], binding["namespace"], binding["name"], name))
    return missing
