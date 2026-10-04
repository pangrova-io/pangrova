"""Typed SQL contract projection; registration availability is never generated."""

import hashlib

from registration_catalog import canonical_json


def digest(value):
    return hashlib.sha256(canonical_json(value).encode("ascii")).digest()


def bytes_literal(value):
    return '"' + "".join(f"\\x{byte:02x}" for byte in value) + '"'


def string_type(target):
    return [f"    {target}->mutable_skeleton()->mutable_string()->set_nullability("
            "substrait::Type::NULLABILITY_REQUIRED);"]


def normalized_argument(source, target):
    lines = [f"    auto* {target} = definition->add_normalized_arguments();",
             f"    {target}->set_ordinal({source['ordinal']});",
             f"    {target}->set_name({canonical_json(source['name'])});",
             f"    {target}->set_constant({str(source['constant']).lower()});",
             f"    {target}->set_optional({str(source['optional']).lower()});"]
    lines.extend(string_type(target + "->mutable_type()"))
    lines.extend([f"    {target}->mutable_default_value()->mutable_type()->set_canonical_sql(\"VARCHAR\");",
                  f"    *{target}->mutable_default_value()->mutable_type()->mutable_substrait_type() = "
                  f"{target}->type().skeleton();",
                  f"    {target}->mutable_default_value()->set_string_value({canonical_json(source['default'])});"])
    lines.extend(f"    {target}->add_enum_values({canonical_json(value)});" for value in source["enumValues"])
    return lines


def governed_argument(source, ordinal):
    if source["type"] != "VARCHAR":
        raise ValueError("Governed typed projection requires its declared VARCHAR representation")
    name = f"governed_{ordinal}"
    lines = [f"    auto* {name} = definition->add_governed_arguments();",
             f"    {name}->set_ordinal({ordinal});",
             f"    {name}->set_name({canonical_json(source['name'])});",
             f"    {name}->set_constant({str(source['source'] != 'VALUE_EXPRESSION').lower()});"]
    lines.extend(string_type(name + "->mutable_type()"))
    return lines


def rewrite_lines(rewrite, direct):
    lines = []
    for ordinal, node in enumerate(rewrite["nodes"]):
        target = f"rewrite_{ordinal}"
        candidates = direct.get(node["implementation"], [])
        if len(candidates) != 1:
            raise ValueError("Rewrite dependency requires one exact direct scalar declaration")
        reference = candidates[0]
        lines.extend([f"    auto* {target} = definition->mutable_rewrite()->add_nodes();",
                      f"    {target}->mutable_target()->set_urn({canonical_json(reference['canonicalUrn'])});",
                      f"    {target}->mutable_target()->set_name({canonical_json(reference['name'])});",
                      f"    {target}->mutable_target()->set_kind(pangrova::protocol::v2::FUNCTION_KIND_SCALAR);"])
        for index in node.get("inputs", []):
            lines.append(f"    {target}->add_arguments()->set_input_ordinal({index});")
        for index in node.get("nodes", []):
            lines.append(f"    {target}->add_arguments()->set_node_ordinal({index});")
    lines.append(f"    definition->mutable_rewrite()->set_result_node_ordinal({rewrite['result']});")
    return lines


def definition_lines(source, layouts, direct):
    lines = ["  {", "    auto* definition = manifest->add_definitions();",
             "    auto* reference = definition->mutable_ref();",
             f"    reference->set_urn({canonical_json(source['canonicalUrn'])});",
             f"    reference->set_name({canonical_json(source['name'])});",
             f"    reference->set_kind(pangrova::protocol::v2::FUNCTION_KIND_{source['kind']});",
             f"    definition->set_semantics_sha256({bytes_literal(digest(source))}, 32);",
             "    definition->set_nullability(pangrova::protocol::v2::FUNCTION_NULLABILITY_CANONICAL_DECLARED_OUTPUT);"]
    for field, setter in (("implementation", "implementation_name"),
                          ("structuralLowering", "structural_lowering"),
                          ("registrationDependency", "registration_dependency")):
        if field in source:
            lines.append(f"    definition->set_{setter}({canonical_json(source[field])});")
    for field, setter in (("sqlNames", "sql_names"), ("operators", "standard_operators")):
        lines.extend(f"    definition->add_{setter}({canonical_json(value)});" for value in source.get(field, []))
    if "rewrite" in source:
        lines.extend(rewrite_lines(source["rewrite"], direct))
    for ordinal, argument in enumerate(source.get("normalizedArguments", [])):
        lines.extend(normalized_argument(argument, f"normalized_{ordinal}"))
    if "governedLayout" in source:
        for ordinal, argument in enumerate(layouts[source["governedLayout"]]):
            lines.extend(governed_argument(argument, ordinal))
    for ordinal, option in enumerate(source.get("options", [])):
        target = f"option_{ordinal}"
        lines.extend([f"    auto* {target} = definition->add_options();",
                      f"    {target}->set_name({canonical_json(option['name'])});",
                      f"    {target}->set_behavior({canonical_json(option['behavior'])});",
                      f"    {target}->add_values({canonical_json(option['disabledValue'])});",
                      f"    {target}->add_values({canonical_json(option['enabledValue'])});",
                      f"    {target}->set_default_value({canonical_json(option['disabledValue'])});"])
    lines.append("  }")
    return lines


def render_binding_manifest(declarations):
    layouts = {layout["id"]: layout["arguments"] for layout in declarations["governedLayouts"]}
    direct = {}
    for binding in declarations["bindings"]:
        if binding["kind"] == "SCALAR" and "implementation" in binding:
            direct.setdefault(binding["implementation"], []).append(binding)
    lines = ["inline void appendDeclaredFunctionDefinitions(",
             "    pangrova::protocol::v2::FunctionCatalogManifest* manifest) {",
             "  if (!manifest || manifest->definitions_size())",
             '    throw std::invalid_argument("SQL definitions require a new canonical manifest");',
             f"  manifest->set_binding_declaration_sha256({bytes_literal(digest(declarations))}, 32);"]
    for binding in declarations["bindings"]:
        lines.extend(definition_lines(binding, layouts, direct))
    lines.append("}")
    return lines
