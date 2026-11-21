"""Typed manifest projection generated from the actual factory bridge program."""

from registration_catalog import canonical_json
from type_formula import load_formula


def cpp_text(value):
    return canonical_json(value)


def formula_lines(source, target, variables, indent="    "):
    formula = load_formula(source, variables)
    result = [indent + target + "->set_operation(pangrova::protocol::v2::FUNCTION_FORMULA_OPERATION_"
              + formula.operation + ");"]
    if formula.operation == "INTEGER":
        value = ("std::numeric_limits<int64_t>::min()" if formula.value == -(1 << 63)
                 else f"int64_t{{{formula.value}}}")
        result.append(indent + target + "->set_integer_value(" + value + ");")
    elif formula.operation == "VARIABLE":
        result.append(indent + target + "->set_variable_name(" + cpp_text(formula.value) + ");")
    for index, operand in enumerate(formula.operands):
        name = target.replace("->", "_") + "_operand_" + str(index)
        result.append(indent + "auto* " + name + " = " + target + "->add_operands();")
        result.extend(formula_lines(operand.canonical(), name, variables, indent))
    return result


def variable(name):
    return {"operation": "VARIABLE", "value": name}


def integer(value):
    return {"operation": "INTEGER", "value": value}


def predicate(operation, left, right):
    return {"operation": operation, "operands": [left, right]}


def manifest_variant(projection, variant):
    variables = {parameter["name"] for parameter in projection["parameters"]}
    lines = ["  {", "    auto* projection = manifest->add_physical_type_projections();",
             "    auto* logical = projection->mutable_logical_type();",
             "    auto* logical_kind = bridgeSkeleton(logical, " + cpp_text(projection["kind"]) + ");",
             "    auto* binding = projection->mutable_binding_type();",
             "    auto* binding_kind = bridgeSkeleton(binding, " + cpp_text(variant["bindingKind"]) + ");",
             "    (void)logical_kind;", "    (void)binding_kind;",
             "    projection->set_native_type_name(" + cpp_text(variant["nativeName"]) + ");",
             "    projection->mutable_physical_type()->set_runtime_tag(" + cpp_text(variant["physicalTag"]) + ");"]
    for trait, target in (("known", "known_type"), ("orderable", "orderable"), ("comparable", "comparable")):
        lines.append("    projection->set_" + target + "(pangrova::protocol::v2::FUNCTION_TYPE_TRAIT_RULE_"
                     + projection[trait] + ");")
    for index, parameter in enumerate(projection["parameters"]):
        name = parameter["name"]
        lines.extend(["    auto* variable_" + str(index) + " = projection->add_variables();",
                      "    variable_" + str(index) + "->set_name(" + cpp_text(name) + ");",
                      "    variable_" + str(index) + "->set_kind(pangrova::protocol::v2::FUNCTION_VARIABLE_KIND_INTEGER);"])
        lines.extend(slot_lines("logical", "logical_kind", parameter["field"], variable(name),
                                variables, "logical_slot_" + str(index)))
    for index, (field, formula) in enumerate(variant["bindingParameters"].items()):
        lines.extend(slot_lines("binding", "binding_kind", field, formula, variables,
                                "binding_slot_" + str(index)))
    for index, parameter in enumerate(projection["parameters"]):
        if not parameter["nativeArgument"]:
            continue
        fields = [field for field, formula in variant["bindingParameters"].items()
                  if formula == variable(parameter["name"])]
        if len(fields) != 1:
            raise ValueError("Native integer parameter requires one exact canonical binding field")
        name = "native_parameter_" + str(index)
        lines.extend(["    auto* " + name + " = projection->add_native_integer_parameter_paths();",
                      "    " + name + "->add_path()->set_field_number(bridgeRootField(binding->skeleton()).number());",
                      "    " + name + "->add_path()->set_field_number(bridgeField(*binding_kind, "
                      + cpp_text(fields[0]) + ").number());"])
    predicates = list(projection.get("predicates", [])) + list(variant["predicates"])
    for parameter in projection["parameters"]:
        predicates.extend([predicate("GREATER_OR_EQUAL", variable(parameter["name"]), integer(parameter["minimum"])),
                           predicate("LESS_OR_EQUAL", variable(parameter["name"]), integer(parameter["maximum"]))])
    for index, expression in enumerate(predicates):
        name = "predicate_" + str(index)
        lines.append("    auto* " + name + " = projection->add_predicates();")
        lines.extend(formula_lines(expression, name, variables))
    for index, child in enumerate(projection["children"]):
        name = "child_" + str(index)
        lines.extend(["    auto* " + name + " = projection->add_recursive_children();",
                      "    " + name + "->add_field_numbers(bridgeRootField(logical->skeleton()).number());",
                      "    " + name + "->add_field_numbers(bridgeField(*logical_kind, " + cpp_text(child["field"]) + ").number());"])
    lines.append("  }")
    return lines


def slot_lines(template, kind, field, formula, variables, name):
    lines = ["    auto* " + name + " = " + template + "->add_slots();",
             "    " + name + "->add_path()->set_field_number(bridgeRootField(" + template + "->skeleton()).number());",
             "    " + name + "->add_path()->set_field_number(bridgeField(*" + kind + ", " + cpp_text(field) + ").number());"]
    pointer = name + "_formula"
    lines.append("    auto* " + pointer + " = " + name + "->mutable_integer();")
    lines.extend(formula_lines(formula, pointer, variables))
    return lines


def render_manifest(value):
    lines = ["inline const google::protobuf::FieldDescriptor& bridgeField(",
             "    const google::protobuf::Message& message, std::string_view name) {",
             "  const auto* field = message.GetDescriptor()->FindFieldByName(name);",
             '  if (!field) throw std::runtime_error("Bridge field is absent from actual canonical descriptor");',
             "  return *field;", "}",
             "inline const google::protobuf::FieldDescriptor& bridgeRootField(const substrait::Type& type) {",
             '  const auto* oneof = type.GetDescriptor()->FindOneofByName("kind");',
             "  const auto* field = oneof ? type.GetReflection()->GetOneofFieldDescriptor(type, oneof) : nullptr;",
             '  if (!field) throw std::runtime_error("Bridge canonical Type has no kind");',
             "  return *field;", "}",
             "inline google::protobuf::Message* bridgeSkeleton(",
             "    pangrova::protocol::v2::FunctionTypeTemplate* target, const std::string& kind) {",
             "  auto* type = target->mutable_skeleton();",
             "  const auto& field = bridgeField(*type, kind);",
             "  if (field.cpp_type() != google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE || !field.containing_oneof())",
             '    throw std::runtime_error("Bridge kind is not an actual canonical Type oneof");',
             "  auto* message = type->GetReflection()->MutableMessage(type, &field);",
             '  const auto& nullable = bridgeField(*message, "nullability");',
             "  message->GetReflection()->SetEnumValue(message, &nullable, substrait::Type::NULLABILITY_REQUIRED);",
             "  return message;", "}",
             "inline void appendCanonicalPhysicalProjections(pangrova::protocol::v2::FunctionCatalogManifest* manifest) {",
             '  if (!manifest) throw std::invalid_argument("Canonical manifest target is null");']
    for projection in value["projections"]:
        for variant in projection["variants"]:
            lines.extend(manifest_variant(projection, variant))
    lines.append("}")
    return lines
