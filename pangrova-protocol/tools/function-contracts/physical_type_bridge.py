"""One canonical bridge declaration drives factories and binding projections."""

import re

from registered_signature import exact_bool, exact_keys
from registration_catalog import canonical_json
from type_formula import load_formula
from physical_type_manifest import render_manifest


IDENTIFIER = re.compile(r"[a-z][a-z0-9_]*\Z")
TRAITS = {"ALWAYS", "NEVER", "ALL_CHILDREN"}


def identifier(value):
    if not isinstance(value, str) or not IDENTIFIER.fullmatch(value):
        raise ValueError("Canonical bridge names must be exact safe identifiers")


def validate_bridge(value):
    exact_keys(value, ("anonymousComputedFieldPrefix", "projections"))
    identifier(value["anonymousComputedFieldPrefix"])
    projections = value["projections"]
    if not isinstance(projections, list) or not projections or len(projections) > 256:
        raise ValueError("Canonical physical projections must be nonempty and bounded")
    kinds = set()
    for projection in projections:
        validate_projection(projection)
        if projection["kind"] in kinds:
            raise ValueError("Canonical physical projections repeat a type kind")
        kinds.add(projection["kind"])
    return value


def validate_projection(value):
    exact_keys(value, ("kind", "parameters", "children", "variants", "known", "orderable", "comparable"),
               ("predicates",))
    identifier(value["kind"])
    names, fields = set(), set()
    for parameter in bounded(value["parameters"]):
        exact_keys(parameter, ("name", "field", "minimum", "maximum", "nativeArgument"))
        identifier(parameter["name"])
        identifier(parameter["field"])
        if parameter["name"] in names or parameter["field"] in fields:
            raise ValueError("Canonical bridge parameters repeat a name or source field")
        names.add(parameter["name"])
        fields.add(parameter["field"])
        low, high = parameter["minimum"], parameter["maximum"]
        if type(low) is not int or type(high) is not int or not -(1 << 31) <= low <= high < (1 << 31):
            raise ValueError("Canonical bridge parameter bounds must be exact signed int32")
        exact_bool(parameter["nativeArgument"])
    for child in bounded(value["children"]):
        exact_keys(child, ("field", "repeated", "computedNames"))
        identifier(child["field"])
        if child["field"] in fields:
            raise ValueError("Canonical bridge child repeats an existing field")
        fields.add(child["field"])
        exact_bool(child["repeated"])
        exact_bool(child["computedNames"])
        if child["computedNames"] and not child["repeated"]:
            raise ValueError("Computed names require a repeated canonical child field")
    formulas(value.get("predicates", []), names)
    variants = bounded(value["variants"])
    if not variants:
        raise ValueError("Canonical projection has no actual runtime factory variant")
    for variant in variants:
        validate_variant(variant, names)
    if any(value[trait] not in TRAITS for trait in ("known", "orderable", "comparable")):
        raise ValueError("Canonical bridge has an unknown type trait rule")


def bounded(value):
    if not isinstance(value, list) or len(value) > 256:
        raise ValueError("Canonical bridge list must be bounded")
    return value


def formulas(value, variables):
    for formula in bounded(value):
        load_formula(formula, variables)


def validate_variant(value, variables):
    exact_keys(value, ("nativeName", "bindingKind", "physicalTag", "bindingParameters", "predicates"))
    identifier(value["bindingKind"])
    for name in ("nativeName", "physicalTag"):
        if not isinstance(value[name], str) or not re.fullmatch(r"[A-Z][A-Z0-9_ ]*", value[name]):
            raise ValueError("Canonical bridge requires an exact actual runtime name/tag")
    if not isinstance(value["bindingParameters"], dict) or len(value["bindingParameters"]) > 256:
        raise ValueError("Binding parameter substitutions must be bounded")
    for field, formula in value["bindingParameters"].items():
        identifier(field)
        load_formula(formula, variables)
    formulas(value["predicates"], variables)


def expression(source):
    node = load_formula(source, _variables(source))
    if node.operation == "VARIABLE":
        return node.value
    if node.operation == "INTEGER":
        if node.value == -(1 << 63):
            return "std::numeric_limits<int64_t>::min()"
        return f"int64_t{{{node.value}}}"
    args = [expression(child.canonical()) for child in node.operands]
    operators = {"EQUAL": "==", "NOT_EQUAL": "!=", "LESS": "<", "LESS_OR_EQUAL": "<=",
                 "GREATER": ">", "GREATER_OR_EQUAL": ">="}
    if node.operation in operators:
        return f"({args[0]} {operators[node.operation]} {args[1]})"
    return "typeFormula" + node.operation.title().replace("_", "") + "(" + ", ".join(args) + ")"


def _variables(source):
    result = {source["value"]} if source.get("operation") == "VARIABLE" else set()
    for operand in source.get("operands", []):
        result.update(_variables(operand))
    return result


def render_factory(projection, prefix):
    kind = projection["kind"]
    case = "k" + "".join(word.title() for word in kind.split("_"))
    lines = [f"    case substrait::Type::{case}: {{"]
    getter = "struct_" if kind == "struct" else kind
    if projection["parameters"] or projection["children"]:
        lines.append(f"      const auto& source = type.{getter}();")
    for parameter in projection["parameters"]:
        name = parameter["name"]
        diagnostic = (f'Canonical bridge {kind}.{parameter["field"]} must be in '
                      f'[{parameter["minimum"]}, {parameter["maximum"]}]; actual=')
        lines.extend([f'      const int64_t {name} = source.{parameter["field"]}();',
                      f'      if ({name} < {parameter["minimum"]} || {name} > {parameter["maximum"]})',
                      f'        throw std::runtime_error(std::string({canonical_json(diagnostic)}) '
                      f'+ std::to_string({name}));'])
    for formula in projection.get("predicates", []):
        condition = expression(formula)
        diagnostic = 'std::string(' + canonical_json(
            f'Canonical bridge {kind} parameter constraint failed: {condition}') + ')'
        for parameter in projection["parameters"]:
            diagnostic += (f' + {canonical_json("; " + parameter["field"] + "=")} '
                           f'+ std::to_string({parameter["name"]})')
        lines.extend([f"      if (!({condition}))",
                      f'        throw std::runtime_error({diagnostic});'])
    lines.append("      std::vector<facebook::velox::TypeParameter> parameters;")
    for parameter in projection["parameters"]:
        if parameter["nativeArgument"]:
            lines.append(f'      parameters.emplace_back({parameter["name"]});')
    for child in projection["children"]:
        if child["repeated"]:
            lines.append(f'      for (const auto& child : source.{child["field"]}()) {{')
            if child["computedNames"]:
                lines.append(f'        parameters.emplace_back(recurse(child), {canonical_json(prefix)} '
                             '+ std::to_string(parameters.size()));')
            else:
                lines.append("        parameters.emplace_back(recurse(child));")
            lines.append("      }")
        else:
            lines.extend([f'      if (!source.has_{child["field"]}())',
                          '        throw std::runtime_error("Canonical bridge child type is absent");',
                          f'      parameters.emplace_back(recurse(source.{child["field"]}()));'])
    for variant in projection["variants"]:
        conditions = " && ".join(f"({expression(formula)})" for formula in variant["predicates"]) or "true"
        lines.extend([f"      if ({conditions}) {{",
                      f'        auto result = facebook::velox::getType({canonical_json(variant["nativeName"])}, parameters);',
                      '        if (!result) throw std::runtime_error("Actual runtime type factory is absent");',
                      "        return result;", "      }"])
    lines.extend(['      throw std::runtime_error("Canonical bridge has no matching factory variant");', "    }"])
    return lines


def render_bridge(value):
    validate_bridge(value)
    lines = ["// Generated from the sole canonical physical type bridge. Do not edit.", "#pragma once",
             '#include "capability/FunctionTypeFormulaOps.h"', '#include "substrait/type.pb.h"',
             '#include "pangrova/protocol/v2/capability.pb.h"', '#include "google/protobuf/descriptor.h"',
             '#include "velox/type/Type.h"', "#include <string>", "#include <vector>",
             "namespace pangrova::capability::generated {",
             "inline constexpr const char* physicalTypeBridgeDeclaration = " + canonical_json(canonical_json(value)) + ";",
             "template <typename Recurse>",
             "facebook::velox::TypePtr projectCanonicalPhysicalType(const substrait::Type& type, Recurse recurse) {",
             "  switch (type.kind_case()) {"]
    for projection in value["projections"]:
        lines.extend(render_factory(projection, value["anonymousComputedFieldPrefix"]))
    lines.extend(["    default:", '      throw std::runtime_error("Unsupported canonical physical type kind");',
                  "  }", "}"])
    lines.extend(render_manifest(value))
    lines.append("}  // namespace pangrova::capability::generated")
    return "\n".join(lines) + "\n"
