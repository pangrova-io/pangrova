"""Generate Native compatibility identities from the canonical declaration."""

import argparse
from pathlib import Path
import re

from registration_catalog import canonical_json

from binding_declarations import read_declarations, structural_contract_digest


def render_header(declarations):
    lines = ["// Generated from the canonical SQL binding declaration. Do not edit.",
             "#pragma once", "#include <array>", "#include <cstdint>",
             "#include <cstddef>", "#include <span>", "#include <string_view>", "#include <stdexcept>",
             "namespace pangrova::capability::generated {",
             "struct StructuralContractIdentity {",
             "  const char* id;", "  const char* version;",
             "  std::array<uint8_t, 32> signature;", "};"]
    prefix = canonical_json(declarations["physicalTypeBridge"]["anonymousComputedFieldPrefix"])
    lines.append(f"inline constexpr const char* anonymousComputedFieldPrefix = {prefix};")
    lines.extend(render_governed_layouts(declarations))
    lines.extend(render_callback_signatures(declarations))
    lines.extend(render_function_options(declarations))
    identifiers = set()
    for entry in declarations["structuralContracts"]:
        name = re.sub(r"[^a-zA-Z0-9_]", "_", entry["id"])
        if name in identifiers:
            raise ValueError("Structural identities collide after C++ identifier normalization")
        identifiers.add(name)
        digest = bytes.fromhex(structural_contract_digest(entry))
        encoded = ", ".join(f"0x{value:02x}" for value in digest)
        lines.extend([f"inline constexpr StructuralContractIdentity {name} = {{",
                      f'  {canonical_json(entry["id"])}, {canonical_json(entry["version"])}, {{{encoded}}}', "};"])
    lines.append("}  // namespace pangrova::capability::generated")
    return "\n".join(lines) + "\n"


def render_governed_layouts(declarations):
    lines = ["struct GovernedArgument {", "  const char* name;", "  const char* source;",
             "  const char* encoding;", "  std::array<const char*, 16> path;", "  size_t pathSize;",
             "  bool allowAbsentMessage;", "};"]
    for layout in declarations["governedLayouts"]:
        lines.append(f'inline constexpr std::array<GovernedArgument, {len(layout["arguments"])}> {layout["id"]} = {{{{')
        for argument in layout["arguments"]:
            path = argument.get("path", [])
            encoded = ", ".join(canonical_json(component) for component in path)
            lines.append("  {" + ", ".join(canonical_json(argument[key]) for key in ("name", "source", "encoding"))
                         + ", {{" + encoded + "}}, " + str(len(path)) + ", "
                         + str(argument.get("allowAbsentMessage", False)).lower() + "},")
        lines.append("}};")
    lines.extend(["inline std::span<const GovernedArgument> governedLayout(std::string_view id) {"])
    for layout in declarations["governedLayouts"]:
        lines.append(f'  if (id == {canonical_json(layout["id"])}) return {layout["id"]};')
    lines.extend(['  throw std::invalid_argument("Unknown governed argument layout");', "}",
                  "constexpr size_t governedOrdinal(std::span<const GovernedArgument> arguments, std::string_view name) {",
                  "  for (size_t ordinal = 0; ordinal < arguments.size(); ++ordinal) {",
                  "    if (name == arguments[ordinal].name) return ordinal;", "  }",
                  '  throw std::invalid_argument("Unknown governed argument ordinal");', "}",
                  "inline std::string_view governedLayoutForImplementation(std::string_view name) {"])
    for binding in declarations["bindings"]:
        if "governedLayout" in binding:
            lines.append(f'  if (name == {canonical_json(binding["implementation"])}) '
                         f'return {canonical_json(binding["governedLayout"])};')
    lines.extend(['  return "";', "}"])
    return lines


def render_function_options(declarations):
    options = [(binding["implementation"], option) for binding in declarations["bindings"]
               for option in binding.get("options", [])]
    lines = ["struct FunctionOptionContract {", "  const char* implementation;", "  const char* name;",
             "  const char* behavior;", "  const char* enabledValue;", "  const char* disabledValue;", "};",
             f"inline constexpr std::array<FunctionOptionContract, {len(options)}> functionOptions = {{{{"]
    for implementation, option in options:
        values = [implementation] + [option[key] for key in ("name", "behavior", "enabledValue", "disabledValue")]
        lines.append("  {" + ", ".join(canonical_json(value) for value in values) + "},")
    lines.append("}};")
    return lines


def render_callback_signatures(declarations):
    layouts = {layout["id"]: [argument["type"] for argument in layout["arguments"]]
               for layout in declarations["governedLayouts"]}
    lines = ["struct RuntimeCallbackSignature {", "  const char* name;", "  const char* returnType;",
             "  std::span<const char* const> arguments;", "};"]
    callbacks = []
    for binding in declarations["bindings"]:
        runtime = binding.get("runtimeSignature")
        if not runtime:
            continue
        name = binding["implementation"]
        if not re.fullmatch(r"[a-z][a-z0-9_]*", name):
            raise ValueError("Callback implementation must have an exact safe generated identifier")
        signatures = ([layouts[runtime["argumentLayout"]]] if "argumentLayout" in runtime
                      else runtime["argumentSignatures"])
        for ordinal, public in enumerate(signatures):
            signature = public + (layouts[binding["governedLayout"]]
                                  if "governedLayout" in binding and "argumentLayout" not in runtime else [])
            identifier = f"{name}_arguments_{ordinal}"
            lines.append(f"inline constexpr std::array<const char*, {len(signature)}> {identifier} = {{{{"
                         + ", ".join(canonical_json(type_name) for type_name in signature) + "}};")
        lines.append(f"inline constexpr std::array<RuntimeCallbackSignature, {len(signatures)}> {name}_signatures = {{{{")
        for ordinal in range(len(signatures)):
            lines.append("  {" + canonical_json(name) + ", " + canonical_json(runtime["returnType"])
                         + f", {name}_arguments_{ordinal}" + "},")
        lines.append("}};")
        callbacks.append(name)
    lines.append("inline std::span<const RuntimeCallbackSignature> runtimeCallbackSignatures(std::string_view name) {")
    for name in callbacks:
        lines.append(f"  if (name == {canonical_json(name)}) return {name}_signatures;")
    lines.extend(['  throw std::invalid_argument("Unknown declared callback registration dependency");', "}"])
    return lines


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--declaration", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    declaration = read_declarations(args.declaration.read_text(encoding="utf-8"))
    output = render_header(declaration)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(output, encoding="utf-8")


if __name__ == "__main__":
    main()
