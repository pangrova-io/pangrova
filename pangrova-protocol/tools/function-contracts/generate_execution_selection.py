"""Render both execution-selection evaluators from one control declaration."""

import argparse
from pathlib import Path
import sys

if sys.version_info < (3, 10):
    raise RuntimeError("Execution-selection generation requires Python 3.10 or newer")

from execution_selection import read_selection, selection_digest
from generate_special_type_programs import render_special_cpp, render_special_java
from contract_generation_inputs import generation_input_digest


FAMILY_NUMBERS = {"SPECIAL_FORM": 1, "VECTOR": 2, "SCALAR": 3}
JAVA_PACKAGE = "org.pangrova.protocol.function.generated"


def selection_steps(language):
    cpp = language == "cpp"
    lines = ["  for (const auto& rule : executionSelectionRules) {" if cpp
             else "        for (Rule rule : RULES) {"]
    lines.append("    std::optional<Result> selected;" if cpp else "            Optional<R> selected = Optional.empty();")
    lines.append("    uint64_t priority = 0;" if cpp else "            long priority = 0;")
    lines.append("    for (const auto& entry : entries) {" if cpp else "            for (E entry : entries) {")
    lines.append("      if (entry.family() != rule.family) continue;" if cpp
                 else "                if (entry.family() != rule.family()) continue;")
    lines.append("      auto result = bind(entry);" if cpp else "                Optional<R> result = bind.apply(entry);")
    lines.append("      if (!result.has_value()) continue;" if cpp
                 else "                if (result.isEmpty()) continue;")
    lines.append("      if (!rule.minimumPriority) return result;" if cpp
                 else "                if (!rule.minimumPriority()) return result;")
    condition = "!selected.has_value()" if cpp else "selected.isEmpty()"
    lines.append(f"      if ({condition} || entry.priority() < priority) {{" if cpp
                 else f"                if ({condition} || entry.priority() < priority) {{")
    lines.append("        selected = std::move(result);" if cpp else "                    selected = result;")
    lines.append("        priority = entry.priority();" if cpp else "                    priority = entry.priority();")
    lines.append("      }" if cpp else "                }")
    lines.append("    }" if cpp else "            }")
    lines.append("    if (selected.has_value()) return selected;" if cpp
                 else "            if (selected.isPresent()) return selected;")
    lines.append("  }" if cpp else "        }")
    return lines


def render_java(value):
    digest = selection_digest(value)
    lines = ["// Generated from execution-selection.json. Do not edit.",
             f"package {JAVA_PACKAGE};", "", "import java.util.List;", "import java.util.Optional;",
             "import java.util.function.Function;", "", "/** Selects one actual immutable entry without values or factories. */",
             "public final class GeneratedExecutionSelection {",
             f'    public static final String CONTRACT_SHA256 = "{digest}";',
             "    private record Rule(int family, boolean minimumPriority) {}",
             "    private static final List<Rule> RULES = List.of(" + ", ".join(
                 f'new Rule({FAMILY_NUMBERS[step["family"]]}, '
                 + str(step["selection"] == "MIN_PRIORITY_FIRST_BOUND").lower() + ")"
                 for step in value["families"]) + ");",
             "    private GeneratedExecutionSelection() {}", "",
             "    /** Projection of one actual registry entry, in its original traversal order. */",
             "    public interface Entry {", "        int family();", "        long priority();", "    }", "",
             "    /** Returns the selected bound result, never searching by a desired output type. */",
             "    public static <E extends Entry, R> Optional<R> select(",
             "            List<E> entries, Function<E, Optional<R>> bind) {",
             "        for (E entry : entries) {",
             "            if (entry.family() < 1 || entry.family() > 3 || entry.priority() < 0",
             "                    || entry.priority() > 0xffffffffL) {",
             '                throw new IllegalArgumentException("Malformed actual execution entry");',
             "            }", "        }"]
    lines.extend(selection_steps("java"))
    lines.extend(["        return Optional.empty();", "    }", "}"])
    return "\n".join(lines) + "\n"


def render_cpp(value, generation_digest):
    digest = selection_digest(value)
    lines = ["// Generated from execution-selection.json. Do not edit.", "#pragma once", "",
             "#include <array>", "#include <cstdint>", "#include <optional>", "#include <span>", "#include <stdexcept>",
             "#include <utility>", "#include <string_view>", "#include <vector>", "", "namespace pangrova::contract::generated {",
             f'inline constexpr const char* executionSelectionSha256 = "{digest}";',
             f'inline constexpr const char* executionSelectionInputSha256 = "{generation_digest}";', "",
             "struct ExecutionSelectionRule { uint32_t family; bool minimumPriority; };",
             "inline constexpr std::array<ExecutionSelectionRule, 3> executionSelectionRules = {{"
             + ", ".join("{" + str(FAMILY_NUMBERS[step["family"]]) + ", "
                         + str(step["selection"] == "MIN_PRIORITY_FIRST_BOUND").lower() + "}"
                         for step in value["families"]) + "}};", "",
             "// Entry exposes family()/priority(); Binder binds types, not values or factories.",
             "template <typename Result, typename Entry, typename Binder>",
             "std::optional<Result> selectExecution(std::span<const Entry> entries, Binder bind) {",
             "  for (const auto& entry : entries) {",
             "    if (entry.family() < 1 || entry.family() > 3 || std::cmp_less(entry.priority(), 0)",
             "        || std::cmp_greater(entry.priority(), UINT32_MAX)) {",
             '      throw std::invalid_argument("Malformed actual execution entry");', "    }", "  }"]
    lines.extend(selection_steps("cpp"))
    lines.extend(["  return std::nullopt;", "}"])
    lines.extend(render_special_cpp(value))
    lines.append("}  // namespace pangrova::contract::generated")
    return "\n".join(lines) + "\n"


def write_stable(path, content):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists() or path.read_text(encoding="utf-8") != content:
        path.write_text(content, encoding="utf-8")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--declaration", type=Path, required=True)
    parser.add_argument("--java-root", type=Path)
    parser.add_argument("--cpp-output", type=Path)
    parser.add_argument("--input-digest-only", action="store_true")
    arguments = parser.parse_args()
    if arguments.java_root is None and arguments.cpp_output is None and not arguments.input_digest_only:
        parser.error("At least one generated output must be requested")
    value = read_selection(arguments.declaration.read_text(encoding="utf-8"))
    input_digest = generation_input_digest(arguments.declaration)
    if arguments.input_digest_only:
        if arguments.java_root is not None or arguments.cpp_output is not None:
            parser.error("Input identity mode must not also write generated outputs")
        print(input_digest)
        return
    if arguments.java_root is not None:
        path = arguments.java_root / Path(*JAVA_PACKAGE.split(".")) / "GeneratedExecutionSelection.java"
        write_stable(path, render_java(value))
        special = arguments.java_root / Path(*JAVA_PACKAGE.split(".")) / "GeneratedSpecialTypePrograms.java"
        write_stable(special, render_special_java(value))
    if arguments.cpp_output is not None:
        write_stable(arguments.cpp_output, render_cpp(value, input_digest))


if __name__ == "__main__":
    main()
