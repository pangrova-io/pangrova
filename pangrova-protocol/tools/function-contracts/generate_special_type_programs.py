"""Emit the same structural constraints for Native and canonical Java types."""

from registration_catalog import canonical_json
from special_type_programs import render_index


def selection_loop(selection, language):
    start = render_index(selection["start"], language)
    end = render_index(selection["end"], language)
    return f"for (int ordinal = {start}; ordinal < {end}; ordinal += {selection['step']}) {{"


def constraints(program, language):
    cpp = language == "cpp"
    lines = []
    for constraint in program["constraints"]:
        lines.append("      " + selection_loop(constraint["selection"], language))
        actual = "arguments[ordinal]" if cpp else "arguments.get(ordinal)"
        if "nativeKinds" in constraint:
            checks = [f"types.kind({actual}) != {canonical_json(kind)}" if cpp
                      else f"!types.kind({actual}).equals({canonical_json(kind)})"
                      for kind in constraint["nativeKinds"]]
            predicate = " && ".join(checks)
        elif "equalToArgument" in constraint:
            other = f"arguments[{constraint['equalToArgument']}]" if cpp \
                else f"arguments.get({constraint['equalToArgument']})"
            predicate = f"!types.equal({actual}, {other})"
        else:
            def kind_checks(expression, kinds):
                return "(" + " || ".join(f"types.kind({expression}) == {canonical_json(kind)}" if cpp
                                           else f"types.kind({expression}).equals({canonical_json(kind)})"
                                           for kind in kinds) + ")"
            predicate = kind_checks(actual, constraint["forbiddenInputKinds"]) + " && " \
                + kind_checks("declaredTarget", constraint["targetKinds"])
        lines.append(f"        if ({predicate}) " + failure("Special form input contract mismatch", cpp))
        lines.append("      }")
    return lines


def failure(message, cpp):
    exception = "std::invalid_argument" if cpp else "new IllegalArgumentException"
    return "throw " + exception + "(" + canonical_json(message) + ");"


def result_lines(result, language):
    cpp = language == "cpp"
    if "argument" in result:
        expression = f"arguments[{result['argument']}]" if cpp else f"arguments.get({result['argument']})"
        return [f"      return {expression};"]
    if "declaredTarget" in result:
        check = "!declaredTarget" if cpp else "declaredTarget == null"
        return [f"      if ({check}) " + failure("Special form requires its explicit declared target", cpp),
                "      return declaredTarget;"]
    if result["nativeKind"] == "BOOLEAN":
        return ["      return types.booleanType();"]
    lines = ["      std::vector<T> children;" if cpp else "      List<T> children = new ArrayList<>();",
             "      " + selection_loop(result["childSelection"], language),
             "        children.push_back(arguments[ordinal]);" if cpp else "        children.add(arguments.get(ordinal));",
             "      }", "      return types.row(children);"]
    return lines


def body(programs, language):
    cpp = language == "cpp"
    lines = ["  const int arity = static_cast<int>(arguments.size());" if cpp else "    int arity = arguments.size();",
             "  if (arguments.size() > 256) " + failure("Special form call arity is unbounded", cpp)]
    for program in programs:
        condition = "program == " + canonical_json(program["id"]) if cpp \
            else "program.equals(" + canonical_json(program["id"]) + ")"
        lines.append(f"    if ({condition}) {{")
        lines.append(f"      if (arity < {program['minimumArity']} || arity > {program.get('maximumArity', 256)}) "
                     + failure("Special form call arity does not match its owned program", cpp))
        if "declaredTarget" in program["result"]:
            check = "!declaredTarget" if cpp else "declaredTarget == null"
            lines.append(f"      if ({check}) " + failure("Special form requires its explicit declared target", cpp))
        lines.extend(constraints(program, language))
        lines.extend(result_lines(program["result"], language))
        lines.append("    }")
    lines.append("    " + failure("Special form has no executable owned type program", cpp))
    return lines


def render_special_cpp(value):
    programs = value["specialTypePrograms"]["programs"]
    lines = ["// Types implement actual runtime operator==, kind and native factories.",
             "// No callback or function factory may be invoked for type evaluation.",
             "template <typename T, typename Types>",
             "T evaluateSpecialType(std::string_view program, const std::vector<T>& arguments,",
             "    const T& declaredTarget, const Types& types) {"]
    lines.extend(body(programs, "cpp"))
    lines.append("}")
    lines.extend(["inline bool hasSpecialTypeProgram(std::string_view program) {",
                  "  return " + " || ".join("program == " + canonical_json(program["id"]) for program in programs) + ";",
                  "}"])
    return lines


def render_special_java(value):
    lines = ["// Generated from execution-selection.json. Do not edit.",
             "package org.pangrova.protocol.function.generated;", "",
             "import java.util.ArrayList;", "import java.util.List;", "import java.util.Objects;", "",
             "/** Evaluates the actual owned structural contract without values or factories. */",
             "public final class GeneratedSpecialTypePrograms {",
             "  private GeneratedSpecialTypePrograms() {}",
             "  /** Host operations implement the sole physical type projection and exact equality. */",
             "  public interface Types<T> {", "    String kind(T type);", "    boolean equal(T left, T right);",
             "    T booleanType();", "    T row(List<T> children);", "  }", "",
             "  /** Missing program and invalid inputs are errors, never signature non-matches. */",
             "  public static <T> T evaluate(String program, List<T> arguments, T declaredTarget, Types<T> types) {",
             '    Objects.requireNonNull(program, "program");', '    Objects.requireNonNull(arguments, "arguments");',
             '    Objects.requireNonNull(types, "types");']
    lines.extend(body(value["specialTypePrograms"]["programs"], "java"))
    lines.extend(["  }", "}"])
    return "\n".join(lines) + "\n"
