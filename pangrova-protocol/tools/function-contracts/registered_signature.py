"""Normalize reflected registration ASTs; no function names or decimal formulas here."""

from copy import deepcopy
import re

from type_formula import FormulaParser


NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")
MAX_DEPTH = 64
MAX_TYPES = 4096


def exact_keys(source, required, optional=()):
    if not isinstance(source, dict):
        raise ValueError("Registered contract must be an object")
    if not set(required) <= source.keys() or source.keys() - set(required) - set(optional):
        raise ValueError("Registered contract has missing or unknown fields")


def exact_bool(source):
    if type(source) is not bool:
        raise ValueError("Registered contract flag must be boolean")
    return source


def formula_dependencies(node):
    result = {node.value} if node.operation == "VARIABLE" else set()
    for operand in node.operands:
        result.update(formula_dependencies(operand))
    return result


def check_acyclic(dependencies):
    visiting, finished = set(), set()

    def visit(name):
        if name in visiting:
            raise ValueError("Cyclic registered integer constraints")
        if name in finished:
            return
        visiting.add(name)
        for dependency in dependencies.get(name, ()):
            visit(dependency)
        visiting.remove(name)
        finished.add(name)

    for name in dependencies:
        visit(name)


def normalize_variables(entries):
    if not isinstance(entries, list) or len(entries) > MAX_TYPES:
        raise ValueError("Registered variables exceed their bound")
    by_name = {}
    for entry in entries:
        exact_keys(entry, ("name", "kind", "constraint"),
                   ("knownTypesOnly", "orderableTypesOnly", "comparableTypesOnly"))
        name, kind, constraint = entry["name"], entry["kind"], entry["constraint"]
        if not isinstance(name, str) or not NAME.fullmatch(name) or name in by_name:
            raise ValueError("Invalid or duplicate registered variable")
        if kind not in ("TYPE", "INTEGER", "ENUM") or not isinstance(constraint, str):
            raise ValueError("Invalid registered variable kind or constraint")
        if kind == "INTEGER" and constraint and not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", name):
            raise ValueError("Registered integer formula assignment is not a valid calculator variable")
        properties = ("knownTypesOnly", "orderableTypesOnly", "comparableTypesOnly")
        if kind == "TYPE":
            if constraint or not set(properties) <= entry.keys():
                raise ValueError("Unsupported or incomplete registered type constraint")
            for property_name in properties:
                exact_bool(entry[property_name])
        elif set(properties) & entry.keys():
            raise ValueError("Integer/enum variable cannot carry type constraints")
        by_name[name] = deepcopy(entry)
    integers = {name for name, entry in by_name.items() if entry["kind"] == "INTEGER"}
    dependencies = {}
    for name, entry in by_name.items():
        if entry["kind"] == "INTEGER" and entry["constraint"]:
            formula = FormulaParser(entry["constraint"], integers).parse()
            entry["formula"] = formula.canonical()
            if entry["constraint"].isdigit():
                value = int(entry["constraint"])
                if value > (1 << 31) - 1:
                    raise ValueError("Input integer constraint exceeds exact signed 32-bit")
                entry["inputIntegerConstant"] = value
            dependencies[name] = formula_dependencies(formula)
    check_acyclic(dependencies)
    return [by_name[name] for name in sorted(by_name)]


class TypeAstNormalizer:
    def __init__(self, variables):
        self.variables = {entry["name"]: entry for entry in variables}
        self.count = 0

    def normalize(self, source, parameter=False, depth=0):
        self.count += 1
        if depth > MAX_DEPTH or self.count > MAX_TYPES:
            raise ValueError("Registered type AST exceeds its bound")
        exact_keys(source, ("base", "parameters", "variableArity"), ("rowFieldName",))
        base, parameters = source["base"], source["parameters"]
        if not isinstance(base, str) or not base or not isinstance(parameters, list):
            raise ValueError("Registered type AST is malformed")
        variable = self.variables.get(base)
        if variable and (parameters or source["variableArity"]):
            raise ValueError("Registered variable cannot contain nested types")
        if variable and variable["kind"] != "TYPE" and not parameter:
            raise ValueError("Integer/enum variable cannot stand for a whole type")
        if base.isdigit() and (not parameter or parameters):
            raise ValueError("Integer literal is legal only as a type parameter")
        if "rowFieldName" in source and not isinstance(source["rowFieldName"], str):
            raise ValueError("Registered row-field name must be a string")
        result = {"base": base, "variableArity": exact_bool(source["variableArity"]),
                  "parameters": [self.normalize(child, True, depth + 1) for child in parameters]}
        if "rowFieldName" in source:
            result["rowFieldName"] = source["rowFieldName"]
        return result


def normalize_signature(source, kind):
    optional = ("intermediate",) if kind in ("AGGREGATE", "WINDOW") else ()
    exact_keys(source, ("arguments", "output", "variables", "constantArguments", "variableArity"),
               optional)
    if kind == "AGGREGATE" and "intermediate" not in source:
        raise ValueError("Aggregate registration is missing its intermediate type")
    arguments, constants = source["arguments"], source["constantArguments"]
    if not isinstance(arguments, list) or not isinstance(constants, list):
        raise ValueError("Registered arguments/constants must be arrays")
    if len(arguments) != len(constants) or len(arguments) > MAX_TYPES:
        raise ValueError("Registered constant-argument mask does not match argument arity")
    variable_arity = exact_bool(source["variableArity"])
    if variable_arity and not arguments:
        raise ValueError("Variadic registration must contain its repeated argument")
    variables = normalize_variables(source["variables"])
    types = TypeAstNormalizer(variables)
    result = {"arguments": [types.normalize(argument) for argument in arguments],
              "output": types.normalize(source["output"]), "variables": variables,
              "constantArguments": [exact_bool(constant) for constant in constants],
              "variableArity": variable_arity}
    if "intermediate" in source:
        result["intermediate"] = types.normalize(source["intermediate"])
    return result
