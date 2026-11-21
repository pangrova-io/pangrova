"""Actual-resolver differential harness for registered integer parameter formulas.

This restricted probe is not a production signature binder or a publication gate
for ambiguous/complex signatures. It never invents a function formula or priority.
"""

import argparse
import itertools
import json
from pathlib import Path
import subprocess

from oracle_responses import OracleResponses
from registration_catalog import canonical_json, normalize_catalog, read_export
from type_formula import evaluate_parameter, load_formula


def decimal_domain():
    return tuple((precision, scale) for precision in range(1, 39) for scale in range(precision + 1))


def argument(base, *parameters):
    return {"base": base, "parameters": [argument(str(value)) for value in parameters], "variableArity": False}


def bind_parameter(formal, actual, variables, bindings):
    name = formal["base"]
    if formal["parameters"] or actual["parameters"] or not actual["base"].isdigit():
        raise ValueError("Decimal formula probe only supports exact integer type parameters")
    value = int(actual["base"])
    if name.isdigit():
        return int(name) == value
    variable = variables.get(name)
    if variable is None or variable["kind"] != "INTEGER":
        raise ValueError("Decimal formula probe found a non-integer parameter")
    if "inputIntegerConstant" in variable and variable["inputIntegerConstant"] != value:
        return False
    if name in bindings and bindings[name] != value:
        return False
    bindings[name] = value
    return True


def resolve_parameter(source, variables, bindings):
    name = source["base"]
    if source["parameters"] or source["variableArity"]:
        raise ValueError("Decimal formula probe cannot certify nested parameter expressions")
    if name.isdigit():
        return int(name)
    if name in bindings:
        return bindings[name]
    variable = variables.get(name)
    if variable is None or variable["kind"] != "INTEGER" or "formula" not in variable:
        raise ValueError("Registered output parameter is not bound or computed")
    # Evaluate at the actual output parameter traversal point. Do not sort
    # derived assignments or evaluate unrequested constraints speculatively.
    bindings[name] = evaluate_parameter(load_formula(variable["formula"], set(variables)), bindings)
    return bindings[name]


def predict_signature(signature, actual_arguments):
    if signature["variableArity"]:
        raise ValueError("Decimal formula probe does not certify variadic resolution")
    if len(signature["arguments"]) != len(actual_arguments):
        return None
    variables = {variable["name"]: variable for variable in signature["variables"]}
    bindings = {}
    for formal, actual in zip(signature["arguments"], actual_arguments):
        if formal["base"].lower() != actual["base"].lower():
            if formal["base"] in variables or formal["base"] == "any":
                raise ValueError("Decimal formula probe cannot certify generic resolver candidates")
            return None
        if len(formal["parameters"]) != len(actual["parameters"]):
            return None
        if not all(bind_parameter(left, right, variables, bindings)
                   for left, right in zip(formal["parameters"], actual["parameters"])):
            return None
    output = signature["output"]
    if output["base"].lower() != "decimal" or len(output["parameters"]) != 2:
        raise ValueError("Decimal formula probe only certifies actual DECIMAL output signatures")
    return argument("decimal", *(resolve_parameter(value, variables, bindings) for value in output["parameters"]))


def predict_unique_output(entry, actual_arguments):
    outputs = [predict_signature(signature, actual_arguments) for signature in entry["overloads"]]
    outputs = {canonical_json(value): value for value in outputs if value is not None}
    if len(outputs) != 1:
        raise ValueError("Registered formula probe cannot certify absent or ambiguous output; actual selection metadata is required")
    return next(iter(outputs.values()))


def request_cases(names, binary):
    domain = decimal_domain()
    for name in names:
        for parameters in (itertools.product(domain, repeat=2) if binary else ((value,) for value in domain)):
            arguments = [argument("decimal", *value) for value in parameters]
            encoded = [f"decimal({precision},{scale})" for precision, scale in parameters]
            yield {"name": name, "arguments": encoded}, arguments


def run_probe(executable, catalog, names, binary):
    entries = {entry["name"]: entry for entry in catalog["functions"] if entry["kind"] == "SCALAR"}
    if any(name not in entries for name in names):
        raise ValueError("Requested function lacks an actual registered scalar entry")
    count = 0
    with subprocess.Popen([str(executable), "--resolve-scalars"], stdin=subprocess.PIPE,
                          stdout=subprocess.PIPE, bufsize=0) as process, OracleResponses(process.stdout) as responses:
        try:
            for request, arguments in request_cases(names, binary):
                expected = predict_unique_output(entries[request["name"]], arguments)
                payload = (canonical_json(request) + "\n").encode("utf-8")
                if process.stdin.write(payload) != len(payload):
                    raise ValueError("Actual Native oracle did not accept the complete request")
                process.stdin.flush()
                line = responses.next_line()
                actual = read_export(line)
                if actual.get("status") != "BOUND" or actual.get("output") != expected:
                    raise ValueError(canonical_json({"request": request, "expected": expected, "actual": actual}))
                count += 1
            process.stdin.close()
            if process.wait(timeout=30) != 0:
                raise ValueError("Actual Native oracle failed after the differential matrix")
        except BaseException:
            process.kill()
            process.wait(timeout=30)
            raise
    return {"status": "PASS", "cases": count, "functions": names, "scope": "REGISTERED_DECIMAL_TYPE_FORMULAS_NOT_VALUES"}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--executable", type=Path, required=True)
    parser.add_argument("--registrations", type=Path, required=True)
    parser.add_argument("--functions", nargs="+", required=True)
    parser.add_argument("--binary", action="store_true")
    args = parser.parse_args()
    catalog = normalize_catalog(read_export(args.registrations.read_text(encoding="utf-8")))
    print(canonical_json(run_probe(args.executable.resolve(), catalog, args.functions, args.binary)))


if __name__ == "__main__":
    main()
