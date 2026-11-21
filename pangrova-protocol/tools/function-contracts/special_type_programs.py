"""Bounded structural type programs, not a SQL-name or runtime registration list."""

import re

from registered_signature import exact_keys
from type_formula import FormulaParser, evaluate


def index_formula(source):
    if not isinstance(source, str):
        raise ValueError("Structural index must use the sole integer formula grammar")
    node = FormulaParser(source, {"argument_count"}).parse()
    def validate(value):
        if value.operation not in {"INTEGER", "VARIABLE", "ADD", "SUBTRACT", "MULTIPLY", "DIVIDE"}:
            raise ValueError("Structural index contains an unsupported operation")
        if value.operation == "DIVIDE" and (value.operands[1].operation != "INTEGER"
                                              or value.operands[1].value <= 0):
            raise ValueError("Structural index division requires a positive constant")
        for child in value.operands:
            validate(child)
    validate(node)
    return node


def validate_selection(selection, minimum, maximum):
    exact_keys(selection, ("start", "end", "step"))
    if type(selection["step"]) is not int or not 1 <= selection["step"] <= 256:
        raise ValueError("Structural selection stride is invalid")
    start, end = index_formula(selection["start"]), index_formula(selection["end"])
    for arity in range(minimum, maximum + 1):
        variables = {"argument_count": arity}
        left, right = evaluate(start, variables), evaluate(end, variables)
        # An optional range may start beyond a short call (e.g. optional ELSE).
        # It must be empty there; all selected ordinals must remain in the call.
        if left < 0 or right < 0 or right > arity or left > 256:
            raise ValueError("Structural selection exceeds actual argument bounds")


def validate_program(program):
    exact_keys(program, ("id", "minimumArity", "constraints", "result"), ("maximumArity",))
    if not isinstance(program["id"], str) or not re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", program["id"]):
        raise ValueError("Structural program has no exact identifier")
    minimum, maximum = program["minimumArity"], program.get("maximumArity", 256)
    if type(minimum) is not int or type(maximum) is not int or not 0 <= minimum <= maximum <= 256:
        raise ValueError("Structural program arity is unbounded")
    if not isinstance(program["constraints"], list) or len(program["constraints"]) > 256:
        raise ValueError("Structural constraints are unbounded")
    for constraint in program["constraints"]:
        exact_keys(constraint, ("selection",), ("nativeKinds", "equalToArgument", "forbiddenInputKinds", "targetKinds"))
        validate_selection(constraint["selection"], minimum, maximum)
        predicates = sum(key in constraint for key in ("nativeKinds", "equalToArgument", "forbiddenInputKinds"))
        if predicates != 1 or ("targetKinds" in constraint) != ("forbiddenInputKinds" in constraint):
            raise ValueError("Structural constraint requires exactly one type predicate")
        if "nativeKinds" in constraint or "forbiddenInputKinds" in constraint:
            kinds = constraint.get("nativeKinds", constraint.get("forbiddenInputKinds"))
            if (not isinstance(kinds, list) or not kinds or len(kinds) > 64
                    or any(not isinstance(kind, str) for kind in kinds) or len(set(kinds)) != len(kinds)):
                raise ValueError("Structural native kind predicate is malformed")
            if any(not isinstance(kind, str) or not re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", kind) for kind in kinds):
                raise ValueError("Structural native kind is malformed")
            if "forbiddenInputKinds" in constraint:
                targets = constraint["targetKinds"]
                if program["result"] != {"declaredTarget": True} or not isinstance(targets, list) or not targets \
                        or len(targets) > 64 or any(not isinstance(kind, str) for kind in targets) \
                        or len(set(targets)) != len(targets) \
                        or any(not isinstance(kind, str) or not re.fullmatch(r"[A-Z][A-Z0-9_]{0,63}", kind) for kind in targets):
                    raise ValueError("Structural target kind constraint is malformed")
        else:
            ordinal = constraint["equalToArgument"]
            if type(ordinal) is not int or not 0 <= ordinal < minimum:
                raise ValueError("Structural equality argument is not guaranteed present")
    validate_result(program["result"], minimum, maximum)


def validate_result(result, minimum, maximum):
    if not isinstance(result, dict):
        raise ValueError("Structural result is not a typed declaration")
    if set(result) == {"argument"}:
        if type(result["argument"]) is not int or not 0 <= result["argument"] < minimum:
            raise ValueError("Structural result argument is not guaranteed present")
    elif set(result) == {"declaredTarget"}:
        if result["declaredTarget"] is not True:
            raise ValueError("Structural declared target marker must be true")
    elif set(result) == {"nativeKind"}:
        if result["nativeKind"] != "BOOLEAN":
            raise ValueError("Structural scalar result requires its actual native factory")
    else:
        exact_keys(result, ("nativeKind", "childSelection", "childFieldNames"))
        if result["nativeKind"] != "ROW" or result["childFieldNames"] != "EMPTY":
            raise ValueError("Structural row result must preserve actual unnamed children")
        validate_selection(result["childSelection"], minimum, maximum)


def validate_programs(declaration):
    exact_keys(declaration, ("typeEquality", "declaredResultCompatibility", "factoryEvaluation", "programs"))
    if declaration["typeEquality"] != "NATIVE_OPERATOR_EQUAL" \
            or declaration["declaredResultCompatibility"] != "NATIVE_EQUIVALENT" \
            or declaration["factoryEvaluation"] != "FORBIDDEN":
        raise ValueError("Structural program changes its execution boundary")
    programs = declaration["programs"]
    if not isinstance(programs, list) or not 1 <= len(programs) <= 256:
        raise ValueError("Structural program declaration is absent or unbounded")
    ids = set()
    for program in programs:
        validate_program(program)
        if program["id"] in ids:
            raise ValueError("Duplicate structural program identifier")
        ids.add(program["id"])


def render_index(source, language):
    """Render the already-validated sole formula AST into both host languages."""
    def render(node):
        if node.operation == "INTEGER":
            return str(node.value)
        if node.operation == "VARIABLE":
            return "arity"
        symbol = {"ADD": "+", "SUBTRACT": "-", "MULTIPLY": "*", "DIVIDE": "/"}[node.operation]
        return "(" + render(node.operands[0]) + " " + symbol + " " + render(node.operands[1]) + ")"
    if language not in {"java", "cpp"}:
        raise ValueError("Unknown structural program generation language")
    return render(index_formula(source))
