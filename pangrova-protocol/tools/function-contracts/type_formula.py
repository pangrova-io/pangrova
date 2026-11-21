"""Strict build-time parser for registered integer type constraints, never SQL."""

from dataclasses import dataclass
import re


MIN_INTEGER = -(1 << 63)
MAX_INTEGER = (1 << 63) - 1
MAX_PARAMETER = (1 << 31) - 1
MIN_PARAMETER = -(1 << 31)
MAX_DEPTH = 64
MAX_NODES = 2048
TOKEN = re.compile(r"\s*(?:(\d+)|([A-Za-z][A-Za-z0-9_]*)|(==|!=|<=|>=|[+*/%?:(),<>-]))")
PRECEDENCE = {"==": 2, "!=": 2, "<": 3, "<=": 3, ">": 3, ">=": 3,
              "+": 4, "-": 4, "*": 5, "/": 5, "%": 5}
OPERATIONS = {"+": "ADD", "-": "SUBTRACT", "*": "MULTIPLY", "/": "DIVIDE",
              "%": "REMAINDER", "==": "EQUAL", "!=": "NOT_EQUAL", "<": "LESS",
              "<=": "LESS_OR_EQUAL", ">": "GREATER", ">=": "GREATER_OR_EQUAL"}


@dataclass(frozen=True)
class FormulaNode:
    operation: str
    operands: tuple = ()
    value: object = None

    def canonical(self):
        result = {"operation": self.operation}
        if self.operands:
            result["operands"] = [node.canonical() for node in self.operands]
        if self.value is not None:
            result["value"] = self.value
        return result


def checked(value):
    if type(value) is not int:
        raise ValueError("Type formula requires an exact integer")
    if not MIN_INTEGER <= value <= MAX_INTEGER:
        raise ValueError("Type formula signed 64-bit overflow")
    return value


def tokenize(source):
    if not source or len(source) > 65536:
        raise ValueError("Type formula is empty or exceeds the text bound")
    tokens, position = [], 0
    while position < len(source):
        if source[position:].isspace():
            break
        match = TOKEN.match(source, position)
        if match is None:
            raise ValueError(f"Unknown type-formula token at position {position}")
        tokens.append(next(part for part in match.groups() if part is not None))
        position = match.end()
    if not tokens or len(tokens) > MAX_NODES * 4:
        raise ValueError("Type formula exceeds its token bound")
    return tokens


class FormulaParser:
    def __init__(self, source, variables):
        self.tokens = tokenize(source)
        self.variables = frozenset(variables)
        self.position = 0
        self.node_count = 0

    def parse(self):
        result = self.expression(0, 0)
        if self.position != len(self.tokens):
            raise ValueError("Type formula contains trailing tokens")
        return result

    def node(self, operation, operands=(), value=None):
        self.node_count += 1
        if self.node_count > MAX_NODES:
            raise ValueError("Type formula exceeds its node bound")
        return FormulaNode(operation, tuple(operands), value)

    def peek(self):
        return self.tokens[self.position] if self.position < len(self.tokens) else None

    def take(self, expected=None):
        token = self.peek()
        if token is None or (expected is not None and token != expected):
            raise ValueError(f"Expected {expected or 'operand'} in type formula")
        self.position += 1
        return token

    def expression(self, minimum, depth):
        if depth > MAX_DEPTH:
            raise ValueError("Type formula exceeds its depth bound")
        left = self.prefix(depth)
        while self.peek() in PRECEDENCE and PRECEDENCE[self.peek()] >= minimum:
            operator = self.take()
            right = self.expression(PRECEDENCE[operator] + 1, depth + 1)
            left = self.node(OPERATIONS[operator], (left, right))
        if self.peek() == "?" and minimum <= 1:
            self.take("?")
            when_true = self.expression(0, depth + 1)
            self.take(":")
            left = self.node("CONDITIONAL", (left, when_true, self.expression(1, depth + 1)))
        return left

    def prefix(self, depth):
        token = self.take()
        if token == "-":
            return self.node("NEGATE", (self.expression(6, depth + 1),))
        if token == "(":
            result = self.expression(0, depth + 1)
            self.take(")")
            return result
        if token in ("min", "max"):
            self.take("(")
            left = self.expression(0, depth + 1)
            self.take(",")
            right = self.expression(0, depth + 1)
            self.take(")")
            return self.node(token.upper(), (left, right))
        if token.isdigit():
            return self.node("INTEGER", value=checked(int(token)))
        if token not in self.variables:
            raise ValueError(f"Unknown type-formula variable: {token}")
        return self.node("VARIABLE", value=token)


def truncate_divide(left, right):
    if right == 0:
        raise ValueError("Type formula division by zero")
    quotient = abs(left) // abs(right)
    return checked(-quotient if (left < 0) != (right < 0) else quotient)


def binary(operation, left, right):
    arithmetic = {"ADD": lambda: left + right, "SUBTRACT": lambda: left - right,
                  "MULTIPLY": lambda: left * right,
                  "DIVIDE": lambda: truncate_divide(left, right),
                  "REMAINDER": lambda: left - truncate_divide(left, right) * right,
                  "MIN": lambda: min(left, right), "MAX": lambda: max(left, right)}
    comparisons = {"EQUAL": left == right, "NOT_EQUAL": left != right,
                   "LESS": left < right, "LESS_OR_EQUAL": left <= right,
                   "GREATER": left > right, "GREATER_OR_EQUAL": left >= right}
    if operation in arithmetic:
        return checked(arithmetic[operation]())
    if operation in comparisons:
        return int(comparisons[operation])
    raise ValueError(f"Unknown type-formula operation: {operation}")


def evaluate(node, variables, depth=0):
    if depth > MAX_DEPTH:
        raise ValueError("Type formula exceeds its evaluation depth bound")
    if node.operation == "INTEGER":
        return checked(node.value)
    if node.operation == "VARIABLE":
        if node.value not in variables:
            raise ValueError(f"Unbound type-formula variable: {node.value}")
        return checked(variables[node.value])
    # The actual registered calculator evaluates all operands before selecting a branch.
    operands = [evaluate(child, variables, depth + 1) for child in node.operands]
    if node.operation == "NEGATE" and len(operands) == 1:
        return checked(-operands[0])
    if node.operation == "CONDITIONAL" and len(operands) == 3:
        return operands[1] if operands[0] else operands[2]
    if len(operands) != 2:
        raise ValueError("Malformed type-formula operation arity")
    return binary(node.operation, *operands)


def evaluate_parameter(node, variables):
    result = evaluate(node, variables)
    if not MIN_PARAMETER <= result <= MAX_PARAMETER:
        raise ValueError("Type formula parameter assignment exceeds signed 32-bit")
    return result


def load_formula(source, variables, depth=0, budget=None):
    """Reject malformed serialized ASTs before any runtime evaluator consumes them."""
    budget = [MAX_NODES] if budget is None else budget
    budget[0] -= 1
    if depth > MAX_DEPTH or budget[0] < 0 or not isinstance(source, dict):
        raise ValueError("Serialized type formula exceeds its bound or is malformed")
    operation = source.get("operation")
    if operation in ("INTEGER", "VARIABLE"):
        if set(source) != {"operation", "value"}:
            raise ValueError("Serialized formula leaf has malformed fields")
        value = source["value"]
        if operation == "INTEGER":
            checked(value)
        elif not isinstance(value, str) or value not in variables:
            raise ValueError("Serialized formula references an undeclared integer variable")
        return FormulaNode(operation, value=value)
    arity = {"NEGATE": 1, "CONDITIONAL": 3}
    arity.update({name: 2 for name in (*OPERATIONS.values(), "MIN", "MAX")})
    operands = source.get("operands")
    if (operation not in arity or set(source) != {"operation", "operands"}
            or not isinstance(operands, list) or len(operands) != arity[operation]):
        raise ValueError("Serialized type formula operation/arity is invalid")
    return FormulaNode(operation, tuple(load_formula(child, variables, depth + 1, budget)
                                        for child in operands))
