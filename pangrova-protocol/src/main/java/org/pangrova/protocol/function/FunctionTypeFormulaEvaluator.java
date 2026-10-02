package org.pangrova.protocol.function;

import org.pangrova.protocol.v2.FunctionFormulaOperation;
import org.pangrova.protocol.v2.FunctionTypeFormula;

import java.util.Map;
import java.util.Objects;
import java.util.function.LongBinaryOperator;

/** Evaluates the strict integer AST generated from actual registered constraints. */
public final class FunctionTypeFormulaEvaluator {
    private static final int MAX_DEPTH = 64;
    private static final int MAX_NODES = 2048;
    private static final Map<FunctionFormulaOperation, LongBinaryOperator> BINARY = binaries();

    private FunctionTypeFormulaEvaluator() {}

    /** All intermediate arithmetic is checked signed 64-bit, including unselected branches. */
    public static long evaluate(FunctionTypeFormula formula, Map<String, Long> variables) {
        Objects.requireNonNull(formula, "formula");
        Objects.requireNonNull(variables, "variables");
        return evaluate(formula, variables, new Budget(), 0);
    }

    /** Registered integer parameter assignment uses exact signed 32-bit. */
    public static int evaluateParameter(FunctionTypeFormula formula, Map<String, Long> variables) {
        return Math.toIntExact(evaluate(formula, variables));
    }

    private static long evaluate(FunctionTypeFormula formula, Map<String, Long> variables,
                                 Budget budget, int depth) {
        budget.consume(depth);
        rejectUnknownFields(formula);
        var operation = formula.getOperation();
        if (operation == FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_INTEGER) {
            checkLeaf(formula, false);
            return formula.getIntegerValue();
        }
        if (operation == FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_VARIABLE) {
            checkLeaf(formula, true);
            var value = variables.get(formula.getVariableName());
            if (value == null) throw new IllegalArgumentException("Unbound formula variable: " + formula.getVariableName());
            return value;
        }
        checkOperatorFields(formula);
        long[] operands = new long[formula.getOperandsCount()];
        for (int index = 0; index < operands.length; index++) {
            operands[index] = evaluate(formula.getOperands(index), variables, budget, depth + 1);
        }
        return apply(operation, operands);
    }

    private static long apply(FunctionFormulaOperation operation, long[] operands) {
        if (operation == FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_NEGATE && operands.length == 1) {
            return Math.negateExact(operands[0]);
        }
        if (operation == FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_CONDITIONAL && operands.length == 3) {
            return operands[0] == 0 ? operands[2] : operands[1];
        }
        var binary = BINARY.get(operation);
        if (binary == null || operands.length != 2) {
            throw new IllegalArgumentException("Unknown formula operation or invalid arity: " + operation);
        }
        return binary.applyAsLong(operands[0], operands[1]);
    }

    private static void rejectUnknownFields(FunctionTypeFormula formula) {
        if (!formula.getUnknownFields().asMap().isEmpty()) {
            throw new IllegalArgumentException("Formula contains unknown protocol fields");
        }
    }

    private static void checkLeaf(FunctionTypeFormula formula, boolean variable) {
        if (formula.getOperandsCount() != 0 || (variable && formula.getVariableName().isBlank())
                || (!variable && !formula.getVariableName().isEmpty())
                || (variable && formula.getIntegerValue() != 0)) {
            throw new IllegalArgumentException("Malformed formula leaf");
        }
    }

    private static void checkOperatorFields(FunctionTypeFormula formula) {
        if (formula.getIntegerValue() != 0 || !formula.getVariableName().isEmpty()
                || formula.getOperandsCount() > 3) {
            throw new IllegalArgumentException("Malformed formula operator");
        }
    }

    private static Map<FunctionFormulaOperation, LongBinaryOperator> binaries() {
        return Map.ofEntries(
                Map.entry(FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_ADD, Math::addExact),
                Map.entry(FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_SUBTRACT, Math::subtractExact),
                Map.entry(FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_MULTIPLY, Math::multiplyExact),
                Map.entry(FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_DIVIDE, FunctionTypeFormulaEvaluator::divide),
                Map.entry(FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_REMAINDER, FunctionTypeFormulaEvaluator::remainder),
                Map.entry(FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_MIN, Math::min),
                Map.entry(FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_MAX, Math::max),
                Map.entry(FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_EQUAL, (a, b) -> a == b ? 1 : 0),
                Map.entry(FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_NOT_EQUAL, (a, b) -> a != b ? 1 : 0),
                Map.entry(FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_LESS, (a, b) -> a < b ? 1 : 0),
                Map.entry(FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_LESS_OR_EQUAL, (a, b) -> a <= b ? 1 : 0),
                Map.entry(FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_GREATER, (a, b) -> a > b ? 1 : 0),
                Map.entry(FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_GREATER_OR_EQUAL, (a, b) -> a >= b ? 1 : 0));
    }

    private static long divide(long left, long right) {
        rejectInvalidDivision(left, right);
        return left / right;
    }

    private static long remainder(long left, long right) {
        rejectInvalidDivision(left, right);
        return left % right;
    }

    private static void rejectInvalidDivision(long left, long right) {
        if (right == 0 || (left == Long.MIN_VALUE && right == -1)) {
            throw new ArithmeticException("Formula division/remainder is undefined or overflows");
        }
    }

    private static final class Budget {
        private int remaining = MAX_NODES;

        void consume(int depth) {
            if (depth > MAX_DEPTH || --remaining < 0) {
                throw new IllegalArgumentException("Formula exceeds its node/depth bound");
            }
        }
    }
}
