package org.pangrova.protocol.function.declaration;

import com.fasterxml.jackson.databind.JsonNode;
import org.pangrova.protocol.v2.FunctionFormulaOperation;
import org.pangrova.protocol.v2.FunctionTypeFormula;

import java.util.Set;

/** Converts the sole exact control AST to its typed protocol message, never parses SQL. */
final class FunctionFormulaDeclaration {
    private static final int MAX_FORMULA_NODES = 2048;

    private FunctionFormulaDeclaration() {}

    static FunctionTypeFormula parse(JsonNode source, Set<String> variables) {
        return parse(source, variables, 0, new int[]{MAX_FORMULA_NODES});
    }

    private static FunctionTypeFormula parse(JsonNode source, Set<String> variables, int depth, int[] budget) {
        if (source == null || !source.isObject() || depth > 64 || --budget[0] < 0) {
            throw new IllegalArgumentException("Type formula declaration exceeds its bound");
        }
        String name = BindingDeclarationJson.text(source, "operation");
        var operation = FunctionFormulaOperation.valueOf("FUNCTION_FORMULA_OPERATION_" + name);
        var result = FunctionTypeFormula.newBuilder().setOperation(operation);
        if (operation == FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_INTEGER) {
            BindingDeclarationJson.keys(source, Set.of("operation", "value"), Set.of());
            var value = source.get("value");
            if (!value.isIntegralNumber() || !value.canConvertToLong()) {
                throw new IllegalArgumentException("Type formula integer is outside the exact signed domain");
            }
            return result.setIntegerValue(value.longValue()).build();
        }
        if (operation == FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_VARIABLE) {
            BindingDeclarationJson.keys(source, Set.of("operation", "value"), Set.of());
            String variable = BindingDeclarationJson.text(source, "value");
            if (!variables.contains(variable)) throw new IllegalArgumentException("Unknown type formula variable");
            return result.setVariableName(variable).build();
        }
        BindingDeclarationJson.keys(source, Set.of("operation", "operands"), Set.of());
        var operands = source.get("operands");
        if (!operands.isArray() || operands.size() != arity(operation)) {
            throw new IllegalArgumentException("Type formula declaration has an invalid operation arity");
        }
        for (JsonNode operand : operands) result.addOperands(parse(operand, variables, depth + 1, budget));
        return result.build();
    }

    private static int arity(FunctionFormulaOperation operation) {
        return switch (operation) {
            case FUNCTION_FORMULA_OPERATION_NEGATE -> 1;
            case FUNCTION_FORMULA_OPERATION_CONDITIONAL -> 3;
            case FUNCTION_FORMULA_OPERATION_UNSPECIFIED, UNRECOGNIZED ->
                    throw new IllegalArgumentException("Unknown type formula declaration operation");
            default -> 2;
        };
    }
}
