package org.pangrova.protocol.function.formula;

import com.google.protobuf.UnknownFieldSet;
import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.FunctionFormulaOperation;
import org.pangrova.protocol.v2.FunctionTypeFormula;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FunctionTypeFormulaEvaluatorTest {
    private static FunctionTypeFormula integer(long value) {
        return FunctionTypeFormula.newBuilder()
                .setOperation(FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_INTEGER)
                .setIntegerValue(value).build();
    }

    private static FunctionTypeFormula expression(FunctionFormulaOperation operation, FunctionTypeFormula... operands) {
        return FunctionTypeFormula.newBuilder().setOperation(operation)
                .addAllOperands(java.util.List.of(operands)).build();
    }

    @Test
    void checkedArithmeticAndActualEagerConditionalSemantics() {
        var divide = FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_DIVIDE;
        var remainder = FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_REMAINDER;
        assertThat(FunctionTypeFormulaEvaluator.evaluate(expression(divide, integer(-7), integer(3)), Map.of())).isEqualTo(-2);
        assertThat(FunctionTypeFormulaEvaluator.evaluate(expression(remainder, integer(-7), integer(3)), Map.of())).isEqualTo(-1);
        var conditional = expression(FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_CONDITIONAL,
                integer(1), integer(9), expression(divide, integer(1), integer(0)));
        assertThatThrownBy(() -> FunctionTypeFormulaEvaluator.evaluate(conditional, Map.of()))
                .isInstanceOf(ArithmeticException.class);
    }

    @Test
    void overflowAndUndefinedNativeRemainderAreRejected() {
        var add = expression(FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_ADD,
                integer(Long.MAX_VALUE), integer(1));
        assertThatThrownBy(() -> FunctionTypeFormulaEvaluator.evaluate(add, Map.of()))
                .isInstanceOf(ArithmeticException.class);
        for (var operation : java.util.List.of(FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_DIVIDE,
                FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_REMAINDER)) {
            var undefined = expression(operation, integer(Long.MIN_VALUE), integer(-1));
            assertThatThrownBy(() -> FunctionTypeFormulaEvaluator.evaluate(undefined, Map.of()))
                    .isInstanceOf(ArithmeticException.class);
        }
        assertThatThrownBy(() -> FunctionTypeFormulaEvaluator.evaluateParameter(integer(1L << 31), Map.of()))
                .isInstanceOf(ArithmeticException.class);
    }

    @Test
    void registeredVariablesAreExactAndUnboundVariablesFail() {
        var variable = FunctionTypeFormula.newBuilder()
                .setOperation(FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_VARIABLE)
                .setVariableName("precision").build();
        assertThat(FunctionTypeFormulaEvaluator.evaluate(variable, Map.of("precision", 38L))).isEqualTo(38);
        assertThatThrownBy(() -> FunctionTypeFormulaEvaluator.evaluate(variable, Map.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Unbound");
    }

    @Test
    void malformedArityUnknownFieldsAndUnknownOperationFailClosed() {
        var unknownFields = UnknownFieldSet.newBuilder().addField(100,
                UnknownFieldSet.Field.newBuilder().addVarint(1).build()).build();
        var invalid = java.util.List.of(FunctionTypeFormula.getDefaultInstance(),
                integer(1).toBuilder().setVariableName("extra").build(),
                integer(1).toBuilder().setUnknownFields(unknownFields).build(),
                expression(FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_ADD, integer(1)),
                FunctionTypeFormula.newBuilder().setOperationValue(999).build());
        for (var formula : invalid) {
            assertThatThrownBy(() -> FunctionTypeFormulaEvaluator.evaluate(formula, Map.of()))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void depthAndNodeBoundsFailBeforeUnboundedTraversal() {
        var deep = integer(1);
        for (int index = 0; index < 66; index++) {
            deep = expression(FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_NEGATE, deep);
        }
        var depthBound = deep;
        assertThatThrownBy(() -> FunctionTypeFormulaEvaluator.evaluate(depthBound, Map.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("bound");
        var broad = integer(1);
        for (int index = 0; index < 11; index++) {
            broad = expression(FunctionFormulaOperation.FUNCTION_FORMULA_OPERATION_MIN, broad, broad);
        }
        var nodeBound = broad;
        assertThatThrownBy(() -> FunctionTypeFormulaEvaluator.evaluate(nodeBound, Map.of()))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("bound");
    }
}
