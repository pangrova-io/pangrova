package org.pangrova.planner.substrait;

import io.substrait.expression.Expression;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Optimizer-created comparisons must satisfy exact Native signatures without implicit coercion. */
class FunctionComparisonNormalizationTest {
    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void aggregateCountComparedWithIntegerZeroUsesBigintAndPreservesNullability(boolean nullable) {
        var types = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        var rex = new RexBuilder(types);
        var mapper = new TypeMapper();
        var count = rex.makeInputRef(types.createTypeWithNullability(
                types.createSqlType(SqlTypeName.BIGINT), nullable), 0);
        var zero = rex.makeExactLiteral(BigDecimal.ZERO, types.createSqlType(SqlTypeName.INTEGER));
        var normalizer = new FunctionArgumentNormalizer(rex, new LiteralConverter(mapper), mapper);
        var literal = new LiteralConverter(mapper);
        for (var operator : List.of(SqlStdOperatorTable.EQUALS, SqlStdOperatorTable.NOT_EQUALS,
                SqlStdOperatorTable.LESS_THAN, SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                SqlStdOperatorTable.GREATER_THAN, SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                SqlStdOperatorTable.IS_DISTINCT_FROM, SqlStdOperatorTable.IS_NOT_DISTINCT_FROM)) {
            var call = (RexCall) rex.makeCall(operator, count, zero);
            var reference = new FunctionMapper().map(operator);
            var arguments = List.of(io.substrait.expression.FieldReference.newRootStructReference(
                    0, mapper.map(count.getType())), literal.convert(zero));
            var normalized = normalizer.normalize(reference, call, arguments);
            assertThat(normalized.values()).as("%s", operator).allSatisfy(value ->
                    assertThat(value.getType()).isInstanceOf(io.substrait.type.Type.I64.class));
            assertThat(normalized.values().get(0).getType().nullable()).isEqualTo(nullable);
            assertThat(normalized.values().get(1).getType().nullable()).isFalse();
            assertThat(normalized.outputType()).isEqualTo(mapper.map(call.getType()));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void arithmeticRetainsTheValidatedDoubleWidthForIntegralOperands(boolean nullable) {
        var types = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        var rex = new RexBuilder(types);
        var mapper = new TypeMapper();
        var inputType = types.createTypeWithNullability(types.createSqlType(SqlTypeName.INTEGER), nullable);
        var first = rex.makeInputRef(inputType, 0);
        var second = rex.makeInputRef(types.createSqlType(SqlTypeName.INTEGER), 1);
        var outputType = types.createTypeWithNullability(types.createSqlType(SqlTypeName.DOUBLE), nullable);
        var call = (RexCall) rex.makeCall(outputType, SqlStdOperatorTable.DIVIDE, List.of(first, second));
        List<Expression> arguments = List.of(
                io.substrait.expression.FieldReference.newRootStructReference(0, mapper.map(first.getType())),
                io.substrait.expression.FieldReference.newRootStructReference(1, mapper.map(second.getType())));
        var normalizer = new FunctionArgumentNormalizer(rex, new LiteralConverter(mapper), mapper);
        var normalized = normalizer.normalize(new FunctionMapper().map(SqlStdOperatorTable.DIVIDE), call, arguments);
        assertThat(normalized.values()).allSatisfy(value ->
                assertThat(value.getType()).isInstanceOf(io.substrait.type.Type.FP64.class));
        assertThat(normalized.values().get(0).getType().nullable()).isEqualTo(nullable);
        assertThat(normalized.values().get(1).getType().nullable()).isFalse();
        assertThat(normalized.outputType()).isEqualTo(mapper.map(outputType));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void mixedDecimalAndDoubleArithmeticHasExactFloatingSignatureInBothOrders(boolean nullable) {
        var types = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        var rex = new RexBuilder(types);
        var mapper = new TypeMapper();
        var normalizer = new FunctionArgumentNormalizer(rex, new LiteralConverter(mapper), mapper);
        var decimal = rex.makeInputRef(types.createTypeWithNullability(
                types.createSqlType(SqlTypeName.DECIMAL, 5, 2), nullable), 0);
        var floating = rex.makeInputRef(types.createSqlType(SqlTypeName.DOUBLE), 1);
        for (List<RexNode> operands : List.<List<RexNode>>of(
                List.of(decimal, floating), List.of(floating, decimal))) {
            var resultType = types.createTypeWithNullability(types.createSqlType(SqlTypeName.DOUBLE), nullable);
            var call = (RexCall) rex.makeCall(resultType, SqlStdOperatorTable.MULTIPLY, operands);
            List<Expression> arguments = operands.stream()
                    .map(node -> (Expression) io.substrait.expression.FieldReference.newRootStructReference(
                            node == decimal ? 0 : 1, mapper.map(node.getType())))
                    .toList();
            var normalized = normalizer.normalize(new FunctionMapper().map(SqlStdOperatorTable.MULTIPLY),
                    call, arguments);
            assertThat(normalized.values()).allSatisfy(value ->
                    assertThat(value.getType()).isInstanceOf(io.substrait.type.Type.FP64.class));
            assertThat(normalized.values().get(0).getType().nullable()).isEqualTo(operands.get(0).getType().isNullable());
            assertThat(normalized.values().get(1).getType().nullable()).isEqualTo(operands.get(1).getType().isNullable());
            assertThat(normalized.outputType()).isEqualTo(mapper.map(resultType));
        }
    }

    @ParameterizedTest
    @CsvSource({"TINYINT,SMALLINT,SMALLINT", "SMALLINT,INTEGER,INTEGER", "INTEGER,BIGINT,BIGINT",
            "BIGINT,BIGINT,BIGINT", "REAL,INTEGER,REAL", "DOUBLE,BIGINT,DOUBLE"})
    void comparisonUsesCalciteCommonNumericTypeInBothOperandOrders(
            SqlTypeName left, SqlTypeName right, SqlTypeName common) {
        var types = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        var rex = new RexBuilder(types);
        var mapper = new TypeMapper();
        var normalizer = new FunctionArgumentNormalizer(rex, new LiteralConverter(mapper), mapper);
        for (var pair : List.of(List.of(left, right), List.of(right, left))) {
            var first = rex.makeInputRef(types.createSqlType(pair.get(0)), 0);
            var second = rex.makeInputRef(types.createSqlType(pair.get(1)), 1);
            var call = (RexCall) rex.makeCall(SqlStdOperatorTable.EQUALS, first, second);
            List<Expression> arguments = List.of(
                    io.substrait.expression.FieldReference.newRootStructReference(0, mapper.map(first.getType())),
                    io.substrait.expression.FieldReference.newRootStructReference(1, mapper.map(second.getType())));
            var result = normalizer.normalize(new FunctionMapper().map(SqlStdOperatorTable.EQUALS), call, arguments);
            assertThat(result.values()).allSatisfy(value ->
                    assertThat(value.getType()).isEqualTo(mapper.map(types.createSqlType(common))));
            assertThat(result.outputType()).isEqualTo(mapper.map(call.getType()));
        }
    }
}
