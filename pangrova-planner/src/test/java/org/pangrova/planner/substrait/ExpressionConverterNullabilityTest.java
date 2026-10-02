package org.pangrova.planner.substrait;

import io.substrait.expression.Expression;
import io.substrait.expression.ImmutableExpression;
import io.substrait.type.Type;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@link ExpressionConverter} wires {@link FunctionNullabilityRegistry}
 * to produce correct output nullability on Substrait function invocations.
 */
class ExpressionConverterNullabilityTest {

    private ExpressionConverter converter;
    private RexBuilder rexBuilder;
    private RelDataTypeFactory typeFactory;

    @BeforeEach
    void setUp() {
        typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        rexBuilder = new RexBuilder(typeFactory);
        TypeMapper typeMapper = new TypeMapper();
        FunctionMapper functionMapper = new FunctionMapper();
        LiteralConverter literalConverter = new LiteralConverter(typeMapper);
        WindowFunctionConverter windowConverter = new WindowFunctionConverter(
                typeMapper, functionMapper, literalConverter, rexBuilder);
        converter = new ExpressionConverter(
                typeMapper, functionMapper, windowConverter, rexBuilder,
                literalConverter, ignored -> {});
    }

    @Test
    void isNullProducesNonNullableOutput() {
        RelDataType nullableInt = typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        RexNode inputRef = rexBuilder.makeInputRef(nullableInt, 0);
        RexNode isNullCall = rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, inputRef);

        Expression result = converter.convert(isNullCall);

        assertThat(result).isInstanceOf(ImmutableExpression.ScalarFunctionInvocation.class);
        Type outputType = ((ImmutableExpression.ScalarFunctionInvocation) result).outputType();
        assertThat(outputType.nullable()).isFalse();
    }

    @Test
    void isNotNullProducesNonNullableOutput() {
        RelDataType nullableVarchar = typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.VARCHAR, 100), true);
        RexNode inputRef = rexBuilder.makeInputRef(nullableVarchar, 0);
        RexNode isNotNullCall = rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, inputRef);

        Expression result = converter.convert(isNotNullCall);

        assertThat(result).isInstanceOf(ImmutableExpression.ScalarFunctionInvocation.class);
        Type outputType = ((ImmutableExpression.ScalarFunctionInvocation) result).outputType();
        assertThat(outputType.nullable()).isFalse();
    }

    @Test
    void addWithNonNullableArgsProducesNonNullableOutput() {
        RelDataType requiredInt = typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.INTEGER), false);
        RexNode left = rexBuilder.makeInputRef(requiredInt, 0);
        RexNode right = rexBuilder.makeInputRef(requiredInt, 1);
        RexNode addCall = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, left, right);

        Expression result = converter.convert(addCall);

        assertThat(result).isInstanceOf(ImmutableExpression.ScalarFunctionInvocation.class);
        Type outputType = ((ImmutableExpression.ScalarFunctionInvocation) result).outputType();
        assertThat(outputType.nullable()).isFalse();
    }

    @Test
    void addWithNullableArgProducesNullableOutput() {
        RelDataType nullableInt = typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.INTEGER), true);
        RelDataType requiredInt = typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.INTEGER), false);
        RexNode left = rexBuilder.makeInputRef(nullableInt, 0);
        RexNode right = rexBuilder.makeInputRef(requiredInt, 1);
        RexNode addCall = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, left, right);

        Expression result = converter.convert(addCall);

        assertThat(result).isInstanceOf(ImmutableExpression.ScalarFunctionInvocation.class);
        Type outputType = ((ImmutableExpression.ScalarFunctionInvocation) result).outputType();
        assertThat(outputType.nullable()).isTrue();
    }
}
