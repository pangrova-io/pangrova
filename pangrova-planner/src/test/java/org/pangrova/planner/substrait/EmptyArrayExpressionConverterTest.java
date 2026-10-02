package org.pangrova.planner.substrait;

import io.substrait.expression.ImmutableExpression;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EmptyArrayExpressionConverterTest {

    private final SqlTypeFactoryImpl typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    private final RexBuilder rexBuilder = new RexBuilder(typeFactory);
    private final TypeMapper typeMapper = new TypeMapper();
    private final FunctionMapper functionMapper = new FunctionMapper();
    private final LiteralConverter literalConverter = new LiteralConverter(typeMapper);
    private final ExpressionConverter converter = new ExpressionConverter(
            typeMapper, functionMapper,
            new WindowFunctionConverter(typeMapper, functionMapper, literalConverter, rexBuilder),
            rexBuilder, literalConverter, ignored -> {});

    @Test
    void castSuppliesElementTypeToEmptyArray() {
        var varchar = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        var targetType = typeFactory.createArrayType(varchar, -1);
        var cast = rexBuilder.makeCall(targetType, SqlStdOperatorTable.CAST,
                List.of(untypedEmptyArray()));

        var result = converter.convert(cast);

        assertThat(result).isInstanceOf(ImmutableExpression.EmptyListLiteral.class);
        var emptyList = (ImmutableExpression.EmptyListLiteral) result;
        assertThat(emptyList.elementType()).isEqualTo(typeMapper.map(varchar));
    }

    @Test
    void emptyArrayWithoutElementTypeFailsClosed() {
        assertThatThrownBy(() -> converter.convert(untypedEmptyArray()))
                .isInstanceOf(SubstraitConversionException.class)
                .hasMessageContaining("typed element context");
    }

    private org.apache.calcite.rex.RexNode untypedEmptyArray() {
        var nullType = typeFactory.createSqlType(SqlTypeName.NULL);
        var arrayType = typeFactory.createArrayType(nullType, -1);
        return rexBuilder.makeCall(arrayType,
                SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR, List.of());
    }
}
