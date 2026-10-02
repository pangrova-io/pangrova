package org.pangrova.planner.substrait;

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlNameMatchers;
import org.junit.jupiter.api.Test;
import org.pangrova.planner.AiInvocationPlanHint;
import org.pangrova.planner.validation.PangrovaSqlOperatorTable;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AiInvocationExpressionConverterTest {

    @Test
    void repeatedCallsBindExactModelWithUniquePerConversionInvocationIds() {
        var typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        var rexBuilder = new RexBuilder(typeFactory);
        List<AiInvocationPlanHint> hints = new ArrayList<>();
        var typeMapper = new TypeMapper();
        var functionMapper = new FunctionMapper();
        var literalConverter = new LiteralConverter(typeMapper);
        var converter = new ExpressionConverter(typeMapper, functionMapper,
                new WindowFunctionConverter(typeMapper, functionMapper, literalConverter, rexBuilder),
                rexBuilder, literalConverter, hints::add);
        var call = rexBuilder.makeCall(aiComplete(), List.of(
                rexBuilder.makeLiteral("exact-registered-model"), rexBuilder.makeLiteral("hello")));
        converter.convert(call);
        converter.convert(call);
        assertThat(hints).extracting(AiInvocationPlanHint::invocationId)
                .containsExactly("ai-invoke-0", "ai-invoke-1");
        assertThat(hints).allSatisfy(hint -> {
            assertThat(hint.modelName()).isEqualTo("exact-registered-model");
            assertThat(hint.operation()).isEqualTo(AiInvocationPlanHint.Operation.TEXT_GENERATION);
            assertThat(hint.mediaInput()).isFalse();
        });
    }

    @Test
    void rejectsRowDependentModelBeforeSerialization() {
        var typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        var rexBuilder = new RexBuilder(typeFactory);
        List<AiInvocationPlanHint> hints = new ArrayList<>();
        var typeMapper = new TypeMapper();
        var functionMapper = new FunctionMapper();
        var literalConverter = new LiteralConverter(typeMapper);
        var converter = new ExpressionConverter(
                typeMapper,
                functionMapper,
                new WindowFunctionConverter(typeMapper, functionMapper, literalConverter, rexBuilder),
                rexBuilder,
                literalConverter,
                hints::add);
        var modelType = typeFactory.createSqlType(SqlTypeName.VARCHAR, 128);
        var call = rexBuilder.makeCall(
                aiComplete(),
                List.of(
                        rexBuilder.makeInputRef(modelType, 0),
                        rexBuilder.makeLiteral("hello")));

        assertThatThrownBy(() -> converter.convert(call))
                .isInstanceOf(SubstraitConversionException.class)
                .hasMessageContaining("literal exact model name");
        assertThat(hints).isEmpty();
    }

    private static SqlOperator aiComplete() {
        List<SqlOperator> operators = new ArrayList<>();
        new PangrovaSqlOperatorTable().lookupOperatorOverloads(
                new SqlIdentifier(
                        "AI_COMPLETE",
                        org.apache.calcite.sql.parser.SqlParserPos.ZERO),
                SqlFunctionCategory.USER_DEFINED_FUNCTION,
                SqlSyntax.FUNCTION,
                operators,
                SqlNameMatchers.withCaseSensitive(false));
        assertThat(operators).hasSize(1);
        return operators.get(0);
    }
}
