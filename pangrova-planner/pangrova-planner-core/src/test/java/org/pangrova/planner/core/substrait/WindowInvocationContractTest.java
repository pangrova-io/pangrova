package org.pangrova.planner.core.substrait;

import com.google.common.collect.ImmutableList;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.FieldReference;
import io.substrait.relation.Project;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.rex.RexWindowExclusion;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.planner.core.type.PlannerTypeSystem;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WindowInvocationContractTest {

    private final SqlTypeFactoryImpl types = new SqlTypeFactoryImpl(PlannerTypeSystem.INSTANCE);
    private final RexBuilder rex = new RexBuilder(types);
    private final TypeMapper mapper = new TypeMapper();
    private final FunctionMapper functions = new FunctionMapper();
    private final LiteralConverter literals = new LiteralConverter(mapper);
    private final WindowFunctionConverter windows = new WindowFunctionConverter(mapper, functions, literals, rex);
    private final ExpressionConverter expressions = new ExpressionConverter(
            mapper, functions, windows, rex, literals, ignored -> {});

    @ParameterizedTest
    @CsvSource({"false, 18, 2, -7.25", "true, 18, 2, -7.25",
            "false, 38, 18, 12345678901234567890.123456789012345678",
            "true, 38, 18, 12345678901234567890.123456789012345678"})
    void decimalDefaultPreservesValuePrecisionAndScale(boolean logical, int precision, int scale, String text) {
        RelDataType valueType = decimal(precision, scale);
        RexLiteral defaultValue = rex.makeExactLiteral(new BigDecimal(text), valueType);

        Expression.WindowFunctionInvocation invocation = lag(logical, valueType, integer(2), defaultValue);

        assertThat(invocation.arguments().get(2))
                .isEqualTo(literals.convert(defaultValue, valueType))
                .isEqualTo(ExpressionCreator.decimal(false, new BigDecimal(text), precision, scale));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void integerDefaultIsNotWidenedToBigint(boolean logical) {
        RelDataType valueType = types.createSqlType(SqlTypeName.INTEGER);

        var invocation = lag(logical, valueType, integer(2), integer(-7));

        assertThat(invocation.arguments().get(1)).isEqualTo(ExpressionCreator.i64(false, 2L));
        assertThat(invocation.arguments().get(2)).isEqualTo(ExpressionCreator.i32(false, -7));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void doubleDefaultUsesItsActualDecimalValue(boolean logical) {
        RelDataType valueType = types.createSqlType(SqlTypeName.DOUBLE);

        var invocation = lag(logical, valueType, integer(2), rex.makeExactLiteral(new BigDecimal("-7.25")));

        assertThat(invocation.arguments().get(2)).isEqualTo(ExpressionCreator.fp64(false, -7.25));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void nullDefaultKeepsTheValueParametersAndNullableType(boolean logical) {
        RelDataType valueType = decimal(38, 18);
        RexLiteral defaultValue = rex.makeNullLiteral(types.createSqlType(SqlTypeName.INTEGER));

        var invocation = lag(logical, valueType, integer(2), defaultValue);

        assertThat(invocation.arguments().get(2)).isInstanceOf(Expression.NullLiteral.class);
        assertThat(((Expression) invocation.arguments().get(2)).getType())
                .isEqualTo(mapper.map(types.createTypeWithNullability(valueType, true)));
    }

    @ParameterizedTest
    @CsvSource({"false, 0", "true, 0", "false, 2.0", "true, 2.0",
            "false, 9223372036854775807", "true, 9223372036854775807",
            "false, -2", "true, -2"})
    void offsetPreservesAnExactIntegralValue(boolean logical, String text) {
        RexLiteral offset = rex.makeExactLiteral(new BigDecimal(text));

        var invocation = lag(logical, types.createSqlType(SqlTypeName.INTEGER), offset, integer(-7));

        assertThat(invocation.arguments().get(1))
                .isEqualTo(ExpressionCreator.i64(false, new BigDecimal(text).longValueExact()));
    }

    @ParameterizedTest
    @CsvSource({"false, 1.5", "true, 1.5", "false, 9223372036854775808",
            "true, 9223372036854775808", "false, -9223372036854775809",
            "true, -9223372036854775809"})
    void offsetRejectsFractionAndOverflowInsteadOfChoosingOne(boolean logical, String text) {
        RexLiteral offset = rex.makeExactLiteral(new BigDecimal(text),
                types.createSqlType(SqlTypeName.DECIMAL, 38, text.contains(".") ? 1 : 0));

        assertThatThrownBy(() -> lag(logical, types.createSqlType(SqlTypeName.INTEGER), offset, integer(-7)))
                .isInstanceOf(SubstraitConversionException.class).hasMessageContaining("exact BIGINT");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void nullOffsetFailsInsteadOfChoosingOne(boolean logical) {
        RexLiteral offset = rex.makeNullLiteral(types.createSqlType(SqlTypeName.BIGINT));

        assertThatThrownBy(() -> lag(logical, types.createSqlType(SqlTypeName.INTEGER), offset, integer(-7)))
                .isInstanceOf(SubstraitConversionException.class).hasMessageContaining("must not be NULL");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void dynamicBigintOffsetIsPreservedAsAFieldReference(boolean logical) {
        RexNode offset = rex.makeInputRef(types.createSqlType(SqlTypeName.BIGINT), 1);

        var invocation = lag(logical, types.createSqlType(SqlTypeName.INTEGER), offset, integer(-7));

        assertThat(invocation.arguments().get(1))
                .isEqualTo(FieldReference.newRootStructReference(1, mapper.map(offset.getType())));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void dynamicFractionalOffsetFailsBeforeNativeExecution(boolean logical) {
        RexNode offset = rex.makeInputRef(types.createSqlType(SqlTypeName.DECIMAL, 18, 2), 1);

        assertThatThrownBy(() -> lag(logical, types.createSqlType(SqlTypeName.INTEGER), offset, integer(-7)))
                .isInstanceOf(SubstraitConversionException.class).hasMessageContaining("materialized BIGINT");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void dynamicDefaultKeepsItsValueTypeAndFieldReference(boolean logical) {
        RelDataType valueType = decimal(38, 18);
        RexNode defaultValue = rex.makeInputRef(valueType, 2);

        var invocation = lag(logical, valueType, integer(2), defaultValue);

        assertThat(invocation.arguments().get(2))
                .isEqualTo(FieldReference.newRootStructReference(2, mapper.map(valueType)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void leadUsesTheSameExactLiteralContractAsLag(boolean logical) {
        RelDataType valueType = decimal(18, 2);
        var defaultValue = rex.makeExactLiteral(new BigDecimal("-7.25"));

        var invocation = convert(logical, SqlStdOperatorTable.LEAD,
                List.of(rex.makeInputRef(valueType, 0), integer(2), defaultValue), true, Set.of());

        assertThat(invocation.arguments().get(2)).isEqualTo(literals.convert(defaultValue, valueType));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void defaultCannotSilentlyRoundToTheValueScale(boolean logical) {
        RelDataType valueType = decimal(18, 2);
        var defaultValue = rex.makeExactLiteral(new BigDecimal("-7.251"));

        assertThatThrownBy(() -> lag(logical, valueType, integer(2), defaultValue))
                .isInstanceOf(SubstraitConversionException.class)
                .hasMessageContaining("cannot be represented");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void ntileBucketKeepsTheRequiredBigintType(boolean logical) {
        var invocation = convert(logical, SqlStdOperatorTable.NTILE, List.of(integer(4)), true, Set.of());

        assertThat(invocation.arguments().get(0)).isEqualTo(ExpressionCreator.i64(false, 4L));
    }

    @ParameterizedTest
    @CsvSource({"false, 0", "true, 0", "false, -1", "true, -1"})
    void ntileRejectsNonPositiveBuckets(boolean logical, long buckets) {
        assertThatThrownBy(() -> convert(logical, SqlStdOperatorTable.NTILE,
                List.of(integer(buckets)), true, Set.of()))
                .isInstanceOf(SubstraitConversionException.class).hasMessageContaining("must be positive");
    }

    @ParameterizedTest
    @CsvSource({"false, true", "true, true", "false, false", "true, false"})
    void retainsRowsVersusRange(boolean logical, boolean rows) {
        var value = rex.makeInputRef(types.createSqlType(SqlTypeName.INTEGER), 0);

        var invocation = convert(logical, SqlStdOperatorTable.SUM, List.of(value), rows, Set.of());

        assertThat(invocation.boundsType())
                .isEqualTo(rows ? Expression.WindowBoundsType.ROWS : Expression.WindowBoundsType.RANGE);
    }

    @ParameterizedTest
    @CsvSource({"false, false, false", "false, false, true", "false, true, false", "false, true, true",
            "true, false, false", "true, false, true", "true, true, false", "true, true, true"})
    void retainsEveryDirectionAndNullOrdering(boolean logical, boolean descending, boolean nullsFirst) {
        Set<SqlKind> flags = descending
                ? Set.of(SqlKind.DESCENDING, nullsFirst ? SqlKind.NULLS_FIRST : SqlKind.NULLS_LAST)
                : Set.of(nullsFirst ? SqlKind.NULLS_FIRST : SqlKind.NULLS_LAST);
        var value = rex.makeInputRef(types.createSqlType(SqlTypeName.INTEGER), 0);

        var invocation = convert(logical, SqlStdOperatorTable.SUM, List.of(value), true, flags);

        assertThat(invocation.sort()).hasSize(1);
        assertThat(invocation.sort().get(0).direction()).isEqualTo(descending
                ? (nullsFirst ? Expression.SortDirection.DESC_NULLS_FIRST : Expression.SortDirection.DESC_NULLS_LAST)
                : (nullsFirst ? Expression.SortDirection.ASC_NULLS_FIRST : Expression.SortDirection.ASC_NULLS_LAST));
    }

    @ParameterizedTest
    @CsvSource({"false, false", "false, true", "true, false", "true, true"})
    void retainsDistinctInvocationInBothCalciteWindowPaths(boolean logical, boolean distinct) {
        var input = rex.makeInputRef(types.createSqlType(SqlTypeName.BIGINT), 0);
        var invocation = convert(logical, SqlStdOperatorTable.SUM, List.of(input), true, Set.of(),
                new WindowFlags(distinct, false));

        assertThat(invocation.invocation()).isEqualTo(distinct
                ? Expression.AggregationInvocation.DISTINCT : Expression.AggregationInvocation.ALL);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void retainsDeclaredIgnoreNullTreatmentInBothCalciteWindowPaths(boolean logical) {
        var input = rex.makeInputRef(types.createSqlType(SqlTypeName.BIGINT), 0);
        var invocation = convert(logical, SqlStdOperatorTable.FIRST_VALUE, List.of(input), true, Set.of(),
                new WindowFlags(false, true));

        assertThat(invocation.options()).hasSize(1);
        assertThat(invocation.options().get(0).getName()).isEqualTo("null_treatment");
        assertThat(invocation.options().get(0).values()).containsExactly("IGNORE_NULLS");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void unsupportedIgnoreNullsCannotSilentlyBecomeRespectNulls(boolean logical) {
        var input = rex.makeInputRef(types.createSqlType(SqlTypeName.BIGINT), 0);
        assertThatThrownBy(() -> convert(logical, SqlStdOperatorTable.SUM, List.of(input), true, Set.of(),
                new WindowFlags(false, true))).isInstanceOf(SubstraitConversionException.class)
                .hasMessageContaining("does not support IGNORE NULLS");
    }

    private Expression.WindowFunctionInvocation lag(
            boolean logical, RelDataType valueType, RexNode offset, RexNode defaultValue) {
        return convert(logical, SqlStdOperatorTable.LAG,
                List.of(rex.makeInputRef(valueType, 0), offset, defaultValue), true, Set.of());
    }

    private RexLiteral integer(long value) {
        return rex.makeExactLiteral(BigDecimal.valueOf(value));
    }

    private RelDataType decimal(int precision, int scale) {
        return new BasicSqlType(types.getTypeSystem(), SqlTypeName.DECIMAL, precision, scale);
    }

    private Expression.WindowFunctionInvocation convert(boolean logical, SqlAggFunction function,
            List<RexNode> operands, boolean rows, Set<SqlKind> flags) {
        return convert(logical, function, operands, rows, flags, new WindowFlags(false, false));
    }

    private record WindowFlags(boolean distinct, boolean ignoreNulls) {}

    private Expression.WindowFunctionInvocation convert(boolean logical, SqlAggFunction function,
            List<RexNode> operands, boolean rows, Set<SqlKind> flags, WindowFlags treatment) {
        if (logical) {
            LogicalWindow window = logicalWindow(function, operands, rows, flags, treatment);
            Project project = (Project) new PlanNodeConverter(mapper, functions, rex).visit(window);
            return (Expression.WindowFunctionInvocation) project.getExpressions().get(3);
        }
        var order = new RexFieldCollation(operands.get(0), flags);
        RexOver over = (RexOver) rex.makeOver(outputType(function, operands), function, operands,
                List.of(), ImmutableList.of(order), RexWindowBounds.UNBOUNDED_PRECEDING,
                RexWindowBounds.CURRENT_ROW, rows, true, false, treatment.distinct(), treatment.ignoreNulls());
        return (Expression.WindowFunctionInvocation) expressions.convert(over);
    }

    private LogicalWindow logicalWindow(SqlAggFunction function, List<RexNode> operands,
                                         boolean rows, Set<SqlKind> flags) {
        return logicalWindow(function, operands, rows, flags, new WindowFlags(false, false));
    }

    private LogicalWindow logicalWindow(SqlAggFunction function, List<RexNode> operands,
                                         boolean rows, Set<SqlKind> flags, WindowFlags treatment) {
        RelOptCluster cluster = RelOptCluster.create(new VolcanoPlanner(), rex);
        RelDataType inputType = types.builder().add("VALUE", operands.get(0).getType())
                .add("OFFSET", SqlTypeName.BIGINT).add("DEFAULT_VALUE", operands.get(0).getType()).build();
        var input = LogicalValues.createEmpty(cluster, inputType);
        List<RexLiteral> constants = new ArrayList<>();
        List<RexNode> references = operands.stream().map(operand -> {
            if (operand instanceof RexLiteral literal) {
                constants.add(literal);
                return (RexNode) rex.makeInputRef(literal.getType(), 3 + constants.size() - 1);
            }
            return operand;
        }).toList();
        var resultType = outputType(function, operands);
        var agg = new Window.RexWinAggCall(SqlParserPos.ZERO, function, resultType,
                references, 0, treatment.distinct(), treatment.ignoreNulls());
        var order = new RexFieldCollation(operands.get(0), flags);
        var collation = new RelFieldCollation(0, order.getDirection(), order.getNullDirection());
        var group = new Window.Group(ImmutableBitSet.of(), rows, RexWindowBounds.UNBOUNDED_PRECEDING,
                RexWindowBounds.CURRENT_ROW, RexWindowExclusion.EXCLUDE_NO_OTHER,
                RelCollations.of(collation), List.of(agg));
        RelDataType outputType = types.builder().addAll(inputType.getFieldList())
                .add("RESULT", resultType).build();
        return LogicalWindow.create(input.getTraitSet(), List.of(), input, constants, outputType, List.of(group));
    }

    private RelDataType outputType(SqlAggFunction function, List<RexNode> operands) {
        return function == SqlStdOperatorTable.NTILE || function == SqlStdOperatorTable.SUM
                ? types.createSqlType(SqlTypeName.BIGINT) : operands.get(0).getType();
    }
}
