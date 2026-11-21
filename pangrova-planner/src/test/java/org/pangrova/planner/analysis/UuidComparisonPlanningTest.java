package org.pangrova.planner.analysis;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.planner.substrait.FunctionMapper;
import org.pangrova.planner.type.PlannerRexBuilder;
import org.pangrova.planner.type.PlannerTypeFactory;

import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

class UuidComparisonPlanningTest {
    @ParameterizedTest
    @ValueSource(ints = {0, 15, 16, 17})
    void binaryUuidCastsRetainNativeValidationAndConversion(int width) throws Exception {
        var compiled = new SqlCompiler().compileToResult(
                "SELECT CAST(X'" + "00".repeat(width) + "' AS UUID) AS id");
        assertThat(compiled.plan()).isNotInstanceOf(Values.class);
        assertThat(RelOptUtil.toString(compiled.plan())).contains("CAST(", "UUID");
    }

    @Test
    void enclosingExpressionsCannotFoldAwayNativeUuidValidation() throws Exception {
        var compiled = new SqlCompiler().compileToResult(
                "SELECT CAST(CAST(X'" + "00".repeat(17) + "' AS UUID) AS VARCHAR) AS id");
        assertThat(compiled.plan()).isNotInstanceOf(Values.class);
        assertThat(RelOptUtil.toString(compiled.plan())).contains("UUID", "VARCHAR");
    }

    static Stream<Arguments> comparisons() {
        List<String> ids = List.of("00000000-0000-0000-0000-000000000000",
                "00000000-0000-0000-8000-000000000000", "80000000-0000-0000-0000-000000000000",
                "ffffffff-ffff-ffff-ffff-ffffffffffff");
        return ids.stream().flatMap(left -> ids.stream().flatMap(right ->
                Stream.of("<", "<=", ">", ">=").map(operator -> Arguments.of(left, right, operator))));
    }

    @ParameterizedTest
    @MethodSource("comparisons")
    void foldedComparisonsMatchUnsignedCanonicalWireBytes(String left, String right, String operator) throws Exception {
        var compiled = new SqlCompiler().compileToResult(
                "SELECT UUID '" + left + "' " + operator + " UUID '" + right + "' AS comparison");
        var hex = HexFormat.of();
        int comparison = Arrays.compareUnsigned(hex.parseHex(left.replace("-", "")),
                hex.parseHex(right.replace("-", "")));
        boolean expected = switch (operator) {
            case "<" -> comparison < 0;
            case "<=" -> comparison <= 0;
            case ">" -> comparison > 0;
            case ">=" -> comparison >= 0;
            default -> throw new AssertionError(operator);
        };
        assertThat(compiled.plan()).isInstanceOfSatisfying(Values.class, values ->
                assertThat(RexLiteral.booleanValue(values.getTuples().get(0).get(0))).isEqualTo(expected));
    }

    @Test
    void constantUuidComparisonUsesTheSameUnsignedOrderAsNativeExecution() throws Exception {
        var compiled = new SqlCompiler().compileToResult("SELECT UUID '00000000-0000-0000-0000-000000000000'"
                + " < UUID 'ffffffff-ffff-ffff-ffff-ffffffffffff' AS smaller");
        assertThat(compiled.plan()).isInstanceOfSatisfying(Values.class, values ->
                assertThat(RexLiteral.booleanValue(values.getTuples().get(0).get(0))).isTrue());
    }

    @Test
    void nullOrderingKeepsSqlUnknown() throws Exception {
        var compiled = new SqlCompiler().compileToResult(
                "SELECT CAST(NULL AS UUID) < UUID 'ffffffff-ffff-ffff-ffff-ffffffffffff' AS comparison");
        assertThat(compiled.plan()).isInstanceOfSatisfying(Values.class, values ->
                assertThat(values.getTuples().get(0).get(0).isNull()).isTrue());
    }

    @Test
    void runtimeOrderingRetainsTheDeclaredNativeFunctionIdentity() {
        var types = new PlannerTypeFactory();
        var builder = new PlannerRexBuilder(types);
        var comparison = (RexCall) builder.makeCall(SqlStdOperatorTable.LESS_THAN,
                builder.makeInputRef(types.createSqlType(SqlTypeName.UUID), 0),
                builder.makeUuidLiteral(UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff")));
        var function = new FunctionMapper().map(comparison.getOperator());
        assertThat(function.namespace()).isEqualTo("comparison");
        assertThat(function.name()).isEqualTo("lt");
    }
}
