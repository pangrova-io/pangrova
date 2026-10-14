package org.pangrova.planner.substrait;

import io.substrait.proto.Expression;
import io.substrait.proto.Plan;
import io.substrait.proto.Type;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.planner.PlannerException;
import org.pangrova.planner.SqlCompiler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Public SQL-to-canonical-protocol checks; Native executes the independent result-value oracle. */
class CanonicalExpressionSqlProtocolTest {

    @Test
    void mapConstructorUsesTypedCanonicalLiteral() throws Exception {
        var map = expression("SELECT MAP['width', 100, 'height', 200]");
        assertThat(map.hasLiteral()).isTrue();
        assertThat(map.getLiteral().hasMap()).isTrue();
        assertThat(map.getLiteral().getMap().getKeyValuesList()).hasSize(2)
                .allMatch(pair -> pair.getKey().hasString() && pair.getValue().hasI32());
        assertThat(map.getLiteral().getMap().getKeyValuesList())
                .extracting(pair -> pair.getKey().getString())
                .containsExactlyInAnyOrder("width", "height");
    }

    @Test
    void mapConstructorPreservesExplicitTrailingSpaceInStringKey() throws Exception {
        var map = expression("SELECT MAP['width ', 100, 'height', 200]");
        assertThat(map.getLiteral().getMap().getKeyValuesList())
                .extracting(pair -> pair.getKey().getString())
                .containsExactlyInAnyOrder("width ", "height");
    }

    @Test
    void ordinaryDecimalArrayPreservesCommonPrecisionScaleAndTypedNulls() throws Exception {
        var expression = expression("SELECT ARRAY[1.23, CAST(NULL AS DECIMAL(10,2)), 2]");
        assertThat(expression.hasLiteral()).isTrue();
        var values = expression.getLiteral().getList().getValuesList();
        assertThat(values).hasSize(3);
        assertThat(values.get(0).hasDecimal()).isTrue();
        assertThat(values.get(0).getDecimal().getPrecision()).isEqualTo(12);
        assertThat(values.get(0).getDecimal().getScale()).isEqualTo(2);
        assertDecimal(values.get(1).getNull(), 12, 2);
        assertThat(values.get(2).hasDecimal()).isTrue();
        assertThat(values.get(2).getDecimal().getPrecision()).isEqualTo(12);
        assertThat(values.get(2).getDecimal().getScale()).isEqualTo(2);
        assertThat(values.get(0).getNullable()).isTrue();
        assertThat(values.get(2).getNullable()).isTrue();
        assertThat(values.get(1).getNull().getDecimal().getNullability())
                .isEqualTo(Type.Nullability.NULLABILITY_NULLABLE);
    }

    @Test
    void integerArrayKeepsBigintEvenWhenLiteralRepresentationIsDecimalAndItsValueFitsInt() throws Exception {
        var values = expression("SELECT ARRAY[CAST(1 AS BIGINT), CAST(NULL AS BIGINT), CAST(2 AS BIGINT)]")
                .getLiteral().getList().getValuesList();
        assertThat(values).hasSize(3);
        assertThat(values.get(0).getLiteralTypeCase()).isEqualTo(Expression.Literal.LiteralTypeCase.I64);
        assertThat(values.get(0).getI64()).isEqualTo(1);
        assertThat(values.get(1).getNull().hasI64()).isTrue();
        assertThat(values.get(2).getLiteralTypeCase()).isEqualTo(Expression.Literal.LiteralTypeCase.I64);
        assertThat(values.get(2).getI64()).isEqualTo(2);
    }

    @Test
    void nestedDecimalArrayPreservesItsDeclaredCommonElementType() throws Exception {
        var rows = expression("SELECT ARRAY[ARRAY[CAST(1.23 AS DECIMAL(10,2))], "
                + "ARRAY[CAST(NULL AS DECIMAL(10,2))]]").getLiteral().getList().getValuesList();
        assertThat(rows).hasSize(2);
        var value = rows.get(0).getList().getValues(0);
        assertThat(value.hasDecimal()).isTrue();
        assertThat(value.getDecimal().getPrecision()).isEqualTo(10);
        assertThat(value.getDecimal().getScale()).isEqualTo(2);
        assertDecimal(rows.get(1).getList().getValues(0).getNull(), 10, 2);
    }

    @Test
    void runtimeRowArrayUsesCanonicalNestedConstructorsWithExactDecimalAndNullability() throws Exception {
        var array = expression("SELECT ARRAY[ROW(x,CAST(y AS DECIMAL(20,2))), "
                + "ROW(x,CAST(NULL AS DECIMAL(20,2)))] "
                + "FROM (SELECT 1 AS x, 2 AS y) t");
        assertThat(array.hasNested()).isTrue();
        var rows = array.getNested().getList().getValuesList();
        assertThat(rows).hasSize(2).allMatch(Expression::hasNested);
        var first = rows.get(0).getNested().getStruct();
        var second = rows.get(1).getNested().getStruct();
        assertThat(first.getFields(0).hasSelection()).isTrue();
        assertDecimal(first.getFields(1).getCast().getType(), 20, 2);
        assertThat(first.getFields(1).getCast().getType().getDecimal().getNullability())
                .isEqualTo(Type.Nullability.NULLABILITY_NULLABLE);
        assertDecimal(second.getFields(1).getLiteral().getNull(), 20, 2);
    }

    @Test
    void caseKeepsValidatedDecimalBranchesAndTypedNullInsteadOfLoweringToDouble() throws Exception {
        var conditional = expression("SELECT CASE WHEN x=1 THEN CAST(1.23 AS DECIMAL(20,2)) "
                + "ELSE CAST(NULL AS DECIMAL(20,2)) END FROM (SELECT 1 AS x) t").getIfThen();
        var value = conditional.getIfs(0).getThen().getLiteral();
        assertThat(value.hasDecimal()).isTrue();
        assertThat(value.getDecimal().getPrecision()).isEqualTo(20);
        assertThat(value.getDecimal().getScale()).isEqualTo(2);
        assertDecimal(conditional.getElse().getLiteral().getNull(), 20, 2);
    }

    @Test
    void scalarBigintAndDecimalLiteralsNeverBecomeValueGuessedIntArguments() throws Exception {
        var bigint = expression("SELECT CAST(x AS BIGINT) + CAST(1 AS BIGINT) FROM (SELECT 1 AS x) t")
                .getScalarFunction();
        assertThat(bigint.getOutputType().hasI64()).isTrue();
        assertThat(bigint.getArguments(1).getValue().getLiteral().getLiteralTypeCase())
                .isEqualTo(Expression.Literal.LiteralTypeCase.I64);
        var decimal = expression("SELECT CAST(x AS DECIMAL(20,0)) + CAST(1 AS DECIMAL(20,0)) "
                + "FROM (SELECT 1 AS x) t").getScalarFunction();
        assertDecimal(decimal.getOutputType(), 21, 0);
        var literal = decimal.getArguments(1).getValue().getLiteral();
        assertThat(literal.hasDecimal()).isTrue();
        assertThat(literal.getDecimal().getPrecision()).isEqualTo(20);
        assertThat(literal.getDecimal().getScale()).isZero();
    }

    @Test
    void mixedIntegerArithmeticPromotesTheNarrowOperand() throws Exception {
        var add = expression("SELECT CAST(x AS BIGINT) + 1 FROM (SELECT 1 AS x) t")
                .getScalarFunction();
        assertThat(add.getOutputType().hasI64()).isTrue();
        var narrow = add.getArguments(1).getValue();
        if (narrow.hasCast()) {
            assertThat(narrow.getCast().getType().hasI64()).isTrue();
        } else {
            assertThat(narrow.getLiteral().hasI64()).isTrue();
        }
    }

    @Test
    void comparisonKeepsTheValidatedCommonTypeCastsInTheProtocol() throws Exception {
        var comparison = expression("SELECT CAST(x AS INTEGER) = CAST(y AS DECIMAL(20,2)) "
                + "FROM (SELECT 1 AS x, 2 AS y) t").getScalarFunction();
        assertThat(comparison.getOutputType().hasBool()).isTrue();
        for (var argument : comparison.getArgumentsList()) {
            assertThat(argument.getValue().hasCast()).isTrue();
            assertDecimal(argument.getValue().getCast().getType(), 20, 2);
            assertThat(argument.getValue().getCast().getInput().hasSelection()).isTrue();
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"L2_DISTANCE", "COSINE_DISTANCE", "INNER_PRODUCT", "COSINE_SIMILARITY"})
    void distanceWritesDoubleArrayCastsWithoutLoweringTheOrdinaryDecimalInputs(String function) throws Exception {
        var invocation = expression("SELECT " + function + "(ARRAY[1.0,2.0], ARRAY[2.0,3.0])")
                .getScalarFunction();
        assertThat(invocation.getOutputType().hasFp64()).isTrue();
        for (var argument : invocation.getArgumentsList()) {
            assertThat(argument.getValue().hasCast()).isTrue();
            var cast = argument.getValue().getCast();
            assertThat(cast.getType().getList().getType().hasFp64()).isTrue();
            assertThat(cast.getInput().getLiteral().getList().getValuesList()).hasSize(2)
                    .allMatch(Expression.Literal::hasDecimal);
        }
    }

    @Test
    void floatVectorSignatureStaysFloatWithAnExplicitQueryCast() throws Exception {
        var invocation = expression("SELECT L2_DISTANCE(ARRAY[CAST(1 AS FLOAT),CAST(2 AS FLOAT)], ARRAY[2.0,3.0])")
                .getScalarFunction();
        assertThat(invocation.getArguments(0).getValue().getCast().getType().getList().getType().hasFp32()).isTrue();
        assertThat(invocation.getArguments(1).getValue().getCast().getType().getList().getType().hasFp32()).isTrue();
        assertThat(invocation.getArguments(1).getValue().getCast().getInput().getLiteral()
                .getList().getValuesList()).allMatch(Expression.Literal::hasDecimal);
    }

    @ParameterizedTest
    @ValueSource(strings = {"L2_DISTANCE(1,2)", "COSINE_DISTANCE(ARRAY['a'],ARRAY['b'])",
            "INNER_PRODUCT(ARRAY[TRUE],ARRAY[FALSE])", "L2_DISTANCE(ARRAY[ARRAY[1]],ARRAY[ARRAY[2]])",
            "L2_DISTANCE(ARRAY[1])", "L2_DISTANCE(ARRAY[1],ARRAY[2],ARRAY[3])"})
    void vectorSignatureRejectsInvalidTypesOrArity(String invocation) {
        assertThatThrownBy(() -> new SqlCompiler().compileForPolicyApplication("SELECT " + invocation))
                .isInstanceOf(PlannerException.class);
    }

    @Test
    void substringWritesTheSameBigintPositionSignatureForMixedDeclaredIntegers() throws Exception {
        var invocation = expression("SELECT SUBSTRING(x,CAST(y AS BIGINT),2) "
                + "FROM (SELECT 'abcdef' AS x, 1 AS y) t").getScalarFunction();
        assertThat(invocation.getOutputType().hasString()).isTrue();
        assertThat(invocation.getArguments(1).getValue().getCast().getType().hasI64()).isTrue();
        var length = invocation.getArguments(2).getValue().getLiteral();
        assertThat(length.getLiteralTypeCase()).isEqualTo(Expression.Literal.LiteralTypeCase.I64);
        assertThat(length.getI64()).isEqualTo(2);
    }

    @ParameterizedTest
    @CsvSource({"'ROUND(CAST(x AS DECIMAL(20,2)))',19,0",
            "'ROUND(CAST(x AS DECIMAL(20,2)),CAST(y AS BIGINT))',21,2",
            "'ROUND(CAST(x AS DECIMAL(38,18)),CAST(y AS BIGINT))',38,18"})
    void roundUsesNativeDecimalReturnParametersAndDeclaredIntegerPosition(String call, int precision, int scale)
            throws Exception {
        var invocation = expression("SELECT " + call + " FROM (SELECT 1 AS x, 1 AS y) t").getScalarFunction();
        assertDecimal(invocation.getOutputType(), precision, scale);
        assertDecimal(invocation.getArguments(0).getValue().getCast().getType(),
                call.contains("38,18") ? 38 : 20, call.contains("38,18") ? 18 : 2);
        if (invocation.getArgumentsCount() == 2) {
            assertThat(invocation.getArguments(1).getValue().hasCast()).isTrue();
            assertThat(invocation.getArguments(1).getValue().getCast().getType().hasI32()).isTrue();
        }
    }

    @Test
    void roundDoubleKeepsAnIntegerPositionAfterNumericPromotion() throws Exception {
        var invocation = expression("SELECT ROUND(CAST(x AS DOUBLE), CAST(y AS BIGINT)) "
                + "FROM (SELECT 1 AS x, 0 AS y) t").getScalarFunction();
        assertThat(invocation.getOutputType().hasFp64()).isTrue();
        assertThat(invocation.getArguments(0).getValue().getCast().getType().hasFp64()).isTrue();
        assertThat(invocation.getArguments(1).getValue().getCast().getType().hasI32()).isTrue();
    }

    @Test
    void dayIntervalLiteralPreservesExactMillisecondsInCanonicalComponents() throws Exception {
        var invocation = expression("SELECT dt + INTERVAL '30' DAY "
                + "FROM (SELECT DATE '2024-01-01' AS dt) t").getScalarFunction();
        var interval = invocation.getArguments(1).getValue().getLiteral().getIntervalDayToSecond();
        assertThat(interval.getDays()).isEqualTo(30);
        assertThat(interval.getSeconds()).isZero();
        assertThat(interval.getSubseconds()).isZero();
        assertThat(interval.getPrecision()).isEqualTo(6);
    }

    private static void assertDecimal(Type type, int precision, int scale) {
        assertThat(type.hasDecimal()).isTrue();
        assertThat(type.getDecimal().getPrecision()).isEqualTo(precision);
        assertThat(type.getDecimal().getScale()).isEqualTo(scale);
    }

    private static Expression expression(String sql) throws Exception {
        var compiler = new SqlCompiler();
        var compiled = compiler.compileForPolicyApplication(sql);
        var plan = Plan.parseFrom(compiler.convertValidateAndSerialize(compiled.plan(),
                compiled.preOptimizationColumnNames()));
        var root = plan.getRelations(0).getRoot().getInput();
        assertThat(root.hasProject()).isTrue();
        assertThat(root.getProject().getExpressionsCount()).isEqualTo(1);
        return root.getProject().getExpressions(0);
    }
}
