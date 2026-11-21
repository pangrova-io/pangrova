package org.pangrova.planner.type;

import io.substrait.proto.Expression;
import io.substrait.proto.Plan;
import io.substrait.proto.Rel;
import io.substrait.proto.Type;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.pangrova.planner.api.exception.PlannerException;
import org.pangrova.planner.analysis.SqlCompiler;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Public SQL and canonical protocol checks, not a Native result-value oracle. */
class DecimalSqlProtocolTest {

    @ParameterizedTest
    @ValueSource(strings = {
            "12345678901234567890.123456789012345678",
            "-12345678901234567890.123456789012345678",
            "99999999999999999999.999999999999999999",
            "-99999999999999999999.999999999999999999"
    })
    void decimal38RetainsItsExactValueInCanonicalSqlCompilation(String value) throws Exception {
        var compiler = new SqlCompiler();
        String sql = "SELECT CAST(" + value + " AS DECIMAL(38,18))";
        var rowType = compiler.compileToResult(sql).plan().getRowType().getFieldList().get(0).getType();
        assertThat(rowType.getPrecision()).isEqualTo(38);
        assertThat(rowType.getScale()).isEqualTo(18);
        var read = read(input(Plan.parseFrom(compiler.compile(sql))));
        assertDecimal(read.getBaseSchema().getStruct().getTypes(0), 38, 18, false);
        var literal = literal(read, 0).getDecimal();
        assertThat(literal.getPrecision()).isEqualTo(38);
        assertThat(literal.getScale()).isEqualTo(18);
        assertThat(literal.getValue().size()).isEqualTo(16);
        assertThat(unscaled(literal.getValue().toByteArray())).isEqualTo(new BigDecimal(value).unscaledValue());
    }

    @ParameterizedTest
    @CsvSource({"DECIMAL,38,0", "DECIMAL(38),38,0", "'NUMERIC(38,18)',38,18",
            "'DECIMAL(38,38)',38,38", "'DECIMAL(1,0)',1,0"})
    void nullableAndDefaultDeclarationsPreserveTheCanonicalType(String declaration, int precision, int scale)
            throws Exception {
        var plan = Plan.parseFrom(new SqlCompiler().compile("SELECT CAST(NULL AS " + declaration + ")"));
        var source = read(input(plan));
        assertDecimal(source.getBaseSchema().getStruct().getTypes(0), precision, scale, true);
        var literal = literal(source, 0);
        assertThat(literal.getLiteralTypeCase()).isEqualTo(Expression.Literal.LiteralTypeCase.NULL);
        assertDecimal(literal.getNull(), precision, scale, true);
    }

    @ParameterizedTest
    @ValueSource(strings = {"DECIMAL(39,18)", "NUMERIC(39,0)", "DECIMAL(0,0)",
            "DECIMAL(38,39)", "DECIMAL(10,11)", "DECIMAL(2147483647,0)"})
    void publicSqlRejectsUnsupportedDeclarationInsteadOfProducingAClampedPlan(String declaration) {
        assertThatThrownBy(() -> new SqlCompiler().compile("SELECT CAST(NULL AS " + declaration + ")"))
                .isInstanceOf(PlannerException.class).hasMessageContaining("DECIMAL");
    }

    @ParameterizedTest
    @CsvSource({
            "+,10,2,20,3,21,3", "-,10,2,20,3,21,3", "*,10,2,20,3,30,5",
            "/,10,2,20,3,14,3", "%,10,2,20,3,11,3", "%,10,0,20,0,10,0",
            "*,38,18,38,18,38,36", "/,38,18,38,18,38,18", "+,38,38,38,38,38,38"
    })
    void nonFoldedSqlKeepsOperandAndResultSchemasInTheCanonicalProtocol(String operator,
            int leftPrecision, int leftScale, int rightPrecision, int rightScale,
            int resultPrecision, int resultScale) throws Exception {
        String sql = arithmeticSql(operator, leftPrecision, leftScale, rightPrecision, rightScale);
        var compiler = new SqlCompiler();
        var compiled = compiler.compileForPolicyApplication(sql);
        assertThat(compiled.plan()).isInstanceOf(Project.class);
        var project = (Project) compiled.plan();
        assertThat(project.getProjects().get(0)).isInstanceOf(RexCall.class);
        assertThat(((RexCall) project.getProjects().get(0)).getOperands()).allMatch(RexInputRef.class::isInstance);
        var resultType = project.getRowType().getFieldList().get(0).getType();
        assertThat(resultType.getPrecision()).isEqualTo(resultPrecision);
        assertThat(resultType.getScale()).isEqualTo(resultScale);
        var root = input(Plan.parseFrom(compiler.convertValidateAndSerialize(
                compiled.plan(), compiled.preOptimizationColumnNames())));
        assertThat(root.hasProject()).isTrue();
        var invocation = root.getProject().getExpressions(0).getScalarFunction();
        assertDecimal(invocation.getOutputType(), resultPrecision, resultScale, false);
        assertThat(invocation.getArgumentsCount()).isEqualTo(2);
        for (int argument = 0; argument < 2; argument++) {
            assertThat(invocation.getArguments(argument).getValue().hasSelection()).isTrue();
            assertThat(invocation.getArguments(argument).getValue().getSelection()
                    .getDirectReference().getStructField().getField()).isEqualTo(argument);
        }
        var source = read(root.getProject().getInput());
        assertDecimal(source.getBaseSchema().getStruct().getTypes(0), leftPrecision, leftScale, false);
        assertDecimal(source.getBaseSchema().getStruct().getTypes(1), rightPrecision, rightScale, false);
        assertThat(literal(source, 0).getDecimal().getPrecision()).isEqualTo(leftPrecision);
        assertThat(literal(source, 0).getDecimal().getScale()).isEqualTo(leftScale);
        assertThat(literal(source, 1).getDecimal().getPrecision()).isEqualTo(rightPrecision);
        assertThat(literal(source, 1).getDecimal().getScale()).isEqualTo(rightScale);
    }

    @ParameterizedTest
    @CsvSource({"*,38,20,38,19,scale exceeds 38", "/,38,0,38,20,rescale exceeds 38"})
    void publicSqlRejectsUnrepresentableArithmetic(String operator, int p1, int s1, int p2, int s2,
            String reason) {
        assertThatThrownBy(() -> new SqlCompiler().compileForPolicyApplication(arithmeticSql(operator, p1, s1, p2, s2)))
                .isInstanceOf(PlannerException.class).hasMessageContaining(reason);
    }

    private static String arithmeticSql(String operator, int p1, int s1, int p2, int s2) {
        // Fractional-only types use zero so the input values remain representable at every boundary.
        return "SELECT x " + operator + " y FROM (SELECT CAST(0 AS DECIMAL(" + p1 + "," + s1
                + ")) AS x, CAST(0 AS DECIMAL(" + p2 + "," + s2 + ")) AS y) t";
    }

    private static void assertDecimal(Type type, int precision, int scale, boolean nullable) {
        assertThat(type.hasDecimal()).isTrue();
        assertThat(type.getDecimal().getPrecision()).isEqualTo(precision);
        assertThat(type.getDecimal().getScale()).isEqualTo(scale);
        assertThat(type.getDecimal().getNullability()).isEqualTo(nullable
                ? Type.Nullability.NULLABILITY_NULLABLE : Type.Nullability.NULLABILITY_REQUIRED);
    }

    private static Rel input(Plan plan) {
        return plan.getRelations(0).getRoot().getInput();
    }

    private static Expression.Literal literal(io.substrait.proto.ReadRel source, int column) {
        var table = source.getVirtualTable();
        assertThat(table.getExpressionsCount()).isEqualTo(1);
        assertThat(table.getExpressions(0).getFieldsCount()).isEqualTo(source.getBaseSchema().getStruct().getTypesCount());
        var field = table.getExpressions(0).getFields(column);
        assertThat(field.hasLiteral()).isTrue();
        return field.getLiteral();
    }

    private static io.substrait.proto.ReadRel read(Rel relation) {
        while (relation.hasProject()) relation = relation.getProject().getInput();
        assertThat(relation.hasRead()).isTrue();
        return relation.getRead();
    }

    private static BigInteger unscaled(byte[] littleEndian) {
        for (int i = 0; i < littleEndian.length / 2; i++) {
            byte next = littleEndian[i];
            littleEndian[i] = littleEndian[littleEndian.length - 1 - i];
            littleEndian[littleEndian.length - 1 - i] = next;
        }
        return new BigInteger(littleEndian);
    }
}
