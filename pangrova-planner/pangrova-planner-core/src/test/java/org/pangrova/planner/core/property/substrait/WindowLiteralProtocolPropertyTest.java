package org.pangrova.planner.core.property.substrait;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.pangrova.planner.core.analysis.PangrovaPlanner;
import org.pangrova.planner.core.substrait.SubstraitConverter;
import org.pangrova.planner.core.substrait.SubstraitPlanSerializer;

import static org.assertj.core.api.Assertions.assertThat;

class WindowLiteralProtocolPropertyTest {

    @Property(tries = 50, seed = "711940684091")
    void publicSqlWindowArgumentsKeepExactValuesInTheCanonicalProtocol(
            @ForAll("functions") String function, @ForAll("offsets") long offset,
            @ForAll int defaultValue) throws com.google.protobuf.InvalidProtocolBufferException {
        String sql = "SELECT " + function + "(ID, " + offset + ", " + defaultValue
                + ") OVER (ORDER BY ID) FROM T1";

        var invocation = window(compile(sql));

        assertThat(invocation.getArguments(1).getValue().getLiteral().getLiteralTypeCase())
                .isEqualTo(io.substrait.proto.Expression.Literal.LiteralTypeCase.I64);
        assertThat(invocation.getArguments(1).getValue().getLiteral().getI64()).isEqualTo(offset);
        assertThat(invocation.getArguments(2).getValue().getLiteral().getLiteralTypeCase())
                .isEqualTo(io.substrait.proto.Expression.Literal.LiteralTypeCase.I32);
        assertThat(invocation.getArguments(2).getValue().getLiteral().getI32()).isEqualTo(defaultValue);
    }

    @Property(tries = 30, seed = "526785459926")
    void publicSqlFrameAndNullOrderingSurviveSerialization(
            @ForAll boolean rows, @ForAll boolean descending, @ForAll boolean nullsFirst)
            throws com.google.protobuf.InvalidProtocolBufferException {
        String sql = "SELECT SUM(ID) OVER (ORDER BY ID " + (descending ? "DESC" : "ASC")
                + " NULLS " + (nullsFirst ? "FIRST" : "LAST") + " " + (rows ? "ROWS" : "RANGE")
                + " BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM T1";

        var invocation = window(compile(sql));

        assertThat(invocation.getBoundsType()).isEqualTo(rows
                ? io.substrait.proto.Expression.WindowFunction.BoundsType.BOUNDS_TYPE_ROWS
                : io.substrait.proto.Expression.WindowFunction.BoundsType.BOUNDS_TYPE_RANGE);
        assertThat(invocation.getSorts(0).getDirection().name())
                .isEqualTo("SORT_DIRECTION_" + (descending ? "DESC" : "ASC")
                        + "_NULLS_" + (nullsFirst ? "FIRST" : "LAST"));
    }

    @Provide
    Arbitrary<String> functions() {
        return Arbitraries.of("LAG", "LEAD");
    }

    @Provide
    Arbitrary<Long> offsets() {
        return Arbitraries.longs().between(0, Long.MAX_VALUE);
    }

    private io.substrait.proto.Rel compile(String sql)
            throws com.google.protobuf.InvalidProtocolBufferException {
        CalciteSchema schema = CalciteSchema.createRootSchema(false);
        schema.add("T1", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory factory) {
                return factory.builder().add("ID", SqlTypeName.INTEGER).build();
            }
        });
        var relationalPlan = new PangrovaPlanner(schema).plan(sql).plan();
        var plan = new SubstraitConverter().convert(relationalPlan);
        return io.substrait.proto.Plan.parseFrom(new SubstraitPlanSerializer().serialize(plan))
                .getRelations(0).getRoot().getInput();
    }

    private io.substrait.proto.Expression.WindowFunction window(io.substrait.proto.Rel rel) {
        assertThat(rel.hasProject()).as("Window SQL must retain its project relation").isTrue();
        for (var expression : rel.getProject().getExpressionsList()) {
            if (expression.hasWindowFunction()) {
                return expression.getWindowFunction();
            }
            var nested = nestedWindow(expression);
            if (nested != null) {
                return nested;
            }
        }
        return window(rel.getProject().getInput());
    }

    private io.substrait.proto.Expression.WindowFunction nestedWindow(io.substrait.proto.Expression expression) {
        if (expression.hasScalarFunction()) {
            for (var argument : expression.getScalarFunction().getArgumentsList()) {
                if (argument.hasValue()) {
                    var nested = nestedWindow(argument.getValue());
                    if (nested != null) return nested;
                }
            }
        }
        return expression.hasWindowFunction() ? expression.getWindowFunction() : null;
    }
}
