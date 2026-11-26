package org.pangrova.planner.analysis;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class JoinLiteralPredicatePushdownTest {
    @Test
    void boundedLiteralBuildKeysFilterOnlyTheMatchingProbeScan() {
        var planner = planner();
        String sql = "SELECT o.ID FROM ORDERS o JOIN (VALUES (1), (3), (3)) v(k) ON o.ID=v.k";
        var plan = planner.plan(sql).plan();
        var tree = org.apache.calcite.plan.RelOptUtil.toString(plan);
        assertThat(tree).contains("SEARCH(", "1", "3");
        assertThat(tree.indexOf("SEARCH(")).isLessThan(tree.indexOf("ORDERS"));
        assertThat(tree).contains("LogicalValues");
    }

    @Test
    void nullableBigintProbeAndCastValuesProduceABoundedPlan() {
        var planner = planner();
        var plan = planner.plan("SELECT o.BIG_ID FROM ORDERS o JOIN "
                + "(VALUES (CAST(1 AS BIGINT)), (CAST(3 AS BIGINT)), (CAST(3 AS BIGINT))) v(k) "
                + "ON o.BIG_ID=v.k ORDER BY o.BIG_ID").plan();
        assertThat(plan.explain().lines().count()).isLessThan(20);
    }

    private static PangrovaPlanner planner() {
        CalciteSchema root = CalciteSchema.createRootSchema(false);
        root.add("ORDERS", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory factory) {
                return factory.builder()
                        .add("ID", factory.createJavaType(int.class))
                        .add("BIG_ID", factory.createTypeWithNullability(factory.createSqlType(SqlTypeName.BIGINT), true))
                        .build();
            }
        });
        return new PangrovaPlanner(root, List.of(""));
    }
}
