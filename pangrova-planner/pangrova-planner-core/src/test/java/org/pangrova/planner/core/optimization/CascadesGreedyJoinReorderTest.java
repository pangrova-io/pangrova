package org.pangrova.planner.core.optimization;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;

import org.pangrova.planner.core.cost.CboConfig;
import org.pangrova.planner.core.cost.CostModelConfig;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.junit.jupiter.api.Test;

/**
 * Regression for F-PLANNER-01: the greedy join reorder must match join predicates by
 * {@link org.apache.calcite.rex.RexInputRef} index (not field name) and remap them into the rebuilt
 * row layout — so it never emits cartesian products or drops predicates. Pre-fix, the name-based
 * matcher matched nothing and every reordered join got a {@code TRUE} condition (cross product).
 */
class CascadesGreedyJoinReorderTest {

    @Test
    void greedyReorderPreservesJoinPredicates_noCartesian() {
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        HepPlanner planner = new HepPlanner(new HepProgramBuilder().build());
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

        RelDataType intType = typeFactory.createSqlType(org.apache.calcite.sql.type.SqlTypeName.INTEGER);
        RelDataType rowType = typeFactory.builder()
                .add("id", intType)
                .add("name", intType)
                .add("value", intType)
                .build();
        RelNode t1 = scan(cluster, "t1", rowType);
        RelNode t2 = scan(cluster, "t2", rowType);
        RelNode t3 = scan(cluster, "t3", rowType);

        // join1 = t1 ⋈ t2 ON t1.id = t2.id  (local layout: t1=[0,1,2], t2=[3,4,5])
        RexNode c12 = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(intType, 0), rexBuilder.makeInputRef(intType, 3));
        RelNode join1 = LogicalJoin.create(t1, t2, List.of(), c12, java.util.Set.of(), JoinRelType.INNER);
        // top = join1 ⋈ t3 ON t2.id = t3.id  (local layout: join1=[0..5], t3=[6,7,8])
        RexNode c23 = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(intType, 3), rexBuilder.makeInputRef(intType, 6));
        RelNode top = LogicalJoin.create(join1, t3, List.of(), c23, java.util.Set.of(), JoinRelType.INNER);

        // volcanoJoinThreshold=1 with 2 joins → greedy reorder path.
        CboConfig cbo = new CboConfig(1, 5000, 50000, 2000, 5000, 1e8);
        OptimizationContext ctx = new OptimizationContext(cbo, CostModelConfig.SSD_DEFAULT, null);

        RelNode result = new CascadesCboPass().optimize(top, ctx);

        List<Join> joins = collectJoins(result);
        assertThat(joins).as("same number of joins after reorder").hasSize(2);
        // The core guarantee: NO join is a cartesian product — every join carries a real predicate.
        assertThat(joins).allSatisfy(j ->
                assertThat(j.getCondition().isAlwaysTrue())
                        .as("reordered join must keep its predicate, not become a cross join")
                        .isFalse());
        // Drop-in replacement: output column count preserved.
        assertThat(result.getRowType().getFieldCount()).isEqualTo(9);
    }

    private static List<Join> collectJoins(RelNode node) {
        List<Join> joins = new ArrayList<>();
        if (node instanceof Join j) {
            joins.add(j);
        }
        for (RelNode in : node.getInputs()) {
            joins.addAll(collectJoins(in));
        }
        return joins;
    }

    private static RelNode scan(RelOptCluster cluster, String name, RelDataType rowType) {
        RelOptSchema schema = new RelOptSchema() {
            @Override
            public RelOptTable getTableForMember(List<String> names) {
                return new org.apache.calcite.plan.RelOptAbstractTable(
                        this, String.join(".", names), rowType) {
                    @Override public double getRowCount() {
                        return 100_000;
                    }
                    @Override public <T> T unwrap(Class<T> clazz) {
                        return null;
                    }
                };
            }
            @Override
            public RelDataTypeFactory getTypeFactory() {
                return new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
            }
            @Override
            public void registerRules(RelOptPlanner planner) {
            }
        };
        return LogicalTableScan.create(cluster, schema.getTableForMember(List.of(name)), List.of());
    }
}
