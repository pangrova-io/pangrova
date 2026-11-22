package org.pangrova.planner.runtimefilter;

import org.pangrova.planner.analysis.PangrovaPlanner;
import org.pangrova.planner.analysis.PlannerContext;
import org.pangrova.types.enums.PangrovaType;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.math.BigDecimal;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests verifying runtime filter plan generation for join queries when DPP is enabled.
 *
 * <p><b>Validates: Requirements 1.40, 2.40</b></p>
 */
class JoinRuntimeFilterPlannerTest {

    @Test
    void joinQuery_dppEnabled_generatesRuntimeFilterDescriptors() {
        PangrovaPlanner planner = buildPlannerWithTables(DynamicPartitionPruningConfig.ENABLED);

        RelNode plan = planner.plan(
                "SELECT o.ID, c.NAME FROM ORDERS o "
                        + "JOIN CUSTOMERS c ON o.CUSTOMER_ID = c.ID").plan();

        List<RuntimeFilterDescriptor> descriptors = planner.analyzeJoinsForPartitionPruning(plan);

        assertThat(descriptors).isNotEmpty();
        RuntimeFilterDescriptor descriptor = descriptors.get(0);
        assertThat(descriptor.filterId()).startsWith("dpp-");
        assertThat(descriptor.joinType()).isEqualTo("INNER");
    }

    @Test
    void boundedLiteralBuildKeysFilterOnlyTheMatchingProbeScan() {
        var planner = buildPlannerWithTables(DynamicPartitionPruningConfig.ENABLED);
        String sql = "SELECT o.ID FROM ORDERS o JOIN (VALUES (1), (3), (3)) v(k) ON o.ID=v.k";
        var plan = planner.plan(sql).plan();
        var tree = org.apache.calcite.plan.RelOptUtil.toString(plan);
        assertThat(tree).contains("SEARCH(", "1", "3");
        assertThat(tree.indexOf("SEARCH(")).isLessThan(tree.indexOf("ORDERS"));
        assertThat(tree).contains("LogicalValues");
    }

    @Test
    void nullableBigintProbeAndCastValuesProduceABoundedPlan() {
        var planner = buildPlannerWithTables(DynamicPartitionPruningConfig.ENABLED);
        var plan = planner.plan("SELECT o.BIG_ID FROM ORDERS o JOIN "
                + "(VALUES (CAST(1 AS BIGINT)), (CAST(3 AS BIGINT)), (CAST(3 AS BIGINT))) v(k) "
                + "ON o.BIG_ID=v.k ORDER BY o.BIG_ID").plan();
        assertThat(plan.explain().lines().count()).isLessThan(20);
    }

    @Test
    void nonJoinQuery_dppEnabled_noDescriptors() {
        PangrovaPlanner planner = buildPlannerWithTables(DynamicPartitionPruningConfig.ENABLED);

        RelNode plan = planner.plan("SELECT ID, AMOUNT FROM ORDERS").plan();

        List<RuntimeFilterDescriptor> descriptors = planner.analyzeJoinsForPartitionPruning(plan);

        assertThat(descriptors).isEmpty();
    }

    @Test
    void constantQuery_dppEnabled_noDescriptors() {
        PangrovaPlanner planner = buildPlannerWithTables(DynamicPartitionPruningConfig.ENABLED);

        RelNode plan = planner.plan("SELECT 1 + 2").plan();

        List<RuntimeFilterDescriptor> descriptors = planner.analyzeJoinsForPartitionPruning(plan);

        assertThat(descriptors).isEmpty();
    }

    @Test
    void dynamicPartitionPruningConfig_defaultsToDisabled() {
        assertThat(DynamicPartitionPruningConfig.DISABLED.enabled()).isFalse();
        assertThat(new DynamicPartitionPruningConfig(false).enabled()).isFalse();
    }

    @Test
    void dynamicPartitionPruningConfig_enabledFlag() {
        assertThat(DynamicPartitionPruningConfig.ENABLED.enabled()).isTrue();
        assertThat(new DynamicPartitionPruningConfig(true).enabled()).isTrue();
    }

    @Test
    void descriptorValidation_rejectsNullFields() {
        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> new RuntimeFilterDescriptor(null, "col", "col", "INNER", PangrovaType.BIGINT))
                .isInstanceOf(IllegalArgumentException.class);

        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> new RuntimeFilterDescriptor("id", null, "col", "INNER", PangrovaType.BIGINT))
                .isInstanceOf(IllegalArgumentException.class);

        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> new RuntimeFilterDescriptor("id", "col", null, "INNER", PangrovaType.BIGINT))
                .isInstanceOf(IllegalArgumentException.class);

        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> new RuntimeFilterDescriptor("id", "col", "col", null, PangrovaType.BIGINT))
                .isInstanceOf(IllegalArgumentException.class);

        org.assertj.core.api.Assertions.assertThatThrownBy(
                () -> new RuntimeFilterDescriptor("id", "col", "col", "INNER", null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void innerJoinWithSmallerLeftInputKeepsRightAsBuild() {
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        VolcanoPlanner volcanoPlanner = new VolcanoPlanner();
        volcanoPlanner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        RelOptCluster cluster = RelOptCluster.create(volcanoPlanner, rexBuilder);

        RelNode left = buildValuesNode(cluster, typeFactory, rexBuilder, 100);
        RelNode right = buildValuesNode(cluster, typeFactory, rexBuilder, 1000);

        int leftFieldCount = left.getRowType().getFieldCount();
        RexNode leftRef = rexBuilder.makeInputRef(
                left.getRowType().getFieldList().get(1).getType(), 1);
        RexNode rightRef = rexBuilder.makeInputRef(
                right.getRowType().getFieldList().get(0).getType(), leftFieldCount);
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, leftRef, rightRef);

        LogicalJoin join = LogicalJoin.create(left, right, ImmutableList.of(),
                condition, ImmutableSet.of(), JoinRelType.INNER);

        JoinRuntimeFilterPlanner planner = new JoinRuntimeFilterPlanner();
        assertThat(planner.analyze(join)).singleElement().satisfies(descriptor -> {
            assertThat(descriptor.buildColumn()).isEqualTo("ID");
            assertThat(descriptor.probeColumn()).isEqualTo("VALUE");
        });
    }

    @Test
    void reversingEqualityOperandsKeepsRightAsBuild() {
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        VolcanoPlanner volcanoPlanner = new VolcanoPlanner();
        volcanoPlanner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        RelOptCluster cluster = RelOptCluster.create(volcanoPlanner, rexBuilder);

        // Left: 1000 rows (large side)
        RelNode left = buildValuesNode(cluster, typeFactory, rexBuilder, 1000);
        // Right: 100 rows (small side)
        RelNode right = buildValuesNode(cluster, typeFactory, rexBuilder, 100);

        int leftFieldCount = left.getRowType().getFieldCount();
        RexNode leftRef = rexBuilder.makeInputRef(
                left.getRowType().getFieldList().get(1).getType(), 1);
        RexNode rightRef = rexBuilder.makeInputRef(
                right.getRowType().getFieldList().get(0).getType(), leftFieldCount);
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, rightRef, leftRef);

        LogicalJoin join = LogicalJoin.create(left, right, ImmutableList.of(),
                condition, ImmutableSet.of(), JoinRelType.INNER);

        JoinRuntimeFilterPlanner planner = new JoinRuntimeFilterPlanner();
        assertThat(planner.analyze(join)).singleElement().satisfies(descriptor -> {
            assertThat(descriptor.buildColumn()).isEqualTo("ID");
            assertThat(descriptor.probeColumn()).isEqualTo("VALUE");
        });
    }

    @Test
    void leftJoinCannotPruneThePreservedProbeSide() {
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        VolcanoPlanner volcanoPlanner = new VolcanoPlanner();
        volcanoPlanner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        RelOptCluster cluster = RelOptCluster.create(volcanoPlanner, rexBuilder);

        // Left: 10 rows (smaller, but LEFT JOIN semantics require right=build)
        RelNode left = buildValuesNode(cluster, typeFactory, rexBuilder, 10);
        // Right: 1000 rows
        RelNode right = buildValuesNode(cluster, typeFactory, rexBuilder, 1000);

        int leftFieldCount = left.getRowType().getFieldCount();
        RexNode leftRef = rexBuilder.makeInputRef(
                left.getRowType().getFieldList().get(0).getType(), 0);
        RexNode rightRef = rexBuilder.makeInputRef(
                right.getRowType().getFieldList().get(0).getType(), leftFieldCount);
        RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, leftRef, rightRef);

        LogicalJoin join = LogicalJoin.create(left, right, ImmutableList.of(),
                condition, ImmutableSet.of(), JoinRelType.LEFT);

        JoinRuntimeFilterPlanner planner = new JoinRuntimeFilterPlanner();
        assertThat(planner.analyze(join)).isEmpty();
    }

    private static RelNode buildValuesNode(RelOptCluster cluster, RelDataTypeFactory typeFactory,
                                           RexBuilder rexBuilder, int rowCount) {
        RelDataType rowType = typeFactory.builder()
                .add("ID", SqlTypeName.INTEGER)
                .add("VALUE", SqlTypeName.INTEGER)
                .build();
        ImmutableList.Builder<ImmutableList<RexLiteral>> tuples = ImmutableList.builder();
        for (int i = 0; i < rowCount; i++) {
            tuples.add(ImmutableList.of(
                    rexBuilder.makeExactLiteral(BigDecimal.valueOf(i)),
                    rexBuilder.makeExactLiteral(BigDecimal.valueOf(i * 10))));
        }
        return LogicalValues.create(cluster, rowType, tuples.build());
    }

    // -- Helpers --

    private static PangrovaPlanner buildPlannerWithTables(DynamicPartitionPruningConfig dynamicPartitionPruningConfig) {
        CalciteSchema rootSchema = CalciteSchema.createRootSchema(false);

        rootSchema.add("ORDERS", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                return typeFactory.builder()
                        .add("ID", typeFactory.createJavaType(int.class))
                        .add("CUSTOMER_ID", typeFactory.createJavaType(int.class))
                        .add("BIG_ID", typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BIGINT), true))
                        .add("AMOUNT", typeFactory.createJavaType(double.class))
                        .build();
            }
        });

        rootSchema.add("CUSTOMERS", new AbstractTable() {
            @Override
            public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                return typeFactory.builder()
                        .add("ID", typeFactory.createJavaType(int.class))
                        .add("NAME", typeFactory.createJavaType(String.class))
                        .build();
            }
        });

        PlannerContext deps = new PlannerContext(null, null, dynamicPartitionPruningConfig, null, null, null);
        return new PangrovaPlanner(rootSchema, List.of(""), deps);
    }
}
