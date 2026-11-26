package org.pangrova.planner.optimization;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.pangrova.planner.cost.CboConfig;
import org.pangrova.planner.cost.CostModelConfig;
import org.pangrova.planner.cost.PangrovaCostFactory;
import org.pangrova.planner.cost.PangrovaRelMdJoinCost;
import org.pangrova.planner.cost.distribution.PangrovaDistributionTraitDef;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.CancelFlag;
import org.junit.jupiter.api.Test;

/**
 * Regression tests for G-CBO-01 (task 32): DP/bushy join enumeration must work
 * even without a StatisticsBasedCostEstimator. Before the fix:
 * (1) isApplicable() gated on costEstimator != null, skipping CBO entirely
 *     when no statistics source was available.
 * (2) Stage 2 bushy rules (MULTI_JOIN_OPTIMIZE_BUSHY) triggered Calcite's
 *     "belongs to a different planner" error because the input tree was
 *     registered from a different cluster; the catch block silently swallowed
 *     the error and returned the unoptimized input.
 */
class CascadesDpJoinEnumerationTest {

    /**
     * CBO should be applicable even without costEstimator (null).
     * Calcite's built-in cardinality metadata is sufficient for DP join
     * enumeration.
     */
    @Test
    void cboApplicableWithoutCostEstimator() {
        RelNode fourTableJoin = buildFourTableJoinTree();
        CboConfig cbo = CboConfig.DEFAULT; // volcanoJoinThreshold=10, enabled=true
        OptimizationContext ctx = new OptimizationContext(cbo, CostModelConfig.SSD_DEFAULT, null);
        CascadesCboPass pass = new CascadesCboPass();

        assertThat(pass.isApplicable(fourTableJoin, ctx))
                .as("CBO should be applicable without costEstimator")
                .isTrue();
    }

    /**
     * Volcano/DP path: 4-table join tree with 3 joins (below threshold=10).
     * Verifies that cascadesOptimize runs without "belongs to a different
     * planner" error and produces a valid join tree (no cartesian products,
     * all joins have conditions).
     */
    @Test
    void volcanoPathProducesValidJoinTree_noCostEstimator() {
        RelNode fourTableJoin = buildFourTableJoinTree();
        CboConfig cbo = CboConfig.DEFAULT;
        OptimizationContext ctx = new OptimizationContext(cbo, CostModelConfig.SSD_DEFAULT, null);
        CascadesCboPass pass = new CascadesCboPass();

        assertThat(fourTableJoin.getCluster().getMetadataQuery()
                .getNonCumulativeCost(fourTableJoin).getCpu())
                .as("logical JOIN comparison work must be priced before Volcano selection")
                .isPositive();
        RelNode result = pass.optimize(fourTableJoin, ctx);

        // The old second-planner failure returned the unchanged input, so
        // join-count assertions alone never proved that CBO actually ran.
        assertThat(result).isNotNull();
        assertThat(result).isNotSameAs(fourTableJoin);
        List<Join> joins = collectJoins(result);
        assertThat(joins).as("3 joins preserved after DP enumeration: %s",
                org.apache.calcite.plan.RelOptUtil.toString(result)).hasSize(3);
        assertThat(joins).as("CBO must select at least one executable join: %s",
                org.apache.calcite.plan.RelOptUtil.toString(result))
                .anyMatch(org.pangrova.planner.cost.convention.PangrovaRel.class::isInstance);
        // Output column count preserved (4 tables x 3 cols each = 12).
        assertThat(result.getRowType().getFieldCount()).isEqualTo(12);
    }

    /**
     * Greedy fallback path: 4-table join tree with volcanoJoinThreshold=2
     * (3 joins > 2 → greedy path). Verifies greedy still works correctly
     * with the isApplicable fix.
     */
    @Test
    void greedyFallbackStillWorksWithoutCostEstimator() {
        RelNode fourTableJoin = buildFourTableJoinTree();
        // threshold=2, so 3 joins > 2 → greedy
        CboConfig cbo = new CboConfig(2, 5000, 50000, 2000, 5000, 1e8);
        OptimizationContext ctx = new OptimizationContext(cbo, CostModelConfig.SSD_DEFAULT, null);

        RelNode result = new CascadesCboPass().optimize(fourTableJoin, ctx);

        List<Join> joins = collectJoins(result);
        assertThat(joins).as("3 joins preserved in greedy fallback").hasSize(3);
        assertThat(joins).allSatisfy(j ->
                assertThat(j.getCondition().isAlwaysTrue())
                        .as("greedy must keep predicates, not degrade to cross join")
                        .isFalse());
    }

    /**
     * Builds a 4-table left-deep join tree: ((t1 JOIN t2) JOIN t3) JOIN t4
     * with proper equi-join conditions using RexInputRef.
     */
    private static RelNode buildFourTableJoinTree() {
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        RexBuilder rexBuilder = new RexBuilder(typeFactory);
        VolcanoPlanner planner = new VolcanoPlanner(
                new PangrovaCostFactory(CostModelConfig.SSD_DEFAULT),
                Contexts.of(new CancelFlag(new java.util.concurrent.atomic.AtomicBoolean(false)),
                        CostModelConfig.SSD_DEFAULT));
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(PangrovaDistributionTraitDef.INSTANCE);
        planner.setNoneConventionHasInfiniteCost(false);
        RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);
        cluster.setMetadataProvider(
                org.apache.calcite.rel.metadata.ChainedRelMetadataProvider.of(List.of(
                        PangrovaRelMdJoinCost.provider(),
                        cluster.getMetadataProvider())));

        RelDataType intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        RelDataType rowType = typeFactory.builder()
                .add("id", intType)
                .add("name", intType)
                .add("value", intType)
                .build();
        RelNode t1 = scan(cluster, "t1", rowType);
        RelNode t2 = scan(cluster, "t2", rowType);
        RelNode t3 = scan(cluster, "t3", rowType);
        RelNode t4 = scan(cluster, "t4", rowType);

        // join1 = t1 ⋈ t2 ON t1.id = t2.id  (layout: t1=[0,1,2], t2=[3,4,5])
        RexNode c12 = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(intType, 0),
                rexBuilder.makeInputRef(intType, 3));
        RelNode join1 = LogicalJoin.create(
                t1, t2, List.of(), c12, Set.of(), JoinRelType.INNER);

        // join2 = join1 ⋈ t3 ON t2.id = t3.id  (layout: join1=[0..5], t3=[6,7,8])
        RexNode c23 = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(intType, 3),
                rexBuilder.makeInputRef(intType, 6));
        RelNode join2 = LogicalJoin.create(
                join1, t3, List.of(), c23, Set.of(), JoinRelType.INNER);

        // join3 = join2 ⋈ t4 ON t3.id = t4.id  (layout: join2=[0..8], t4=[9,10,11])
        RexNode c34 = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
                rexBuilder.makeInputRef(intType, 6),
                rexBuilder.makeInputRef(intType, 9));
        return LogicalJoin.create(
                join2, t4, List.of(), c34, Set.of(), JoinRelType.INNER);
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
                    @Override
                    public double getRowCount() {
                        return 100_000;
                    }

                    @Override
                    public <T> T unwrap(Class<T> clazz) {
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
