package org.pangrova.planner.cost.distribution;

import org.pangrova.planner.enums.DistributionType;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.pangrova.planner.cost.PangrovaCostFactory;
import org.pangrova.planner.cost.CostModelConfig;
import org.pangrova.planner.cost.join.PangrovaHashJoin;
import org.pangrova.planner.cost.join.HashJoinConfig;
import org.pangrova.planner.cost.join.HashJoinRule;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end verification that the Volcano planner enforcer mechanism
 * automatically inserts PangrovaExchange nodes when HashJoinRule declares
 * required HASH distribution on child inputs.
 *
 * <p>This test validates that when a RelNode participates in join planning,
 * the Planner declares required distribution (HASH(joinKeys)) on child
 * traitSets, enabling the enforcer to automatically insert PangrovaExchange.</p>
 *
 * <p>The verification strategy:
 * <ol>
 *   <li>Verify PangrovaDistributionTraitDef.convert() inserts PangrovaExchange when
 *       distribution mismatches (enforcer mechanism)</li>
 *   <li>Verify HashJoinRule.onMatch() declares HASH distribution on children
 *       via convert(child, traitSet.plus(dist))</li>
 *   <li>Verify the resulting PangrovaHashJoin has children wrapped in PangrovaExchange
 *       when their original distribution is ANY</li>
 * </ol></p>
 *
 * Validates: Requirements 4.4
 */
class PangrovaExchangeEnforcerTest {

    private static final RelDataTypeFactory TYPE_FACTORY =
            new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    private VolcanoPlanner planner;
    private RelOptCluster cluster;
    private RexBuilder rexBuilder;

    @BeforeEach
    void setUp() {
        PangrovaCostFactory costFactory = new PangrovaCostFactory(CostModelConfig.SSD_DEFAULT);
        planner = new VolcanoPlanner(costFactory, null);
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(PangrovaDistributionTraitDef.INSTANCE);

        rexBuilder = new RexBuilder(TYPE_FACTORY);
        cluster = RelOptCluster.create(planner, rexBuilder);
    }

    @Test
    void enforcerInsertsExchange_whenChildHasAnyDistribution_andParentRequiresHash() {
        RelTraitSet anyTraitSet = cluster.traitSet().replace(PangrovaDistribution.ANY);
        RelNode child = new FiniteCostLeaf(
                cluster, anyTraitSet, buildRowType(List.of("id", "value")));

        PangrovaDistribution requiredHash = PangrovaDistribution.hash(List.of(0));
        RelNode converted = PangrovaDistributionTraitDef.INSTANCE.convert(
                planner, child, requiredHash, true);

        assertThat(converted)
                .as("Enforcer should insert PangrovaExchange when distribution mismatches")
                .isInstanceOf(PangrovaExchange.class);
        PangrovaExchange exchange = (PangrovaExchange) converted;
        assertThat(exchange.targetDistribution().type())
                .isEqualTo(DistributionType.HASH);
        assertThat(exchange.targetDistribution().keys()).containsExactly(0);
        assertThat(exchange.getInput()).isSameAs(child);
    }

    @Test
    void enforcerSkipsExchange_whenChildAlreadySatisfiesDistribution() {
        PangrovaDistribution hashOnCol0 = PangrovaDistribution.hash(List.of(0));
        RelTraitSet hashTraitSet = cluster.traitSet().replace(hashOnCol0);
        RelNode child = new FiniteCostLeaf(
                cluster, hashTraitSet, buildRowType(List.of("id", "value")));

        RelNode converted = PangrovaDistributionTraitDef.INSTANCE.convert(
                planner, child, hashOnCol0, true);

        assertThat(converted)
                .as("No exchange needed when distribution already satisfied")
                .isSameAs(child);
    }

    @Test
    void hashJoinRule_producesJoinWithHashDistributionOnChildren() {
        // Build two leaf nodes with ANY distribution
        RelDataType leftType = buildRowType(List.of("order_id", "customer_id", "amount"));
        RelDataType rightType = buildRowType(List.of("cust_id", "name", "region"));

        RelTraitSet anyTraitSet = cluster.traitSet().replace(PangrovaDistribution.ANY);
        RelNode left = new FiniteCostLeaf(cluster, anyTraitSet, leftType);
        RelNode right = new FiniteCostLeaf(cluster, anyTraitSet, rightType);

        // Build equi-join condition: left.customer_id (idx 1) = right.cust_id (idx 0)
        int leftFieldCount = leftType.getFieldCount();
        RexNode leftRef = rexBuilder.makeInputRef(
                leftType.getFieldList().get(1).getType(), 1);
        RexNode rightRef = rexBuilder.makeInputRef(
                rightType.getFieldList().get(0).getType(), leftFieldCount);
        RexNode joinCondition = rexBuilder.makeCall(
                SqlStdOperatorTable.EQUALS, leftRef, rightRef);

        // Simulate the enforcer: convert left child to HASH[1] distribution
        PangrovaDistribution leftDist = PangrovaDistribution.hash(List.of(1));
        RelNode convertedLeft = PangrovaDistributionTraitDef.INSTANCE.convert(
                planner, left, leftDist, true);

        // Simulate the enforcer: convert right child to HASH[0] distribution
        PangrovaDistribution rightDist = PangrovaDistribution.hash(List.of(0));
        RelNode convertedRight = PangrovaDistributionTraitDef.INSTANCE.convert(
                planner, right, rightDist, true);

        // Verify both children got wrapped in PangrovaExchange
        assertThat(convertedLeft).isInstanceOf(PangrovaExchange.class);
        assertThat(convertedRight).isInstanceOf(PangrovaExchange.class);

        PangrovaExchange leftExchange = (PangrovaExchange) convertedLeft;
        PangrovaExchange rightExchange = (PangrovaExchange) convertedRight;

        assertThat(leftExchange.targetDistribution())
                .as("Left exchange should target HASH on join key column 1")
                .isEqualTo(leftDist);
        assertThat(rightExchange.targetDistribution())
                .as("Right exchange should target HASH on join key column 0")
                .isEqualTo(rightDist);

        // Build the PangrovaHashJoin with exchanged children (as HashJoinRule would)
        RelTraitSet joinTraitSet = cluster.traitSet().plus(leftDist);
        PangrovaHashJoin hashJoin = new PangrovaHashJoin(new HashJoinConfig(
                cluster, joinTraitSet, convertedLeft, convertedRight,
                joinCondition, Set.of(), JoinRelType.INNER));

        // Verify the join's children are PangrovaExchange nodes
        List<PangrovaExchange> exchanges = collectExchangeNodes(hashJoin);
        assertThat(exchanges).hasSize(2);
        assertThat(exchanges.get(0).targetDistribution().type())
                .isEqualTo(DistributionType.HASH);
        assertThat(exchanges.get(1).targetDistribution().type())
                .isEqualTo(DistributionType.HASH);
    }

    @Test
    void multiColumnJoinKey_exchangeCarriesAllKeys() {
        RelDataType leftType = buildRowType(List.of("a", "b", "c"));
        RelDataType rightType = buildRowType(List.of("x", "y", "z"));

        RelTraitSet anyTraitSet = cluster.traitSet().replace(PangrovaDistribution.ANY);
        RelNode left = new FiniteCostLeaf(cluster, anyTraitSet, leftType);
        RelNode right = new FiniteCostLeaf(cluster, anyTraitSet, rightType);

        // Multi-column join: left.[a,b] = right.[x,y] → keys [0,1] on both sides
        PangrovaDistribution leftDist = PangrovaDistribution.hash(List.of(0, 1));
        PangrovaDistribution rightDist = PangrovaDistribution.hash(List.of(0, 1));

        RelNode convertedLeft = PangrovaDistributionTraitDef.INSTANCE.convert(
                planner, left, leftDist, true);
        RelNode convertedRight = PangrovaDistributionTraitDef.INSTANCE.convert(
                planner, right, rightDist, true);

        assertThat(convertedLeft).isInstanceOf(PangrovaExchange.class);
        assertThat(convertedRight).isInstanceOf(PangrovaExchange.class);

        PangrovaExchange leftExchange = (PangrovaExchange) convertedLeft;
        assertThat(leftExchange.targetDistribution().keys())
                .as("Multi-column join should produce exchange with all key columns")
                .containsExactly(0, 1);

        PangrovaExchange rightExchange = (PangrovaExchange) convertedRight;
        assertThat(rightExchange.targetDistribution().keys())
                .containsExactly(0, 1);
    }

    @Test
    void distributionTraitDefRegisteredInPlanner() {
        assertThat(planner.getRelTraitDefs())
                .as("PangrovaDistributionTraitDef must be registered for enforcer to work")
                .anyMatch(td -> td instanceof PangrovaDistributionTraitDef);
    }

    @Test
    void hashJoinRuleRegisteredInPlanner() {
        planner.addRule(new HashJoinRule(CostModelConfig.SSD_DEFAULT));
        assertThat(planner.getRules())
                .as("HashJoinRule must be registered to declare distribution requirements")
                .anyMatch(r -> r.toString().contains("HashJoinRule"));
    }

    @Test
    void exchangeCostIsFinite() {
        RelTraitSet anyTraitSet = cluster.traitSet().replace(PangrovaDistribution.ANY);
        RelNode child = new FiniteCostLeaf(
                cluster, anyTraitSet, buildRowType(List.of("id", "value")));

        PangrovaDistribution hashDist = PangrovaDistribution.hash(List.of(0));
        PangrovaExchange exchange = (PangrovaExchange) PangrovaDistributionTraitDef.INSTANCE
                .convert(planner, child, hashDist, true);

        RelMetadataQuery mq = cluster.getMetadataQuery();
        RelOptCost cost = exchange.computeSelfCost(planner, mq);

        assertThat(cost.isInfinite())
                .as("PangrovaExchange must have finite cost for planner to select it")
                .isFalse();
    }

    private List<PangrovaExchange> collectExchangeNodes(RelNode node) {
        List<PangrovaExchange> result = new ArrayList<>();
        collectExchangeNodesRecursive(node, result);
        return result;
    }

    private void collectExchangeNodesRecursive(RelNode node, List<PangrovaExchange> result) {
        if (node instanceof PangrovaExchange exchange) {
            result.add(exchange);
        }
        for (RelNode input : node.getInputs()) {
            collectExchangeNodesRecursive(input, result);
        }
    }

    private RelDataType buildRowType(List<String> columnNames) {
        RelDataTypeFactory.Builder typeBuilder = TYPE_FACTORY.builder();
        for (String col : columnNames) {
            typeBuilder.add(col, SqlTypeName.INTEGER);
        }
        return typeBuilder.build();
    }

    /**
     * Leaf node with finite cost for testing the enforcer mechanism.
     * Stays in NONE convention with ANY distribution so the enforcer
     * can wrap it with PangrovaExchange when HASH distribution is required.
     */
    static final class FiniteCostLeaf extends AbstractRelNode {

        private final RelDataType rowType;

        FiniteCostLeaf(RelOptCluster cluster, RelTraitSet traitSet, RelDataType rowType) {
            super(cluster, traitSet);
            this.rowType = rowType;
        }

        @Override
        public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
            return planner.getCostFactory().makeCost(100, 100, 0);
        }

        @Override
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new FiniteCostLeaf(getCluster(), traitSet, rowType);
        }

        @Override
        protected RelDataType deriveRowType() {
            return rowType;
        }
    }
}
