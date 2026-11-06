package org.pangrova.planner.core.cost.distribution;

import java.util.List;

import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that PangrovaDistributionTraitDef inserts PangrovaExchange nodes
 * when a child's distribution does not satisfy the parent's requirement.
 */
class PangrovaDistributionTraitDefTest {

    private VolcanoPlanner planner;
    private RelOptCluster cluster;

    @BeforeEach
    void setUp() {
        planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        planner.addRelTraitDef(PangrovaDistributionTraitDef.INSTANCE);

        var typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        var rexBuilder = new RexBuilder(typeFactory);
        cluster = RelOptCluster.create(planner, rexBuilder);
    }

    @Test
    void convert_insertsExchange_whenDistributionMismatches() {
        RelTraitSet anyTraitSet = cluster.traitSet().replace(PangrovaDistribution.ANY);
        RelNode child = new StubRelNode(cluster, anyTraitSet);

        RelNode result = PangrovaDistributionTraitDef.INSTANCE.convert(
                planner, child, PangrovaDistribution.HASH, true);

        assertThat(result).isInstanceOf(PangrovaExchange.class);
        PangrovaExchange exchange = (PangrovaExchange) result;
        assertThat(exchange.targetDistribution()).isEqualTo(PangrovaDistribution.HASH);
        assertThat(exchange.getInput()).isSameAs(child);
        assertThat(exchange.getTraitSet().getTrait(PangrovaDistributionTraitDef.INSTANCE))
                .isEqualTo(PangrovaDistribution.HASH);
    }

    @Test
    void convert_returnsOriginal_whenDistributionAlreadySatisfied() {
        RelTraitSet hashTraitSet = cluster.traitSet().replace(PangrovaDistribution.HASH);
        RelNode child = new StubRelNode(cluster, hashTraitSet);

        RelNode result = PangrovaDistributionTraitDef.INSTANCE.convert(
                planner, child, PangrovaDistribution.HASH, true);

        assertThat(result).isSameAs(child);
    }

    @Test
    void convert_returnsOriginal_whenRequirementIsAny() {
        RelTraitSet singletonTraitSet = cluster.traitSet().replace(PangrovaDistribution.SINGLETON);
        RelNode child = new StubRelNode(cluster, singletonTraitSet);

        RelNode result = PangrovaDistributionTraitDef.INSTANCE.convert(
                planner, child, PangrovaDistribution.ANY, true);

        assertThat(result).isSameAs(child);
    }

    @Test
    void convert_insertsBroadcastExchange_whenHashDoesNotSatisfyBroadcast() {
        RelTraitSet hashTraitSet = cluster.traitSet().replace(PangrovaDistribution.HASH);
        RelNode child = new StubRelNode(cluster, hashTraitSet);

        RelNode result = PangrovaDistributionTraitDef.INSTANCE.convert(
                planner, child, PangrovaDistribution.BROADCAST, true);

        assertThat(result).isInstanceOf(PangrovaExchange.class);
        assertThat(((PangrovaExchange) result).targetDistribution())
                .isEqualTo(PangrovaDistribution.BROADCAST);
    }

    @Test
    void convert_insertsRoundRobinExchange() {
        RelTraitSet anyTraitSet = cluster.traitSet().replace(PangrovaDistribution.ANY);
        RelNode child = new StubRelNode(cluster, anyTraitSet);

        RelNode result = PangrovaDistributionTraitDef.INSTANCE.convert(
                planner, child, PangrovaDistribution.ROUND_ROBIN, true);

        assertThat(result).isInstanceOf(PangrovaExchange.class);
        assertThat(((PangrovaExchange) result).targetDistribution())
                .isEqualTo(PangrovaDistribution.ROUND_ROBIN);
    }

    @Test
    void satisfies_hashDoesNotSatisfyBroadcast() {
        assertThat(PangrovaDistribution.HASH.satisfies(PangrovaDistribution.BROADCAST)).isFalse();
    }

    @Test
    void satisfies_anyRequirementSatisfiedByAll() {
        assertThat(PangrovaDistribution.HASH.satisfies(PangrovaDistribution.ANY)).isTrue();
        assertThat(PangrovaDistribution.BROADCAST.satisfies(PangrovaDistribution.ANY)).isTrue();
        assertThat(PangrovaDistribution.SINGLETON.satisfies(PangrovaDistribution.ANY)).isTrue();
        assertThat(PangrovaDistribution.ROUND_ROBIN.satisfies(PangrovaDistribution.ANY)).isTrue();
        assertThat(PangrovaDistribution.ANY.satisfies(PangrovaDistribution.ANY)).isTrue();
    }

    @Test
    void satisfies_anyDoesNotSatisfySpecificRequirement() {
        assertThat(PangrovaDistribution.ANY.satisfies(PangrovaDistribution.HASH)).isFalse();
        assertThat(PangrovaDistribution.ANY.satisfies(PangrovaDistribution.BROADCAST)).isFalse();
        assertThat(PangrovaDistribution.ANY.satisfies(PangrovaDistribution.SINGLETON)).isFalse();
        assertThat(PangrovaDistribution.ANY.satisfies(PangrovaDistribution.ROUND_ROBIN)).isFalse();
    }

    @Test
    void canConvert_alwaysTrue() {
        assertThat(PangrovaDistributionTraitDef.INSTANCE.canConvert(
                planner, PangrovaDistribution.ANY, PangrovaDistribution.HASH)).isTrue();
        assertThat(PangrovaDistributionTraitDef.INSTANCE.canConvert(
                planner, PangrovaDistribution.HASH, PangrovaDistribution.BROADCAST)).isTrue();
    }

    /**
     * Minimal RelNode for testing trait conversion without requiring full
     * Calcite table/schema infrastructure.
     */
    private static final class StubRelNode extends org.apache.calcite.rel.AbstractRelNode {

        StubRelNode(RelOptCluster cluster, RelTraitSet traitSet) {
            super(cluster, traitSet);
        }

        @Override
        public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
            return new StubRelNode(getCluster(), traitSet);
        }

        @Override
        protected RelDataType deriveRowType() {
            return getCluster().getTypeFactory().builder()
                    .add("id", SqlTypeName.INTEGER)
                    .add("name", SqlTypeName.VARCHAR)
                    .build();
        }
    }
}
