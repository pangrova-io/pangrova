package org.pangrova.coordinator.query.stage;

import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for StageNode and StageDAG.
 */
class StageDAGTest {

    private static final RelDataTypeFactory TYPE_FACTORY = new JavaTypeFactoryImpl();
    private static final RelOptCluster CLUSTER = RelOptCluster.create(
            new VolcanoPlanner(), new RexBuilder(TYPE_FACTORY));

    private org.pangrova.planner.PlanFragment dummyPlan() {
        return new org.pangrova.planner.PlanFragment(LogicalValues.createOneRow(CLUSTER), -1L, -1L);
    }

    @Test
    void stageNodeRejectsNullPlanFragment() {
        // StageNode.checkNotNull(planFragment) must reject a null fragment reference
        // — distinct from a non-null fragment whose inner RelNode is null.
        assertThatThrownBy(() -> new StageNode(0, null, 1, "COMPUTE", List.of()))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("stagePlan");
    }

    @Test
    void stageNodeRejectsZeroParallelism() {
        assertThatThrownBy(() -> new StageNode(0, dummyPlan(), 0, "COMPUTE", List.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("parallelism");
    }

    @Test
    void stageNodeDefaultsNullExchangeSourcesToEmpty() {
        StageNode node = new StageNode(1, dummyPlan(), 2, "OUTPUT", null);
        assertThat(node.exchangeSources()).isEmpty();
    }

    @Test
    void leafStagesReturnsNodesWithoutUpstreamDeps() {
        StageNode leaf = new StageNode(0, dummyPlan(), 1, "COMPUTE", List.of());
        StageNode downstream = new StageNode(1, dummyPlan(), 2, "OUTPUT", List.of());
        // stage 1 depends on stage 0
        StageDAG dag = StageDAG.fromNodes(List.of(leaf, downstream), Map.of(1, List.of(0)), List.of());

        List<StageNode> leaves = dag.leafStages();
        assertThat(leaves).hasSize(1);
        assertThat(leaves.get(0).stageId()).isEqualTo(0);
    }

    @Test
    void readyStagesReturnsStagesWhoseAllDepsCompleted() {
        StageNode s0 = new StageNode(0, dummyPlan(), 1, "COMPUTE", List.of());
        StageNode s1 = new StageNode(1, dummyPlan(), 1, "COMPUTE", List.of());
        StageNode s2 = new StageNode(2, dummyPlan(), 1, "OUTPUT", List.of());
        // s2 depends on both s0 and s1
        StageDAG dag = StageDAG.fromNodes(
                List.of(s0, s1, s2),
                Map.of(2, List.of(0, 1)),
                List.of()
        );

        // only s0 completed — s2 not ready yet
        assertThat(dag.readyStages(Set.of(0)))
                .extracting(StageNode::stageId)
                .containsExactly(1);

        // both s0 and s1 completed — s2 now ready
        assertThat(dag.readyStages(Set.of(0, 1)))
                .extracting(StageNode::stageId)
                .containsExactly(2);
    }

    @Test
    void readyStagesExcludesAlreadyCompleted() {
        StageNode s0 = new StageNode(0, dummyPlan(), 1, "COMPUTE", List.of());
        StageNode s1 = new StageNode(1, dummyPlan(), 1, "OUTPUT", List.of());
        StageDAG dag = StageDAG.fromNodes(List.of(s0, s1), Map.of(1, List.of(0)), List.of());

        // both completed — nothing ready
        assertThat(dag.readyStages(Set.of(0, 1))).isEmpty();
    }

    @Test
    void emptyDagHasNoLeaves() {
        StageDAG dag = StageDAG.fromNodes(List.of(), Map.of(), List.of());
        assertThat(dag.leafStages()).isEmpty();
        assertThat(dag.readyStages(Set.of())).isEmpty();
    }

    @Test
    void singleStageIsLeafAndReady() {
        StageNode only = new StageNode(0, dummyPlan(), 4, "COMPUTE", List.of());
        StageDAG dag = StageDAG.fromNodes(List.of(only), Map.of(), List.of());

        assertThat(dag.leafStages()).hasSize(1);
        assertThat(dag.readyStages(Set.of())).hasSize(1);
    }
}
