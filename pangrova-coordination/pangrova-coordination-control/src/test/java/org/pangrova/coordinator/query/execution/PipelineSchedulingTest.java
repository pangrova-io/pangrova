package org.pangrova.coordinator.query.execution;

import org.pangrova.query.api.enums.StageState;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.pangrova.coordinator.query.execution.dispatch.dag.PipelineConfig;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageEdge;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.shuffle.exchange.ExchangeReadMode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validates unified pipeline scheduling behavior in DAGDispatchEngine.
 * Covers REQ-18: downstream scheduled when upstream reaches RUNNING.
 */
class PipelineSchedulingTest {

    @Test
    void pipelineConfigDefaults() {
        assertEquals(false, PipelineConfig.DISABLED.enabled());
        assertEquals(0, PipelineConfig.DISABLED.maxConcurrentStages());
        assertTrue(PipelineConfig.ENABLED.enabled());
    }

    @Test
    void invalidMaxConcurrentStagesRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new PipelineConfig(true, -1, 5000));
    }

    @Test
    void invalidCommitTimeoutRejected() {
        assertThrows(IllegalArgumentException.class,
                () -> new PipelineConfig(true, 0, 0));
    }

    @Test
    void stageEdgeReadModeFromDAG() {
        StageEdge hashEdge = new StageEdge(0, 1, org.pangrova.planner.enums.ShuffleType.HASH);
        StageEdge broadcastEdge = new StageEdge(2, 1, org.pangrova.planner.enums.ShuffleType.BROADCAST,
                List.of(), "exchange-2-to-1", 1,
                org.pangrova.protocol.v2.BatchSerde.BATCH_SERDE_ARROW_BATCH, false);
        StageDAG dag = StageDAG.fromNodes(List.of(), Map.of(), List.of(hashEdge, broadcastEdge));

        assertEquals(ExchangeReadMode.BLOCKING, dag.getEdgeReadMode(0, 1));
        assertEquals(ExchangeReadMode.STREAMING, dag.getEdgeReadMode(2, 1));
        // Unknown edge defaults to BLOCKING
        assertEquals(ExchangeReadMode.BLOCKING, dag.getEdgeReadMode(99, 100));
    }

    @Test
    void collectActiveStageIdsIncludesDispatchedRunningFlushingFinished() {
        // Build a 3-stage DAG: 0 → 1 → 2
        StageNode s0 = new StageNode(0, dummyFragment(), 1, "COMPUTE", List.of());
        StageNode s1 = new StageNode(1, dummyFragment(), 1, "COMPUTE", List.of());
        StageNode s2 = new StageNode(2, dummyFragment(), 1, "OUTPUT", List.of());
        StageDAG dag = StageDAG.fromNodes(
                List.of(s0, s1, s2),
                Map.of(1, List.of(0), 2, List.of(1)),
                List.of()
        ).withStages("q-active");

        // Advance s0 to RUNNING
        dag.stageEntities().get(0).stateMachine().transitionTo(StageState.SCHEDULING);
        dag.stageEntities().get(0).stateMachine().transitionTo(StageState.RUNNING);

        // s1 still PLANNED, s2 still PLANNED
        Set<Integer> activeIds = collectActiveIds(dag);
        assertTrue(activeIds.contains(0));
        assertFalse(activeIds.contains(1));
        assertFalse(activeIds.contains(2));

        // readyStageEntities with active IDs should include s1 (upstream 0 is active)
        List<Stage> ready = dag.readyStageEntities(activeIds);
        assertEquals(1, ready.size());
        assertEquals(1, ready.get(0).stageId());
    }

    @Test
    void readyStageEntitiesRequiresAllUpstreamsActive() {
        // s2 depends on both s0 and s1
        StageNode s0 = new StageNode(0, dummyFragment(), 1, "COMPUTE", List.of());
        StageNode s1 = new StageNode(1, dummyFragment(), 1, "COMPUTE", List.of());
        StageNode s2 = new StageNode(2, dummyFragment(), 1, "OUTPUT", List.of());
        StageDAG dag = StageDAG.fromNodes(
                List.of(s0, s1, s2),
                Map.of(2, List.of(0, 1)),
                List.of()
        ).withStages("q-multi");

        // Only s0 is RUNNING
        dag.stageEntities().get(0).stateMachine().transitionTo(StageState.SCHEDULING);
        dag.stageEntities().get(0).stateMachine().transitionTo(StageState.RUNNING);

        Set<Integer> activeIds = collectActiveIds(dag);
        List<Stage> ready = dag.readyStageEntities(activeIds);
        // s2 NOT ready because s1 is still PLANNED
        assertTrue(ready.stream().noneMatch(s -> s.stageId() == 2));

        // Now advance s1 to RUNNING too
        dag.stageEntities().get(1).stateMachine().transitionTo(StageState.SCHEDULING);
        dag.stageEntities().get(1).stateMachine().transitionTo(StageState.RUNNING);

        activeIds = collectActiveIds(dag);
        ready = dag.readyStageEntities(activeIds);
        assertEquals(1, ready.size());
        assertEquals(2, ready.get(0).stageId());
    }

    /**
     * Replicates the collectActiveStageIds logic from DAGDispatchEngine
     * for unit testing without needing the full engine.
     */
    private static Set<Integer> collectActiveIds(StageDAG dag) {
        java.util.Set<Integer> ids = new java.util.HashSet<>();
        for (Stage s : dag.stageEntities()) {
            StageState st = s.stateMachine().getState();
            if (st == StageState.RUNNING
                    || st == StageState.FLUSHING
                    || st == StageState.FINISHED) {
                ids.add(s.stageId());
            }
        }
        return ids;
    }

    private static org.pangrova.planner.analysis.PlanFragment dummyFragment() {
        var cluster = org.apache.calcite.plan.RelOptCluster.create(
                new org.apache.calcite.plan.volcano.VolcanoPlanner(),
                new org.apache.calcite.rex.RexBuilder(
                        new org.apache.calcite.jdbc.JavaTypeFactoryImpl()));
        return new org.pangrova.planner.analysis.PlanFragment(
                org.apache.calcite.rel.logical.LogicalValues.createOneRow(cluster), -1L, -1L);
    }
}
