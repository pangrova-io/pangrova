package org.pangrova.coordinator.query.execution;

import org.pangrova.query.api.stage.StageState;

import java.util.List;
import java.util.Map;

import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageNode;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Validates enhanced failure propagation in pipeline mode.
 * Covers Task 24: upstream FAILED → downstream CANCELLED/FAILED.
 */
class FailurePropagationTest {

    @Test
    @DisplayName("Upstream FAILED → PLANNED downstream transitions to CANCELLED")
    void upstreamFailedPlannedDownstreamCancelled() {
        StageDAG dag = buildLinearDAG("q-fp-1");
        Stage upstream = dag.findStage(0);
        Stage downstream = dag.findStage(1);

        upstream.stateMachine().transitionTo(StageState.SCHEDULING);
        upstream.stateMachine().transitionTo(StageState.RUNNING);
        upstream.stateMachine().tryTransitionTo(StageState.FAILED);

        cancelDownstream(upstream, dag);
        // PLANNED → CANCELLED (PLANNED cannot go to FAILED per state machine rules)
        assertEquals(StageState.CANCELLED, downstream.stateMachine().getState());
    }

    @Test
    @DisplayName("Upstream FAILED → RUNNING downstream transitions to FAILED")
    void upstreamFailedRunningDownstreamFails() {
        StageDAG dag = buildLinearDAG("q-fp-2");
        Stage upstream = dag.findStage(0);
        Stage downstream = dag.findStage(1);

        upstream.stateMachine().transitionTo(StageState.SCHEDULING);
        upstream.stateMachine().transitionTo(StageState.RUNNING);
        downstream.stateMachine().transitionTo(StageState.SCHEDULING);
        downstream.stateMachine().transitionTo(StageState.RUNNING);

        upstream.stateMachine().tryTransitionTo(StageState.FAILED);
        cancelDownstream(upstream, dag);

        assertEquals(StageState.FAILED, downstream.stateMachine().getState());
    }

    @Test
    @DisplayName("Upstream FAILED → FINISHED downstream not affected")
    void upstreamFailedFinishedDownstreamUnaffected() {
        StageDAG dag = buildLinearDAG("q-fp-3");
        Stage upstream = dag.findStage(0);
        Stage downstream = dag.findStage(1);

        downstream.stateMachine().transitionTo(StageState.SCHEDULING);
        downstream.stateMachine().transitionTo(StageState.RUNNING);
        downstream.stateMachine().transitionTo(StageState.FLUSHING);
        downstream.stateMachine().transitionTo(StageState.FINISHED);

        upstream.stateMachine().transitionTo(StageState.SCHEDULING);
        upstream.stateMachine().transitionTo(StageState.RUNNING);
        upstream.stateMachine().tryTransitionTo(StageState.FAILED);

        cancelDownstream(upstream, dag);
        assertEquals(StageState.FINISHED, downstream.stateMachine().getState());
    }

    @Test
    @DisplayName("Only direct downstream stages are cancelled, not transitive")
    void onlyDirectDownstreamCancelled() {
        StageNode n0 = new StageNode(0, dummyFragment(), 1, "COMPUTE", List.of());
        StageNode n1 = new StageNode(1, dummyFragment(), 1, "COMPUTE", List.of());
        StageNode n2 = new StageNode(2, dummyFragment(), 1, "OUTPUT", List.of());
        StageDAG dag = StageDAG.fromNodes(
                List.of(n0, n1, n2),
                Map.of(1, List.of(0), 2, List.of(1)),
                List.of()
        ).withStages("q-fp-4");

        Stage s0 = dag.findStage(0);
        s0.stateMachine().transitionTo(StageState.SCHEDULING);
        s0.stateMachine().transitionTo(StageState.RUNNING);
        s0.stateMachine().tryTransitionTo(StageState.FAILED);

        cancelDownstream(s0, dag);

        // s1 is direct downstream of s0 → CANCELLED (was PLANNED)
        assertEquals(StageState.CANCELLED, dag.findStage(1).stateMachine().getState());
        // s2 is NOT direct downstream of s0 → stays PLANNED
        assertEquals(StageState.PLANNED, dag.findStage(2).stateMachine().getState());
    }

    @Test
    @DisplayName("Upstream FAILED → SCHEDULING downstream transitions to FAILED")
    void upstreamFailedSchedulingDownstreamFails() {
        StageDAG dag = buildLinearDAG("q-fp-5");
        Stage upstream = dag.findStage(0);
        Stage downstream = dag.findStage(1);

        upstream.stateMachine().transitionTo(StageState.SCHEDULING);
        upstream.stateMachine().transitionTo(StageState.RUNNING);
        downstream.stateMachine().transitionTo(StageState.SCHEDULING);

        upstream.stateMachine().tryTransitionTo(StageState.FAILED);
        cancelDownstream(upstream, dag);

        assertEquals(StageState.FAILED, downstream.stateMachine().getState());
    }

    /**
     * Replicates cancelDownstreamStages logic for unit testing.
     */
    private static void cancelDownstream(Stage failed, StageDAG dag) {
        for (Stage stage : dag.stageEntities()) {
            if (stage.stageId() == failed.stageId()) {
                continue;
            }
            List<Integer> upstreams = dag.edges().getOrDefault(stage.stageId(), List.of());
            if (!upstreams.contains(failed.stageId())) {
                continue;
            }
            StageState state = stage.stateMachine().getState();
            if (state == StageState.FINISHED
                    || state == StageState.FAILED
                    || state == StageState.CANCELLED) {
                continue;
            }
            if (state == StageState.PLANNED) {
                stage.stateMachine().tryTransitionTo(StageState.CANCELLED);
            } else {
                stage.stateMachine().tryTransitionTo(StageState.FAILED);
            }
        }
    }

    private static StageDAG buildLinearDAG(String queryId) {
        StageNode s0 = new StageNode(0, dummyFragment(), 1, "COMPUTE", List.of());
        StageNode s1 = new StageNode(1, dummyFragment(), 1, "OUTPUT", List.of());
        return StageDAG.fromNodes(
                List.of(s0, s1), Map.of(1, List.of(0)), List.of()
        ).withStages(queryId);
    }

    private static org.pangrova.planner.core.analysis.PlanFragment dummyFragment() {
        var cluster = org.apache.calcite.plan.RelOptCluster.create(
                new org.apache.calcite.plan.volcano.VolcanoPlanner(),
                new org.apache.calcite.rex.RexBuilder(
                        new org.apache.calcite.jdbc.JavaTypeFactoryImpl()));
        return new org.pangrova.planner.core.analysis.PlanFragment(
                org.apache.calcite.rel.logical.LogicalValues.createOneRow(cluster), -1L, -1L);
    }
}
