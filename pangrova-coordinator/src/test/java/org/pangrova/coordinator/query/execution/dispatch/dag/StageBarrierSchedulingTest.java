package org.pangrova.coordinator.query.execution.dispatch.dag;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.coordinator.query.stage.StageStateMachine;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies stage scheduling readiness semantics.
 *
 * <p>Tests both active-stage-based scheduling (pipeline mode: RUNNING counts
 * as ready) and finished-only scheduling (barrier mode: only FINISHED counts
 * as ready), using the actual {@link DAGCompletionEvaluator#collectActiveStageIds}
 * method and a local barrier helper.</p>
 */
class StageBarrierSchedulingTest {

    private static final String QUERY_ID = "q-barrier-test";

    // ---- collectActiveStageIds tests ----

    @Test
    @DisplayName("collectActiveStageIds includes RUNNING stages")
    void activeIdsIncludesRunningStages() {
        StageDAG dag = buildLinearDAG();
        transitionTo(dag, 0, StageStateMachine.State.SCHEDULING);
        transitionTo(dag, 0, StageStateMachine.State.RUNNING);

        Set<Integer> activeIds = DAGCompletionEvaluator.collectActiveStageIds(dag);
        assertTrue(activeIds.contains(0), "RUNNING stage must be in active set");
    }

    @Test
    @DisplayName("collectActiveStageIds includes FINISHED stages")
    void activeIdsIncludesFinishedStages() {
        StageDAG dag = buildLinearDAG();
        finishStage(dag, 0);

        Set<Integer> activeIds = DAGCompletionEvaluator.collectActiveStageIds(dag);
        assertTrue(activeIds.contains(0), "FINISHED stage must be in active set");
    }

    @Test
    @DisplayName("collectActiveStageIds excludes PLANNED stages")
    void activeIdsExcludesPlannedStages() {
        StageDAG dag = buildLinearDAG();
        // Stage 0 is PLANNED by default
        Set<Integer> activeIds = DAGCompletionEvaluator.collectActiveStageIds(dag);
        assertFalse(activeIds.contains(0), "PLANNED stage must not be in active set");
    }

    // ---- Barrier scheduling helper tests ----

    @Test
    @DisplayName("Barrier: RUNNING stage is NOT in finished-only set")
    void barrierExcludesRunning() {
        StageDAG dag = buildLinearDAG();
        transitionTo(dag, 0, StageStateMachine.State.SCHEDULING);
        transitionTo(dag, 0, StageStateMachine.State.RUNNING);

        Set<Integer> finishedIds = collectFinishedStageIds(dag);
        assertFalse(finishedIds.contains(0),
                "RUNNING stage must not be in finished set");
    }

    @Test
    @DisplayName("Barrier: only FINISHED stages are in finished-only set")
    void barrierIncludesOnlyFinished() {
        StageDAG dag = buildLinearDAG();
        finishStage(dag, 0);
        transitionTo(dag, 1, StageStateMachine.State.SCHEDULING);
        transitionTo(dag, 1, StageStateMachine.State.RUNNING);

        Set<Integer> finishedIds = collectFinishedStageIds(dag);
        assertTrue(finishedIds.contains(0), "FINISHED stage must be in finished set");
        assertFalse(finishedIds.contains(1), "RUNNING stage must not be in finished set");
        assertFalse(finishedIds.contains(2), "PLANNED stage must not be in finished set");
    }

    // ---- Pipeline vs barrier contrast ----

    @Test
    @DisplayName("Pipeline: downstream IS ready when upstream is RUNNING")
    void pipelineDownstreamReadyWhenUpstreamRunning() {
        StageDAG dag = buildLinearDAG();
        transitionTo(dag, 0, StageStateMachine.State.SCHEDULING);
        transitionTo(dag, 0, StageStateMachine.State.RUNNING);

        // Pipeline uses collectActiveStageIds which includes RUNNING
        Set<Integer> activeIds = DAGCompletionEvaluator.collectActiveStageIds(dag);
        List<Stage> ready = dag.readyStageEntities(activeIds);

        assertTrue(ready.stream().anyMatch(s -> s.stageId() == 1),
                "In pipeline mode, downstream should be ready when upstream is RUNNING");
    }

    @Test
    @DisplayName("Barrier: downstream NOT ready when upstream is RUNNING")
    void barrierDownstreamNotReadyWhenUpstreamRunning() {
        StageDAG dag = buildLinearDAG();
        transitionTo(dag, 0, StageStateMachine.State.SCHEDULING);
        transitionTo(dag, 0, StageStateMachine.State.RUNNING);

        // Barrier uses only FINISHED stages
        Set<Integer> finishedIds = collectFinishedStageIds(dag);
        List<Stage> ready = dag.readyStageEntities(finishedIds);

        assertTrue(ready.stream().noneMatch(s -> s.stageId() == 1),
                "Downstream stage must NOT be ready when upstream is only RUNNING");
    }

    @Test
    @DisplayName("Barrier: downstream IS ready when upstream is FINISHED")
    void barrierDownstreamReadyWhenUpstreamFinished() {
        StageDAG dag = buildLinearDAG();
        finishStage(dag, 0);

        Set<Integer> finishedIds = collectFinishedStageIds(dag);
        List<Stage> ready = dag.readyStageEntities(finishedIds);

        assertTrue(ready.stream().anyMatch(s -> s.stageId() == 1),
                "Downstream stage must be ready when upstream is FINISHED");
        // Stage 2 depends on stage 1 which is still PLANNED -- NOT ready
        assertTrue(ready.stream().noneMatch(s -> s.stageId() == 2),
                "Stage 2 must NOT be ready -- its upstream (stage 1) is not finished");
    }

    @Test
    @DisplayName("Barrier: multi-upstream -- downstream waits for ALL upstreams FINISHED")
    void barrierMultiUpstreamRequiresAllFinished() {
        StageDAG dag = buildDiamondDAG();

        // Stage 0 FINISHED, stage 1 only RUNNING
        finishStage(dag, 0);
        transitionTo(dag, 1, StageStateMachine.State.SCHEDULING);
        transitionTo(dag, 1, StageStateMachine.State.RUNNING);

        Set<Integer> finishedIds = collectFinishedStageIds(dag);
        List<Stage> ready = dag.readyStageEntities(finishedIds);

        // Stage 2 depends on both 0 AND 1 -- not ready because 1 is only RUNNING
        assertTrue(ready.stream().noneMatch(s -> s.stageId() == 2),
                "Downstream must wait for ALL upstreams to FINISH, not just one");

        // Now finish stage 1 as well
        transitionTo(dag, 1, StageStateMachine.State.FLUSHING);
        transitionTo(dag, 1, StageStateMachine.State.FINISHED);

        finishedIds = collectFinishedStageIds(dag);
        ready = dag.readyStageEntities(finishedIds);

        assertTrue(ready.stream().anyMatch(s -> s.stageId() == 2),
                "Downstream must be ready once ALL upstreams are FINISHED");
    }

    @Test
    @DisplayName("Barrier: FLUSHING upstream does NOT satisfy downstream")
    void barrierFlushingDoesNotSatisfy() {
        StageDAG dag = buildLinearDAG();
        transitionTo(dag, 0, StageStateMachine.State.SCHEDULING);
        transitionTo(dag, 0, StageStateMachine.State.RUNNING);
        transitionTo(dag, 0, StageStateMachine.State.FLUSHING);

        Set<Integer> finishedIds = collectFinishedStageIds(dag);
        List<Stage> ready = dag.readyStageEntities(finishedIds);

        assertTrue(ready.stream().noneMatch(s -> s.stageId() == 1),
                "FLUSHING stage must NOT satisfy barrier -- data not yet committed");
    }

    @Test
    @DisplayName("allStagesFinished returns false when stages are still running")
    void allStagesFinished_false() {
        StageDAG dag = buildLinearDAG();
        finishStage(dag, 0);
        assertFalse(DAGCompletionEvaluator.allStagesFinished(dag));
    }

    @Test
    @DisplayName("allStagesFinished returns true when all stages are FINISHED")
    void allStagesFinished_true() {
        StageDAG dag = buildLinearDAG();
        finishStage(dag, 0);
        finishStage(dag, 1);
        finishStage(dag, 2);
        assertTrue(DAGCompletionEvaluator.allStagesFinished(dag));
    }

    // ---- Helpers ----

    /**
     * Collects only FINISHED stage IDs from the DAG.
     * This is the barrier-mode equivalent of {@code collectActiveStageIds}.
     */
    private static Set<Integer> collectFinishedStageIds(StageDAG dag) {
        Set<Integer> ids = new HashSet<>();
        for (Stage s : dag.stageEntities()) {
            if (s.stateMachine().getState() == StageStateMachine.State.FINISHED) {
                ids.add(s.stageId());
            }
        }
        return ids;
    }

    /**
     * Linear 3-stage DAG: 0 -> 1 -> 2
     */
    private static StageDAG buildLinearDAG() {
        StageNode s0 = new StageNode(0, dummyPlan(), 1, "SCAN", List.of());
        StageNode s1 = new StageNode(1, dummyPlan(), 1, "COMPUTE", List.of());
        StageNode s2 = new StageNode(2, dummyPlan(), 1, "OUTPUT", List.of());
        return StageDAG.fromNodes(
                List.of(s0, s1, s2),
                Map.of(1, List.of(0), 2, List.of(1)),
                List.of()
        ).withStages(QUERY_ID);
    }

    /**
     * Diamond DAG: 0 and 1 are roots, both feed into 2.
     */
    private static StageDAG buildDiamondDAG() {
        StageNode s0 = new StageNode(0, dummyPlan(), 1, "SCAN", List.of());
        StageNode s1 = new StageNode(1, dummyPlan(), 1, "SCAN", List.of());
        StageNode s2 = new StageNode(2, dummyPlan(), 1, "OUTPUT", List.of());
        return StageDAG.fromNodes(
                List.of(s0, s1, s2),
                Map.of(2, List.of(0, 1)),
                List.of()
        ).withStages(QUERY_ID);
    }

    private static void transitionTo(StageDAG dag, int stageId, StageStateMachine.State target) {
        Stage stage = dag.findStage(stageId);
        assertNotNull(stage, "Stage " + stageId + " must exist in DAG");
        assertTrue(stage.stateMachine().tryTransitionTo(target),
                "Transition to " + target + " must succeed for stage " + stageId
                        + " (current state: " + stage.stateMachine().getState() + ")");
    }

    private static void finishStage(StageDAG dag, int stageId) {
        Stage stage = dag.findStage(stageId);
        assertNotNull(stage, "Stage " + stageId + " must exist in DAG");
        StageStateMachine sm = stage.stateMachine();
        if (sm.getState() == StageStateMachine.State.PLANNED) {
            sm.tryTransitionTo(StageStateMachine.State.SCHEDULING);
        }
        if (sm.getState() == StageStateMachine.State.SCHEDULING) {
            sm.tryTransitionTo(StageStateMachine.State.RUNNING);
        }
        if (sm.getState() == StageStateMachine.State.RUNNING) {
            sm.tryTransitionTo(StageStateMachine.State.FLUSHING);
        }
        if (sm.getState() == StageStateMachine.State.FLUSHING) {
            sm.tryTransitionTo(StageStateMachine.State.FINISHED);
        }
        assertEquals(StageStateMachine.State.FINISHED, sm.getState(),
                "Stage " + stageId + " must be FINISHED after finishStage()");
    }

    private static org.pangrova.planner.PlanFragment dummyPlan() {
        var cluster = org.apache.calcite.plan.RelOptCluster.create(
                new org.apache.calcite.plan.volcano.VolcanoPlanner(),
                new org.apache.calcite.rex.RexBuilder(
                        new org.apache.calcite.jdbc.JavaTypeFactoryImpl()));
        return new org.pangrova.planner.PlanFragment(
                org.apache.calcite.rel.logical.LogicalValues.createOneRow(cluster), -1L, -1L);
    }
}
