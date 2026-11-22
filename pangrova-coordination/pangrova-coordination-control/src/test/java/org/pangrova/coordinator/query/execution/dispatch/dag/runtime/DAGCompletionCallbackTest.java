package org.pangrova.coordinator.query.execution.dispatch.dag.runtime;

import org.pangrova.coordinator.query.execution.dispatch.dag.DAGDispatchContext;
import org.pangrova.coordinator.query.execution.dispatch.dag.DAGDispatchEngine;
import org.pangrova.coordinator.query.execution.dispatch.dag.PipelineConfig;


import org.pangrova.query.api.enums.StageState;

import java.util.List;
import java.util.Map;

import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.query.stage.StageStateMachine;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Verifies DAG completion detection via {@link DAGCompletionEvaluator}.
 *
 * <p>Tests that allStagesFinished correctly detects when all stages in a DAG
 * have reached FINISHED state, and that clearQuery cleans up engine state.</p>
 */
class DAGCompletionCallbackTest {

    private static final String QUERY_ID = "q-callback-test";

    @Test
    @DisplayName("allStagesFinished returns true when all stages are FINISHED")
    void allStagesFinished_whenAllDone() {
        StageDAG dag = buildTwoStageDAG();
        finishAllStages(dag);

        assertTrue(DAGCompletionEvaluator.allStagesFinished(dag),
                "All stages FINISHED => allStagesFinished must return true");
    }

    @Test
    @DisplayName("allStagesFinished returns false when some stages are still running")
    void allStagesFinished_whenPartiallyDone() {
        StageDAG dag = buildTwoStageDAG();
        List<Stage> stages = dag.stageEntities();
        finishStage(stages.get(0));
        // Stage 1 still PLANNED

        assertFalse(DAGCompletionEvaluator.allStagesFinished(dag),
                "Not all stages FINISHED => allStagesFinished must return false");
    }

    @Test
    @DisplayName("collectActiveStageIds includes FINISHED stages")
    void activeStageIds_includesFinished() {
        StageDAG dag = buildTwoStageDAG();
        finishAllStages(dag);

        var activeIds = DAGCompletionEvaluator.collectActiveStageIds(dag);
        assertTrue(activeIds.contains(1));
        assertTrue(activeIds.contains(2));
    }

    @Test
    @DisplayName("collectActiveStageIds excludes PLANNED stages")
    void activeStageIds_excludesPlanned() {
        StageDAG dag = buildTwoStageDAG();
        // Both stages are PLANNED by default

        var activeIds = DAGCompletionEvaluator.collectActiveStageIds(dag);
        assertFalse(activeIds.contains(1));
        assertFalse(activeIds.contains(2));
    }

    @Test
    @DisplayName("clearQuery removes query state from the engine")
    void clearQueryRemovesState() {
        var deps = new DAGDispatchContext(
                new NoOpTaskDispatcher(),
                new org.pangrova.coordinator.task.CoordinatorTaskTracker(),
                null, null, 300, null, null,
                null, // speculativeStageHandler
                PipelineConfig.DISABLED,
                org.pangrova.coordinator.query.execution.dispatch.shuffle.UnavailableExternalShuffleLifecycle.INSTANCE,
                new org.pangrova.coordinator.query.execution.split.SplitAssignmentManager(),
                null, null);
        DAGDispatchEngine engine = new DAGDispatchEngine(deps, List.of(), null, null, null);

        // clearQuery should not throw even when no DAG is registered
        engine.clearQuery(QUERY_ID);
    }

    // --- Helpers ---

    private static StageDAG buildTwoStageDAG() {
        StageNode s1 = new StageNode(1, dummyPlan(), 1, "SCAN", List.of());
        StageNode s2 = new StageNode(2, dummyPlan(), 1, "OUTPUT", List.of());
        Map<Integer, List<Integer>> edges = Map.of(2, List.of(1));
        return StageDAG.fromNodes(List.of(s1, s2), edges, List.of()).withStages(QUERY_ID);
    }

    private static void finishAllStages(StageDAG dag) {
        for (Stage stage : dag.stageEntities()) {
            finishStage(stage);
        }
    }

    private static void finishStage(Stage stage) {
        StageStateMachine sm = stage.stateMachine();
        if (sm.getState() == StageState.PLANNED) {
            sm.tryTransitionTo(StageState.SCHEDULING);
        }
        if (sm.getState() == StageState.SCHEDULING) {
            sm.tryTransitionTo(StageState.RUNNING);
        }
        if (sm.getState() == StageState.RUNNING) {
            sm.tryTransitionTo(StageState.FLUSHING);
        }
        if (sm.getState() == StageState.FLUSHING) {
            sm.tryTransitionTo(StageState.FINISHED);
        }
    }

    private static org.pangrova.planner.analysis.PlanFragment dummyPlan() {
        var cluster = org.apache.calcite.plan.RelOptCluster.create(
                new org.apache.calcite.plan.volcano.VolcanoPlanner(),
                new org.apache.calcite.rex.RexBuilder(
                        new org.apache.calcite.jdbc.JavaTypeFactoryImpl()));
        return new org.pangrova.planner.analysis.PlanFragment(
                org.apache.calcite.rel.logical.LogicalValues.createOneRow(cluster), -1L, -1L);
    }

    /**
     * No-op task dispatcher for test isolation.
     */
    static class NoOpTaskDispatcher implements org.pangrova.coordinator.scheduler.TaskDispatcher {
        @Override
        public org.pangrova.execution.task.DispatchResult dispatch(
                org.pangrova.protocol.rest.task.SubmitTaskRequest request,
                org.pangrova.coordinator.worker.WorkerInfo worker) {
            return null;
        }
    }
}
