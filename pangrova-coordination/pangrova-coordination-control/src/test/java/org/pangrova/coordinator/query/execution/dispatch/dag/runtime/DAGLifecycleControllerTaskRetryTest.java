package org.pangrova.coordinator.query.execution.dispatch.dag.runtime;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageDAG;
import org.pangrova.coordinator.query.stage.StageEdge;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.query.api.enums.StageState;
import org.pangrova.query.stage.StageStateMachine;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Task-level retry admission and terminal routing after partial worker loss. */
class DAGLifecycleControllerTaskRetryTest {

    private static org.pangrova.planner.analysis.PlanFragment fragment() {
        var typeFactory = new org.apache.calcite.jdbc.JavaTypeFactoryImpl();
        var cluster = org.apache.calcite.plan.RelOptCluster.create(
                new org.apache.calcite.plan.volcano.VolcanoPlanner(),
                new org.apache.calcite.rex.RexBuilder(typeFactory));
        return new org.pangrova.planner.analysis.PlanFragment(
                org.apache.calcite.rel.logical.LogicalValues.createOneRow(cluster), -1L, -1L);
    }

    private static StageDAG twoStageDag() {
        StageNode n0 = new StageNode(0, fragment(), 2, "COMPUTE", List.of());
        StageNode n1 = new StageNode(1, fragment(), 2, "OUTPUT", List.of());
        StageEdge edge = new StageEdge(0, 1,
                org.pangrova.planner.enums.ShuffleType.HASH);
        return StageDAG.fromNodes(List.of(n0, n1), Map.of(1, List.of(0)), List.of(edge))
                .withStages("q-retry");
    }

    private static DAGLifecycleController controller(StageDAG dag) {
        var controller = new DAGLifecycleController(null, null, null, null, List.of());
        controller.registerQuery("q-retry", dag, null);
        return controller;
    }

    private static void running(Stage stage) {
        stage.stateMachine().transitionTo(StageState.SCHEDULING);
        stage.stateMachine().transitionTo(StageState.RUNNING);
    }

    @Test
    void retryIsAdmittedWhileDownstreamHasNotStarted() {
        StageDAG dag = twoStageDag();
        var controller = controller(dag);
        Stage stage = dag.findStage(0);
        stage.addTaskId("t1");
        stage.addTaskId("t2");
        running(stage);
        controller.registerTask("q-retry", "t1", stage);
        controller.registerTask("q-retry", "t2", stage);

        assertThat(controller.tryPrepareTaskRetry("q-retry", "t1", "r1", 2)).isTrue();

        // A stale failure for the replaced attempt must not fail the Stage.
        controller.onTaskTerminal("q-retry", "t1", "FAILED",
                new org.pangrova.query.api.error.QueryFailure(
                        "WORKER-0002", "WORKER_LOST", "worker died"));
        assertThat(stage.stateMachine().getState()).isEqualTo(StageState.RUNNING);

        controller.onTaskTerminal("q-retry", "r1", "FINISHED", null);
        controller.onTaskTerminal("q-retry", "t2", "FINISHED", null);
        assertThat(stage.stateMachine().getState()).isEqualTo(StageState.FINISHED);
    }

    @Test
    void retryIsRejectedOnceADownstreamStageStarted() {
        StageDAG dag = twoStageDag();
        var controller = controller(dag);
        Stage upstream = dag.findStage(0);
        upstream.addTaskId("t1");
        running(upstream);
        running(dag.findStage(1));

        assertThat(controller.tryPrepareTaskRetry("q-retry", "t1", "r1", 2)).isFalse();
        assertThat(upstream.taskIds()).containsExactly("t1");
    }

    @Test
    void retryOfUnknownTaskIsRejected() {
        StageDAG dag = twoStageDag();
        var controller = controller(dag);
        assertThat(controller.tryPrepareTaskRetry("q-retry", "missing", "r1", 2)).isFalse();
        assertThat(controller.tryPrepareTaskRetry("unknown-query", "t1", "r1", 2)).isFalse();
    }

    @Test
    void exhaustedTaskBudgetRejectsFurtherRetries() {
        StageDAG dag = twoStageDag();
        var controller = controller(dag);
        Stage stage = dag.findStage(0);
        stage.addTaskId("t1");
        running(stage);

        assertThat(controller.tryPrepareTaskRetry("q-retry", "t1", "r1", 1)).isTrue();
        assertThat(controller.tryPrepareTaskRetry("q-retry", "r1", "r2", 1)).isFalse();
    }
}
