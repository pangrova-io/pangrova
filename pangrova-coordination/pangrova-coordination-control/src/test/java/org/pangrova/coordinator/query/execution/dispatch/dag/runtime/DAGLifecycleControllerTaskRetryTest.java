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

    private static StageDAG independentDag() {
        var node = new StageNode(0, fragment(), 2, "COMPUTE", List.of());
        return StageDAG.fromNodes(List.of(node), Map.of(), List.of()).withStages("q-retry");
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
    void retryIsAdmittedForAnExchangeFreeTask() {
        StageDAG dag = independentDag();
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
    void rollbackRedeliversFirstTerminalReceivedWhileTheOriginalSlotWasReplaced() {
        var dag = independentDag();
        var controller = controller(dag);
        var stage = dag.findStage(0);
        stage.addTaskId("old");
        running(stage);
        controller.registerTask("q-retry", "old", stage);
        assertThat(controller.tryPrepareTaskRetry("q-retry", "old", "replacement", 1)).isTrue();
        controller.onTaskTerminal("q-retry", "old", "FINISHED", null);
        controller.onTaskTerminal("q-retry", "old", "FAILED", null);
        controller.rollbackTaskRetry("q-retry", "old", "replacement");
        assertThat(stage.stateMachine().getState()).isEqualTo(StageState.FINISHED);
        controller.onTaskTerminal("q-retry", "replacement", "CANCELLED", null);
        assertThat(stage.taskIds()).containsExactly("old");
    }

    @Test
    void publishedClientEndpointAndWriteAttemptCannotBeReplaced() {
        for (boolean write : List.of(true, false)) {
            var dag = independentDag();
            var stage = dag.findStage(0);
            var controller = controller(dag);
            if (write) stage.markWriteAttempt();
            else stage.markClientResultPublished();
            stage.addTaskId("issued");
            running(stage);
            assertThat(controller.tryPrepareTaskRetry("q-retry", "issued", "replacement", 1)).isFalse();
        }
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
    void bothShuffleEndpointsDeclineMovingOneTaskWithAnUnchangedExchangeBinding() {
        var dag = twoStageDag();
        var controller = controller(dag);
        for (int stageId : List.of(0, 1)) {
            var stage = dag.findStage(stageId);
            String task = "task-" + stageId;
            stage.addTaskId(task);
            running(stage);
            assertThat(controller.tryPrepareTaskRetry("q-retry", task, "replacement", 1)).isFalse();
            assertThat(stage.taskIds()).containsExactly(task);
        }
    }

    @Test
    void unknownAndRetiredQueriesCannotPoisonFutureTaskRegistration() {
        var controller = new DAGLifecycleController(null, null, null, null, List.of());
        controller.onTaskTerminal("q-retry", "task", "FAILED", null);
        var old = independentDag();
        controller.registerQuery("q-retry", old, null);
        controller.onTaskTerminal("q-retry", "task", "CANCELLED", null);
        controller.clearQuery("q-retry");
        var current = independentDag();
        controller.registerQuery("q-retry", current, null);
        controller.failQuery("q-retry", old);
        assertThat(controller.owns("q-retry", current)).isTrue();
        var owner = current.findStage(0);
        owner.addTaskId("task");
        running(owner);
        controller.registerTask("q-retry", "task", old.findStage(0));
        controller.registerTask("q-retry", "task", owner);
        controller.onTaskTerminal("other", "task", "FAILED", null);
        assertThat(owner.stateMachine().getState()).isEqualTo(StageState.RUNNING);
        controller.onTaskTerminal("q-retry", "task", "FINISHED", null);
        assertThat(owner.stateMachine().getState()).isEqualTo(StageState.FINISHED);
    }

    @Test
    void registrationCanReplayAndRetireConcurrentlyWithoutHoldingEitherCallbackLock() throws Exception {
        var dag = independentDag();
        var controller = controller(dag);
        var owner = dag.findStage(0);
        owner.addTaskId("early");
        running(owner);
        var entered = new java.util.concurrent.CountDownLatch(1);
        var cleared = new java.util.concurrent.CountDownLatch(1);
        owner.stateMachine().addStateChangeListener((id, previous, next) -> {
            if (next != StageState.FINISHED) return;
            entered.countDown();
            try {
                if (!cleared.await(5, java.util.concurrent.TimeUnit.SECONDS)) throw new AssertionError("clear deadlock");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new AssertionError(e);
            }
        });
        controller.onTaskTerminal("q-retry", "early", "FINISHED", null);
        var pool = java.util.concurrent.Executors.newFixedThreadPool(2);
        try {
            var replay = pool.submit(() -> controller.registerTask("q-retry", "early", owner));
            assertThat(entered.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            var cleanup = pool.submit(() -> {
                controller.clearQuery("q-retry");
                cleared.countDown();
            });
            cleanup.get(5, java.util.concurrent.TimeUnit.SECONDS);
            replay.get(5, java.util.concurrent.TimeUnit.SECONDS);
        } finally {
            cleared.countDown();
            pool.shutdownNow();
        }
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
        StageDAG dag = independentDag();
        var controller = controller(dag);
        Stage stage = dag.findStage(0);
        stage.addTaskId("t1");
        running(stage);

        assertThat(controller.tryPrepareTaskRetry("q-retry", "t1", "r1", 1)).isTrue();
        assertThat(controller.tryPrepareTaskRetry("q-retry", "r1", "r2", 1)).isFalse();
    }
}
