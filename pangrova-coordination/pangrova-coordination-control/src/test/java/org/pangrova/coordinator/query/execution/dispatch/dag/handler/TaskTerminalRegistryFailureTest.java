package org.pangrova.coordinator.query.execution.dispatch.dag.handler;

import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.planner.analysis.PlanFragment;
import org.pangrova.query.api.error.QueryFailure;
import org.pangrova.query.api.enums.StageState;
import static org.assertj.core.api.Assertions.assertThat;

/** The actual terminal routing path, not an unassembled recovery simulation. */
class TaskTerminalRegistryFailureTest {
    @Test
    void shuffleFailureReachesFailedStageWithoutReopeningCompletedOutput() {
        var registry = new TaskTerminalRegistry("q");
        var upstream = runningStage(0, "upstream");
        var downstream = runningStage(1, "downstream");
        registry.register("q", "upstream", upstream);
        registry.onTerminal("q", "upstream", "FINISHED", null);
        registry.register("q", "downstream", downstream);
        var failure = new QueryFailure("EXCHANGE-0004", "ExchangeFetchFailed", "source unavailable", "stack");
        registry.onTerminal("q", "downstream", "FAILED", failure);
        assertThat(downstream.stateMachine().getState()).isEqualTo(StageState.FAILED);
        assertThat(downstream.lastError()).isSameAs(failure);
        assertThat(upstream.stateMachine().getState()).isEqualTo(StageState.FINISHED);
        assertThat(upstream.stateMachine().reset()).isFalse();
        assertThat(upstream.currentGeneration()).isZero();
        assertThat(upstream.retryCount()).isZero();
        assertThat(upstream.taskIds()).containsExactly("upstream");
    }

    @Test
    void failureArrivingBeforeRegistrationRetainsAllDiagnostics() {
        var registry = new TaskTerminalRegistry("q");
        var stage = runningStage(0, "task");
        var failure = new QueryFailure("EXCHANGE-0004", null, "source unavailable", "stack");
        registry.onTerminal("q", "task", "FAILED", failure);
        registry.register("q", "task", stage);
        assertThat(stage.stateMachine().getState()).isEqualTo(StageState.FAILED);
        assertThat(stage.lastError()).isSameAs(failure);
        assertThat(stage.currentGeneration()).isZero();
    }

    @Test
    void cancellationWithoutDiagnosticsKeepsTheExistingCancellationClassification() {
        var registry = new TaskTerminalRegistry("q");
        var stage = runningStage(0, "task");
        registry.register("q", "task", stage);
        registry.onTerminal("q", "task", "CANCELLED", null);
        assertThat(stage.stateMachine().getState()).isEqualTo(StageState.FAILED);
        assertThat(stage.lastError()).isEqualTo(new QueryFailure(
                "TASK_CANCELLED", "TaskCancelled", "Task was cancelled"));
    }

    @Test
    void firstReportWinsBeforeAndAfterRegistrationAndNeverRecountsASibling() {
        for (boolean early : List.of(false, true)) {
            var registry = new TaskTerminalRegistry("q");
            var stage = runningStage(0, "first");
            stage.addTaskId("sibling");
            if (!early) registry.register("q", "first", stage);
            registry.onTerminal("q", "first", "FINISHED", null);
            registry.onTerminal("q", "first", "CANCELLED", null);
            registry.register("q", "first", stage);
            registry.onTerminal("q", "first", "FAILED", new QueryFailure("wrong", "duplicate", "second report"));
            assertThat(stage.stateMachine().getState()).isEqualTo(StageState.RUNNING);
            assertThat(stage.lastError()).isNull();
            registry.register("q", "sibling", stage);
            registry.onTerminal("q", "sibling", "FINISHED", null);
            assertThat(stage.stateMachine().getState()).isEqualTo(StageState.FINISHED);
        }
    }

    @Test
    void staleRegistrationCannotDeliverToCurrentGenerationEvenIfTaskIdIsReused() {
        var registry = new TaskTerminalRegistry("q");
        var stage = runningStage(0, "reused");
        registry.register("q", "reused", stage);
        stage.onTaskFailed("reused", new QueryFailure("original", "worker", "failed"));
        assertThat(stage.tryRestartFailedGeneration(0, 1, "original")).isTrue();
        stage.stateMachine().transitionTo(StageState.SCHEDULING);
        stage.stateMachine().transitionTo(StageState.RUNNING);
        stage.addTaskId("reused");
        registry.onTerminal("q", "reused", "FAILED", new QueryFailure("old", "old", "old"));
        assertThat(stage.lastError()).isNull();
        assertThat(stage.stateMachine().getState()).isEqualTo(StageState.RUNNING);
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> registry.register("q", "reused", stage))
                .hasMessageContaining("another stage attempt");
    }

    private static Stage runningStage(int id, String taskId) {
        var cluster = RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(new JavaTypeFactoryImpl()));
        var stage = Stage.create(new StageNode(id,
                new PlanFragment(LogicalValues.createOneRow(cluster), -1L, -1L), 1, "COMPUTE", List.of()), "q");
        assertThat(stage.stateMachine().tryTransitionTo(StageState.SCHEDULING)).isTrue();
        assertThat(stage.stateMachine().tryTransitionTo(StageState.RUNNING)).isTrue();
        stage.addTaskId(taskId);
        return stage;
    }
}
