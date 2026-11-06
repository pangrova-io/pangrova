package org.pangrova.coordinator.property.query.execution.dispatch;

import org.pangrova.query.api.stage.StageState;

import java.util.List;
import java.util.Map;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.query.api.stage.StageRetryPolicy;
import org.pangrova.query.core.stage.StageStateMachine;
import org.pangrova.query.api.error.QueryFailure;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for DAG completion semantics, task retry, and fault recovery.
 * Tests observable Stage behavior without depending on package-private internals.
 *
 * <p><b>Validates: Requirements R1, R8, R11, R12</b></p>
 */
class DAGCompletionPropertyTest {

    private static final RelOptCluster CLUSTER;
    static {
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        CLUSTER = RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(typeFactory));
    }

    /**
     * All tasks FINISHED → stage transitions to FINISHED.
     * For any number of tasks (1..N), when all report FINISHED,
     * the stage reaches FINISHED state.
     *
     * <b>Validates: Requirements R1, R8</b>
     */
    @Property(tries = 50)
    void allTasksFinishedResolvesStage(
            @ForAll @IntRange(min = 1, max = 8) int taskCount) {
        Stage stage = createRunningStage(taskCount);
        for (int i = 0; i < taskCount; i++) {
            stage.onTaskFinished("task-" + i);
        }
        assertThat(stage.stateMachine().getState())
                .isEqualTo(StageState.FINISHED);
    }

    /**
     * All tasks reported (some FAILED) with NO_RETRY policy → stage FAILED.
     * When all tasks have reported and at least one failed with a non-retryable
     * error, the stage transitions to FAILED. The failed task must report last
     * to trigger the allTasksReported check in onTaskFailed.
     *
     * <b>Validates: Requirements R11</b>
     */
    @Property(tries = 30)
    void nonRetryableTaskFailureCausesStageFailure(
            @ForAll @IntRange(min = 1, max = 4) int taskCount) {
        Stage stage = createRunningStage(taskCount, StageRetryPolicy.NO_RETRY);
        QueryFailure userError = new QueryFailure(
                "VALIDATION-0001", "InvalidArgument", "bad input");
        for (int i = 1; i < taskCount; i++) {
            stage.onTaskFinished("task-" + i);
        }
        stage.onTaskFailed("task-0", userError);
        assertThat(stage.stateMachine().getState())
                .isEqualTo(StageState.FAILED);
    }

    /**
     * Task retry counter increments correctly per task.
     * Each call to incrementTaskRetryCount returns a monotonically increasing value.
     *
     * <b>Validates: Requirements R11</b>
     */
    @Property(tries = 30)
    void taskRetryCounterIncrementsMonotonically(
            @ForAll @IntRange(min = 1, max = 5) int retryAttempts) {
        Stage stage = createRunningStage(1);
        for (int i = 1; i <= retryAttempts; i++) {
            int count = stage.incrementTaskRetryCount("task-0");
            assertThat(count).isEqualTo(i);
        }
    }

    /**
     * Task retry exhausted beyond MAX_TASK_RETRIES → counter exceeds limit.
     * After MAX_TASK_RETRIES+1 increments, the counter exceeds the limit,
     * signaling that retry should not be attempted.
     *
     * <b>Validates: Requirements R11</b>
     */
    @Property(tries = 20)
    void taskRetryExhaustedExceedsLimit(
            @ForAll @IntRange(min = 1, max = 3) int ignored) {
        Stage stage = createRunningStage(1);
        for (int i = 0; i < Stage.MAX_TASK_RETRIES; i++) {
            stage.incrementTaskRetryCount("task-0");
        }
        int finalCount = stage.incrementTaskRetryCount("task-0");
        assertThat(finalCount).isGreaterThan(Stage.MAX_TASK_RETRIES);
    }

    /**
     * Stage tryPrepareRetry resets task tracking for re-dispatch.
     * After retry preparation, all task IDs, finished/failed sets, and
     * retry counters are cleared. Generation is incremented.
     *
     * <b>Validates: Requirements R12</b>
     */
    @Property(tries = 20)
    void stageRetryResetsTaskTracking(
            @ForAll @IntRange(min = 1, max = 4) int taskCount) {
        Stage stage = createRunningStage(taskCount);
        stage.onTaskFailed("task-0", new QueryFailure(
                "EXCHANGE-0004", "ExchangeFetchFailed", "shuffle data lost"));
        int genBefore = stage.currentGeneration();
        boolean prepared = stage.tryPrepareRetry();
        assertThat(prepared).isTrue();
        assertThat(stage.taskIds()).isEmpty();
        assertThat(stage.currentGeneration()).isEqualTo(genBefore + 1);
        assertThat(stage.retryCount()).isEqualTo(1);
    }

    /**
     * Stale generation callbacks are discarded.
     * After tryPrepareRetry increments the generation, callbacks from the
     * old generation should not affect stage state.
     *
     * <b>Validates: Requirements R12</b>
     */
    @Property(tries = 20)
    void staleGenerationCallbacksDiscarded(
            @ForAll @IntRange(min = 1, max = 3) int ignored) {
        Stage stage = createRunningStage(2);
        int oldGen = stage.currentGeneration();
        stage.tryPrepareRetry();
        stage.addTaskId("new-task-0");
        stage.addTaskId("new-task-1");
        stage.stateMachine().tryTransitionTo(StageState.SCHEDULING);
        stage.stateMachine().tryTransitionTo(StageState.RUNNING);
        stage.onTaskFinished("task-0", oldGen);
        assertThat(stage.stateMachine().getState())
                .as("stale callback should not trigger FINISHED")
                .isEqualTo(StageState.RUNNING);
        stage.onTaskFinished("new-task-0");
        stage.onTaskFinished("new-task-1");
        assertThat(stage.stateMachine().getState())
                .isEqualTo(StageState.FINISHED);
    }

    private Stage createRunningStage(int taskCount) {
        return createRunningStage(taskCount, StageRetryPolicy.DEFAULT);
    }

    private Stage createRunningStage(int taskCount, StageRetryPolicy policy) {
        StageNode node = new StageNode(
                0, new org.pangrova.planner.core.analysis.PlanFragment(LogicalValues.createOneRow(CLUSTER), -1L, -1L), taskCount, "OUTPUT", List.of());
        StageStateMachine sm = new StageStateMachine("0", "q-test");
        Stage stage = new Stage(node, sm, policy);
        sm.tryTransitionTo(StageState.SCHEDULING);
        sm.tryTransitionTo(StageState.RUNNING);
        for (int i = 0; i < taskCount; i++) {
            stage.addTaskId("task-" + i);
        }
        return stage;
    }
}
