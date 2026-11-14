package org.pangrova.coordinator.property.query.execution.dispatch;

import org.pangrova.query.api.enums.StageState;

import java.util.List;

import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.constraints.IntRange;

import org.pangrova.coordinator.query.stage.Stage;
import org.pangrova.coordinator.query.stage.StageNode;
import org.pangrova.query.api.stage.StageRetryPolicy;
import org.pangrova.query.stage.StageStateMachine;
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
 * Property tests for task-level retry mechanics via the public {@link Stage} API.
 * Validates retry counter behavior, exhaustion, generation isolation, and
 * interaction with stage-level retry.
 *
 * <p><b>Validates: Requirements R11</b></p>
 */
class TaskRetryPropertyTest {

    private static final RelOptCluster CLUSTER;
    static {
        RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
        CLUSTER = RelOptCluster.create(new VolcanoPlanner(), new RexBuilder(typeFactory));
    }

    private static final QueryFailure RETRYABLE_EXCHANGE =
            new QueryFailure("EXCHANGE-0001", "ExchangeUnavailable", "source down");
    private static final QueryFailure RETRYABLE_RESOURCE =
            new QueryFailure("RESOURCE-0001", "MemoryExceeded", "OOM");
    private static final QueryFailure NON_RETRYABLE_USER =
            new QueryFailure("VALIDATION-0001", "InvalidArgument", "bad input");

    /**
     * Per-task retry counter starts at 0 and increments monotonically.
     * Each call returns the new count (1-based).
     */
    @Property(tries = 30)
    void taskRetryCounterStartsAtZeroAndIncrements(
            @ForAll @IntRange(min = 1, max = 6) int attempts) {
        Stage stage = createRunningStage(1);
        for (int i = 1; i <= attempts; i++) {
            int count = stage.incrementTaskRetryCount("task-0");
            assertThat(count).isEqualTo(i);
        }
    }

    /**
     * Different tasks have independent retry counters within the same stage.
     */
    @Property(tries = 20)
    void perTaskCountersAreIndependent(
            @ForAll @IntRange(min = 2, max = 5) int taskCount) {
        Stage stage = createRunningStage(taskCount);
        stage.incrementTaskRetryCount("task-0");
        stage.incrementTaskRetryCount("task-0");
        int task0Count = stage.incrementTaskRetryCount("task-0");
        int task1Count = stage.incrementTaskRetryCount("task-1");
        assertThat(task0Count).isEqualTo(3);
        assertThat(task1Count)
                .as("task-1 counter should be independent from task-0")
                .isEqualTo(1);
    }

    /**
     * After MAX_TASK_RETRIES increments, the counter exceeds the limit,
     * signaling that task-level retry should not be attempted.
     */
    @Property(tries = 20)
    void retryExhaustedAfterMaxAttempts(
            @ForAll @IntRange(min = 1, max = 3) int ignored) {
        Stage stage = createRunningStage(1);
        for (int i = 0; i < Stage.MAX_TASK_RETRIES; i++) {
            int count = stage.incrementTaskRetryCount("task-0");
            assertThat(count).isLessThanOrEqualTo(Stage.MAX_TASK_RETRIES);
        }
        int exhausted = stage.incrementTaskRetryCount("task-0");
        assertThat(exhausted).isGreaterThan(Stage.MAX_TASK_RETRIES);
    }

    /**
     * tryPrepareRetry clears task retry counters along with task tracking.
     * After stage retry, task counters reset to 0.
     */
    @Property(tries = 20)
    void stageRetryResetsTaskRetryCounters(
            @ForAll @IntRange(min = 1, max = 4) int taskCount) {
        Stage stage = createRunningStage(taskCount);
        stage.incrementTaskRetryCount("task-0");
        stage.incrementTaskRetryCount("task-0");
        stage.onTaskFailed("task-0", RETRYABLE_EXCHANGE);
        boolean prepared = stage.tryPrepareRetry();
        assertThat(prepared).isTrue();
        int countAfterReset = stage.incrementTaskRetryCount("task-0");
        assertThat(countAfterReset)
                .as("counter should restart at 1 after stage retry")
                .isEqualTo(1);
    }

    /**
     * Generation isolation: callbacks from old generation are discarded.
     * After tryPrepareRetry, onTaskFinished with old generation has no effect.
     */
    @Property(tries = 20)
    void staleGenerationFinishedCallbackDiscarded(
            @ForAll @IntRange(min = 1, max = 3) int ignored) {
        Stage stage = createRunningStage(2);
        int oldGen = stage.currentGeneration();
        stage.tryPrepareRetry();
        stage.addTaskId("new-0");
        stage.addTaskId("new-1");
        stage.stateMachine().tryTransitionTo(StageState.SCHEDULING);
        stage.stateMachine().tryTransitionTo(StageState.RUNNING);
        stage.onTaskFinished("task-0", oldGen);
        assertThat(stage.stateMachine().getState())
                .as("stale FINISHED should not trigger completion")
                .isEqualTo(StageState.RUNNING);
    }

    /**
     * Generation isolation: failed callbacks from old generation are discarded.
     * After tryPrepareRetry, onTaskFailed with old generation has no effect.
     */
    @Property(tries = 20)
    void staleGenerationFailedCallbackDiscarded(
            @ForAll @IntRange(min = 1, max = 3) int ignored) {
        Stage stage = createRunningStage(1, StageRetryPolicy.NO_RETRY);
        int oldGen = stage.currentGeneration();
        stage.tryPrepareRetry();
        stage.addTaskId("new-0");
        stage.stateMachine().tryTransitionTo(StageState.SCHEDULING);
        stage.stateMachine().tryTransitionTo(StageState.RUNNING);
        stage.onTaskFailed("new-0", NON_RETRYABLE_USER, oldGen);
        assertThat(stage.stateMachine().getState())
                .as("stale FAILED should not trigger stage failure")
                .isEqualTo(StageState.RUNNING);
    }

    /**
     * TaskRetryCallback can be set and retrieved on a Stage.
     */
    @Property(tries = 10)
    void taskRetryCallbackSetAndRetrieved(
            @ForAll @IntRange(min = 1, max = 2) int ignored) {
        Stage stage = createRunningStage(1);
        assertThat(stage.taskRetryCallback()).isNull();
        stage.setTaskRetryCallback((taskId, stageId, gen, excludeWorker) -> {});
        assertThat(stage.taskRetryCallback()).isNotNull();
    }

    /**
     * Non-retryable error with NO_RETRY policy: single task failure
     * transitions stage to FAILED when all tasks have reported.
     */
    @Property(tries = 20)
    void singleTaskNonRetryableFailureCausesStageFailure(
            @ForAll @IntRange(min = 1, max = 4) int taskCount) {
        Stage stage = createRunningStage(taskCount, StageRetryPolicy.NO_RETRY);
        for (int i = 1; i < taskCount; i++) {
            stage.onTaskFinished("task-" + i);
        }
        stage.onTaskFailed("task-0", NON_RETRYABLE_USER);
        assertThat(stage.stateMachine().getState())
                .isEqualTo(StageState.FAILED);
        assertThat(stage.lastError()).isEqualTo(NON_RETRYABLE_USER);
    }

    private Stage createRunningStage(int taskCount) {
        return createRunningStage(taskCount, StageRetryPolicy.DEFAULT);
    }

    private Stage createRunningStage(int taskCount, StageRetryPolicy policy) {
        StageNode node = new StageNode(
                0, new org.pangrova.planner.analysis.PlanFragment(LogicalValues.createOneRow(CLUSTER), -1L, -1L), taskCount,
                "OUTPUT", List.of());
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
