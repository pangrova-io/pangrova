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

    @Property(tries = 30)
    void retryBudgetFollowsEachReplacementAndRemainsIndependentForSiblings(
            @ForAll @IntRange(min = 1, max = 6) int budget) {
        Stage stage = createRunningStage(2);
        for (String first : java.util.List.of("task-0", "task-1")) {
            String current = first;
            for (int i = 1; i <= budget; i++) {
                String next = first + "-retry-" + i;
                assertThat(stage.tryPrepareTaskRetry(current, next, budget)).isTrue();
                current = next;
            }
            assertThat(stage.tryPrepareTaskRetry(current, first + "-extra", budget)).isFalse();
        }
    }

    @Property(tries = 20)
    void stageRetryResetsTaskRetryBudgets(@ForAll @IntRange(min = 1, max = 4) int taskCount) {
        Stage stage = createRunningStage(taskCount);
        assertThat(stage.tryPrepareTaskRetry("task-0", "retry", 1)).isTrue();
        assertThat(stage.tryPrepareTaskRetry("retry", "extra", 1)).isFalse();
        stage.onTaskFailed("retry", RETRYABLE_EXCHANGE);
        for (int i = 1; i < taskCount; i++) stage.onTaskFinished("task-" + i);
        assertThat(stage.tryRestartFailedGeneration(stage.currentGeneration(), 1, RETRYABLE_EXCHANGE.errorCode())).isTrue();
        stage.stateMachine().transitionTo(StageState.SCHEDULING);
        stage.stateMachine().transitionTo(StageState.RUNNING);
        stage.addTaskId("retry");
        assertThat(stage.tryPrepareTaskRetry("retry", "next-generation", 1)).isTrue();
    }

    /**
     * Generation isolation: callbacks from old generation are discarded.
     * After restarting the failed generation, onTaskFinished with old generation has no effect.
     */
    @Property(tries = 20)
    void staleGenerationFinishedCallbackDiscarded(
            @ForAll @IntRange(min = 1, max = 3) int ignored) {
        Stage stage = createRunningStage(2);
        int oldGen = stage.currentGeneration();
        stage.taskIds().forEach(id -> stage.onTaskFailed(id, RETRYABLE_EXCHANGE));
        assertThat(stage.tryRestartFailedGeneration(oldGen, 3, RETRYABLE_EXCHANGE.errorCode())).isTrue();
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
     * After restarting the failed generation, onTaskFailed with old generation has no effect.
     */
    @Property(tries = 20)
    void staleGenerationFailedCallbackDiscarded(
            @ForAll @IntRange(min = 1, max = 3) int ignored) {
        Stage stage = createRunningStage(1);
        int oldGen = stage.currentGeneration();
        stage.taskIds().forEach(id -> stage.onTaskFailed(id, RETRYABLE_EXCHANGE));
        assertThat(stage.tryRestartFailedGeneration(oldGen, 3, RETRYABLE_EXCHANGE.errorCode())).isTrue();
        stage.addTaskId("new-0");
        stage.stateMachine().tryTransitionTo(StageState.SCHEDULING);
        stage.stateMachine().tryTransitionTo(StageState.RUNNING);
        stage.onTaskFailed("new-0", NON_RETRYABLE_USER, oldGen);
        assertThat(stage.stateMachine().getState())
                .as("stale FAILED should not trigger stage failure")
                .isEqualTo(StageState.RUNNING);
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
