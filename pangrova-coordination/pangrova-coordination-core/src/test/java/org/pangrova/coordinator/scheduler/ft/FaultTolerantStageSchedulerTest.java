package org.pangrova.coordinator.scheduler.ft;

import java.util.List;
import java.util.Optional;

import org.pangrova.scheduler.core.admission.StageScheduler;
import org.pangrova.coordinator.scheduler.ft.FaultTolerantStageScheduler.FailureAction;
import org.pangrova.coordinator.scheduler.ft.FaultTolerantStageScheduler.FailureActionType;
import org.pangrova.shuffle.scheduling.AdaptiveRepartitionTrigger;
import org.pangrova.shuffle.scheduling.AdaptiveRepartitionTrigger.RepartitionDecision;
import org.pangrova.scheduler.api.placement.FaultTolerancePolicy;
import org.pangrova.scheduler.api.placement.RetryBudget;
import org.pangrova.shuffle.api.ShuffleCapabilities;
import org.pangrova.shuffle.api.ShuffleDurability;
import org.pangrova.shuffle.api.ShufflePartitionStats;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Unit tests for {@link FaultTolerantStageScheduler} — policy-based failure
 * handling, retry budget enforcement, and adaptive repartition.
 */
class FaultTolerantStageSchedulerTest {

    private final StageScheduler baseScheduler = new StageScheduler();

    // ---- NONE policy tests ----

    @Test
    void nonePolicy_failsQueryImmediately() {
        FaultTolerantStageScheduler scheduler = create(
                FaultTolerancePolicy.NONE,
                RetryBudget.withDefaults(),
                ShuffleCapabilities.of(ShuffleDurability.LOCAL_SPILL),
                null);

        FailureAction action = scheduler.handleTaskFailure("task-1", "stage-0");

        assertThat(action.type()).isEqualTo(FailureActionType.FAIL_QUERY);
        assertThat(action.reason()).contains("NONE");
    }

    // ---- TASK_RETRY policy tests ----

    @Test
    void taskRetryPolicy_retriesWhenShuffleSupportsDurability() {
        FaultTolerantStageScheduler scheduler = create(
                FaultTolerancePolicy.TASK_RETRY,
                RetryBudget.withDefaults(),
                ShuffleCapabilities.of(ShuffleDurability.LOCAL_SPILL),
                null);

        FailureAction action = scheduler.handleTaskFailure("task-1", "stage-0");

        assertThat(action.type()).isEqualTo(FailureActionType.RETRY_TASK);
        assertThat(action.attemptNumber()).isEqualTo(1);
    }

    @Test
    void taskRetryPolicy_failsWhenShuffleIsEphemeral() {
        FaultTolerantStageScheduler scheduler = create(
                FaultTolerancePolicy.TASK_RETRY,
                RetryBudget.withDefaults(),
                ShuffleCapabilities.of(ShuffleDurability.EPHEMERAL),
                null);

        FailureAction action = scheduler.handleTaskFailure("task-1", "stage-0");

        assertThat(action.type()).isEqualTo(FailureActionType.FAIL_QUERY);
        assertThat(action.reason()).contains("EPHEMERAL");
    }

    @Test
    void taskRetryPolicy_incrementsAttemptCounter() {
        FaultTolerantStageScheduler scheduler = create(
                FaultTolerancePolicy.TASK_RETRY,
                RetryBudget.withDefaults(),
                ShuffleCapabilities.of(ShuffleDurability.DURABLE),
                null);

        FailureAction first = scheduler.handleTaskFailure("task-1", "stage-0");
        FailureAction second = scheduler.handleTaskFailure("task-1", "stage-0");
        FailureAction third = scheduler.handleTaskFailure("task-1", "stage-0");

        assertThat(first.attemptNumber()).isEqualTo(1);
        assertThat(second.attemptNumber()).isEqualTo(2);
        assertThat(third.attemptNumber()).isEqualTo(3);
    }

    @Test
    void taskRetryPolicy_exhaustsPerTaskBudget() {
        RetryBudget budget = RetryBudget.builder()
                .maxRetriesPerTask(2)
                .maxRetriesPerStage(100)
                .maxRetriesPerQuery(100)
                .build();

        FaultTolerantStageScheduler scheduler = create(
                FaultTolerancePolicy.TASK_RETRY, budget,
                ShuffleCapabilities.of(ShuffleDurability.LOCAL_SPILL), null);

        // 2 retries succeed
        assertThat(scheduler.handleTaskFailure("task-1", "stage-0").type())
                .isEqualTo(FailureActionType.RETRY_TASK);
        assertThat(scheduler.handleTaskFailure("task-1", "stage-0").type())
                .isEqualTo(FailureActionType.RETRY_TASK);

        // 3rd exceeds budget
        FailureAction third = scheduler.handleTaskFailure("task-1", "stage-0");
        assertThat(third.type()).isEqualTo(FailureActionType.FAIL_QUERY);
        assertThat(third.reason()).contains("Task retry budget exhausted");
    }

    @Test
    void taskRetryPolicy_exhaustsPerStageBudget() {
        RetryBudget budget = RetryBudget.builder()
                .maxRetriesPerTask(10)
                .maxRetriesPerStage(2)
                .maxRetriesPerQuery(100)
                .build();

        FaultTolerantStageScheduler scheduler = create(
                FaultTolerancePolicy.TASK_RETRY, budget,
                ShuffleCapabilities.of(ShuffleDurability.LOCAL_SPILL), null);

        // Different tasks in same stage
        assertThat(scheduler.handleTaskFailure("task-1", "stage-0").type())
                .isEqualTo(FailureActionType.RETRY_TASK);
        assertThat(scheduler.handleTaskFailure("task-2", "stage-0").type())
                .isEqualTo(FailureActionType.RETRY_TASK);

        // 3rd retry in same stage exceeds stage budget
        FailureAction third = scheduler.handleTaskFailure("task-3", "stage-0");
        assertThat(third.type()).isEqualTo(FailureActionType.FAIL_QUERY);
        assertThat(third.reason()).contains("Stage retry budget exhausted");
    }

    @Test
    void taskRetryPolicy_exhaustsPerQueryBudget() {
        RetryBudget budget = RetryBudget.builder()
                .maxRetriesPerTask(10)
                .maxRetriesPerStage(10)
                .maxRetriesPerQuery(2)
                .build();

        FaultTolerantStageScheduler scheduler = create(
                FaultTolerancePolicy.TASK_RETRY, budget,
                ShuffleCapabilities.of(ShuffleDurability.LOCAL_SPILL), null);

        // Retries in different stages
        assertThat(scheduler.handleTaskFailure("task-1", "stage-0").type())
                .isEqualTo(FailureActionType.RETRY_TASK);
        assertThat(scheduler.handleTaskFailure("task-2", "stage-1").type())
                .isEqualTo(FailureActionType.RETRY_TASK);

        // 3rd retry across query exceeds query budget
        FailureAction third = scheduler.handleTaskFailure("task-3", "stage-2");
        assertThat(third.type()).isEqualTo(FailureActionType.FAIL_QUERY);
        assertThat(third.reason()).contains("Query retry budget exhausted");
    }

    @Test
    void taskRetryPolicy_independentTaskCounters() {
        RetryBudget budget = RetryBudget.builder()
                .maxRetriesPerTask(2)
                .maxRetriesPerStage(100)
                .maxRetriesPerQuery(100)
                .build();

        FaultTolerantStageScheduler scheduler = create(
                FaultTolerancePolicy.TASK_RETRY, budget,
                ShuffleCapabilities.of(ShuffleDurability.LOCAL_SPILL), null);

        // Each task has its own counter
        scheduler.handleTaskFailure("task-1", "stage-0");
        scheduler.handleTaskFailure("task-1", "stage-0");
        scheduler.handleTaskFailure("task-2", "stage-0");
        scheduler.handleTaskFailure("task-2", "stage-0");

        // task-1 budget exhausted, but task-2 still has room at exactly 2
        FailureAction task1 = scheduler.handleTaskFailure("task-1", "stage-0");
        assertThat(task1.type()).isEqualTo(FailureActionType.FAIL_QUERY);
    }

    // ---- STAGE_RETRY policy tests ----

    @Test
    void stageRetryPolicy_retriesTaskFirstWhenShuffleSupports() {
        FaultTolerantStageScheduler scheduler = create(
                FaultTolerancePolicy.STAGE_RETRY,
                RetryBudget.withDefaults(),
                ShuffleCapabilities.of(ShuffleDurability.LOCAL_SPILL),
                null);

        FailureAction action = scheduler.handleTaskFailure("task-1", "stage-0");

        assertThat(action.type()).isEqualTo(FailureActionType.RETRY_TASK);
    }

    @Test
    void stageRetryPolicy_fallsBackToStageRetryWhenEphemeral() {
        FaultTolerantStageScheduler scheduler = create(
                FaultTolerancePolicy.STAGE_RETRY,
                RetryBudget.withDefaults(),
                ShuffleCapabilities.of(ShuffleDurability.EPHEMERAL),
                null);

        FailureAction action = scheduler.handleTaskFailure("task-1", "stage-0");

        assertThat(action.type()).isEqualTo(FailureActionType.RETRY_STAGE);
    }

    @Test
    void stageRetryPolicy_fallsBackToStageRetryWhenTaskBudgetExhausted() {
        RetryBudget budget = RetryBudget.builder()
                .maxRetriesPerTask(1)
                .maxRetriesPerStage(10)
                .maxRetriesPerQuery(100)
                .build();

        FaultTolerantStageScheduler scheduler = create(
                FaultTolerancePolicy.STAGE_RETRY, budget,
                ShuffleCapabilities.of(ShuffleDurability.LOCAL_SPILL), null);

        // First retry: task retry
        FailureAction first = scheduler.handleTaskFailure("task-1", "stage-0");
        assertThat(first.type()).isEqualTo(FailureActionType.RETRY_TASK);

        // Second retry: task budget exhausted -> stage retry
        FailureAction second = scheduler.handleTaskFailure("task-1", "stage-0");
        assertThat(second.type()).isEqualTo(FailureActionType.RETRY_STAGE);
    }

    @Test
    void stageRetryPolicy_exhaustsStageBudget() {
        RetryBudget budget = RetryBudget.builder()
                .maxRetriesPerTask(0)
                .maxRetriesPerStage(1)
                .maxRetriesPerQuery(100)
                .build();

        FaultTolerantStageScheduler scheduler = create(
                FaultTolerancePolicy.STAGE_RETRY, budget,
                ShuffleCapabilities.of(ShuffleDurability.EPHEMERAL), null);

        // First: stage retry
        FailureAction first = scheduler.handleTaskFailure("task-1", "stage-0");
        assertThat(first.type()).isEqualTo(FailureActionType.RETRY_STAGE);

        // Second: stage budget exhausted
        FailureAction second = scheduler.handleTaskFailure("task-2", "stage-0");
        assertThat(second.type()).isEqualTo(FailureActionType.FAIL_QUERY);
        assertThat(second.reason()).contains("Stage recompute budget exhausted");
    }

    // ---- Reset tests ----

    @Test
    void resetCounters_allowsNewRetries() {
        RetryBudget budget = RetryBudget.builder()
                .maxRetriesPerTask(1)
                .maxRetriesPerStage(1)
                .maxRetriesPerQuery(1)
                .build();

        FaultTolerantStageScheduler scheduler = create(
                FaultTolerancePolicy.TASK_RETRY, budget,
                ShuffleCapabilities.of(ShuffleDurability.LOCAL_SPILL), null);

        scheduler.handleTaskFailure("task-1", "stage-0");
        // Budget exhausted now
        assertThat(scheduler.handleTaskFailure("task-1", "stage-0").type())
                .isEqualTo(FailureActionType.FAIL_QUERY);

        // Reset and retry
        scheduler.resetCounters();
        FailureAction afterReset = scheduler.handleTaskFailure("task-1", "stage-0");
        assertThat(afterReset.type()).isEqualTo(FailureActionType.RETRY_TASK);
    }

    // ---- AQE repartition tests ----

    @Test
    void evaluateRepartition_returnsEmptyWhenNoTrigger() {
        FaultTolerantStageScheduler scheduler = create(
                FaultTolerancePolicy.NONE,
                RetryBudget.withDefaults(),
                ShuffleCapabilities.of(ShuffleDurability.EPHEMERAL),
                null);

        Optional<RepartitionDecision> result = scheduler.evaluateRepartition(
                "stage-0",
                List.of(new ShufflePartitionStats(0, 1024, 100)),
                8);

        assertThat(result).isEmpty();
    }

    @Test
    void evaluateRepartition_delegatesToTrigger() {
        AdaptiveRepartitionTrigger trigger = (stageId, stats, parallelism) ->
                Optional.of(RepartitionDecision.coalesce(2));

        FaultTolerantStageScheduler scheduler = create(
                FaultTolerancePolicy.TASK_RETRY,
                RetryBudget.withDefaults(),
                ShuffleCapabilities.of(ShuffleDurability.LOCAL_SPILL),
                trigger);

        Optional<RepartitionDecision> result = scheduler.evaluateRepartition(
                "stage-0",
                List.of(new ShufflePartitionStats(0, 100, 10)),
                8);

        assertThat(result).isPresent();
        assertThat(result.get().newPartitionCount()).isEqualTo(2);
    }

    @Test
    void evaluateRepartition_propagatesTriggerEmpty() {
        AdaptiveRepartitionTrigger trigger = (stageId, stats, parallelism) ->
                Optional.empty();

        FaultTolerantStageScheduler scheduler = create(
                FaultTolerancePolicy.TASK_RETRY,
                RetryBudget.withDefaults(),
                ShuffleCapabilities.of(ShuffleDurability.LOCAL_SPILL),
                trigger);

        Optional<RepartitionDecision> result = scheduler.evaluateRepartition(
                "stage-0",
                List.of(new ShufflePartitionStats(0, 1024, 100)),
                8);

        assertThat(result).isEmpty();
    }

    // ---- DAG scheduling delegation tests ----

    @Test
    void scheduleStages_delegatesToBaseScheduler() {
        FaultTolerantStageScheduler scheduler = create(
                FaultTolerancePolicy.TASK_RETRY,
                RetryBudget.withDefaults(),
                ShuffleCapabilities.of(ShuffleDurability.LOCAL_SPILL),
                null);

        List<StageScheduler.StageInfo> stages = List.of(
                new StageScheduler.StageInfo("scan", List.of(), false),
                new StageScheduler.StageInfo("agg", List.of("scan"), false),
                new StageScheduler.StageInfo("output", List.of("agg"), true));

        List<String> order = scheduler.scheduleStages(stages);

        assertThat(order).containsExactly("scan", "agg", "output");
    }

    // ---- Constructor validation tests ----

    @Test
    void constructorRejectsNullPolicy() {
        assertThatThrownBy(() -> new FaultTolerantStageScheduler(
                baseScheduler, null, RetryBudget.withDefaults(),
                ShuffleCapabilities.of(ShuffleDurability.EPHEMERAL), null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void constructorRejectsNullBudget() {
        assertThatThrownBy(() -> new FaultTolerantStageScheduler(
                baseScheduler, FaultTolerancePolicy.NONE, null,
                ShuffleCapabilities.of(ShuffleDurability.EPHEMERAL), null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void constructorRejectsNullCapabilities() {
        assertThatThrownBy(() -> new FaultTolerantStageScheduler(
                baseScheduler, FaultTolerancePolicy.NONE,
                RetryBudget.withDefaults(), null, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void accessors_returnConfiguredValues() {
        RetryBudget budget = RetryBudget.withDefaults();
        FaultTolerantStageScheduler scheduler = create(
                FaultTolerancePolicy.TASK_RETRY, budget,
                ShuffleCapabilities.of(ShuffleDurability.LOCAL_SPILL), null);

        assertThat(scheduler.policy()).isEqualTo(FaultTolerancePolicy.TASK_RETRY);
        assertThat(scheduler.retryBudget()).isSameAs(budget);
    }

    // ---- Helpers ----

    private FaultTolerantStageScheduler create(
            FaultTolerancePolicy policy,
            RetryBudget budget,
            ShuffleCapabilities capabilities,
            AdaptiveRepartitionTrigger trigger) {
        return new FaultTolerantStageScheduler(
                baseScheduler, policy, budget, capabilities, trigger);
    }
}
