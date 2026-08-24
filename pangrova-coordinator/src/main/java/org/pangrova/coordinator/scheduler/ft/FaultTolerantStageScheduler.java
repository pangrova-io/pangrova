package org.pangrova.coordinator.scheduler.ft;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.pangrova.coordinator.scheduler.StageScheduler;
import org.pangrova.shuffle.scheduling.AdaptiveRepartitionTrigger;
import org.pangrova.shuffle.scheduling.AdaptiveRepartitionTrigger.RepartitionDecision;
import org.pangrova.spi.execution.scheduling.FaultTolerancePolicy;
import org.pangrova.spi.execution.scheduling.RetryBudget;
import org.pangrova.shuffle.ShuffleCapabilities;
import org.pangrova.shuffle.ShufflePartitionStats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Extends the base {@link StageScheduler} with fault-tolerant execution (FTE)
 * and adaptive repartitioning capabilities.
 *
 * <p>When a task fails, this scheduler consults the configured
 * {@link FaultTolerancePolicy} and {@link ShuffleCapabilities} to decide
 * whether to retry the task, recompute the upstream stage, or fail the query.
 *
 * <h3>Decision matrix</h3>
 * <pre>
 * Policy       | supportsTaskRetry() | Action
 * -------------|---------------------|-----------------------------------
 * NONE         | any                 | Fail query immediately
 * TASK_RETRY   | true                | Retry task (up to budget)
 * TASK_RETRY   | false               | Fail query (shuffle too ephemeral)
 * STAGE_RETRY  | true                | Retry task first, then stage
 * STAGE_RETRY  | false               | Retry entire upstream stage
 * </pre>
 *
 * <p>Retry counters are tracked per-task, per-stage, and per-query. When any
 * budget is exhausted, the query fails with an informative error.</p>
 *
 * <p>After each stage completes, the optional {@link AdaptiveRepartitionTrigger}
 * is invoked with actual partition statistics to enable partition-level AQE
 * (coalescing small partitions or splitting skewed ones).</p>
 */
public class FaultTolerantStageScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(FaultTolerantStageScheduler.class);

    private final StageScheduler stageScheduler;
    private final FaultTolerancePolicy policy;
    private final RetryBudget retryBudget;
    private final ShuffleCapabilities shuffleCapabilities;
    private final AdaptiveRepartitionTrigger repartitionTrigger;

    // Retry counters: taskId -> attempt count
    private final Map<String, AtomicInteger> taskRetryCounters = new ConcurrentHashMap<>();
    // Stage-level retry counters: stageId -> total retries in stage
    private final Map<String, AtomicInteger> stageRetryCounters = new ConcurrentHashMap<>();
    // Query-level retry counter
    private final AtomicInteger queryRetryCounter = new AtomicInteger(0);

    /**
     * Creates a fault-tolerant stage scheduler.
     *
     * @param stageScheduler       the underlying DAG scheduler for topological ordering
     * @param policy               the fault-tolerance policy to apply
     * @param retryBudget          retry limits and backoff configuration
     * @param shuffleCapabilities  capabilities of the active shuffle provider
     * @param repartitionTrigger   optional AQE trigger (nullable — no AQE if null)
     */
    public FaultTolerantStageScheduler(
            StageScheduler stageScheduler,
            FaultTolerancePolicy policy,
            RetryBudget retryBudget,
            ShuffleCapabilities shuffleCapabilities,
            AdaptiveRepartitionTrigger repartitionTrigger) {
        this.stageScheduler = checkNotNull(stageScheduler, "stageScheduler must not be null");
        this.policy = checkNotNull(policy, "policy must not be null");
        this.retryBudget = checkNotNull(retryBudget, "retryBudget must not be null");
        this.shuffleCapabilities = checkNotNull(shuffleCapabilities,
                "shuffleCapabilities must not be null");
        this.repartitionTrigger = repartitionTrigger; // nullable
    }

    /** Returns the configured fault-tolerance policy. */
    public FaultTolerancePolicy policy() {
        return policy;
    }

    /** Returns the retry budget. */
    public RetryBudget retryBudget() {
        return retryBudget;
    }

    /**
     * Delegates to the underlying {@link StageScheduler} for topological ordering.
     */
    public List<String> scheduleStages(List<StageScheduler.StageInfo> stages) {
        return stageScheduler.scheduleStages(stages);
    }

    /**
     * Handles a task failure according to the configured policy and shuffle capabilities.
     *
     * @param taskId  the failed task identifier
     * @param stageId the stage containing the failed task
     * @return the recovery action the caller should take
     */
    public FailureAction handleTaskFailure(String taskId, String stageId) {
        checkNotNull(taskId, "taskId must not be null");
        checkNotNull(stageId, "stageId must not be null");

        LOG.info("event=task_failure_handling taskId={} stageId={} policy={}",
                taskId, stageId, policy);

        return switch (policy) {
            case NONE -> {
                LOG.info("event=query_fail_no_retry taskId={} stageId={} reason=policy_none",
                        taskId, stageId);
                yield FailureAction.failQuery("Fault tolerance disabled (policy=NONE)");
            }
            case TASK_RETRY -> handleTaskRetryPolicy(taskId, stageId);
            case STAGE_RETRY -> handleStageRetryPolicy(taskId, stageId);
        };
    }

    private FailureAction handleTaskRetryPolicy(String taskId, String stageId) {
        if (!shuffleCapabilities.supportsTaskRetry()) {
            LOG.warn("event=query_fail_no_task_retry taskId={} stageId={} "
                    + "reason=shuffle_does_not_support_task_retry durability={}",
                    taskId, stageId, shuffleCapabilities.durability());
            return FailureAction.failQuery(
                    "Shuffle durability " + shuffleCapabilities.durability()
                    + " does not support task retry");
        }
        return attemptTaskRetry(taskId, stageId);
    }

    private FailureAction handleStageRetryPolicy(String taskId, String stageId) {
        // STAGE_RETRY: try task retry first if shuffle supports it
        if (shuffleCapabilities.supportsTaskRetry()) {
            FailureAction taskAction = attemptTaskRetry(taskId, stageId);
            if (taskAction.type() != FailureActionType.FAIL_QUERY) {
                return taskAction;
            }
            // Task budget exhausted — fall through to stage retry
        }
        return attemptStageRetry(taskId, stageId);
    }

    private FailureAction attemptTaskRetry(String taskId, String stageId) {
        int taskAttempt = taskRetryCounters
                .computeIfAbsent(taskId, k -> new AtomicInteger(0))
                .incrementAndGet();

        if (taskAttempt > retryBudget.maxRetriesPerTask()) {
            LOG.warn("event=task_retry_budget_exhausted taskId={} stageId={} "
                    + "attempts={} maxPerTask={}",
                    taskId, stageId, taskAttempt, retryBudget.maxRetriesPerTask());
            return FailureAction.failQuery(
                    "Task retry budget exhausted for task " + taskId
                    + " (attempts=" + taskAttempt + ", max=" + retryBudget.maxRetriesPerTask() + ")");
        }

        int stageTotal = stageRetryCounters
                .computeIfAbsent(stageId, k -> new AtomicInteger(0))
                .incrementAndGet();

        if (stageTotal > retryBudget.maxRetriesPerStage()) {
            LOG.warn("event=stage_retry_budget_exhausted taskId={} stageId={} "
                    + "stageRetries={} maxPerStage={}",
                    taskId, stageId, stageTotal, retryBudget.maxRetriesPerStage());
            return FailureAction.failQuery(
                    "Stage retry budget exhausted for stage " + stageId
                    + " (retries=" + stageTotal + ", max=" + retryBudget.maxRetriesPerStage() + ")");
        }

        int queryTotal = queryRetryCounter.incrementAndGet();
        if (queryTotal > retryBudget.maxRetriesPerQuery()) {
            LOG.warn("event=query_retry_budget_exhausted taskId={} stageId={} "
                    + "queryRetries={} maxPerQuery={}",
                    taskId, stageId, queryTotal, retryBudget.maxRetriesPerQuery());
            return FailureAction.failQuery(
                    "Query retry budget exhausted (retries=" + queryTotal
                    + ", max=" + retryBudget.maxRetriesPerQuery() + ")");
        }

        LOG.info("event=task_retry_scheduled taskId={} stageId={} attempt={} "
                + "backoff={}", taskId, stageId, taskAttempt,
                retryBudget.computeBackoff(taskAttempt));
        return FailureAction.retryTask(taskAttempt);
    }

    private FailureAction attemptStageRetry(String taskId, String stageId) {
        int stageTotal = stageRetryCounters
                .computeIfAbsent(stageId, k -> new AtomicInteger(0))
                .incrementAndGet();

        if (stageTotal > retryBudget.maxRetriesPerStage()) {
            LOG.warn("event=stage_recompute_budget_exhausted taskId={} stageId={} "
                    + "stageRetries={} maxPerStage={}",
                    taskId, stageId, stageTotal, retryBudget.maxRetriesPerStage());
            return FailureAction.failQuery(
                    "Stage recompute budget exhausted for stage " + stageId
                    + " (retries=" + stageTotal + ", max=" + retryBudget.maxRetriesPerStage() + ")");
        }

        int queryTotal = queryRetryCounter.incrementAndGet();
        if (queryTotal > retryBudget.maxRetriesPerQuery()) {
            LOG.warn("event=query_retry_budget_exhausted_on_stage_retry taskId={} stageId={} "
                    + "queryRetries={} maxPerQuery={}",
                    taskId, stageId, queryTotal, retryBudget.maxRetriesPerQuery());
            return FailureAction.failQuery(
                    "Query retry budget exhausted during stage retry (retries=" + queryTotal
                    + ", max=" + retryBudget.maxRetriesPerQuery() + ")");
        }

        LOG.info("event=stage_recompute_scheduled taskId={} stageId={} stageRetryCount={}",
                taskId, stageId, stageTotal);
        return FailureAction.retryStage();
    }

    /**
     * Invokes the adaptive repartition trigger after a stage completes.
     * Returns the repartition decision, or empty if no adjustment is needed
     * or no trigger is configured.
     *
     * @param stageId            the completed stage identifier
     * @param partitionStats     per-partition statistics
     * @param currentParallelism the current downstream parallelism
     * @return repartition decision, or empty
     */
    public Optional<RepartitionDecision> evaluateRepartition(
            String stageId,
            List<ShufflePartitionStats> partitionStats,
            int currentParallelism) {
        if (repartitionTrigger == null) {
            return Optional.empty();
        }
        checkNotNull(stageId, "stageId must not be null");
        checkNotNull(partitionStats, "partitionStats must not be null");

        Optional<RepartitionDecision> decision =
                repartitionTrigger.evaluate(stageId, partitionStats, currentParallelism);

        decision.ifPresent(d ->
                LOG.info("event=adaptive_repartition stageId={} oldParallelism={} "
                        + "newPartitionCount={} reason={}",
                        stageId, currentParallelism, d.newPartitionCount(), d.reason()));

        return decision;
    }

    /**
     * Resets all retry counters. Called when a query is complete or cancelled.
     */
    public void resetCounters() {
        taskRetryCounters.clear();
        stageRetryCounters.clear();
        queryRetryCounter.set(0);
    }

    /**
     * Resets retry counters for a specific stage after it completes successfully.
     * Prevents early-stage failures from consuming the retry budget of later stages.
     *
     * @param stageId the successfully completed stage identifier
     */
    public void resetStageCounters(String stageId) {
        stageRetryCounters.remove(stageId);
        // Remove task-level counters belonging to this stage.
        // Task IDs are prefixed with stageId by convention (stageId-taskIndex).
        taskRetryCounters.keySet().removeIf(taskId -> taskId.startsWith(stageId + "-"));
        LOG.debug("event=stage_counters_reset stageId={}", stageId);
    }

    // -- Inner types --

    /** The type of recovery action to take after a failure. */
    public enum FailureActionType {
        /** Retry the individual task on the same or different worker. */
        RETRY_TASK,
        /** Recompute the entire upstream stage from scratch. */
        RETRY_STAGE,
        /** Fail the entire query — no further recovery possible. */
        FAIL_QUERY
    }

    /**
     * Describes the recovery action the scheduler should take after a task failure.
     *
     * @param type         the action type
     * @param attemptNumber the retry attempt number (for RETRY_TASK), or 0
     * @param reason       human-readable reason (for FAIL_QUERY), or empty
     */
    public record FailureAction(FailureActionType type, int attemptNumber, String reason) {

        static FailureAction retryTask(int attempt) {
            return new FailureAction(FailureActionType.RETRY_TASK, attempt, "");
        }

        static FailureAction retryStage() {
            return new FailureAction(FailureActionType.RETRY_STAGE, 0, "");
        }

        static FailureAction failQuery(String reason) {
            return new FailureAction(FailureActionType.FAIL_QUERY, 0, reason);
        }
    }
}
