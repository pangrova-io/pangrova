package org.pangrova.coordinator.query.stage;

import org.pangrova.query.api.enums.StageState;

import org.pangrova.query.api.stage.StageRetryPolicy;
import org.pangrova.query.stage.StageStateMachine;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.planner.api.adaptive.AdaptiveDecision;
import org.pangrova.query.api.task.TaskRetryCallback;
import org.pangrova.query.api.error.QueryFailure;
import org.pangrova.protocol.rest.shuffle.ShuffleStageStatsReport;
import org.pangrova.shuffle.api.ShuffleHandle;
import org.pangrova.shuffle.api.ShuffleCommitReceipt;
import org.pangrova.shuffle.api.ShufflePublication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Stage entity: wraps an immutable {@link StageNode} plan with a
 * {@link StageStateMachine}, task tracking, and retry policy.
 *
 * <p>Thread-safe: task tracking uses ConcurrentHashMap; retry count uses AtomicInteger.
 */
@ThreadSafe
public final class Stage {

    private static final Logger LOG = LoggerFactory.getLogger(Stage.class);

    private final StageNode plan;
    private final StageStateMachine stateMachine;
    private final Set<String> taskIds = ConcurrentHashMap.newKeySet();
    private final Set<String> finishedTaskIds = ConcurrentHashMap.newKeySet();
    private final Set<String> failedTaskIds = ConcurrentHashMap.newKeySet();
    private final StageRetryPolicy retryPolicy;
    private final AtomicInteger retryCount = new AtomicInteger(0);
    /**
     * Generation counter for retry isolation. Incremented on each retry attempt.
     * Task callbacks carry the generation at dispatch time; stale callbacks
     * (generation mismatch) are silently discarded to prevent cross-attempt pollution.
     */
    private final AtomicInteger attemptGeneration = new AtomicInteger(0);
    private volatile QueryFailure lastError;
    private volatile ShuffleStageStatsReport shuffleStatsReport;
    private volatile ShuffleHandle shuffleHandle;
    private volatile long shufflePlanEpoch;
    private volatile ShuffleCommitReceipt shuffleReceipt;
    private volatile org.pangrova.protocol.v2.ExecutionOwnerIdentity shuffleExecutionOwner;
    private volatile AdaptiveDecision adaptiveDecision;

    /** Per-task retry counters for task-level retry before stage-level retry. */
    private final ConcurrentHashMap<String, AtomicInteger> taskRetryCounters =
            new ConcurrentHashMap<>();
    public static final int MAX_TASK_RETRIES = 3;
    private volatile TaskRetryCallback taskRetryCallback;
    /** Callback to cancel all running tasks for this stage before retry. */
    private volatile Consumer<List<String>> taskCancelCallback;

    public Stage(StageNode plan, StageStateMachine stateMachine, StageRetryPolicy retryPolicy) {
        this.plan = checkNotNull(plan, "plan must not be null");
        this.stateMachine = checkNotNull(stateMachine, "stateMachine must not be null");
        this.retryPolicy = checkNotNull(retryPolicy, "retryPolicy must not be null");
    }

    /** Convenience factory: creates Stage with default retry policy. */
    public static Stage create(StageNode plan, String queryId) {
        String stageId = String.valueOf(plan.stageId());
        StageStateMachine sm = new StageStateMachine(stageId, queryId);
        return new Stage(plan, sm, StageRetryPolicy.DEFAULT);
    }

    /** Register a task ID belonging to this Stage. */
    public void addTaskId(String taskId) {
        checkNotNull(taskId, "taskId must not be null");
        taskIds.add(taskId);
    }

    /**
     * Called when a task in this Stage finishes successfully.
     * Callbacks from stale generations (prior retry attempts) are discarded.
     * If all tasks are finished, transitions the Stage to FLUSHING then FINISHED.
     *
     * @param taskId     the finished task
     * @param generation the attempt generation at dispatch time
     */
    public void onTaskFinished(String taskId, int generation) {
        checkNotNull(taskId, "taskId must not be null");
        if (generation != attemptGeneration.get()) {
            return; // stale callback from a previous attempt — discard
        }
        if (!taskIds.contains(taskId)) {
            return; // replaced by a prepared task retry; the replacement owns the slot
        }
        finishedTaskIds.add(taskId);
        failedTaskIds.remove(taskId);
        if (allTasksFinished()) {
            stateMachine.tryTransitionTo(StageState.FLUSHING);
            stateMachine.tryTransitionTo(StageState.FINISHED);
        } else if (allTasksReported() && !failedTaskIds.isEmpty()) {
            stateMachine.tryTransitionTo(StageState.FAILED);
        }
    }

    /** Convenience overload for callers that do not track generation (defaults to current). */
    public void onTaskFinished(String taskId) {
        onTaskFinished(taskId, attemptGeneration.get());
    }

    /**
     * Called when a task in this Stage fails.
     * Callbacks from stale generations (prior retry attempts) are discarded.
     *
     * @param taskId     the failed task
     * @param error      error detail
     * @param generation the attempt generation at dispatch time
     */
    public void onTaskFailed(String taskId, QueryFailure error, int generation) {
        checkNotNull(taskId, "taskId must not be null");
        if (generation != attemptGeneration.get()) {
            return; // stale callback from a previous attempt — discard
        }
        if (!taskIds.contains(taskId)) {
            return; // replaced by a prepared task retry; the replacement owns the slot
        }
        failedTaskIds.add(taskId);
        this.lastError = error;
        if (allTasksReported()) {
            // Always transition to FAILED; RetryStageHandler (via DAGDispatchEngine
            // state listener) will decide whether to retry and reset to PLANNED.
            stateMachine.tryTransitionTo(StageState.FAILED);
        }
    }

    /** Convenience overload for callers that do not track generation (defaults to current). */
    public void onTaskFailed(String taskId, QueryFailure error) {
        onTaskFailed(taskId, error, attemptGeneration.get());
    }

    /** Returns true if the Stage can be retried based on its retry policy. */
    public boolean shouldRetry() {
        return retryPolicy.canRetry(retryCount.get());
    }

    /**
     * Atomically checks retry eligibility and prepares for a new attempt.
     * Returns true if retry was prepared, false if retry limit is exhausted.
     * Merges shouldRetry+prepareRetry into a single synchronized operation
     * to prevent concurrent callers from both seeing canRetry=true and
     * exceeding maxRetries.
     *
     * <p>Cancels all tasks from the previous attempt before clearing state,
     * preventing stale tasks from continuing to consume Worker resources
     * or writing to shuffle partitions concurrently with the new attempt.</p>
     */
    public synchronized boolean tryPrepareRetry() {
        if (!retryPolicy.canRetry(retryCount.get())) {
            return false;
        }
        // Cancel old tasks before incrementing generation to prevent
        // concurrent writes from stale and new tasks to the same
        // shuffle partitions.
        cancelOldTasks();
        retryCount.incrementAndGet();
        attemptGeneration.incrementAndGet();
        finishedTaskIds.clear();
        failedTaskIds.clear();
        taskIds.clear();
        taskRetryCounters.clear();
        return true;
    }

    private void cancelOldTasks() {
        Consumer<List<String>> callback = this.taskCancelCallback;
        if (callback != null && !taskIds.isEmpty()) {
            List<String> tasksToCancel = List.copyOf(taskIds);
            try {
                callback.accept(tasksToCancel);
            } catch (Exception e) {
                LOG.warn("event=cancel_old_tasks_failed stageId={} taskCount={} error={}",
                        stageId(), tasksToCancel.size(), e.getMessage());
            }
        }
    }

    /**
     * Reserves one task slot for a retry of {@code failedTaskId} before the
     * replacement is dispatched. The stage keeps the same size, so a report for
     * the dead task can no longer complete (or fail) the stage prematurely.
     *
     * <p>Unlike {@link #tryPrepareRetry()} this does not increment the stage
     * generation or clear sibling state: only one logical task is being
     * replaced, not the whole stage. The retry budget follows the lineage by
     * transferring the counter to the new id.</p>
     *
     * @return true when the slot was swapped; false when the budget is exhausted
     *         or the failed id is not owned by this stage
     */
    public synchronized boolean tryPrepareTaskRetry(
            String failedTaskId, String retryTaskId, int maxRetries) {
        checkNotNull(failedTaskId, "failedTaskId must not be null");
        checkNotNull(retryTaskId, "retryTaskId must not be null");
        if (maxRetries < 0) {
            throw new IllegalArgumentException("maxRetries must not be negative");
        }
        if (!taskIds.contains(failedTaskId)) {
            return false;
        }
        AtomicInteger counter = taskRetryCounters.computeIfAbsent(failedTaskId, ignored -> new AtomicInteger());
        if (counter.get() >= maxRetries || !taskIds.remove(failedTaskId)) {
            return false;
        }
        counter.incrementAndGet();
        taskRetryCounters.remove(failedTaskId);
        failedTaskIds.remove(failedTaskId);
        finishedTaskIds.remove(failedTaskId);
        taskIds.add(retryTaskId);
        taskRetryCounters.put(retryTaskId, counter);
        LOG.info("event=stage_task_retry_prepared stageId={} failedTaskId={} retryTaskId={} used={} max={}",
                stageId(), failedTaskId, retryTaskId, counter.get(), maxRetries);
        return true;
    }

    /** Reverts an uncommitted {@link #tryPrepareTaskRetry} after dispatch failed. */
    public synchronized void rollbackTaskRetry(String failedTaskId, String retryTaskId) {
        if (!taskIds.remove(retryTaskId)) {
            return;
        }
        AtomicInteger counter = taskRetryCounters.remove(retryTaskId);
        if (counter != null) {
            counter.decrementAndGet();
            taskRetryCounters.put(failedTaskId, counter);
        }
        taskIds.add(failedTaskId);
        failedTaskIds.add(failedTaskId);
        LOG.info("event=stage_task_retry_rolled_back stageId={} failedTaskId={} retryTaskId={}",
                stageId(), failedTaskId, retryTaskId);
    }

    /** Sets the callback for task-level retry dispatch. */
    public void setTaskRetryCallback(TaskRetryCallback callback) {
        this.taskRetryCallback = callback;
    }

    /** Returns the task retry callback, or null if not set. */
    public TaskRetryCallback taskRetryCallback() {
        return taskRetryCallback;
    }

    /**
     * Sets the callback invoked to cancel all running tasks before a
     * stage-level retry. The callback receives the list of task IDs
     * from the previous attempt that should be cancelled on their
     * respective Workers.
     */
    public void setTaskCancelCallback(Consumer<List<String>> callback) {
        this.taskCancelCallback = callback;
    }

    /**
     * Increments and returns the retry count for a specific task.
     * Thread-safe via ConcurrentHashMap + AtomicInteger.
     */
    public int incrementTaskRetryCount(String taskId) {
        return taskRetryCounters
                .computeIfAbsent(taskId, k -> new AtomicInteger(0))
                .incrementAndGet();
    }

    /** Returns the current attempt generation for dispatch-time capture. */
    public int currentGeneration() {
        return attemptGeneration.get();
    }

    public StageNode plan() {
        return plan;
    }

    public StageStateMachine stateMachine() {
        return stateMachine;
    }

    public List<String> taskIds() {
        return List.copyOf(taskIds);
    }

    public StageRetryPolicy retryPolicy() {
        return retryPolicy;
    }

    public int retryCount() {
        return retryCount.get();
    }

    public QueryFailure lastError() {
        return lastError;
    }

    /** Stores real shuffle statistics reported by Worker after stage completion. */
    public void setShuffleStatsReport(ShuffleStageStatsReport report) {
        this.shuffleStatsReport = report;
    }

    /** Returns the Worker-reported shuffle statistics, or null if not yet received. */
    public ShuffleStageStatsReport shuffleStatsReport() {
        return shuffleStatsReport;
    }

    /**
     * Binds the external shuffle allocation for the current stage generation.
     * A newer generation replaces the failed generation's allocation; stale
     * generations and allocation changes within one generation are rejected.
     */
    public synchronized void bindShuffleAllocation(ShuffleHandle handle, long planEpoch) {
        checkNotNull(handle, "handle must not be null");
        if (planEpoch <= 0) {
            throw new IllegalArgumentException("shuffle planEpoch must be positive");
        }
        if (shufflePlanEpoch > planEpoch) {
            throw new IllegalStateException("stale shuffle allocation generation");
        }
        if (shufflePlanEpoch == planEpoch && shuffleHandle != null
                && !shuffleHandle.shuffleId().equals(handle.shuffleId())) {
            throw new IllegalStateException(
                    "shuffle allocation changed within one stage generation");
        }
        if (shufflePlanEpoch < planEpoch) {
            shuffleExecutionOwner = null;
            shuffleStatsReport = null;
            shuffleReceipt = null;
        }
        shuffleHandle = handle;
        shufflePlanEpoch = planEpoch;
    }

    /** Captures the allocation and epoch under the same lock used to replace them. */
    public synchronized ShufflePublication shufflePublication(String queryId) {
        if (shuffleHandle == null) throw new IllegalStateException("Stage has no registered shuffle allocation");
        return new ShufflePublication(queryId, stageId(), shufflePlanEpoch, shuffleHandle.shuffleId());
    }

    /** Binds verified publication evidence before dispatch admits downstream readers. */
    public synchronized void bindShuffleReceipt(ShufflePublication requested, ShuffleCommitReceipt receipt) {
        if (!receipt.publication().equals(requested) || requested.stageId() != stageId()
                || requested.generation() != shufflePlanEpoch || shuffleHandle == null
                || !requested.allocationId().equals(shuffleHandle.shuffleId())
                || receipt.producerCount() != shuffleHandle.expectedProducerCount()
                || !receipt.producerSetSha256().equals(java.util.HexFormat.of().formatHex(shuffleHandle.expectedProducerSetChecksum()))) {
            throw new IllegalStateException("Shuffle receipt does not match registered allocation and producer set");
        }
        shuffleHandle.validatePartitionCoverage(receipt.partitions().size());
        if (shuffleReceipt != null && !shuffleReceipt.equals(receipt)) {
            throw new IllegalStateException("Conflicting shuffle publication for the same allocation");
        }
        shuffleReceipt = receipt;
    }

    /** Direct outputs become available through issued tickets; a registered external output requires publication. */
    public synchronized boolean exchangeReadyForConsumers() {
        return shuffleHandle == null || shuffleReceipt != null;
    }

    /** Captures owner, routing and publication from one generation even while another thread retries a stage. */
    public synchronized PublishedShuffle publishedShuffle() {
        var receipt = requireShuffleReceipt();
        if (shuffleExecutionOwner == null) {
            throw new IllegalStateException("External shuffle allocation is missing producer identity: stage=" + stageId());
        }
        return new PublishedShuffle(shuffleHandle, shuffleExecutionOwner, receipt);
    }

    public record PublishedShuffle(ShuffleHandle handle, org.pangrova.protocol.v2.ExecutionOwnerIdentity owner,
            ShuffleCommitReceipt receipt) {}

    public ShuffleCommitReceipt requireShuffleReceipt() {
        var receipt = shuffleReceipt;
        if (receipt == null) throw new IllegalStateException("Shuffle allocation has not been published");
        return receipt;
    }

    /** Returns the registered shuffle allocation, or null before scheduling. */
    public ShuffleHandle shuffleHandle() {
        return shuffleHandle;
    }

    /** Binds the immutable producer identity governing this stage's shuffle allocation. */
    public synchronized void bindShuffleExecutionIdentity(
            long planEpoch,
            org.pangrova.protocol.v2.ExecutionOwnerIdentity executionOwner) {
        checkNotNull(executionOwner, "executionOwner must not be null");
        if (planEpoch <= 0 || executionOwner.getFencingToken() <= 0) {
            throw new IllegalArgumentException(
                    "shuffle execution identity must be fenced");
        }
        if (shufflePlanEpoch != planEpoch) {
            throw new IllegalStateException(
                    "task planEpoch does not own the stage shuffle allocation");
        }
        if (shuffleExecutionOwner != null
                && !shuffleExecutionOwner.equals(executionOwner)) {
            throw new IllegalStateException(
                    "stage shuffle execution owner changed after binding");
        }
        shuffleExecutionOwner = executionOwner;
    }

    /** Returns the physical-plan epoch owning the shuffle allocation. */
    public long shufflePlanEpoch() {
        return shufflePlanEpoch;
    }

    /** Returns the fenced owner of the shuffle allocation, or null before dispatch. */
    public org.pangrova.protocol.v2.ExecutionOwnerIdentity shuffleExecutionOwner() {
        return shuffleExecutionOwner;
    }

    /** Stores the AQE decision produced after this stage completed. */
    public void setAdaptiveDecision(AdaptiveDecision decision) {
        this.adaptiveDecision = decision;
    }

    /** Returns the AQE decision for this stage, or null if AQE was disabled or not yet evaluated. */
    public AdaptiveDecision adaptiveDecision() {
        return adaptiveDecision;
    }

    public int stageId() {
        return plan.stageId();
    }

    private boolean allTasksFinished() {
        return !taskIds.isEmpty() && finishedTaskIds.containsAll(taskIds);
    }

    private boolean allTasksReported() {
        if (taskIds.isEmpty()) {
            return false;
        }
        int reported = finishedTaskIds.size() + failedTaskIds.size();
        return reported >= taskIds.size();
    }
}
