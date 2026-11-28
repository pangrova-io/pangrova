package org.pangrova.coordinator.query.stage;

import org.pangrova.query.api.enums.StageState;

import org.pangrova.query.api.stage.StageRetryPolicy;
import org.pangrova.query.stage.StageStateMachine;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.query.api.error.QueryFailure;
import org.pangrova.shuffle.api.ShuffleHandle;
import org.pangrova.shuffle.api.ShuffleCommitReceipt;
import org.pangrova.query.api.stage.StageCompletionCallback.BuildKeyRange;
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
    private volatile ShuffleHandle shuffleHandle;
    private volatile long shufflePlanEpoch;
    private volatile ShuffleCommitReceipt shuffleReceipt;
    private volatile org.pangrova.protocol.v2.ExecutionOwnerIdentity shuffleExecutionOwner;
    private volatile org.pangrova.planner.api.adaptive.CoalesceResult coalescedInputs;

    /** Per-task retry counters for task-level retry before stage-level retry. */
    private final ConcurrentHashMap<String, AtomicInteger> taskRetryCounters =
            new ConcurrentHashMap<>();
    /** Protected by this Stage. State listeners run after this lock is released. */
    private boolean terminalDecisionClaimed;
    private boolean clientResultPublished;
    private boolean replayUnsafeWrite;
    private boolean consumerAdmitted;
    private Map<Integer, List<BuildKeyRange>> buildKeyRanges;
    private int buildKeyReports;

    /** Pins this generation when a consumer begins to bind its issued producer records. */
    public synchronized boolean admitDownstream() {
        if (terminalDecisionClaimed && !failedTaskIds.isEmpty()) return false;
        StageState state = stateMachine.getState();
        if (state != StageState.RUNNING && state != StageState.FLUSHING && state != StageState.FINISHED) {
            return false;
        }
        if (!exchangeReadyForConsumers()) return false;
        consumerAdmitted = true;
        return true;
    }

    /** Freeze the physical cohort before allocating its shuffle output or issuing any task. */
    public synchronized void freezeCoalescedInputs(org.pangrova.planner.api.adaptive.CoalesceResult inputs) {
        if (inputs.isCoalesced() && inputs.originalCount() != plan.parallelism()) {
            throw new IllegalArgumentException("Coalesced inputs must cover the original stage partitions");
        }
        if (coalescedInputs != null && !coalescedInputs.equals(inputs)) {
            throw new IllegalStateException("A stage retry cannot change its admitted input ranges");
        }
        coalescedInputs = inputs;
    }

    public java.util.List<org.pangrova.planner.api.adaptive.PartitionGroup> coalescedInputRanges() {
        var inputs = coalescedInputs;
        return inputs == null ? java.util.List.of() : inputs.groups();
    }

    public int physicalTaskCount() {
        var inputs = coalescedInputs;
        return inputs == null || !inputs.isCoalesced() ? plan.parallelism() : inputs.coalescedCount();
    }

    /** A client can already hold this endpoint; transparent retries cannot redirect a Flight stream. */
    public synchronized void markClientResultPublished() {
        clientResultPublished = true;
    }

    /** Previously collected write fragments are keyed by physical attempt. They cannot be replayed as a set. */
    public synchronized void markWriteAttempt() {
        replayUnsafeWrite = true;
    }

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
    public synchronized void addTaskId(String taskId) {
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
        onTaskFinished(taskId, generation, null);
    }

    /** Merges the winning summary under the same generation fence as the terminal task. */
    public boolean onTaskFinished(String taskId, int generation, Map<Integer, List<BuildKeyRange>> ranges) {
        return applyTaskReport(taskId, generation, true, null, ranges);
    }

    /** Returns a bounded per-column union for a complete cohort, or null if any summary is absent. */
    public synchronized Map<Integer, List<BuildKeyRange>> completeBuildKeyRanges() {
        if (!terminalDecisionClaimed || !failedTaskIds.isEmpty() || buildKeyReports != taskIds.size()
                || buildKeyRanges == null) return null;
        Map<Integer, List<BuildKeyRange>> union = new HashMap<>();
        buildKeyRanges.forEach((ordinal, ranges) -> union.put(ordinal, unionBuildKeyRanges(ranges)));
        return Map.copyOf(union);
    }

    private static List<BuildKeyRange> unionBuildKeyRanges(List<BuildKeyRange> ranges) {
        if (ranges.size() <= 1) return List.copyOf(ranges);

        ranges.sort(Comparator.comparingLong(BuildKeyRange::minimum));
        List<BuildKeyRange> union = new ArrayList<>(Math.min(BuildKeyRange.MAX_RANGES, ranges.size()));
        BuildKeyRange current = ranges.get(0);
        for (int index = 1; index < ranges.size(); index++) {
            BuildKeyRange next = ranges.get(index);
            if (current.maximum() == Long.MAX_VALUE || next.minimum() <= current.maximum() + 1) {
                current = current.merge(next);
                continue;
            }
            union.add(current);
            if (union.size() == BuildKeyRange.MAX_RANGES) {
                long maximum = ranges.stream().mapToLong(BuildKeyRange::maximum).max().orElseThrow();
                return List.of(new BuildKeyRange(ranges.get(0).minimum(), maximum));
            }
            current = next;
        }
        union.add(current);
        return List.copyOf(union);
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
        applyTaskReport(taskId, generation, false, error, null);
    }

    private boolean applyTaskReport(String taskId, int generation, boolean success, QueryFailure error,
            Map<Integer, List<BuildKeyRange>> ranges) {
        checkNotNull(taskId, "taskId must not be null");
        boolean allSucceeded;
        synchronized (this) {
            if (generation != attemptGeneration.get() || !taskIds.contains(taskId) || terminalDecisionClaimed
                    || finishedTaskIds.contains(taskId) || failedTaskIds.contains(taskId)) return false;
            if (success) {
                finishedTaskIds.add(taskId);
                if (ranges != null && !ranges.isEmpty()) {
                    if (buildKeyRanges == null) buildKeyRanges = new HashMap<>();
                    if (buildKeyRanges.isEmpty() || buildKeyRanges.keySet().equals(ranges.keySet())) {
                        ranges.forEach((ordinal, bounds) -> buildKeyRanges
                                .computeIfAbsent(ordinal, ignored -> new ArrayList<>()).addAll(bounds));
                        buildKeyReports++;
                    }
                }
            } else {
                failedTaskIds.add(taskId);
                lastError = error;
            }
            if (!allTasksReported()) return true;
            terminalDecisionClaimed = true;
            allSucceeded = failedTaskIds.isEmpty();
        }
        // State listeners can retire the query, dispatch the next stage or synchronously retry.
        if (allSucceeded) {
            if (stateMachine.tryTransitionTo(StageState.FLUSHING)) stateMachine.tryTransitionTo(StageState.FINISHED);
        } else {
            stateMachine.tryTransitionTo(StageState.FAILED);
        }
        return true;
    }

    /** Convenience overload for callers that do not track generation (defaults to current). */
    public void onTaskFailed(String taskId, QueryFailure error) {
        onTaskFailed(taskId, error, attemptGeneration.get());
    }

    /** Advances only a classified failed generation whose output has not been bound to a reader. */
    public synchronized boolean tryRestartFailedGeneration(int generation, int maxRetries, String expectedErrorCode) {
        if (clientResultPublished || replayUnsafeWrite || consumerAdmitted
                || generation != attemptGeneration.get() || stateMachine.getState() != StageState.FAILED
                || retryCount.get() >= maxRetries || !retryPolicy.canRetry(retryCount.get())
                || lastError == null || !expectedErrorCode.equals(lastError.errorCode()) || !stateMachine.reset()) {
            return false;
        }
        terminalDecisionClaimed = false;
        lastError = null;
        retryCount.incrementAndGet();
        attemptGeneration.incrementAndGet();
        finishedTaskIds.clear();
        buildKeyRanges = null;
        buildKeyReports = 0;
        failedTaskIds.clear();
        taskIds.clear();
        taskRetryCounters.clear();
        return true;
    }

    /**
     * Reserves one task slot for a retry of {@code failedTaskId} before the
     * replacement is dispatched. The stage keeps the same size, so a report for
     * the dead task can no longer complete (or fail) the stage prematurely.
     *
     * <p>Unlike a whole-stage restart this does not increment the stage
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
        if (clientResultPublished || replayUnsafeWrite || terminalDecisionClaimed || stateMachine.isTerminal()
                || finishedTaskIds.contains(failedTaskId)
                || failedTaskIds.contains(failedTaskId) || taskIds.contains(retryTaskId) || !taskIds.contains(
                        failedTaskId)) {
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
    public synchronized boolean rollbackTaskRetry(String failedTaskId, String retryTaskId) {
        if (terminalDecisionClaimed || finishedTaskIds.contains(retryTaskId) || failedTaskIds.contains(retryTaskId)
                || !taskIds.remove(retryTaskId)) {
            return false;
        }
        AtomicInteger counter = taskRetryCounters.remove(retryTaskId);
        if (counter != null) {
            counter.decrementAndGet();
            taskRetryCounters.put(failedTaskId, counter);
        }
        taskIds.add(failedTaskId);
        LOG.info("event=stage_task_retry_rolled_back stageId={} failedTaskId={} retryTaskId={}",
                stageId(), failedTaskId, retryTaskId);
        return true;
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
                || !receipt.producerSetSha256().equals(java.util.HexFormat.of().formatHex(
                        shuffleHandle.expectedProducerSetChecksum()))) {
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
            throw new IllegalStateException("External shuffle allocation is missing producer identity: stage="
                    + stageId());
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

    public int stageId() {
        return plan.stageId();
    }

    private boolean allTasksReported() {
        if (taskIds.isEmpty()) {
            return false;
        }
        int reported = finishedTaskIds.size() + failedTaskIds.size();
        return reported >= taskIds.size();
    }
}
