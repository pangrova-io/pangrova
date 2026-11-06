package org.pangrova.scheduler.core.speculation;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.scheduler.api.placement.HealthState;
import org.pangrova.scheduler.api.placement.PlacementWorker;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages speculative task copies: tracks original -> speculative task pairs
 * and dispatches speculative copies to different workers.
 *
 * <p>{@link #launchSpeculative} schedules a real duplicate task on a different
 * worker and fails closed when dispatch dependencies are absent.</p>
 *
 * <p>Concurrency strategy: ConcurrentHashMap for both forward and reverse pair registries.</p>
 */
@ThreadSafe
public class SpeculativeTaskLauncher {

    private static final Logger LOG = LoggerFactory.getLogger(SpeculativeTaskLauncher.class);

    // original taskId -> speculative taskId
    private final ConcurrentHashMap<String, SpeculativeAttemptPair> speculativePairs = new ConcurrentHashMap<>();
    // reverse: speculative taskId -> original taskId
    private final ConcurrentHashMap<String, String> reversePairs = new ConcurrentHashMap<>();

    // Dispatch dependencies are set after construction due to bootstrap ordering.
    private volatile SpeculativeAttemptDispatcher taskDispatcher;
    private volatile SpeculativeWorkerInventory workerInventory;
    private volatile SpeculativeTaskRegistry taskRegistry;
    private volatile SpeculativeAttemptCanceller cancelRetrier;

    /** Shares the already scheduled query cancellation owner, not an unpolled private retry loop. */
    public void setCancelRetrier(SpeculativeAttemptCanceller retrier) {
        cancelRetrier = java.util.Objects.requireNonNull(retrier);
    }

    /**
     * Configures dispatch dependencies so speculative launches actually schedule
     * a duplicate task on a different worker. Called from bootstrap after all
     * components are created.
     *
     * @param dispatcher     dispatches tasks to workers via HTTP
     * @param inventory      provides active workers for alternate selection
     * @param registry       registers speculative tasks and looks up original requests
     */
    public void setDispatchDependencies(SpeculativeAttemptDispatcher dispatcher,
                                         SpeculativeWorkerInventory inventory,
                                         SpeculativeTaskRegistry registry) {
        this.taskDispatcher = dispatcher;
        this.workerInventory = inventory;
        this.taskRegistry = registry;
        LOG.info("event=speculative_dispatch_configured");
    }

    /**
     * Launches a speculative copy for the given original task.
     * When dispatch dependencies are configured, this method:
     * <ol>
     *   <li>Looks up the original task's {@link SubmitTaskRequest}</li>
     *   <li>Selects a different active worker (avoiding {@code excludeWorkerId})</li>
     *   <li>Dispatches a cloned task with a new speculative task ID</li>
     *   <li>Registers the speculative task in the task tracker</li>
     * </ol>
     * @param originalTaskId the straggler task to speculatively re-execute
     * @param excludeWorkerId the worker running the original (slow) task
     * @param queryId the query this task belongs to
     * @return the generated speculative task ID
     */
    public String launchSpeculative(String originalTaskId, String excludeWorkerId, String queryId) {
        if (queryId == null || queryId.isBlank()) {
            throw new IllegalArgumentException("queryId is required for speculative dispatch");
        }
        if (isSpeculative(originalTaskId)) {
            return null;
        }
        if (taskDispatcher == null || workerInventory == null || taskRegistry == null || cancelRetrier == null) {
            throw new IllegalStateException("speculative dispatch dependencies are not configured");
        }
        String speculativeId = "spec-" + UUID.randomUUID();
        var candidate = new SpeculativeAttemptPair(originalTaskId, speculativeId);
        synchronized (this) {
            if (speculativePairs.putIfAbsent(originalTaskId, candidate) != null) {
                return null;
            }
            try {
                reversePairs.put(speculativeId, originalTaskId);
            } catch (RuntimeException | Error failure) {
                speculativePairs.remove(originalTaskId, candidate);
                throw failure;
            }
        }

        boolean dispatched;
        try {
            dispatched = dispatchSpeculativeCopy(queryId, originalTaskId, speculativeId, excludeWorkerId);
        } catch (RuntimeException | Error failure) {
            candidate.dispatching = false;
            try {
                removePair(originalTaskId);
            } catch (RuntimeException | Error cleanup) {
                if (cleanup != failure) failure.addSuppressed(cleanup);
            }
            throw failure;
        }
        candidate.dispatching = false;
        retirePairIfDrained(candidate);
        if (!dispatched) {
            removePair(originalTaskId);
            return null;
        }
        return speculativeId;
    }

    public boolean isSpeculative(String taskId) {
        return reversePairs.containsKey(taskId);
    }

    public Optional<String> getOriginalTaskId(String speculativeTaskId) {
        return Optional.ofNullable(reversePairs.get(speculativeTaskId));
    }

    /**
     * Returns the speculative task ID for a given original task, if one exists.
     */
    public Optional<String> getSpeculativeTaskId(String originalTaskId) {
        return Optional.ofNullable(speculativePairs.get(originalTaskId)).map(pair -> pair.copyId);
    }

    /**
     * Returns the set of original task IDs that have speculative copies.
     */
    public Set<String> getOriginalTasksWithCopies() {
        return Set.copyOf(speculativePairs.keySet());
    }

    /**
     * Removes tracking for a speculative pair (cleanup after resolution).
     */
    public void removePair(String originalTaskId) {
        var pair = speculativePairs.get(originalTaskId);
        if (pair == null) return;
        synchronized (pair) {
            pair.cleanupRequested = true;
        }
        retirePairIfDrained(pair);
    }

    /** Called only with the task registry's authenticated, exact-issued drain receipt. */
    public void executionDrained(org.pangrova.protocol.v2.TaskReport report) {
        String taskId = report.getAttempt().getTaskId();
        String originalId = reversePairs.getOrDefault(taskId, taskId);
        var pair = speculativePairs.get(originalId);
        if (pair == null) return;
        var retained = taskRegistry.executionDrainReceipt(taskId).orElseThrow(() ->
                new IllegalStateException("Speculative drain notification lacks exact retained authority"));
        if (!retained.getAttempt().equals(report.getAttempt())) {
            throw new IllegalStateException("Speculative attempt changed");
        }
        if (taskId.equals(pair.copyId)) {
            pair.copyDrained = true;
        } else if (taskId.equals(pair.originalId)) {
            pair.originalDrained = true;
        }
        retirePairIfDrained(pair);
    }

    public boolean isAbortedCopy(String taskId) {
        String originalId = reversePairs.get(taskId);
        var pair = originalId == null ? null : speculativePairs.get(originalId);
        return pair != null && pair.aborted;
    }

    /** Uses the shared scheduled retry owner; a 202/transport failure keeps the exact request live. */
    public void cancelIssuedAttempt(String queryId, String taskId, PlacementWorker worker) {
        java.util.Objects.requireNonNull(cancelRetrier, "Speculative cancellation owner is not wired")
                .cancelWithRetry(queryId, taskId, worker.host(), worker.httpPort());
    }

    private void retirePairIfDrained(SpeculativeAttemptPair pair) {
        if (!pair.readyToRetire() || !pair.retiring.compareAndSet(false, true)) return;
        try {
            // Only the copy identity is removed here. The original logical
            // slot is still needed by the original winner/DAG lifecycle.
            if (pair.copyIssued && taskRegistry.getTaskRequest(pair.copyId) != null) {
                if (!taskRegistry.unregisterRecordedDrainedTask(pair.copyId)) {
                    throw new IllegalStateException("Speculative copy has no actual drain receipt");
                }
            }
            synchronized (this) {
                speculativePairs.remove(pair.originalId, pair);
                reversePairs.remove(pair.copyId, pair.originalId);
            }
        } finally {
            pair.retiring.set(false);
        }
    }

    // --- Internal dispatch logic ---

    private boolean dispatchSpeculativeCopy(String queryId, String originalTaskId,
                                             String speculativeId, String excludeWorkerId) {
        SubmitTaskRequest originalRequest = taskRegistry.getTaskRequest(originalTaskId);
        if (originalRequest == null) {
            LOG.warn("event=speculative_dispatch_skip originalTaskId={} speculativeId={}"
                    + " reason=original_request_not_found", originalTaskId, speculativeId);
            return false;
        }
        if (!queryId.equals(
                originalRequest.taskRequest().getAttempt().getQueryOrJobId())) {
            LOG.warn("event=speculative_dispatch_skip originalTaskId={} speculativeId={}"
                    + " reason=query_identity_mismatch", originalTaskId, speculativeId);
            return false;
        }

        PlacementWorker targetWorker = selectAlternateWorker(excludeWorkerId);
        if (targetWorker == null) {
            LOG.warn("event=speculative_dispatch_skip originalTaskId={} speculativeId={}"
                    + " excludeWorkerId={} reason=no_alternate_worker_available",
                    originalTaskId, speculativeId, excludeWorkerId);
            return false;
        }

        try {
            var targeted = taskDispatcher.bindWorkerIdentity(
                    cloneWithNewTaskId(originalRequest, speculativeId), targetWorker);
            var request = taskDispatcher.bindSourceInput(taskDispatcher.bindControlIdentity(targeted));
            registerAndDispatchCopy(queryId, originalTaskId, request, targetWorker);
            return true;
        } catch (RuntimeException | Error e) {
            return handleCopyFailure(queryId, originalTaskId, speculativeId, targetWorker, e);
        }
    }

    private boolean handleCopyFailure(
            String queryId, String originalId, String copyId, PlacementWorker worker, Throwable failure) {
        LOG.warn("event=speculative_dispatch_failed originalTaskId={} speculativeId={} targetWorker={} error={}",
                originalId, copyId, worker.workerId(), failure.getMessage(), failure);
        var pair = speculativePairs.get(originalId);
        pair.aborted = true;
        try {
            if (pair.copyIssued) {
                java.util.Objects.requireNonNull(cancelRetrier, "Speculative cancellation owner is not wired")
                        .cancelWithRetry(queryId, copyId, worker.host(), worker.httpPort());
            } else {
                // No dispatch call entered. Only the local registration exits;
                // this is not evidence about a Native execution owner.
                taskRegistry.unregisterUndispatchedTask(queryId, copyId);
            }
        } catch (RuntimeException | Error cleanup) {
            if (cleanup != failure) failure.addSuppressed(cleanup);
            if (failure instanceof Error error) throw error;
            throw (RuntimeException) failure;
        }
        // The original remains authoritative; do not fail the entire query.
        if (failure instanceof Error error) throw error;
        return false;
    }

    private void registerAndDispatchCopy(
            String queryId, String originalId, SubmitTaskRequest request, PlacementWorker worker) {
        String copyId = request.taskRequest().getAttempt().getTaskId();
        taskRegistry.registerTask(queryId, copyId);
        taskRegistry.registerTaskRequest(copyId, request);
        taskRegistry.bindDispatchTarget(request, worker);
        taskRegistry.markScheduled(queryId, copyId);
        taskRegistry.assignWorker(copyId, worker.workerId());
        taskRegistry.setDeadline(copyId, java.time.Instant.ofEpochMilli(request.taskRequest().getDeadlineEpochMs()));
        var pair = speculativePairs.get(originalId);
        synchronized (pair) {
            if (pair.cleanupRequested) throw new IllegalStateException("Speculative dispatch admission is sealed");
            pair.copyIssued = true;
        }
        taskRegistry.enterTaskDispatch(request);
        String acknowledgedTaskId = taskDispatcher.dispatch(request, worker);
        if (!copyId.equals(acknowledgedTaskId)) {
            throw new IllegalStateException("Worker acknowledgement changed the issued speculative identity");
        }
        taskRegistry.assignWorker(copyId, worker.workerId());
        taskRegistry.updateTaskState(queryId, copyId, "RUNNING");
        taskRegistry.recordTaskStart(copyId, String.valueOf(request.taskRequest().getAttempt().getStageId()));
        LOG.info("event=speculative_dispatch_success originalTaskId={} speculativeId={} targetWorker={} queryId={}",
                originalId, copyId, worker.workerId(), queryId);
    }

    /**
     * Selects an active worker different from the excluded one.
     * Picks the worker with the lowest running task count to balance load.
     */
    private PlacementWorker selectAlternateWorker(String excludeWorkerId) {
        List<? extends PlacementWorker> active = workerInventory.activeWorkers();
        return active.stream()
                .filter(w -> w.healthState() == HealthState.ACTIVE)
                .filter(w -> !w.workerId().equals(excludeWorkerId))
                .min(java.util.Comparator.comparingInt(PlacementWorker::runningTaskCount))
                .orElse(null);
    }

    /**
     * Creates a copy of the original task request with a new task ID.
     * All other fields (plan, splits, session properties, etc.) are preserved.
     */
    private static SubmitTaskRequest cloneWithNewTaskId(SubmitTaskRequest original, String newTaskId) {
        var canonical = original.taskRequest();
        var next = org.pangrova.protocol.rest.task.TaskAttemptDescriptor
                .from(canonical.getAttempt()).nextAttempt();
        return SubmitTaskRequest.from(canonical.toBuilder().clearCopySourceStagingCommandSha256()
                .setAttempt(canonical.getAttempt().toBuilder()
                        .setTaskId(newTaskId)
                        .clearWorkerExecutionIdentity()
                        .setPlanEpoch(next.planEpoch())
                        .setAttemptId(next.attemptId())
                        .setFencingToken(next.fencingToken()))
                .build());
    }
}
