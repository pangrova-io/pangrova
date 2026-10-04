package org.pangrova.coordinator.speculation;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.coordinator.query.execution.dispatch.DispatchResult;
import org.pangrova.coordinator.scheduler.TaskDispatcher;
import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.coordinator.worker.HealthState;
import org.pangrova.coordinator.worker.WorkerInfo;
import org.pangrova.coordinator.worker.WorkerManager;
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
    private volatile TaskDispatcher taskDispatcher;
    private volatile WorkerManager workerManager;
    private volatile CoordinatorTaskTracker taskTracker;
    private volatile org.pangrova.coordinator.task.TaskCancelRetrier cancelRetrier;

    /** Shares the already scheduled query cancellation owner, not an unpolled private retry loop. */
    public void setCancelRetrier(org.pangrova.coordinator.task.TaskCancelRetrier retrier) {
        cancelRetrier = java.util.Objects.requireNonNull(retrier);
    }

    /**
     * Configures dispatch dependencies so speculative launches actually schedule
     * a duplicate task on a different worker. Called from bootstrap after all
     * components are created.
     *
     * @param dispatcher   dispatches tasks to workers via HTTP
     * @param workerManagerDependency provides active worker list for alternate selection
     * @param taskTrackerDependency  registers speculative tasks and looks up original requests
     */
    public void setDispatchDependencies(TaskDispatcher dispatcher,
                                         WorkerManager workerManagerDependency,
                                         CoordinatorTaskTracker taskTrackerDependency) {
        this.taskDispatcher = dispatcher;
        this.workerManager = workerManagerDependency;
        this.taskTracker = taskTrackerDependency;
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
        if (taskDispatcher == null || workerManager == null || taskTracker == null || cancelRetrier == null) {
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
            try { removePair(originalTaskId); }
            catch (RuntimeException | Error cleanup) { if (cleanup != failure) failure.addSuppressed(cleanup); }
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
        var retained = taskTracker.executionDrainReceipt(taskId).orElseThrow(() ->
                new IllegalStateException("Speculative drain notification lacks exact retained authority"));
        if (!retained.getAttempt().equals(report.getAttempt())) throw new IllegalStateException("Speculative attempt changed");
        if (taskId.equals(pair.copyId)) pair.copyDrained = true;
        else if (taskId.equals(pair.originalId)) pair.originalDrained = true;
        retirePairIfDrained(pair);
    }

    public boolean isAbortedCopy(String taskId) {
        String originalId = reversePairs.get(taskId);
        var pair = originalId == null ? null : speculativePairs.get(originalId);
        return pair != null && pair.aborted;
    }

    /** Uses the shared scheduled retry owner; a 202/transport failure keeps the exact request live. */
    public void cancelIssuedAttempt(String queryId, String taskId, WorkerInfo worker) {
        java.util.Objects.requireNonNull(cancelRetrier, "Speculative cancellation owner is not wired")
                .cancelWithRetry(queryId, taskId, worker.host(), worker.httpPort());
    }

    private void retirePairIfDrained(SpeculativeAttemptPair pair) {
        if (!pair.readyToRetire() || !pair.retiring.compareAndSet(false, true)) return;
        try {
            // Only the copy identity is removed here. The original logical
            // slot is still needed by the original winner/DAG lifecycle.
            if (pair.copyIssued && taskTracker.getTaskRequest(pair.copyId) != null) {
                var receipt = taskTracker.executionDrainReceipt(pair.copyId).orElseThrow(() ->
                        new IllegalStateException("Speculative copy has no actual drain receipt"));
                taskTracker.unregisterDrainedTask(receipt);
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
        SubmitTaskRequest originalRequest = taskTracker.getTaskRequest(originalTaskId);
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

        WorkerInfo targetWorker = selectAlternateWorker(excludeWorkerId);
        if (targetWorker == null) {
            LOG.warn("event=speculative_dispatch_skip originalTaskId={} speculativeId={}"
                    + " excludeWorkerId={} reason=no_alternate_worker_available",
                    originalTaskId, speculativeId, excludeWorkerId);
            return false;
        }

        try {
            var request = taskDispatcher.bindControlIdentity(cloneWithNewTaskId(originalRequest, speculativeId));
            registerAndDispatchCopy(queryId, originalTaskId, request, targetWorker);
            return true;
        } catch (RuntimeException | Error e) {
            return handleCopyFailure(queryId, originalTaskId, speculativeId, targetWorker, e);
        }
    }

    private boolean handleCopyFailure(String queryId, String originalId, String copyId, WorkerInfo worker, Throwable failure) {
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
                taskTracker.unregisterUndispatchedTask(queryId, copyId);
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

    private void registerAndDispatchCopy(String queryId, String originalId, SubmitTaskRequest request, WorkerInfo worker) {
        String copyId = request.taskRequest().getAttempt().getTaskId();
        taskTracker.registerTask(queryId, copyId);
        taskTracker.registerTaskRequest(copyId, request);
        taskTracker.bindDispatchTarget(request, worker);
        taskTracker.markScheduled(queryId, copyId);
        taskTracker.assignWorker(copyId, worker.workerId());
        taskTracker.setDeadline(copyId, java.time.Instant.ofEpochMilli(request.taskRequest().getDeadlineEpochMs()));
        var pair = speculativePairs.get(originalId);
        synchronized (pair) {
            if (pair.cleanupRequested) throw new IllegalStateException("Speculative dispatch admission is sealed");
            pair.copyIssued = true;
        }
        taskTracker.enterTaskDispatch(request);
        DispatchResult result = taskDispatcher.dispatch(request, worker);
        if (result == null || !copyId.equals(result.taskId())) {
            throw new IllegalStateException("Worker acknowledgement changed the issued speculative identity");
        }
        taskTracker.assignWorker(copyId, worker.workerId());
        taskTracker.updateTaskState(queryId, copyId, "RUNNING");
        taskTracker.recordTaskStart(copyId, String.valueOf(request.taskRequest().getAttempt().getStageId()));
        LOG.info("event=speculative_dispatch_success originalTaskId={} speculativeId={} targetWorker={} queryId={}",
                originalId, copyId, worker.workerId(), queryId);
    }

    /**
     * Selects an active worker different from the excluded one.
     * Picks the worker with the lowest running task count to balance load.
     */
    private WorkerInfo selectAlternateWorker(String excludeWorkerId) {
        List<WorkerInfo> active = workerManager.getActiveWorkers();
        return active.stream()
                .filter(w -> w.healthState() == HealthState.ACTIVE)
                .filter(w -> !w.workerId().equals(excludeWorkerId))
                .min(java.util.Comparator.comparingInt(WorkerInfo::runningTaskCount))
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
        return SubmitTaskRequest.from(canonical.toBuilder()
                .setAttempt(canonical.getAttempt().toBuilder()
                        .setTaskId(newTaskId)
                        .setPlanEpoch(next.planEpoch())
                        .setAttemptId(next.attemptId())
                        .setFencingToken(next.fencingToken()))
                .build());
    }
}
