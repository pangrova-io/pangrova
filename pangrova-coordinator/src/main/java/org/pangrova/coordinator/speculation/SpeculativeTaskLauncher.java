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
    private final ConcurrentHashMap<String, String> speculativePairs = new ConcurrentHashMap<>();
    // reverse: speculative taskId -> original taskId
    private final ConcurrentHashMap<String, String> reversePairs = new ConcurrentHashMap<>();

    // Dispatch dependencies are set after construction due to bootstrap ordering.
    private volatile TaskDispatcher taskDispatcher;
    private volatile WorkerManager workerManager;
    private volatile CoordinatorTaskTracker taskTracker;

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
        if (taskDispatcher == null || workerManager == null || taskTracker == null) {
            throw new IllegalStateException("speculative dispatch dependencies are not configured");
        }
        String speculativeId = "spec-" + UUID.randomUUID();
        synchronized (this) {
            if (speculativePairs.putIfAbsent(originalTaskId, speculativeId) != null) {
                return null;
            }
            reversePairs.put(speculativeId, originalTaskId);
        }

        if (!dispatchSpeculativeCopy(
                queryId, originalTaskId, speculativeId, excludeWorkerId)) {
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
        return Optional.ofNullable(speculativePairs.get(originalTaskId));
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
    public synchronized void removePair(String originalTaskId) {
        String specId = speculativePairs.remove(originalTaskId);
        if (specId != null) {
            reversePairs.remove(specId);
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

        SubmitTaskRequest specRequest = cloneWithNewTaskId(originalRequest, speculativeId);
        try {
            taskTracker.registerTask(queryId, speculativeId);
            // Status fencing and Native report validation require the exact
            // cloned canonical request, not only its coordinator task ID.
            taskTracker.registerTaskRequest(speculativeId, specRequest);
            taskTracker.markScheduled(queryId, speculativeId);

            DispatchResult result = taskDispatcher.dispatch(specRequest, targetWorker);

            taskTracker.assignWorker(speculativeId, targetWorker.workerId());
            taskTracker.updateTaskState(queryId, speculativeId, "RUNNING");
            taskTracker.recordTaskStart(speculativeId,
                    String.valueOf(originalRequest.taskRequest().getAttempt().getStageId()));

            LOG.info("event=speculative_dispatch_success originalTaskId={} speculativeId={}"
                    + " targetWorker={} queryId={}",
                    originalTaskId, speculativeId, targetWorker.workerId(), queryId);
            return true;
        } catch (Exception e) {
            LOG.warn("event=speculative_dispatch_failed originalTaskId={} speculativeId={}"
                    + " targetWorker={} error={}",
                    originalTaskId, speculativeId, targetWorker.workerId(), e.getMessage(), e);
            try {
                taskDispatcher.cancelTask(
                        speculativeId, targetWorker.host(), targetWorker.httpPort());
            } catch (Exception cancelFailure) {
                LOG.warn("event=speculative_cancel_after_dispatch_failure_failed"
                        + " originalTaskId={} speculativeId={} targetWorker={} error={}",
                        originalTaskId, speculativeId, targetWorker.workerId(),
                        cancelFailure.getMessage(), cancelFailure);
            }
            // The original task remains authoritative. A speculative dispatch
            // failure must not invoke the query-wide task failure callback.
            taskTracker.retireSpeculativeLoser(queryId, speculativeId);
            return false;
        }
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
