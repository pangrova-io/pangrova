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
 * <p>When dispatch dependencies are configured (via {@link #setDispatchDependencies}),
 * {@link #launchSpeculative} actually schedules a duplicate task on a different worker.
 * When unconfigured (e.g. in unit tests), the method falls back to bookkeeping-only mode.</p>
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

    // Dispatch dependencies -- set after construction due to bootstrap ordering.
    // When null, launchSpeculative does bookkeeping only (backward compat with tests).
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
     * When dependencies are not configured, falls back to bookkeeping-only.
     *
     * @param originalTaskId the straggler task to speculatively re-execute
     * @param excludeWorkerId the worker running the original (slow) task
     * @param queryId the query this task belongs to
     * @return the generated speculative task ID
     */
    public String launchSpeculative(String originalTaskId, String excludeWorkerId, String queryId) {
        String speculativeId = "spec-" + UUID.randomUUID();
        speculativePairs.put(originalTaskId, speculativeId);
        reversePairs.put(speculativeId, originalTaskId);

        if (taskDispatcher != null && workerManager != null && taskTracker != null) {
            dispatchSpeculativeCopy(queryId, originalTaskId, speculativeId, excludeWorkerId);
        } else {
            LOG.debug("event=speculative_bookkeeping_only originalTaskId={} speculativeId={}"
                    + " reason=dispatch_dependencies_not_configured",
                    originalTaskId, speculativeId);
        }
        return speculativeId;
    }

    /**
     * Backward-compatible overload for callers that do not pass queryId.
     * Falls back to bookkeeping-only mode.
     */
    public String launchSpeculative(String originalTaskId, String excludeWorkerId) {
        return launchSpeculative(originalTaskId, excludeWorkerId, null);
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
    public void removePair(String originalTaskId) {
        String specId = speculativePairs.remove(originalTaskId);
        if (specId != null) {
            reversePairs.remove(specId);
        }
    }

    // --- Internal dispatch logic ---

    private void dispatchSpeculativeCopy(String queryId, String originalTaskId,
                                          String speculativeId, String excludeWorkerId) {
        SubmitTaskRequest originalRequest = taskTracker.getTaskRequest(originalTaskId);
        if (originalRequest == null) {
            LOG.warn("event=speculative_dispatch_skip originalTaskId={} speculativeId={}"
                    + " reason=original_request_not_found", originalTaskId, speculativeId);
            return;
        }
        if (queryId == null) {
            queryId = originalRequest.queryId();
        }

        WorkerInfo targetWorker = selectAlternateWorker(excludeWorkerId);
        if (targetWorker == null) {
            LOG.warn("event=speculative_dispatch_skip originalTaskId={} speculativeId={}"
                    + " excludeWorkerId={} reason=no_alternate_worker_available",
                    originalTaskId, speculativeId, excludeWorkerId);
            return;
        }

        SubmitTaskRequest specRequest = cloneWithNewTaskId(originalRequest, speculativeId);
        try {
            taskTracker.registerTask(queryId, speculativeId);
            taskTracker.markScheduled(queryId, speculativeId);

            DispatchResult result = taskDispatcher.dispatch(specRequest, targetWorker);

            taskTracker.assignWorker(speculativeId, targetWorker.workerId());
            taskTracker.updateTaskState(queryId, speculativeId, "RUNNING");
            taskTracker.recordTaskStart(speculativeId,
                    String.valueOf(originalRequest.stageId()));

            LOG.info("event=speculative_dispatch_success originalTaskId={} speculativeId={}"
                    + " targetWorker={} queryId={}",
                    originalTaskId, speculativeId, targetWorker.workerId(), queryId);
        } catch (Exception e) {
            LOG.warn("event=speculative_dispatch_failed originalTaskId={} speculativeId={}"
                    + " targetWorker={} error={}",
                    originalTaskId, speculativeId, targetWorker.workerId(), e.getMessage(), e);
            // Best-effort: the original task continues regardless.
            // Clean up the speculative task registration if dispatch failed.
            try {
                taskTracker.updateTaskState(queryId, speculativeId, "FAILED");
            } catch (Exception cleanup) {
                LOG.debug("event=speculative_cleanup_failed speculativeId={}", speculativeId);
            }
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
        return new SubmitTaskRequest(
                newTaskId,
                original.queryId(),
                original.stageId(),
                original.partitionId(),
                original.taskType(),
                original.planFragment(),
                original.splits(),
                original.exchangeSources(),
                original.resourceLimit(),
                original.sessionProperties(),
                original.outputConfig(),
                original.writeConfig(),
                original.dynamicFilters(),
                original.shuffleConfig(),
                original.splitMode(),
                original.tenantId(),
                original.principal(),
                original.vectorSearch(),
                original.coordinatorEpoch());
    }
}
