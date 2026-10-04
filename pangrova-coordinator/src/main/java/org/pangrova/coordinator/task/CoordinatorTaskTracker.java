package org.pangrova.coordinator.task;

import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskAttemptDescriptor;

/**
 * Owns issued task identities, state machines and exact physical-drain receipts.
 * Local publication is serialized with issuance sealing; transport operations run outside that gate.
 */
@ThreadSafe
public class CoordinatorTaskTracker {

    private final ConcurrentHashMap<String, ConcurrentHashMap<String, TaskStateMachine>> queryTasks =
            new ConcurrentHashMap<>();

    private final ConcurrentHashMap<String, String> taskWorkerAssignment =
            new ConcurrentHashMap<>();

    /** Reverse index: workerId -> set of queryIds with active tasks on that worker. */
    private final ConcurrentHashMap<String, Set<String>> workerQueryIndex =
            new ConcurrentHashMap<>();

    private final TaskTimingTracker timingTracker = new TaskTimingTracker();

    /** Atomic task identity used for status fencing and optional speculative cloning. */
    private final TaskIdentityRegistry taskIdentityStore = new TaskIdentityRegistry();

    private volatile TaskFailureCallback taskFailureCallback;

    public void setTaskFailureCallback(TaskFailureCallback callback) {
        this.taskFailureCallback = callback;
    }

    public void registerTask(String queryId, String taskId) {
        taskIdentityStore.registerLogicalTask(queryId, () -> {
            var tasks = queryTasks.computeIfAbsent(queryId, k -> new ConcurrentHashMap<>());
            var candidate = new TaskStateMachine();
            if (tasks.putIfAbsent(taskId, candidate) != null) {
                throw new IllegalStateException("Task already registered: queryId=" + queryId + ", taskId=" + taskId);
            }
            try {
                timingTracker.registerTaskQuery(taskId, queryId);
            } catch (RuntimeException | Error failure) {
                tasks.remove(taskId, candidate);
                if (tasks.isEmpty()) queryTasks.remove(queryId, tasks);
                timingTracker.clearTaskTracking(taskId);
                throw failure;
            }
        });
    }

    /** Rolls back only before transport entry; an ambiguous HTTP failure may leave a real Worker owner. */
    public void unregisterUndispatchedTask(String queryId, String taskId) {
        ConcurrentHashMap<String, TaskStateMachine> tasks = queryTasks.get(queryId);
        if (tasks == null) {
            return;
        }
        TaskStateMachine state = tasks.get(taskId);
        if (state == null) {
            return;
        }
        if (state.getState() != TaskStateMachine.State.PENDING
                && state.getState() != TaskStateMachine.State.SCHEDULED) {
            throw new IllegalStateException(
                    "cannot unregister a started task: " + taskId);
        }
        taskIdentityStore.remove(taskId);
        if (tasks.remove(taskId, state)) {
            if (tasks.isEmpty()) {
                queryTasks.remove(queryId, tasks);
            }
            taskWorkerAssignment.remove(taskId);
            timingTracker.clearTaskTracking(taskId);
        }
    }

    public void assignWorker(String taskId, String workerId) {
        taskWorkerAssignment.put(taskId, workerId);
        // Maintain reverse index: find queryId from timing tracker and add to worker index
        String queryId = timingTracker.getQueryIdForTask(taskId);
        if (queryId != null) {
            workerQueryIndex.computeIfAbsent(workerId,
                    k -> ConcurrentHashMap.newKeySet()).add(queryId);
        }
    }

    public String getWorkerForTask(String taskId) {
        return taskWorkerAssignment.get(taskId);
    }

    /** Removes a pre-dispatch assignment only when it still names the expected worker. */
    public boolean unassignWorker(String taskId, String workerId) {
        return taskWorkerAssignment.remove(taskId, workerId);
    }

    public void markScheduled(String queryId, String taskId) {
        TaskStateMachine sm = getStateMachine(queryId, taskId);
        sm.tryTransition(TaskStateMachine.State.SCHEDULED);
    }

    /** Returns whether the reported state is accepted, including an exact idempotent state observation. */
    public boolean updateTaskState(String queryId, String taskId, String workerState) {
        TaskStateMachine sm = getStateMachine(queryId, taskId);
        TaskStateMachine.State target = TaskLogicalCompletion.mapWorkerState(workerState);
        boolean transitioned = sm.tryTransition(target);
        boolean accepted = transitioned || sm.getState() == target;
        if (accepted && (target == TaskStateMachine.State.FAILED
                || target == TaskStateMachine.State.ABANDONED)) {
            completeTaskFutureIfTerminal(taskId, sm);
        }
        if (transitioned && target == TaskStateMachine.State.FAILED) {
            TaskFailureCallback callback = this.taskFailureCallback;
            if (callback != null) {
                callback.onTaskFailed(queryId, taskId);
            }
        }
        return accepted;
    }

    public void signalTaskTerminal(String queryId, String taskId) {
        TaskStateMachine sm = findStateMachine(queryId, taskId);
        if (sm != null) {
            completeTaskFutureIfTerminal(taskId, sm);
        }
    }

    private TaskStateMachine findStateMachine(String queryId, String taskId) {
        var tasks = queryTasks.get(queryId);
        return tasks != null ? tasks.get(taskId) : null;
    }

    public boolean allTasksFinished(String queryId) {
        ConcurrentHashMap<String, TaskStateMachine> tasks = queryTasks.get(queryId);
        if (tasks == null || tasks.isEmpty()) {
            return false;
        }
        for (TaskStateMachine sm : tasks.values()) {
            if (sm.getState() != TaskStateMachine.State.FINISHED) {
                return false;
            }
        }
        return true;
    }

    public boolean hasFailedTask(String queryId) {
        ConcurrentHashMap<String, TaskStateMachine> tasks = queryTasks.get(queryId);
        if (tasks == null) {
            return false;
        }
        for (TaskStateMachine sm : tasks.values()) {
            if (sm.getState() == TaskStateMachine.State.FAILED) {
                return true;
            }
        }
        return false;
    }

    public TaskStateMachine.State getTaskState(String queryId, String taskId) {
        return getStateMachine(queryId, taskId).getState();
    }

    public Set<String> getQueriesForWorker(String workerId) {
        Set<String> indexed = workerQueryIndex.get(workerId);
        if (indexed != null && !indexed.isEmpty()) {
            // Filter indexed results to only include queries with active tasks,
            // since the index is not pruned on terminal state transitions.
            Set<String> active = new HashSet<>();
            for (String queryId : indexed) {
                ConcurrentHashMap<String, TaskStateMachine> tasks = queryTasks.get(queryId);
                if (tasks != null && hasActiveTaskOnWorker(tasks, workerId)) {
                    active.add(queryId);
                }
            }
            return active;
        }
        // Fallback: full scan if index is empty (should not happen in normal flow)
        Set<String> affected = new HashSet<>();
        for (Map.Entry<String, ConcurrentHashMap<String, TaskStateMachine>> queryEntry : queryTasks.entrySet()) {
            if (hasActiveTaskOnWorker(queryEntry.getValue(), workerId)) {
                affected.add(queryEntry.getKey());
            }
        }
        return affected;
    }

    public Set<String> getTaskIds(String queryId) {
        ConcurrentHashMap<String, TaskStateMachine> tasks = queryTasks.get(queryId);
        if (tasks == null) {
            return Set.of();
        }
        return Set.copyOf(tasks.keySet());
    }

    public String getLatestStartedTaskId(String queryId) {
        Set<String> taskIds = getTaskIds(queryId);
        if (taskIds.isEmpty()) {
            return null;
        }
        String latest = null;
        Instant latestTime = null;
        for (String taskId : taskIds) {
            Instant startTime = timingTracker.getStartTime(taskId);
            if (startTime != null && (latestTime == null || startTime.isAfter(latestTime))) {
                latestTime = startTime;
                latest = taskId;
            }
        }
        return latest != null ? latest : taskIds.stream()
                .max(java.util.Comparator.naturalOrder())
                .orElse(null);
    }

    public Map<String, Set<String>> getActiveTasksByQueryForWorker(String workerId) {
        Map<String, Set<String>> result = new HashMap<>();
        for (Map.Entry<String, ConcurrentHashMap<String, TaskStateMachine>> queryEntry : queryTasks.entrySet()) {
            String queryId = queryEntry.getKey();
            for (Map.Entry<String, TaskStateMachine> taskEntry : queryEntry.getValue().entrySet()) {
                String taskId = taskEntry.getKey();
                String assignedWorker = taskWorkerAssignment.get(taskId);
                if (workerId.equals(assignedWorker) && !taskEntry.getValue().isTerminal()) {
                    result.computeIfAbsent(queryId, k -> new HashSet<>()).add(taskId);
                }
            }
        }
        return result;
    }

    // --- Delegation to TaskTimingTracker ---

    public void setDeadline(String taskId, Instant deadline) {
        timingTracker.setDeadline(taskId, deadline);
    }

    public Map<String, String> getTimedOutTasks(Instant now) {
        return timingTracker.getTimedOutTasks(now, this::isNonTerminal);
    }

    public void removeDeadline(String taskId) {
        timingTracker.removeDeadline(taskId);
    }

    public void recordTaskStart(String taskId, String stageId) {
        timingTracker.recordTaskStart(taskId, stageId);
    }

    public void recordTaskStart(String taskId, String stageId, Instant startTime) {
        timingTracker.recordTaskStart(taskId, stageId, startTime);
    }

    public List<TaskDuration> getTaskDurationSnapshots(
            String queryId, String stageId, Set<String> speculativeTaskIds, Instant now) {
        return timingTracker.getTaskDurationSnapshots(
                queryTasks.get(queryId), stageId, speculativeTaskIds, now);
    }

    /** Returns stage IDs with running tasks. */
    public Set<String> getRunningStageIds(String queryId) {
        return timingTracker.getRunningStageIds(queryTasks.get(queryId));
    }

    public void clearTaskTracking(String taskId) {
        timingTracker.clearTaskTracking(taskId);
        // Logical terminal timing is not evidence that the issued Native owner
        // exited. Keep its request until exact release evidence permits retirement.
    }

    /** Retires a settled logical task without losing retained physical-exit evidence. */
    public void unregisterSettledTask(String queryId, String taskId) {
        taskIdentityStore.remove(taskId);
        ConcurrentHashMap<String, TaskStateMachine> tasks = queryTasks.get(queryId);
        if (tasks != null) {
            TaskStateMachine state = tasks.get(taskId);
            if (state != null && !state.isTerminal()) {
                throw new IllegalStateException("cannot unregister a non-terminal task: " + taskId);
            }
            tasks.remove(taskId);
            if (tasks.isEmpty()) {
                queryTasks.remove(queryId, tasks);
            }
        }
        taskWorkerAssignment.remove(taskId);
        timingTracker.clearTaskTracking(taskId);
    }

    /** Retires execution resources without awarding rejected durable publication a DAG win. */
    public void unregisterDrainedTask(org.pangrova.protocol.v2.TaskReport receipt) {
        String taskId = receipt.getAttempt().getTaskId();
        var retained = taskIdentityStore.executionDrainReceipt(taskId).orElseThrow(() ->
                new IllegalStateException("Cannot retire execution without its retained exact drain receipt"));
        if (!retained.equals(receipt)) throw new IllegalArgumentException("Execution drain receipt changed");
        var request = taskIdentityStore.request(taskId);
        if (request == null || !request.taskRequest().getAttempt().equals(receipt.getAttempt())) {
            throw new IllegalStateException("Execution drain receipt lost its exact issued request");
        }
        String executionId = receipt.getAttempt().getQueryOrJobId();
        var tasks = queryTasks.get(executionId);
        if (tasks != null) {
            var state = tasks.remove(taskId);
            if (state != null && !state.completionSignal().isDone()) {
                state.completionSignal().completeExceptionally(new java.util.concurrent.CancellationException(
                        "Execution owner drained without an accepted publication completion"));
            }
            if (tasks.isEmpty()) queryTasks.remove(executionId, tasks);
        }
        taskWorkerAssignment.remove(taskId);
        timingTracker.clearTaskTracking(taskId);
        taskIdentityStore.compactDrainedTask(taskId, request);
    }

    // --- Task identity store (status fencing and speculative cloning) ---

    /** Stores the exact issued request for status fencing and speculative cloning. */
    public void registerTaskRequest(String taskId, SubmitTaskRequest request) {
        taskIdentityStore.registerRequest(taskId, request);
    }

    /** Opens the sole issuance authority before planning. */
    public void openQueryIssuance(String queryId) { taskIdentityStore.openQuery(queryId); }

    public void markRecoveredIssuanceUnresolved(String queryId) { taskIdentityStore.markRecoveryUnresolved(queryId); }

    public QueryIssuanceOperation enterQueryOperation(String queryId) {
        Runnable exited = taskIdentityStore.enterQueryOperation(queryId);
        try { return new QueryIssuanceOperation(exited); }
        catch (RuntimeException | Error failure) {
            exited.run();
            throw failure;
        }
    }

    /** Enters an already issued scope for a state transition, including terminal cleanup metadata. */
    public QueryIssuanceOperation enterQueryMetadataOperation(String queryId) {
        Runnable exited = taskIdentityStore.enterMetadataOperation(queryId);
        try { return new QueryIssuanceOperation(exited); }
        catch (RuntimeException | Error failure) { exited.run(); throw failure; }
    }

    /** Registers the required publication before retaining a report's drain evidence. */
    public TaskReportPublication enterReportPublication(SubmitTaskRequest request,
            org.pangrova.protocol.rest.task.TaskStatusReport report, boolean resourceOnly) {
        return taskIdentityStore.enterReport(request, report, resourceOnly, () -> notifyExecutionDrain(report.taskId()));
    }

    /** Captures the actual target before transport admission, never a later WorkerId lookup. */
    public void bindDispatchTarget(SubmitTaskRequest request, org.pangrova.coordinator.worker.WorkerInfo worker) {
        taskIdentityStore.bindTarget(request, worker);
    }

    /** Conservative entered boundary: a throw after this point still requires Worker seal/drain evidence. */
    public void enterTaskDispatch(SubmitTaskRequest request) { taskIdentityStore.enterDispatch(request); }

    public List<IssuedTask> sealQueryIssuance(String queryId) { return taskIdentityStore.sealQuery(queryId); }

    public void requireQueryExecutionDrained(String queryId) {
        taskIdentityStore.requireQueryDrained(queryId);
        var tasks = queryTasks.get(queryId);
        if (tasks != null) {
            for (String taskId : tasks.keySet()) {
                if (taskIdentityStore.executionDrainReceipt(taskId).isEmpty()) {
                    throw new IllegalStateException("Tracked task has no exact execution drain receipt: " + taskId);
                }
            }
        }
    }

    /** Called only after all dependent query owners have actually cleaned up. */
    public void retireQueryIssuance(String queryId) {
        taskIdentityStore.retireQuery(queryId);
        var removed = queryTasks.remove(queryId);
        if (removed != null) removed.keySet().forEach(taskId -> {
            taskWorkerAssignment.remove(taskId);
            timingTracker.clearTaskTracking(taskId);
        });
        workerQueryIndex.values().forEach(index -> index.remove(queryId));
    }

    /** Reacknowledges only an already validated exact exit receipt, never outputs or a new winner. */
    public boolean acknowledgesExecutionDrain(org.pangrova.protocol.rest.task.TaskStatusReport report) {
        return taskIdentityStore.acknowledgesDrain(report);
    }

    /** Reacknowledges completed report work without claiming the execution has drained. */
    public boolean acknowledgesReportPublication(org.pangrova.protocol.rest.task.TaskStatusReport report) {
        return taskIdentityStore.acknowledgesPublication(report);
    }

    public record IssuedTask(SubmitTaskRequest request, org.pangrova.coordinator.worker.WorkerInfo worker,
                            java.util.Optional<org.pangrova.protocol.v2.TaskReport> drainReceipt) {}

    /** Removes a prepared request only when it is still the exact registered request. */
    public boolean unregisterTaskRequest(String taskId, SubmitTaskRequest request) {
        return taskIdentityStore.removeRequest(taskId, request);
    }

    /** Registers the only attempt identity authorized to report for a task ID. */
    public void registerTaskAttempt(String taskId, TaskAttemptDescriptor attempt) {
        taskIdentityStore.registerAttempt(taskId, attempt);
    }

    /** Returns the expected status-report attempt, or null for an unknown task identity. */
    public TaskAttemptDescriptor getTaskAttempt(String taskId) {
        return taskIdentityStore.attempt(taskId);
    }

    /** Returns the original issued request, or null if unavailable. */
    public SubmitTaskRequest getTaskRequest(String taskId) {
        return taskIdentityStore.request(taskId);
    }

    /** Returns the original issued scheduled fact, preserving event identity and producer time on retry. */
    public org.pangrova.spi.coordination.TaskStateEvent scheduledJournalEvent(String taskId) {
        return taskIdentityStore.scheduledEvent(taskId);
    }

    public void recordExecutionDrain(SubmitTaskRequest request, org.pangrova.protocol.v2.TaskReport report) {
        taskIdentityStore.recordExecutionDrain(request, report);
        notifyExecutionDrain(report.getAttempt().getTaskId());
    }

    private final TaskExecutionDrainNotifications executionDrainListeners = new TaskExecutionDrainNotifications();

    private void notifyExecutionDrain(String taskId) {
        if (taskIdentityStore.drainNotificationReady(taskId)) {
            taskIdentityStore.executionDrainReceipt(taskId).ifPresent(executionDrainListeners::publish);
        }
    }

    public void addExecutionDrainListener(java.util.function.Consumer<org.pangrova.protocol.v2.TaskReport> listener) {
        executionDrainListeners.add(java.util.Objects.requireNonNull(listener));
    }

    public java.util.Optional<org.pangrova.protocol.v2.TaskReport> executionDrainReceipt(String taskId) {
        return taskIdentityStore.executionDrainReceipt(taskId);
    }

    public CompletableFuture<Void> awaitTaskCompletion(String queryId, String taskId) {
        return getStateMachine(queryId, taskId).completionSignal();
    }

    private void completeTaskFutureIfTerminal(String taskId, TaskStateMachine sm) {
        TaskLogicalCompletion.complete(taskId, sm, timingTracker);
    }

    // --- Internal helpers ---

    private boolean isNonTerminal(String queryId, String taskId) {
        ConcurrentHashMap<String, TaskStateMachine> tasks = queryTasks.get(queryId);
        if (tasks == null) {
            return false;
        }
        TaskStateMachine sm = tasks.get(taskId);
        return sm != null && !sm.isTerminal();
    }

    private boolean hasActiveTaskOnWorker(ConcurrentHashMap<String, TaskStateMachine> tasks,
                                          String workerId) {
        for (Map.Entry<String, TaskStateMachine> taskEntry : tasks.entrySet()) {
            String assignedWorker = taskWorkerAssignment.get(taskEntry.getKey());
            if (workerId.equals(assignedWorker) && !taskEntry.getValue().isTerminal()) {
                return true;
            }
        }
        return false;
    }

    private TaskStateMachine getStateMachine(String queryId, String taskId) {
        ConcurrentHashMap<String, TaskStateMachine> tasks = queryTasks.get(queryId);
        if (tasks == null) {
            throw new IllegalArgumentException("Unknown queryId: " + queryId);
        }
        TaskStateMachine sm = tasks.get(taskId);
        if (sm == null) {
            throw new IllegalArgumentException(
                    "Unknown taskId: " + taskId + " for queryId: " + queryId);
        }
        return sm;
    }


}
