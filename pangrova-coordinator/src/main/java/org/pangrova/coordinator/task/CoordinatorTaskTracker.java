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
 * Coordinator-side task tracker.
 * Manages per-query task state machines using existing {@link TaskStateMachine}.
 * Each query maps to multiple tasks, each tracked by a TaskStateMachine instance.
 * Also tracks task-to-worker assignment for dead worker drain.
 *
 * <p>Deadline and duration tracking are delegated to {@link TaskTimingTracker}.</p>
 *
 * <p>Concurrency strategy: all maps are ConcurrentHashMap for lock-free concurrent access.
 * Individual operations are atomic at the map level; composite operations (e.g. register + assign)
 * rely on caller ordering.</p>
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
        ConcurrentHashMap<String, TaskStateMachine> tasks =
                queryTasks.computeIfAbsent(queryId, k -> new ConcurrentHashMap<>());
        TaskStateMachine existing = tasks.putIfAbsent(taskId, new TaskStateMachine());
        if (existing != null) {
            throw new IllegalStateException(
                    "Task already registered: queryId=" + queryId + ", taskId=" + taskId);
        }
        timingTracker.registerTaskQuery(taskId, queryId);
    }

    /**
     * Rolls back a task that was prepared but never submitted to a Worker.
     * This must not be used after dispatch has started: an ambiguous HTTP
     * failure can still leave a live Worker reporting this attempt.
     */
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
        if (tasks.remove(taskId, state)) {
            if (tasks.isEmpty()) {
                queryTasks.remove(queryId, tasks);
            }
            taskWorkerAssignment.remove(taskId);
            timingTracker.clearTaskTracking(taskId);
            taskIdentityStore.remove(taskId);
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
        TaskStateMachine.State target = mapWorkerState(workerState);
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

    /**
     * Returns the distinct stage IDs that have at least one running task for the given query.
     */
    public Set<String> getRunningStageIds(String queryId) {
        return timingTracker.getRunningStageIds(queryTasks.get(queryId));
    }

    public void clearTaskTracking(String taskId) {
        timingTracker.clearTaskTracking(taskId);
        // Task identities may retain the serialized request for speculative
        // cloning, so terminal cleanup must remove the identity atomically.
        taskIdentityStore.remove(taskId);
    }

    /**
     * Retires a physical speculative loser after its logical partition has
     * completed on another attempt. Cancellation is not a query task failure.
     */
    public void retireSpeculativeLoser(String queryId, String taskId) {
        taskIdentityStore.remove(taskId);
        ConcurrentHashMap<String, TaskStateMachine> tasks = queryTasks.get(queryId);
        if (tasks != null) {
            TaskStateMachine retired = tasks.remove(taskId);
            if (retired != null) {
                retired.completionSignal().complete(null);
            }
        }
        taskWorkerAssignment.remove(taskId);
        timingTracker.clearTaskTracking(taskId);
    }

    /**
     * Removes one fully settled task attempt so a durable logical work unit can
     * be retried under a fresh attempt-specific task ID without leaking state.
     */
    public void unregisterSettledTask(String queryId, String taskId) {
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
        taskIdentityStore.remove(taskId);
    }

    // --- Task identity store (status fencing and speculative cloning) ---

    /**
     * Stores the original {@link SubmitTaskRequest} for a dispatched task,
     * keyed by confirmed task ID. Used by speculative execution to clone
     * the request with a new task ID when launching a speculative copy.
     */
    public void registerTaskRequest(String taskId, SubmitTaskRequest request) {
        taskIdentityStore.registerRequest(taskId, request);
    }

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

    /**
     * Returns the original task request for the given task ID, or null if not stored.
     */
    public SubmitTaskRequest getTaskRequest(String taskId) {
        return taskIdentityStore.request(taskId);
    }

    // --- Task completion futures ---

    public CompletableFuture<Void> awaitTaskCompletion(String queryId, String taskId) {
        return getStateMachine(queryId, taskId).completionSignal();
    }

    private void completeTaskFutureIfTerminal(String taskId, TaskStateMachine sm) {
        TaskStateMachine.State state = sm.getState();
        CompletableFuture<Void> future = sm.completionSignal();
        if (state == TaskStateMachine.State.FINISHED) {
            timingTracker.removeDeadline(taskId);
            future.complete(null);
        } else if (state == TaskStateMachine.State.FAILED
                || state == TaskStateMachine.State.ABANDONED) {
            timingTracker.removeDeadline(taskId);
            future.completeExceptionally(
                    new IllegalStateException("Task terminated: " + taskId
                            + ", state=" + state));
        }
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

    private static TaskStateMachine.State mapWorkerState(String workerState) {
        return switch (workerState) {
            case "RUNNING", "FLUSHING" -> TaskStateMachine.State.RUNNING;
            case "FINISHED" -> TaskStateMachine.State.FINISHED;
            case "FAILED" -> TaskStateMachine.State.FAILED;
            case "CANCELLED" -> TaskStateMachine.State.FAILED;
            case "SCHEDULED" -> TaskStateMachine.State.SCHEDULED;
            case "ABANDONED" -> TaskStateMachine.State.ABANDONED;
            default -> throw new IllegalArgumentException("Unknown worker state: " + workerState);
        };
    }
}
