package org.pangrova.coordinator.streaming;

import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.v2.TaskReport;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Stores terminal Worker reports for CDC tasks after the normal report validation.
 *
 * <p>It updates only the coordinator task state machine. It never enters SQL
 * query completion, DML commit, or artifact publication paths.</p>
 */
public final class CdcTaskCompletionRegistry {

    private final CoordinatorTaskTracker tracker;
    private final ConcurrentHashMap<String, TaskStatusReport> terminals = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, CompletableFuture<TaskStatusReport>> waiters =
            new ConcurrentHashMap<>();

    public CdcTaskCompletionRegistry(CoordinatorTaskTracker tracker) {
        this.tracker = java.util.Objects.requireNonNull(tracker, "tracker");
    }

    /** Accepts an already validated report and wakes any exact waiter on terminal state. */
    public boolean accept(SubmitTaskRequest expected, TaskStatusReport report, TaskReport canonical) {
        java.util.Objects.requireNonNull(expected, "expected");
        java.util.Objects.requireNonNull(report, "report");
        java.util.Objects.requireNonNull(canonical, "canonical");
        String queryId = expected.taskRequest().getAttempt().getQueryOrJobId();
        String taskId = expected.taskRequest().getAttempt().getTaskId();
        if (!tracker.updateTaskState(queryId, taskId, report.state())) {
            return false;
        }
        if (isTerminal(report.state())) {
            terminals.put(taskId, report);
            waiters.computeIfAbsent(taskId, ignored -> new CompletableFuture<>())
                    .complete(report);
        }
        return true;
    }

    /** Waits for the exact terminal report; a report that arrived earlier is returned immediately. */
    public CompletableFuture<TaskStatusReport> await(String taskId) {
        TaskStatusReport existing = terminals.get(taskId);
        if (existing != null) {
            return CompletableFuture.completedFuture(existing);
        }
        return waiters.computeIfAbsent(taskId, ignored -> new CompletableFuture<>());
    }

    /** Removes retained terminal state after the caller has consumed the result. */
    public Optional<TaskStatusReport> remove(String taskId) {
        waiters.remove(taskId);
        return Optional.ofNullable(terminals.remove(taskId));
    }

    private static boolean isTerminal(String state) {
        return "FINISHED".equals(state) || "FAILED".equals(state) || "CANCELLED".equals(state);
    }
}
