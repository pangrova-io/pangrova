package org.pangrova.coordinator.streaming;

import org.pangrova.coordinator.task.CoordinatorTaskTracker;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.v2.ExecutionDrainState;
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
        return accept(expected, report, Optional.of(canonical));
    }

    /** Progress reports may omit canonical bytes. Terminal reports require the validated drain. */
    public boolean accept(SubmitTaskRequest expected, TaskStatusReport report, Optional<TaskReport> canonical) {
        java.util.Objects.requireNonNull(expected, "expected");
        java.util.Objects.requireNonNull(report, "report");
        java.util.Objects.requireNonNull(canonical, "canonical");
        String queryId = expected.taskRequest().getAttempt().getQueryOrJobId();
        String taskId = expected.taskRequest().getAttempt().getTaskId();
        if (!isTerminal(report.state())) {
            return tracker.updateTaskState(queryId, taskId, report.state());
        }
        if (canonical.isEmpty()
                || canonical.get().getExecutionDrainState() == ExecutionDrainState.EXECUTION_DRAIN_STATE_UNSPECIFIED) {
            return false;
        }
        try (var publication = tracker.enterReportPublication(expected, report, false)) {
            if (!publication.publicationCompleted()) {
                tracker.recordExecutionDrain(expected, canonical.get());
                if (!tracker.updateTaskState(queryId, taskId, report.state())) return false;
                publication.accepted();
            }
        }
        TaskStatusReport existing = terminals.putIfAbsent(taskId, report);
        waiters.computeIfAbsent(taskId, ignored -> new CompletableFuture<>())
                .complete(existing == null ? report : existing);
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
        TaskStatusReport report = terminals.remove(taskId);
        if (report != null) tracker.unregisterRecordedDrainedTask(taskId);
        return Optional.ofNullable(report);
    }

    void retireBatch(String executionId) {
        tracker.sealQueryIssuance(executionId);
        tracker.retireQueryIssuance(executionId);
    }

    private static boolean isTerminal(String state) {
        return "FINISHED".equals(state) || "FAILED".equals(state) || "CANCELLED".equals(state);
    }
}
