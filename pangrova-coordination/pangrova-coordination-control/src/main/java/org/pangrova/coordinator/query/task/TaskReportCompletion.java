package org.pangrova.coordinator.query.task;

import java.util.List;
import java.util.Objects;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.query.api.error.QueryFailure;
import org.pangrova.query.api.stage.StageCompletionCallback;

/** Converts validated task-report diagnostics into the query owner's value domain. */
public final class TaskReportCompletion {
    private TaskReportCompletion() { }

    /** Routes an accepted attempt to its selected logical slot when DAG tracking is enabled. */
    static void notifyStageTerminal(StageCompletionCallback callback, String logicalTaskId, TaskStatusReport report) {
        if (callback == null) return;
        if (!"FINISHED".equals(report.state())) {
            callback.onTaskTerminal(report.queryId(), logicalTaskId, report.state(), failure(report));
            return;
        }
        var decoded = report.decodedStatus();
        List<StageCompletionCallback.BuildKeyRange> buildKeys = decoded == null
                || !decoded.getNativeTaskStats().hasBuildKeys() ? null
                : decoded.getNativeTaskStats().getBuildKeys().getRangesList().stream()
                        .map(range -> new StageCompletionCallback.BuildKeyRange(range.getMinimum(), range.getMaximum()))
                        .toList();
        var attempt = report.attempt();
        var selected = new StageCompletionCallback.SuccessfulAttempt(report.taskId(), report.partitionId(),
                attempt.planEpoch(), attempt.attemptId());
        callback.onSuccessfulTaskTerminal(report.queryId(), logicalTaskId, selected, buildKeys);
    }

    /**
     * Copies all four diagnostics without reclassifying or synthesizing a failure.
     * Report validation, admission fencing and publication remain with the receiver;
     * this stateless conversion neither accepts a report nor changes its frozen fact.
     */
    public static QueryFailure failure(TaskStatusReport report) {
        var error = Objects.requireNonNull(report, "report").error();
        return error == null ? null : new QueryFailure(
                error.errorCode(), error.errorType(), error.message(), error.stackTrace());
    }
}
