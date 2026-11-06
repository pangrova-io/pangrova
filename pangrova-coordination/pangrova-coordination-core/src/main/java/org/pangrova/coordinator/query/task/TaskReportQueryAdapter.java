package org.pangrova.coordinator.query.task;

import java.util.Objects;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.query.api.error.QueryFailure;

/** Converts validated task-report diagnostics into the query owner's value domain. */
public final class TaskReportQueryAdapter {
    private TaskReportQueryAdapter() { }

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
