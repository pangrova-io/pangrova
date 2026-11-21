package org.pangrova.coordinator.query.task.report;

import org.pangrova.protocol.write.PhysicalFileWriteReceipts;
import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.rest.task.TaskStatusReport;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskState;

/** Validates physical write metadata before any completion or DAG side effects. */
final class PhysicalWriteTaskReportValidator {

    private PhysicalWriteTaskReportValidator() {}

    static void validate(TaskReport status, TaskStatusReport report, SubmitTaskRequest expected) {
        if (expected == null || status.getState() != TaskState.TASK_STATE_FINISHED) {
            return;
        }
        var request = expected.taskRequest();
        var output = request.getExecutionOptions().getWrite().getOutputBinding();
        if (!output.hasPhysical()) {
            return;
        }
        if (request.getTaskKind() != org.pangrova.protocol.v2.TaskKind.TASK_KIND_WRITE
                || report.writeResult() == null) {
            throw new IllegalArgumentException("Finished physical write is missing its dispatched receipt");
        }
        PhysicalFileWriteReceipts.validateTaskFragment(
                output.getPhysical(), request.getAttempt(), report.writeResult().commitMetadata());
    }
}
