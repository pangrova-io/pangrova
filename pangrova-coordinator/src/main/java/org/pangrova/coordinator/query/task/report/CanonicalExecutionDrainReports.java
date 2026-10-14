package org.pangrova.coordinator.query.task.report;

import org.pangrova.protocol.rest.task.SubmitTaskRequest;
import org.pangrova.protocol.v2.ExecutionDrainState;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskReportOrigin;
import org.pangrova.protocol.v2.TaskState;

/** Validation shared by ordinary canonical reports and the exact admission-seal control response. */
final class CanonicalExecutionDrainReports {
    private CanonicalExecutionDrainReports() {}

    static void validateOrigin(TaskReport status) {
        switch (status.getReportOrigin()) {
            case TASK_REPORT_ORIGIN_NATIVE -> { }
            case TASK_REPORT_ORIGIN_WORKER_RECONCILED -> {
                if (!unsuccessful(status)) {
                    throw invalid("Worker-reconciled TaskReport cannot claim success");
                }
                if (!status.hasError()) {
                    throw invalid("Worker-reconciled TaskReport needs a structured error");
                }
            }
            case TASK_REPORT_ORIGIN_UNSPECIFIED, UNRECOGNIZED -> throw invalid("terminal TaskReport has no valid report origin");
            default -> throw invalid("terminal TaskReport has no valid report origin");
        }
        switch (status.getExecutionDrainState()) {
            case EXECUTION_DRAIN_STATE_UNSPECIFIED -> { }
            case EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED -> {
                if (!terminal(status)) {
                    throw invalid("Released Native owner has a nonterminal receipt");
                }
            }
            case EXECUTION_DRAIN_STATE_ADMISSION_CLOSED_WITHOUT_NATIVE_OWNER -> {
                if (status.getReportOrigin() != TaskReportOrigin.TASK_REPORT_ORIGIN_WORKER_RECONCILED
                        || !unsuccessful(status) || status.hasNativeTaskStats()) {
                    throw invalid("No-owner receipt violates its sealed Worker admission authority");
                }
            }
            case UNRECOGNIZED -> throw invalid("Unknown execution drain state");
            default -> throw invalid("Unknown execution drain state");
        }
    }

    static void validateReceipt(TaskReport status, SubmitTaskRequest expected) {
        if (expected == null || !status.getUnknownFields().asMap().isEmpty()
                || !status.getAttempt().equals(expected.taskRequest().getAttempt())
                || status.getExecutionDrainState() == ExecutionDrainState.EXECUTION_DRAIN_STATE_UNSPECIFIED
                || !terminal(status)) {
            throw invalid("Execution drain receipt has no exact issued terminal authority");
        }
        validateOrigin(status);
        validateErrorAndStatistics(status);
        if (status.hasCoverage() || status.hasUsage() || status.hasApproximation() || status.hasArtifactWork()
                || status.hasNativeTaskStats() || status.hasOutputManifestChecksum()
                || !status.getWinningAttemptId().isEmpty()
                || status.getError().getFailedPartitionIdsCount() != 0) {
            throw invalid("Compact execution drain receipt cannot claim execution/output payload");
        }
    }

    private static void validateErrorAndStatistics(TaskReport status) {
        if (status.getState() == TaskState.TASK_STATE_FINISHED && status.hasError()
                || status.getState() == TaskState.TASK_STATE_FAILED && !status.hasError()) {
            throw invalid("Execution drain receipt contradicts its terminal error state");
        }
        if (status.hasError()) {
            var error = status.getError();
            if (!error.getUnknownFields().asMap().isEmpty() || error.getCode().isBlank()
                    || error.getMessage().isBlank() || error.getFailedComponent().isBlank()
                    || error.getErrorClass() == org.pangrova.protocol.v2.ErrorClass.ERROR_CLASS_UNSPECIFIED
                    || error.getErrorClass() == org.pangrova.protocol.v2.ErrorClass.UNRECOGNIZED) {
                throw invalid("Execution drain receipt lost its canonical error classification");
            }
        }
        if (status.hasNativeTaskStats() && !status.getNativeTaskStats().getUnknownFields().asMap().isEmpty()) {
            throw invalid("Execution drain statistics are outside the negotiated descriptor");
        }
    }

    private static boolean unsuccessful(TaskReport status) {
        return status.getState() == TaskState.TASK_STATE_FAILED || status.getState() == TaskState.TASK_STATE_CANCELLED;
    }
    private static boolean terminal(TaskReport status) {
        return unsuccessful(status) || status.getState() == TaskState.TASK_STATE_FINISHED;
    }
    private static IllegalArgumentException invalid(String message) { return new IllegalArgumentException(message); }
}
