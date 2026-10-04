package org.pangrova.worker.task;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.task.WorkerTerminalTaskReports;
import org.pangrova.protocol.rest.error.ErrorDetail;
import org.pangrova.protocol.v2.ErrorClass;
import org.pangrova.protocol.v2.ExecutionError;
import org.pangrova.protocol.v2.ResourceUsage;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskReportOrigin;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Generated-wire validation at the actual production boundary, not task execution. */
class TaskTerminalEvidenceContractTest {
    @Test
    void missingNativeTerminalReportFailsRatherThanCreatingAResultFinish() throws Exception {
        var request = ActualNativeTaskFixture.valuesTaskRequest(true);
        for (byte[] missing : new byte[][] {null, new byte[0]}) {
            assertThatThrownBy(() -> TaskCompletionMonitor.requireCanonicalTaskReport(request, missing))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("canonical TaskReport");
        }
        var failure = new ErrorDetail("INTERNAL_ERROR", "MissingNativeEvidence", "Missing canonical TaskReport");
        var reconciled = TaskReport.parseFrom(WorkerTerminalTaskReports.unsuccessful(
                request, TaskState.TASK_STATE_FAILED, failure));
        assertThat(reconciled.getState()).isEqualTo(TaskState.TASK_STATE_FAILED);
        assertThat(reconciled.getReportOrigin()).isEqualTo(TaskReportOrigin.TASK_REPORT_ORIGIN_WORKER_RECONCILED);
        assertThat(reconciled.getAttempt()).isEqualTo(request.getAttempt());
        assertThat(reconciled.getError().getMessage()).contains("canonical TaskReport");
        assertThat(reconciled.getCoverage().getCompletedWorkUnits()).isZero();
        assertThatThrownBy(() -> WorkerTerminalTaskReports.unsuccessful(
                request, TaskState.TASK_STATE_FINISHED, failure)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void nativeFailureRetainsObservedUsageAndStructuredError() throws Exception {
        var request = ActualNativeTaskFixture.valuesTaskRequest(true);
        var nativeFailure = TaskReport.newBuilder().setAttempt(request.getAttempt())
                .setState(TaskState.TASK_STATE_FAILED).setReportOrigin(TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE)
                .setUsage(ResourceUsage.newBuilder().setPeakNativeMemoryBytes(8L * 1024 * 1024))
                .setError(ExecutionError.newBuilder().setCode("PROVIDER_FAILED")
                        .setErrorClass(ErrorClass.ERROR_CLASS_PROVIDER).setMessage("provider rejected request")
                        .setFailedComponent("provider")).build();
        assertThat(TaskCompletionMonitor.requireCanonicalTaskReport(request, nativeFailure.toByteArray()))
                .isEqualTo(nativeFailure);
        var error = new ErrorDetail("INTERNAL_ERROR", "WorkerFailure", "Preserve Native failure evidence");
        var reconciled = TaskReport.parseFrom(WorkerTerminalTaskReports.unsuccessful(
                request, TaskState.TASK_STATE_FAILED, error, nativeFailure.toByteArray()));
        assertThat(reconciled.getState()).isEqualTo(TaskState.TASK_STATE_FAILED);
        assertThat(reconciled.getReportOrigin()).isEqualTo(TaskReportOrigin.TASK_REPORT_ORIGIN_WORKER_RECONCILED);
        assertThat(reconciled.getUsage().getPeakNativeMemoryBytes()).isEqualTo(8L * 1024 * 1024);
        assertThat(reconciled.getError().getCode()).isEqualTo("PROVIDER_FAILED");
        assertThat(reconciled.getError().getErrorClass()).isEqualTo(ErrorClass.ERROR_CLASS_PROVIDER);
        assertThat(reconciled.getError()).isEqualTo(nativeFailure.getError());
    }

    @Test
    void rejectsCrossAttemptNonNativeAndNonTerminalReportsBeforeCapturingEvidence() {
        var request = ActualNativeTaskFixture.valuesTaskRequest(true);
        var valid = TaskReport.newBuilder().setAttempt(request.getAttempt()).setState(TaskState.TASK_STATE_FINISHED)
                .setReportOrigin(TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE).build();
        var other = valid.toBuilder().setAttempt(request.getAttempt().toBuilder().setTaskId("other-task")).build();
        var nonNative = valid.toBuilder().setReportOrigin(TaskReportOrigin.TASK_REPORT_ORIGIN_WORKER_RECONCILED).build();
        var running = valid.toBuilder().setState(TaskState.TASK_STATE_RUNNING).build();
        for (var rejected : new TaskReport[] {other, nonNative, running}) {
            assertThatThrownBy(() -> TaskCompletionMonitor.requireCanonicalTaskReport(request, rejected.toByteArray()))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("terminal task authority");
        }
    }

    @Test
    void rejectsFinishedWithErrorAndFailedWithoutErrorButRetainsCancelledCleanupError() {
        var request = ActualNativeTaskFixture.valuesTaskRequest(true);
        var error = ExecutionError.newBuilder().setCode("RESOURCE_DRAIN_FAILED")
                .setMessage("Actual writer drain failed after cancellation")
                .setErrorClass(ErrorClass.ERROR_CLASS_INTERNAL).build();
        var base = TaskReport.newBuilder().setAttempt(request.getAttempt())
                .setReportOrigin(TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE);
        var contradictoryFinished = base.clone().setState(TaskState.TASK_STATE_FINISHED).setError(error).build();
        var incompleteFailed = base.clone().setState(TaskState.TASK_STATE_FAILED).build();
        assertThatThrownBy(() -> TaskCompletionMonitor.requireCanonicalTaskReport(
                request, contradictoryFinished.toByteArray())).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> TaskCompletionMonitor.requireCanonicalTaskReport(
                request, incompleteFailed.toByteArray())).isInstanceOf(IllegalStateException.class);
        var cancelled = base.setState(TaskState.TASK_STATE_CANCELLED).setError(error).build();
        assertThat(TaskCompletionMonitor.requireCanonicalTaskReport(request, cancelled.toByteArray()))
                .isEqualTo(cancelled);
    }

    @Test
    void typedAdmissionPreservesClassificationAndActualNativeFirstFailureWins() throws Exception {
        var request = ActualNativeTaskFixture.valuesTaskRequest(true);
        var admission = ExecutionError.newBuilder().setErrorClass(ErrorClass.ERROR_CLASS_ADMISSION)
                .setCode("SHUFFLE_MANAGED_CHANNEL_LIMIT").setMessage("Worker channel credit exhausted")
                .setRetryable(true).setFailedComponent("remote-shuffle-channel-admission").build();
        var rejected = TaskReport.parseFrom(WorkerTerminalTaskReports.unsuccessfulWithError(
                request, TaskState.TASK_STATE_FAILED, admission, null));
        assertThat(rejected.getAttempt()).isEqualTo(request.getAttempt());
        assertThat(rejected.getError()).isEqualTo(admission);
        assertThat(rejected.getReportOrigin()).isEqualTo(TaskReportOrigin.TASK_REPORT_ORIGIN_WORKER_RECONCILED);
        assertThat(rejected.getCoverage().getCompletedWorkUnits()).isZero();
        var first = TaskReport.newBuilder().setAttempt(request.getAttempt())
                .setState(TaskState.TASK_STATE_FAILED).setReportOrigin(TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE)
                .setError(ExecutionError.newBuilder().setErrorClass(ErrorClass.ERROR_CLASS_DATA_CORRUPTION)
                        .setCode("NATIVE_FIRST_FAILURE").setMessage("Native rejected corrupt input")
                        .setFailedComponent("native-exchange")).build();
        var retained = TaskReport.parseFrom(WorkerTerminalTaskReports.unsuccessfulWithError(
                request, TaskState.TASK_STATE_FAILED, admission, first.toByteArray()));
        assertThat(retained.getError()).isEqualTo(first.getError());
        var other = first.toBuilder().setAttempt(first.getAttempt().toBuilder().setTaskId("different-attempt"));
        assertThat(TaskReport.parseFrom(WorkerTerminalTaskReports.unsuccessfulWithError(
                request, TaskState.TASK_STATE_FAILED, admission, other.build().toByteArray())).getError())
                .isEqualTo(admission);
    }
}
