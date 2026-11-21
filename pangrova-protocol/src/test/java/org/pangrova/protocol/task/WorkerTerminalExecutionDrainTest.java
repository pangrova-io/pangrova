package org.pangrova.protocol.task;

import org.pangrova.protocol.CanonicalTaskRequestTestFixture;

import org.junit.jupiter.api.Test;
import org.pangrova.common.client.error.ErrorDetail;
import org.pangrova.protocol.v2.CompletionState;
import org.pangrova.protocol.v2.ExecutionDrainState;
import org.pangrova.protocol.v2.NativeTaskStats;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskReportOrigin;
import org.pangrova.protocol.v2.TaskState;

import static org.assertj.core.api.Assertions.assertThat;

/** Reconciliation of canonical metadata only; not evidence that a synthetic task exited. */
class WorkerTerminalExecutionDrainTest {
    @Test
    void buildsTheOnlySuccessfulWorkerExecutedCdcReport() throws Exception {
        var request = CanonicalTaskRequestTestFixture.builder(
                "task", "query", TaskKind.TASK_KIND_CDC_INGEST, new byte[]{1}).build();

        var report = TaskReport.parseFrom(WorkerTerminalTaskReports.workerExecuted(request));

        assertThat(report.getAttempt()).isEqualTo(request.getAttempt());
        assertThat(report.getState()).isEqualTo(TaskState.TASK_STATE_FINISHED);
        assertThat(report.getReportOrigin()).isEqualTo(TaskReportOrigin.TASK_REPORT_ORIGIN_WORKER_EXECUTED);
        assertThat(report.getExecutionDrainState()).isEqualTo(
                ExecutionDrainState.EXECUTION_DRAIN_STATE_WORKER_TASK_COMPLETED);
        assertThat(report.getCoverage().getCompletionState())
                .isEqualTo(CompletionState.COMPLETION_STATE_COMPLETE);
        assertThat(report.getCoverage().getExpectedWorkUnits()).isEqualTo(1);
        assertThat(report.getCoverage().getCompletedWorkUnits()).isEqualTo(1);
        assertThat(report.getCoverage().getCoverageRatio()).isEqualTo(1.0);
        assertThat(report.hasNativeTaskStats()).isFalse();
        assertThat(report.hasError()).isFalse();
        assertThat(org.assertj.core.api.Assertions.catchThrowable(() ->
                WorkerTerminalTaskReports.workerExecuted(request.toBuilder()
                        .setTaskKind(TaskKind.TASK_KIND_READ).build())))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("CDC ingest");
    }

    @Test
    void workerExecutedFailureCarriesNoNativeOwnerOrStatistics() throws Exception {
        var request = CanonicalTaskRequestTestFixture.builder(
                "task", "query", TaskKind.TASK_KIND_CDC_INGEST, new byte[]{1}).build();
        var failure = org.pangrova.protocol.v2.ExecutionError.newBuilder()
                .setErrorClass(org.pangrova.protocol.v2.ErrorClass.ERROR_CLASS_INTERNAL)
                .setCode("CDC_INGEST_FAILED").setMessage("source unavailable")
                .setFailedComponent("worker-cdc").build();

        var report = TaskReport.parseFrom(WorkerTerminalTaskReports.workerExecutedFailure(
                request, TaskState.TASK_STATE_FAILED, failure));

        assertThat(report.getState()).isEqualTo(TaskState.TASK_STATE_FAILED);
        assertThat(report.getReportOrigin()).isEqualTo(TaskReportOrigin.TASK_REPORT_ORIGIN_WORKER_RECONCILED);
        assertThat(report.getExecutionDrainState()).isEqualTo(
                ExecutionDrainState.EXECUTION_DRAIN_STATE_ADMISSION_CLOSED_WITHOUT_NATIVE_OWNER);
        assertThat(report.getError()).isEqualTo(failure);
        assertThat(report.hasNativeTaskStats()).isFalse();
    }

    @Test
    void actualReleaseMetadataAndStatisticsSurviveWorkerReconciliationWithoutInventingSuccess() throws Exception {
        var request = request();
        var release = TaskReport.newBuilder().setAttempt(request.getAttempt())
                .setState(TaskState.TASK_STATE_CANCELLED).setReportOrigin(TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE)
                .setExecutionDrainState(ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED)
                .setNativeTaskStats(NativeTaskStats.newBuilder().setRowsWritten(37)).build();
        var reconciled = reconcile(request, release);
        assertThat(reconciled.getReportOrigin()).isEqualTo(TaskReportOrigin.TASK_REPORT_ORIGIN_WORKER_RECONCILED);
        assertThat(reconciled.getState()).isEqualTo(TaskState.TASK_STATE_CANCELLED);
        assertThat(reconciled.getExecutionDrainState()).isEqualTo(release.getExecutionDrainState());
        assertThat(reconciled.getNativeTaskStats()).isEqualTo(release.getNativeTaskStats());
        assertThat(reconciled.hasError()).isTrue();
    }

    @Test
    void pendingNativeReleaseMayCarryTrueDrainWithoutVeloxTaskStatistics() throws Exception {
        var request = request();
        var release = TaskReport.newBuilder().setAttempt(request.getAttempt()).setState(TaskState.TASK_STATE_CANCELLED)
                .setReportOrigin(TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE)
                .setExecutionDrainState(ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED).build();
        var reconciled = reconcile(request, release);
        assertThat(reconciled.hasNativeTaskStats()).isFalse();
        assertThat(reconciled.getExecutionDrainState()).isEqualTo(ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED);
    }

    @Test
    void pollingStatisticsAndCrossAttemptReportsCannotBecomeReleaseEvidence() throws Exception {
        var request = request();
        var polling = TaskReport.newBuilder().setAttempt(request.getAttempt()).setState(TaskState.TASK_STATE_CANCELLED)
                .setReportOrigin(TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE)
                .setNativeTaskStats(NativeTaskStats.newBuilder().setRowsWritten(37)).build();
        var reconciled = reconcile(request, polling);
        assertThat(reconciled.hasNativeTaskStats()).isFalse();
        assertThat(reconciled.getExecutionDrainState()).isEqualTo(ExecutionDrainState.EXECUTION_DRAIN_STATE_UNSPECIFIED);
        var crossAttempt = polling.toBuilder().setAttempt(polling.getAttempt().toBuilder().setFencingToken(7))
                .setExecutionDrainState(ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED).build();
        assertThat(reconcile(request, crossAttempt).getExecutionDrainState())
                .isEqualTo(ExecutionDrainState.EXECUTION_DRAIN_STATE_UNSPECIFIED);
    }

    private static org.pangrova.protocol.v2.TaskRequest request() {
        return CanonicalTaskRequestTestFixture.builder("task", "query", TaskKind.TASK_KIND_READ, new byte[]{1}).build();
    }
    private static TaskReport reconcile(org.pangrova.protocol.v2.TaskRequest request, TaskReport actual) throws Exception {
        return TaskReport.parseFrom(WorkerTerminalTaskReports.unsuccessful(request, TaskState.TASK_STATE_CANCELLED,
                new ErrorDetail("TASK_CANCELLED", "CANCELLED", "Actual Worker cancellation metadata"), actual.toByteArray()));
    }
}
