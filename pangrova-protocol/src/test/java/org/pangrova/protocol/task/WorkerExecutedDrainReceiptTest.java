package org.pangrova.protocol.task;

import org.pangrova.protocol.CanonicalTaskRequestTestFixture;

import org.junit.jupiter.api.Test;
import org.pangrova.protocol.v2.ExecutionDrainState;
import org.pangrova.protocol.v2.TaskKind;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskReportOrigin;
import org.pangrova.protocol.v2.TaskState;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class WorkerExecutedDrainReceiptTest {

    @Test
    void acceptsWorkerJvmCompletionWithItsExactDrainState() {
        var receipt = CanonicalTaskAdmissionSeals.compactExecutionDrainReceipt(report().build());
        assertThat(receipt.getReportOrigin()).isEqualTo(TaskReportOrigin.TASK_REPORT_ORIGIN_WORKER_EXECUTED);
        assertThat(receipt.getExecutionDrainState())
                .isEqualTo(ExecutionDrainState.EXECUTION_DRAIN_STATE_WORKER_TASK_COMPLETED);
    }

    @Test
    void rejectsWorkerJvmSuccessWithoutItsDrainEvidence() {
        assertThatThrownBy(() -> CanonicalTaskAdmissionSeals.compactExecutionDrainReceipt(
                report().setExecutionDrainState(ExecutionDrainState.EXECUTION_DRAIN_STATE_UNSPECIFIED).build()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsNativeSuccessClaimedAsWorkerJvmCompletion() {
        assertThatThrownBy(() -> CanonicalTaskAdmissionSeals.compactExecutionDrainReceipt(
                report().setReportOrigin(TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE).build()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    private static TaskReport.Builder report() {
        var request = CanonicalTaskRequestTestFixture.builder(
                "cdc-task", "cdc-query", TaskKind.TASK_KIND_CDC_INGEST, new byte[0]).build();
        return TaskReport.newBuilder()
                .setAttempt(request.getAttempt())
                .setState(TaskState.TASK_STATE_FINISHED)
                .setReportOrigin(TaskReportOrigin.TASK_REPORT_ORIGIN_WORKER_EXECUTED)
                .setExecutionDrainState(ExecutionDrainState.EXECUTION_DRAIN_STATE_WORKER_TASK_COMPLETED);
    }
}
