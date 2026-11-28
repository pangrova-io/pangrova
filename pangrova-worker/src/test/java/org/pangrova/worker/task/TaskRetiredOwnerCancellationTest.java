package org.pangrova.worker.task;

import org.pangrova.worker.enums.TaskState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.protocol.task.CanonicalTaskAdmissionSeals;
import org.pangrova.protocol.v2.ExecutionDrainState;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
/** Actual Native owner retirement must never acknowledge a failed cancel as successful. */
@Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class TaskRetiredOwnerCancellationTest {
    @Test
    void sealAndTerminalPublicationAgreeWhenCancellationWinsAfterNativeSuccess() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "actual-cancel-after-native-success")) {
            var request = runtime.valuesRequest(false);
            var info = tasks.preparePending(request);
            long handle = info.executorHandle();
            runtime.executor().startTask(handle);
            assertThat(runtime.executor().awaitFinished(handle, 5_000)).isTrue();
            assertThat(TaskReport.parseFrom(runtime.executor().getTaskStatus(handle)).getState())
                    .isEqualTo(org.pangrova.protocol.v2.TaskState.TASK_STATE_FINISHED);

            var seal = CanonicalTaskAdmissionSeals.create(request);
            assertThat(tasks.manager().sealAdmission(seal)).isEmpty();
            assertThat(info.getState()).isEqualTo(TaskState.CANCELLING);
            tasks.manager().releaseTaskExecutor(info.taskId());

            var terminal = tasks.terminalReport(info);
            var receipt = tasks.manager().sealAdmission(seal).orElseThrow();
            assertThat(info.executorReleaseReceipt().report().getState())
                    .isEqualTo(org.pangrova.protocol.v2.TaskState.TASK_STATE_FINISHED);
            assertThat(terminal.state()).isEqualTo("CANCELLED");
            assertThat(receipt).isEqualTo(
                    CanonicalTaskAdmissionSeals.compactExecutionDrainReceipt(terminal.canonicalStatus()));
            assertThat(receipt.getExecutionDrainState()).isEqualTo(
                    ExecutionDrainState.EXECUTION_DRAIN_STATE_NATIVE_OWNER_RELEASED);
            assertThat(receipt.getError().getCode()).isEqualTo("TASK_CANCELLED");
        }
    }

    @Test
    void retiredActualNativeOwnerDoesNotAcknowledgeSuccessfulCancellation() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "actual-cancel-retired-owner")) {
            var info = tasks.preparePending(runtime.valuesRequest(false));
            long handle = info.executorHandle();
            byte[] actualReceipt = runtime.executor().releaseTask(handle).orElseThrow();
            // Preserve the exact joined Native receipt for cleanup, while observing the stale
            // still-published TaskInfo before reconciliation. No synthetic cancellation owner.
            info.stageExecutorReleaseReceipt(actualReceipt);
            assertThatThrownBy(() -> tasks.manager().cancelTask(info.taskId()))
                    .isInstanceOf(RuntimeException.class);
            assertThat(info.getState()).isEqualTo(TaskState.CANCELLING);
            assertThat(tasks.reports().reports()).noneMatch(report -> report.state().equals("CANCELLED"));
            tasks.manager().releaseTaskExecutor(info.taskId());
            assertThat(info.executorHandle()).isZero();
            assertThat(info.executorReleaseReceipt().bytes().toByteArray()).isEqualTo(actualReceipt);
            assertThat(runtime.arbitrator().getUsedMemory()).isZero();
            assertThat(runtime.nodePool().getUsedBytes()).isZero();
        }
    }

    @Test
    void anotherActualAttemptReceiptCannotReleaseThisOwnerBudget() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "actual-cancel-receipt-owner")) {
            var info = tasks.preparePending(runtime.valuesRequest(false));
            long handle = info.executorHandle();
            byte[] ownReceipt = runtime.executor().releaseTask(handle).orElseThrow();
            var otherRequest = runtime.valuesRequest(false);
            long otherHandle = runtime.createPending(otherRequest);
            byte[] otherReceipt = runtime.executor().releaseTask(otherHandle).orElseThrow();
            assertThat(org.pangrova.protocol.v2.TaskReport.parseFrom(otherReceipt).getAttempt())
                    .isNotEqualTo(info.request().taskRequest().getAttempt());
            long budgetBefore = runtime.arbitrator().getUsedMemory();
            long nodeBefore = runtime.nodePool().getUsedBytes();
            assertThat(budgetBefore).isPositive();
            // Reach the same real CANCELLING retry path as the retained original manager failure.
            assertThatThrownBy(() -> tasks.manager().cancelTask(info.taskId()))
                    .isInstanceOf(RuntimeException.class);
            assertThat(info.getState()).isEqualTo(TaskState.CANCELLING);
            assertThat(tasks.reports().reports())
                    .noneMatch(report -> report.state().equals("CANCELLED"));
            info.stageExecutorReleaseReceipt(otherReceipt);
            assertThatThrownBy(() -> tasks.manager().releaseTaskExecutor(info.taskId()))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("terminal task authority");
            assertThat(info.executorHandle()).isEqualTo(handle);
            assertThat(runtime.arbitrator().getUsedMemory()).isEqualTo(budgetBefore);
            assertThat(runtime.nodePool().getUsedBytes()).isEqualTo(nodeBefore);
            // Retry with this exact owner's actual joined report, never a manufactured report.
            info.stageExecutorReleaseReceipt(ownReceipt);
            tasks.manager().releaseTaskExecutor(info.taskId());
            assertThat(info.executorHandle()).isZero();
            assertThat(info.executorReleaseReceipt().bytes().toByteArray()).isEqualTo(ownReceipt);
            assertThat(runtime.arbitrator().getUsedMemory()).isZero();
            assertThat(runtime.nodePool().getUsedBytes()).isZero();
        }
    }

}
