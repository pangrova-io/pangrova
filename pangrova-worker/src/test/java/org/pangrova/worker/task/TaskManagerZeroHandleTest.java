package org.pangrova.worker.task;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.worker.jni.NativeRuntimeException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual Native creation rejects invalid input instead of returning an invented zero handle. */
@Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class TaskManagerZeroHandleTest {
    @Test
    void taskRejectsInvalidNativeRequestWithoutRegisteringHandle() {
        try (var runtime = new ActualNativeTaskFixture()) {
            long available = runtime.arbitrator().getAvailableMemory();
            assertThatThrownBy(() -> runtime.executor().createTask(new byte[]{(byte) 0x80}))
                    .isInstanceOf(NativeRuntimeException.class);
            assertThat(runtime.arbitrator().getAvailableMemory()).isEqualTo(available);
            var request = runtime.valuesRequest(false);
            long handle = runtime.createPending(request);
            assertThat(handle).isPositive();
            assertThat(runtime.executor().releaseTask(handle)).isPresent();
            assertThat(runtime.executor().releaseTask(handle)).isEmpty();
        }
    }

    @Test
    void taskSucceedsOnNonZeroHandle() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "actual-positive-handle")) {
            var info = tasks.submit(runtime.valuesRequest(true));
            tasks.awaitState(info, TaskManager.TaskState.FINISHED);
            assertThat(info.executorHandle()).isPositive();
            assertThat(tasks.consume(info).values()).containsExactly(42L);
            assertThat(info.executorHandle()).isZero();
            var receipt = org.pangrova.protocol.v2.TaskReport.parseFrom(info.executorReleaseReport());
            assertThat(receipt.getAttempt()).isEqualTo(info.request().taskRequest().getAttempt());
            assertThat(receipt.getReportOrigin()).isEqualTo(
                    org.pangrova.protocol.v2.TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE);
            assertThat(receipt.getState()).isEqualTo(org.pangrova.protocol.v2.TaskState.TASK_STATE_FINISHED);
            assertThat(receipt.hasNativeTaskStats()).isTrue();
        }
    }
}
