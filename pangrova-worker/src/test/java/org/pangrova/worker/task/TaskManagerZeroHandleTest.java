package org.pangrova.worker.task;

import org.pangrova.worker.enums.TaskState;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;

/** Actual Native creation rejects invalid input instead of returning an invented zero handle. */
@Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class TaskManagerZeroHandleTest {
    @Test
    void taskRejectsInvalidNativeRequestWithoutRegisteringHandle() {
        try (var runtime = new ActualNativeTaskFixture()) {
            long available = runtime.arbitrator().getAvailableMemory();
            assertThatThrownBy(() -> runtime.executor().createTask(
                            org.pangrova.protocol.v2.TaskRequest.getDefaultInstance()))
                    .isInstanceOf(org.pangrova.worker.exception.NativeTaskCreationFailure.class)
                    .hasCauseInstanceOf(IllegalArgumentException.class)
                    .satisfies(
                            failure -> {
                                var creation =
                                        (org.pangrova.worker.exception.NativeTaskCreationFailure)
                                                failure;
                                assertThat(creation.actualHandle()).isZero();
                                assertThat(creation.withoutNativeOwner()).isTrue();
                            });
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
            assertThat(info.executorHandle()).isPositive();
            assertThat(tasks.consume(info).values()).containsExactly(42L);
            tasks.awaitState(info, TaskState.FINISHED);
            assertThat(info.executorHandle()).isZero();
            var receipt =
                    info.executorReleaseReceipt().report();
            assertThat(receipt.getAttempt()).isEqualTo(info.request().taskRequest().getAttempt());
            assertThat(receipt.getReportOrigin())
                    .isEqualTo(org.pangrova.protocol.v2.TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE);
            assertThat(receipt.getState())
                    .isEqualTo(org.pangrova.protocol.v2.TaskState.TASK_STATE_FINISHED);
            assertThat(receipt.hasNativeTaskStats()).isTrue();
        }
    }
}
