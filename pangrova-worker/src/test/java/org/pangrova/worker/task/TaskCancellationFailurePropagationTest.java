package org.pangrova.worker.task;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
/** Actual Native owner retirement must never acknowledge a failed cancel as successful. */
@Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class TaskCancellationFailurePropagationTest {
    @Test
    void retiredActualNativeOwnerDoesNotAcknowledgeSuccessfulCancellation() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "actual-cancel-retired-owner")) {
            var request = runtime.valuesRequest(false);
            var submitted = org.pangrova.protocol.rest.task.SubmitTaskRequest.from(request);
            var info = new TaskInfo(request.getAttempt().getTaskId(),
                    request.getAttempt().getQueryOrJobId(), submitted,
                    org.pangrova.protocol.rest.task.convert.TaskRequestDtoConverter.toTaskDescriptor(submitted),
                    java.time.Instant.now());
            info.beginNativeCreation();
            long handle = runtime.createPending(request);
            info.setExecutorHandle(handle);
            byte[] actualReceipt = runtime.executor().releaseTask(handle).orElseThrow();
            assertThat(org.pangrova.protocol.v2.TaskReport.parseFrom(actualReceipt).getAttempt())
                    .isEqualTo(request.getAttempt());
            // The real Native receipt proves retirement; this exact stale handle must fail
            // cancellation. Existing full-manager tests separately retain every FSM/join oracle.
            assertThatThrownBy(() -> tasks.manager().lifecycleManager.cancelNativeExecutor(info))
                    .isInstanceOf(RuntimeException.class);
            assertThat(runtime.arbitrator().getUsedMemory()).isZero();
            assertThat(runtime.nodePool().getUsedBytes()).isZero();
        }
    }

}
