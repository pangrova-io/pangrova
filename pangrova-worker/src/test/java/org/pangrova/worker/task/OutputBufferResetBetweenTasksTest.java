package org.pangrova.worker.task;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.testing.nativefixture.ActualNativeTaskPlans;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Per-attempt Native result owners replace the unconnected global Java buffer swap. */
@Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class OutputBufferResetBetweenTasksTest {
    @Test
    void bufferIsEmptyAtStartOfSecondTask() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "actual-result-isolation")) {
            var first = tasks.submit(ActualNativeTaskFixture.planTaskRequest(true,
                    ActualNativeTaskPlans.values(2, 3)));
            tasks.awaitState(first, TaskManager.TaskState.FINISHED);
            assertThat(tasks.consume(first).values()).containsExactlyInAnyOrder(0L, 1L, 2L, 3L, 4L, 5L);
            var second = tasks.submit(runtime.valuesRequest(true));
            tasks.awaitState(second, TaskManager.TaskState.FINISHED);
            assertThat(tasks.consume(second).values()).containsExactly(42L);
            assertThat(first.executorHandle()).isZero();
            assertThat(second.executorHandle()).isZero();
        }
    }

    @Test
    void releaseClosesPreviousNativeBuffer() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "actual-result-retire")) {
            var first = tasks.submit(runtime.valuesRequest(true));
            tasks.awaitState(first, TaskManager.TaskState.FINISHED);
            long oldHandle = first.executorHandle();
            tasks.consume(first);
            assertThatThrownBy(() -> runtime.executor().getTaskStatus(oldHandle))
                    .isInstanceOf(IllegalStateException.class);
            assertThat(runtime.executor().releaseTask(oldHandle)).isEmpty();
            assertThat(first.executorReleaseReport()).isNotEmpty();
            assertThat(first.allocatedBufferMemory()).isZero();
        }
    }

    @Test
    void nextEmptyResultHasNoPreviousBatches() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "actual-empty-isolation")) {
            var first = tasks.submit(runtime.valuesRequest(true));
            tasks.awaitState(first, TaskManager.TaskState.FINISHED);
            assertThat(tasks.consume(first).rows()).isOne();
            var second = tasks.submit(ActualNativeTaskFixture.planTaskRequest(true,
                    ActualNativeTaskPlans.values(0, 0)));
            tasks.awaitState(second, TaskManager.TaskState.FINISHED);
            var received = tasks.consume(second);
            assertThat(received.rows()).isZero();
            assertThat(received.values()).isEmpty();
            assertThat(first.terminalTaskReport()).isNotEqualTo(second.terminalTaskReport());
            assertThat(second.allocatedBufferMemory()).isZero();
        }
    }
}
