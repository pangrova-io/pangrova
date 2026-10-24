package org.pangrova.worker.task;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.protocol.v2.TaskExecutionOptions;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.testing.nativefixture.ActualNativeTaskPlans;

/** Actual canonical output admission, JNI execution, Flight values and buffer ownership. */
@Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class TaskManagerOutputConfigWiringTest {
    @Test
    void taskWithCustomBufferGrantCompletesWithItsActualValues() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
                var tasks = new ActualTaskManagerFixture(runtime, "output-custom-grant")) {
            var request = runtime.bindWorker(request(true, 4L * 1024 * 1024, 3));
            var info = tasks.submit(request);
            assertThat(info.allocatedBufferMemory())
                    .isEqualTo(request.getResourceGrant().getShuffleBufferBytes());
            assertThat(tasks.consume(info).values()).containsExactly(0L, 1L, 2L);
            tasks.awaitState(info, TaskManager.TaskState.FINISHED);
            assertThat(info.executorHandle()).isZero();
        }
    }

    @Test
    void taskWithoutDirectResultOutputUsesTheActualComputeCompletionPath() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
                var tasks = new ActualTaskManagerFixture(runtime, "output-no-result")) {
            var request =
                    runtime.bindWorker(request(false, 8L * 1024 * 1024, 2)).toBuilder().clearExecutionOptions().build();
            var info = tasks.submit(request);
            tasks.awaitState(info, TaskManager.TaskState.FINISHED);
            assertThat(request.getExecutionOptions().getDirectResultOutput()).isFalse();
            assertThat(info.executorHandle()).isZero();
            assertThat(info.allocatedBufferMemory()).isZero();
            var report = TaskReport.parseFrom(info.terminalTaskReport());
            assertThat(report.getState()).isEqualTo(TaskState.TASK_STATE_FINISHED);
            assertThat(report.hasNativeTaskStats()).isTrue();
            assertThat(report.getNativeTaskStats().getOperatorStatsList())
                    .anyMatch(operator -> operator.getOutputRows() == 2);
        }
    }

    @Test
    void outputReservationRespectsTheExactCanonicalByteGrant() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
                var tasks = new ActualTaskManagerFixture(runtime, "output-exact-budget")) {
            var request = runtime.bindWorker(request(true, 1024L * 1024, 1));
            var info = tasks.submit(request);
            assertThat(info.allocatedBufferMemory()).isEqualTo(1024L * 1024);
            assertThat(runtime.arbitrator().queryAllocationsSnapshot())
                    .containsEntry("buffer:" + info.taskId(), 1024L * 1024);
            assertThat(tasks.consume(info).values()).containsExactly(0L);
            tasks.awaitState(info, TaskManager.TaskState.FINISHED);
            assertThat(info.allocatedBufferMemory()).isZero();
            assertThat(runtime.arbitrator().queryAllocationsSnapshot())
                    .doesNotContainKey("buffer:" + info.taskId());
        }
    }

    @Test
    void computeWithoutOutputGrantDoesNotInventADefaultReservation() {
        try (var runtime = new ActualNativeTaskFixture();
                var tasks = new ActualTaskManagerFixture(runtime, "output-no-budget")) {
            var request = runtime.bindWorker(request(false, 0, 1));
            var info = tasks.submit(request);
            tasks.awaitState(info, TaskManager.TaskState.FINISHED);
            assertThat(info.allocatedBufferMemory()).isZero();
            assertThat(info.executorHandle()).isZero();
            assertThat(runtime.arbitrator().queryAllocationsSnapshot())
                    .doesNotContainKey("buffer:" + info.taskId());
        }
    }

    @Test
    void fixtureDefaultGrantIsCarriedByTheAdmittedRequestNotAJavaBufferConstructor()
            throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
                var tasks = new ActualTaskManagerFixture(runtime, "output-default-request")) {
            var request = runtime.valuesRequest(true);
            var info = tasks.submit(request);
            assertThat(request.getResourceGrant().getShuffleBufferBytes())
                    .isEqualTo(8L * 1024 * 1024);
            assertThat(info.allocatedBufferMemory())
                    .isEqualTo(request.getResourceGrant().getShuffleBufferBytes());
            assertThat(tasks.consume(info).values()).containsExactly(42L);
            tasks.awaitState(info, TaskManager.TaskState.FINISHED);
        }
    }

    @Test
    void zeroDirectResultGrantFailsClosedBeforeNativeRegistration() {
        try (var runtime = new ActualNativeTaskFixture();
                var tasks = new ActualTaskManagerFixture(runtime, "output-zero-budget")) {
            long baseline = runtime.arbitrator().getUsedMemory();
            assertThatThrownBy(() -> tasks.submit(runtime.bindWorker(request(true, 0, 1))))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("positive serialized-buffer grant");
            assertThat(tasks.manager().tasks).isEmpty();
            assertThat(tasks.reports().reports()).isEmpty();
            assertThat(runtime.arbitrator().getUsedMemory()).isEqualTo(baseline);
        }
    }

    private static TaskRequest request(boolean result, long bufferBytes, int rows) {
        var request =
                ActualNativeTaskFixture.planTaskRequest(
                        result, ActualNativeTaskPlans.values(1, rows))
                        .toBuilder();
        request.getResourceGrantBuilder().setShuffleBufferBytes(bufferBytes);
        request.setExecutionOptions(
                TaskExecutionOptions.newBuilder().setDirectResultOutput(result));
        return request.build();
    }
}
