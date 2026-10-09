package org.pangrova.worker.task;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.protocol.v2.TaskExecutionOptions;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskRequest;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.testing.nativefixture.ActualNativeTaskPlans;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual canonical output admission, JNI execution, Flight values and buffer ownership. */
@Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class TaskManagerOutputConfigWiringTest {
    @Test
    void taskWithCustomBufferGrantCompletesWithItsActualValues() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "output-custom-grant")) {
            var request = request(true, 4L * 1024 * 1024, 3);
            var info = tasks.submit(request);
            tasks.awaitState(info, TaskManager.TaskState.RUNNING);
            assertThat(info.allocatedBufferMemory()).isEqualTo(request.getResourceGrant().getShuffleBufferBytes());
            assertThat(tasks.consume(info).values()).containsExactly(0L, 1L, 2L);
            assertThat(info.executorHandle()).isZero();
        }
    }

    @Test
    void taskWithoutDirectResultOutputUsesTheActualComputeCompletionPath() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "output-no-result")) {
            var request = request(false, 8L * 1024 * 1024, 2).toBuilder()
                    .clearExecutionOptions().build();
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
            var request = request(true, 1024L * 1024, 1);
            var info = tasks.submit(request);
            tasks.awaitState(info, TaskManager.TaskState.RUNNING);
            assertThat(info.allocatedBufferMemory()).isEqualTo(1024L * 1024);
            // Serialized grants use the query-allocation API, not the named-pool snapshot.
            assertThat(runtime.arbitrator().getUsedMemory()).isGreaterThanOrEqualTo(1024L * 1024);
            assertThat(runtime.nodePool().getUsedBytes()).isGreaterThanOrEqualTo(1024L * 1024);
            assertThat(tasks.consume(info).values()).containsExactly(0L);
            assertThat(info.allocatedBufferMemory()).isZero();
            assertThat(runtime.arbitrator().getUsedMemory()).isZero();
            assertThat(runtime.nodePool().getUsedBytes()).isZero();
        }
    }

    @Test
    void computeWithoutOutputGrantDoesNotInventADefaultReservation() {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "output-no-budget")) {
            var request = request(false, 0, 1);
            var info = tasks.submit(request);
            tasks.awaitState(info, TaskManager.TaskState.FINISHED);
            assertThat(info.allocatedBufferMemory()).isZero();
            assertThat(info.executorHandle()).isZero();
            assertThat(runtime.arbitrator().snapshot()).doesNotContainKey("buffer:" + info.taskId());
        }
    }

    @Test
    void fixtureDefaultGrantIsCarriedByTheAdmittedRequestNotAJavaBufferConstructor() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "output-default-request")) {
            var request = runtime.valuesRequest(true);
            var info = tasks.submit(request);
            tasks.awaitState(info, TaskManager.TaskState.RUNNING);
            assertThat(request.getResourceGrant().getShuffleBufferBytes()).isEqualTo(8L * 1024 * 1024);
            assertThat(info.allocatedBufferMemory()).isEqualTo(request.getResourceGrant().getShuffleBufferBytes());
            assertThat(tasks.consume(info).values()).containsExactly(42L);
        }
    }

    @Test
    void zeroDirectResultGrantFailsClosedBeforeNativeRegistration() {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "output-zero-budget")) {
            long baseline = runtime.arbitrator().getUsedMemory();
            assertThatThrownBy(() -> tasks.submit(request(true, 0, 1)))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("positive serialized-buffer grant");
            assertThat(tasks.manager().tasks).isEmpty();
            assertThat(tasks.reports().reports()).isEmpty();
            assertThat(runtime.arbitrator().getUsedMemory()).isEqualTo(baseline);
        }
    }

    private static TaskRequest request(boolean result, long bufferBytes, int rows) {
        var request = ActualNativeTaskFixture.planTaskRequest(result, ActualNativeTaskPlans.values(1, rows)).toBuilder();
        request.getResourceGrantBuilder().setShuffleBufferBytes(bufferBytes);
        request.setExecutionOptions(TaskExecutionOptions.newBuilder().setDirectResultOutput(result));
        return request.build();
    }
}
