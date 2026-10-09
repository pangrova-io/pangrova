package org.pangrova.worker.task;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.testing.nativefixture.ActualNativeShuffleRequests;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.testing.nativefixture.ActualNativeTaskPlans;

import static org.assertj.core.api.Assertions.assertThat;

/** Actual Native producer errors must not become successful/incomplete shuffle output. */
@Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class ShuffleErrorCallbackPropagationTest {
    @Test
    void shuffleProducerFailurePropagatesTaskToFailed() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "actual-shuffle-error")) {
            var request = ActualNativeShuffleRequests.producer(4, ActualNativeTaskPlans.failingCast());
            var info = tasks.submit(request);
            tasks.awaitState(info, TaskManager.TaskState.FAILED);
            var report = TaskReport.parseFrom(info.terminalTaskReport());
            assertThat(report.getAttempt()).isEqualTo(request.getAttempt());
            assertThat(report.getState()).isEqualTo(TaskState.TASK_STATE_FAILED);
            assertThat(report.hasError()).isTrue();
            assertThat(report.getError().getMessage()).contains("not-an-integer");
            assertThat(tasks.reports().reports()).noneMatch(status -> status.state().equals("FINISHED"));
        }
    }

    @Test
    void computeTaskWithoutShuffleConfigIgnoresShufflePipeline() throws Exception {
        try (var runtime = new ActualNativeTaskFixture();
             var tasks = new ActualTaskManagerFixture(runtime, "actual-no-shuffle")) {
            var info = tasks.submit(runtime.valuesRequest(true));
            tasks.awaitState(info, TaskManager.TaskState.RUNNING);
            assertThat(info.shuffleServerHandle()).isNull();
            assertThat(tasks.consume(info).values()).containsExactly(42L);
            assertThat(info.executorHandle()).isZero();
            assertThat(info.allocatedBufferMemory()).isZero();
        }
    }
}
