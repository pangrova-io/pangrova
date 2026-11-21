package org.pangrova.worker.bootstrap;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.testing.nativefixture.ActualNativeResultReader;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.testing.nativefixture.ActualNativeTaskPlans;

import java.time.Duration;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Real packaged JNI tasks, Flight batches, terminal receipts and Native handle ownership.
 * WorkerBootstrap registration/lifecycle oracles live in WorkerBootstrapE2ETest.
 */
@Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class WorkerExecutionChainTest {
    @Provide
    Arbitrary<Integer> batchCounts() {
        return Arbitraries.integers().between(1, 5);
    }

    @Property(tries = 5)
    void nativeExecutorProducesExactBatchesThenEof(@ForAll("batchCounts") int batchCount) throws Exception {
        verifyActualBatches(batchCount);
    }

    @Test
    void nativeTaskExecutorFullLifecycle() throws Exception {
        verifyActualBatches(3);
    }

    private static void verifyActualBatches(int batchCount) throws Exception {
        try (var runtime = new ActualNativeTaskFixture()) {
            var request = ActualNativeTaskFixture.planTaskRequest(true,
                    ActualNativeTaskPlans.values(batchCount, 3));
            long handle = runtime.createStarted(request);
            var received = ActualNativeResultReader.read(runtime, request);
            assertThat(received.batches()).isEqualTo(batchCount);
            assertThat(received.rows()).isEqualTo(batchCount * 3L);
            assertThat(received.values()).containsExactlyInAnyOrderElementsOf(
                    LongStream.range(0, batchCount * 3L).boxed().toList());
            assertThat(runtime.awaitFinished(handle).getState()).isEqualTo(TaskState.TASK_STATE_FINISHED);
            await().atMost(Duration.ofSeconds(20)).until(() -> runtime.drainEvidence(handle, request).getDrained());
            var receipt = TaskReport.parseFrom(runtime.executor().releaseTask(handle).orElseThrow());
            assertThat(receipt.getAttempt()).isEqualTo(request.getAttempt());
            assertThat(receipt.hasNativeTaskStats()).isTrue();
            assertThat(runtime.executor().releaseTask(handle)).isEmpty();
            assertThatThrownBy(() -> runtime.executor().getTaskStatus(handle))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("not tracked");
            assertThat(runtime.nodePool().getUsedBytes()).isZero();
        }
    }

    @Test
    void nativeExecutorTracksTwoThenOneThenNoActualHandleOwners() throws Exception {
        try (var runtime = new ActualNativeTaskFixture()) {
            var first = runtime.valuesRequest(false);
            var second = runtime.valuesRequest(false);
            long h1 = runtime.createStarted(first);
            long h2 = runtime.createStarted(second);
            assertThat(runtime.awaitFinished(h1).getAttempt()).isEqualTo(first.getAttempt());
            assertThat(runtime.awaitFinished(h2).getAttempt()).isEqualTo(second.getAttempt());
            assertThat(runtime.executor().releaseTask(h1)).isPresent();
            assertThatThrownBy(() -> runtime.executor().getTaskStatus(h1))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("not tracked");
            assertThat(TaskReport.parseFrom(runtime.executor().getTaskStatus(h2)).getAttempt())
                    .isEqualTo(second.getAttempt());
            assertThat(runtime.executor().releaseTask(h2)).isPresent();
            assertThatThrownBy(() -> runtime.executor().getTaskStatus(h2))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("not tracked");
            assertThat(runtime.nodePool().getUsedBytes()).isZero();
        }
    }
}
