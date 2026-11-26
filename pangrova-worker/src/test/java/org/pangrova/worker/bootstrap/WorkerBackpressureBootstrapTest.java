package org.pangrova.worker.bootstrap;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.testing.nativefixture.ActualNativeResultReader;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.testing.nativefixture.ActualNativeTaskPlans;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Real runtime bootstrap and Native result owner wiring, not a Worker/Coordinator SQL E2E. */
@Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class WorkerBackpressureBootstrapTest {
    @Test
    void bootstrapResultTransportConsumesAnActualUnpressuredProducer() throws Exception {
        try (var runtime = new ActualNativeTaskFixture()) {
            var request = runtime.valuesRequest(true);
            long handle = runtime.createStarted(request);
            assertThat(runtime.resultPort()).isPositive();
            assertThat(ActualNativeResultReader.read(runtime, request).values()).containsExactly(42L);
            assertThat(runtime.awaitFinished(handle).getState()).isEqualTo(TaskState.TASK_STATE_FINISHED);
            await().atMost(Duration.ofSeconds(20)).until(() -> runtime.drainEvidence(handle, request).getDrained());
            assertThat(runtime.executor().releaseTask(handle)).isPresent();
            assertThat(runtime.nodePool().getUsedBytes()).isZero();
        }
    }

    @Test
    void bootstrapRuntimeCancelsAndReleasesTheActualUnconsumedProducer() throws Exception {
        try (var runtime = new ActualNativeTaskFixture()) {
            var request = ActualNativeTaskFixture.planTaskRequest(true, ActualNativeTaskPlans.largeCross(4096));
            long handle = runtime.createStarted(request);
            runtime.awaitConsumerPressure(handle);
            assertThat(runtime.drainEvidence(handle, request).getDrained()).isFalse();
            runtime.executor().cancelTask(handle);
            var receipt = TaskReport.parseFrom(runtime.executor().releaseTask(handle).orElseThrow());
            assertThat(receipt.getState()).isEqualTo(TaskState.TASK_STATE_CANCELLED);
            assertThat(receipt.getAttempt()).isEqualTo(request.getAttempt());
            assertThat(receipt.hasNativeTaskStats()).isTrue();
            assertThat(receipt.getNativeTaskStats().getOperatorStatsList())
                    .filteredOn(op -> "CallbackSink".equals(op.getOperatorType()))
                    .anySatisfy(op -> {
                        assertThat(op.getWaitForConsumerNanos()).isPositive();
                        assertThat(op.getBlockedNanos()).isGreaterThanOrEqualTo(op.getWaitForConsumerNanos());
                        assertThat(op.getWaitForProducerNanos()).isZero();
                    });
            assertThat(runtime.nodePool().getUsedBytes()).isZero();
            assertThat(runtime.executor().releaseTask(handle)).isEmpty();
        }
    }
}
