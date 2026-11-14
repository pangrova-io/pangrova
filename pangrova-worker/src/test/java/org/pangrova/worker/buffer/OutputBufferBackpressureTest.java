package org.pangrova.worker.buffer;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.pangrova.protocol.v2.NativeTaskStats;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskState;
import org.pangrova.testing.nativefixture.ActualNativeResultReader;
import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.testing.nativefixture.ActualNativeTaskPlans;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/** Actual Native buffer owners and producer pressure; policy thresholds live in resilience tests. */
@Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class OutputBufferBackpressureTest {
    @Test
    void emptyBufferProceedsThroughActualFlightEof() throws Exception {
        try (var runtime = new ActualNativeTaskFixture()) {
            var request = ActualNativeTaskFixture.planTaskRequest(true, ActualNativeTaskPlans.values(0, 0));
            long handle = runtime.createStarted(request);
            var received = ActualNativeResultReader.count(runtime, request);
            assertThat(received.rows()).isZero();
            assertThat(received.sum()).isZero();
            assertThat(runtime.awaitFinished(handle).getState()).isEqualTo(TaskState.TASK_STATE_FINISHED);
            await().atMost(Duration.ofSeconds(20)).until(() -> runtime.drainEvidence(handle, request).getDrained());
            assertThat(runtime.executor().releaseTask(handle)).isPresent();
            assertThat(runtime.nodePool().getUsedBytes()).isZero();
        }
    }

    @Test
    void fullNativeBufferResumesOnlyWhenActualConsumerDrainsAllValues() throws Exception {
        try (var runtime = new ActualNativeTaskFixture()) {
            var request = ActualNativeTaskFixture.planTaskRequest(true, ActualNativeTaskPlans.largeCross(4096));
            long handle = runtime.createStarted(request);
            runtime.awaitConsumerPressure(handle);
            assertThat(runtime.drainEvidence(handle, request).getDrained()).isFalse();
            var received = ActualNativeResultReader.count(runtime, request);
            assertThat(received.rows()).isEqualTo(4096L * 4096L);
            assertThat(received.sum()).isEqualTo(4096L * (4096L * 4095L / 2L));
            assertThat(received.batches()).isGreaterThan(1);
            assertThat(runtime.awaitFinished(handle).getState()).isEqualTo(TaskState.TASK_STATE_FINISHED);
            await().atMost(Duration.ofSeconds(20)).until(() -> runtime.drainEvidence(handle, request).getDrained());
            var receipt = TaskReport.parseFrom(runtime.executor().releaseTask(handle).orElseThrow());
            assertThat(receipt.getAttempt()).isEqualTo(request.getAttempt());
            assertThat(receipt.getNativeTaskStats().getPeakMemoryBytes())
                    .isLessThanOrEqualTo(request.getResourceGrant().getNativeMemoryBytes());
            assertThat(runtime.nodePool().getUsedBytes()).isZero();
        }
    }

    @Test
    void nativeBackpressureWaitIsRealProducerTelemetryNotAJavaSlowdownSimulation() throws Exception {
        try (var runtime = new ActualNativeTaskFixture()) {
            var request = ActualNativeTaskFixture.planTaskRequest(true, ActualNativeTaskPlans.largeCross(4096));
            long handle = runtime.createStarted(request);
            runtime.awaitConsumerPressure(handle);
            var received = ActualNativeResultReader.count(runtime, request);
            assertThat(received.rows()).isEqualTo(4096L * 4096L);
            assertThat(runtime.awaitFinished(handle).getState()).isEqualTo(TaskState.TASK_STATE_FINISHED);
            var stats = NativeTaskStats.parseFrom(runtime.executor().getTaskStats(handle));
            assertThat(stats.getBackpressureWaitMs()).isPositive();
            await().atMost(Duration.ofSeconds(20)).until(() -> runtime.drainEvidence(handle, request).getDrained());
            assertThat(runtime.executor().releaseTask(handle)).isPresent();
        }
    }

    @Test
    void capacityMethodsReflectTheExistingBoundedQueueContract() throws InterruptedException {
        OutputBufferManager buffer = new OutputBufferManager(5);
        assertThat(buffer.isFull()).isFalse();
        assertThat(buffer.getRemainingCapacity()).isEqualTo(5);
        assertThat(buffer.getMaxBatches()).isEqualTo(5);
        buffer.addBatch(new byte[]{1});
        assertThat(buffer.getBufferedCount()).isEqualTo(1);
        assertThat(buffer.getRemainingCapacity()).isEqualTo(4);
        for (int i = 0; i < 4; i++) buffer.addBatch(new byte[]{(byte) i});
        assertThat(buffer.isFull()).isTrue();
        assertThat(buffer.getRemainingCapacity()).isEqualTo(0);
    }
}
