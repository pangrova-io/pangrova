package org.pangrova.worker.execution;

import org.pangrova.testing.nativefixture.ActualNativeTaskFixture;
import org.pangrova.worker.nativebridge.jni.NativeBridgeException;
import org.pangrova.worker.nativebridge.jni.NativeRuntime;
import org.pangrova.protocol.v2.TaskReport;
import org.pangrova.protocol.v2.TaskReportOrigin;
import org.pangrova.protocol.v2.TaskState;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/** Exercises actual operation admission and exit barriers against real packaged JNI handles. */
@Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class NativeTaskExecutorReleaseRaceTest {

    @Test
    void releaseTaskRemovesActualHandleOwner() throws Exception {
        try (var fixture = new ActualNativeTaskFixture()) {
            var request = fixture.valuesRequest(false);
            long handle = fixture.createStarted(request);
            fixture.awaitFinished(handle);
            var receipt = fixture.executor().releaseTask(handle);
            assertThat(receipt).isPresent();
            var report = TaskReport.parseFrom(receipt.orElseThrow());
            assertThat(report.getAttempt()).isEqualTo(request.getAttempt());
            assertThat(report.getReportOrigin()).isEqualTo(TaskReportOrigin.TASK_REPORT_ORIGIN_NATIVE);
            assertThat(report.getState()).isEqualTo(TaskState.TASK_STATE_FINISHED);
            assertThat(report.hasError()).isFalse();
            assertThat(report.hasNativeTaskStats()).isTrue();
            assertThat(report.getUnknownFields().asMap()).isEmpty();
            assertThat(report.getNativeTaskStats().getUnknownFields().asMap()).isEmpty();
            assertNativeHandleGone(handle);
            assertThatThrownBy(() -> fixture.executor().acquireHandleLease(handle))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("not tracked");
        }
    }

    @Test
    void malformedCommandOnLiveOwnerRejectsWithoutRetiringItsActualHandle() throws Exception {
        try (var fixture = new ActualNativeTaskFixture()) {
            long handle = fixture.createPending(fixture.valuesRequest(false));
            assertThatThrownBy(() -> fixture.executor().executeCommand(handle, new byte[]{1}))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("Malformed");
            assertThat(fixture.executor().getTaskStatus(handle)).isNotEmpty();
            assertThat(fixture.executor().releaseTask(handle)).isPresent();
            assertNativeHandleGone(handle);
        }
    }

    @Test
    void operationOnReleasedHandleThrows() {
        try (var fixture = new ActualNativeTaskFixture()) {
            long handle = fixture.createPending(fixture.valuesRequest(false));
            var executor = fixture.executor();
            executor.releaseTask(handle);
            assertThatThrownBy(() -> executor.startTask(handle))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("not tracked");
            assertThatThrownBy(() -> executor.cancelTask(handle))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("not tracked");
            assertThatThrownBy(() -> executor.executeCommand(handle, new byte[]{1}))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("not tracked");
            assertThatThrownBy(() -> executor.isFinished(handle))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("not tracked");
            assertNativeHandleGone(handle);
        }
    }

    @Test
    void unknownHandleCannotInventTaskCompletionOrAReleaseReceipt() {
        try (var fixture = new ActualNativeTaskFixture()) {
            assertThatThrownBy(() -> fixture.executor().isFinished(Long.MAX_VALUE))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("not tracked");
            assertThat(fixture.executor().releaseTask(Long.MAX_VALUE)).isEmpty();
        }
    }

    @Test
    void releaseTaskAfterCloseIsNoOp() {
        try (var fixture = new ActualNativeTaskFixture()) {
            long handle = fixture.createPending(fixture.valuesRequest(false));
            fixture.stopTransports();
            fixture.executor().close();
            fixture.executor().releaseTask(handle);
            assertNativeHandleGone(handle);
        }
    }

    @Test
    void doubleReleaseIsIdempotent() {
        try (var fixture = new ActualNativeTaskFixture()) {
            long handle = fixture.createPending(fixture.valuesRequest(false));
            assertThat(fixture.executor().releaseTask(handle)).isPresent();
            assertThat(fixture.executor().releaseTask(handle)).isEmpty();
            assertNativeHandleGone(handle);
        }
    }

    @Test
    void concurrentReleaseOnSameHandleIsSafe() throws Exception {
        try (var fixture = new ActualNativeTaskFixture()) {
            for (int iteration = 0; iteration < 100; iteration++) {
                long handle = fixture.createPending(fixture.valuesRequest(false));
                var failure = new AtomicReference<Throwable>();
                var barrier = new CyclicBarrier(2);
                Runnable release = () -> {
                    try {
                        barrier.await(5, TimeUnit.SECONDS);
                        fixture.executor().releaseTask(handle);
                    } catch (Throwable error) {
                        failure.compareAndSet(null, error);
                    }
                };
                Thread first = new Thread(release, "release-1");
                Thread second = new Thread(release, "release-2");
                first.start();
                second.start();
                first.join(5000);
                second.join(5000);
                assertThat(first.isAlive()).as("first release %d", iteration).isFalse();
                assertThat(second.isAlive()).as("second release %d", iteration).isFalse();
                assertThat(failure.get()).as("release iteration %d", iteration).isNull();
                assertNativeHandleGone(handle);
                assertThatThrownBy(() -> NativeRuntime.nativeReleaseExecutor(handle))
                        .isInstanceOf(NativeBridgeException.class);
            }
        }
    }

    @Test
    void closeJoinsEnteredHandleOperations() throws Exception {
        try (var fixture = new ActualNativeTaskFixture()) {
            long first = fixture.createPending(fixture.valuesRequest(false));
            long second = fixture.createPending(fixture.valuesRequest(false));
            fixture.stopTransports();
            var operation = fixture.executor().acquireHandleLease(first);
            var completed = new CountDownLatch(1);
            var failure = new AtomicReference<Throwable>();
            Thread closer = new Thread(() -> {
                try {
                    fixture.executor().close();
                } catch (Throwable error) {
                    failure.set(error);
                } finally {
                    completed.countDown();
                }
            }, "close-with-real-handle");
            closer.start();
            try {
                await().atMost(Duration.ofSeconds(5)).until(() -> operation.blocks(closer));
                assertThat(completed.getCount()).isEqualTo(1);
            } finally {
                operation.close();
            }
            assertThat(completed.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(failure.get()).isNull();
            assertNativeHandleGone(first);
            assertNativeHandleGone(second);
            assertThatThrownBy(() -> fixture.executor().acquireHandleLease(0L))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("not tracked");
        }
    }

    static void assertNativeHandleGone(long handle) {
        assertThatThrownBy(() -> NativeRuntime.nativeGetTaskStatus(handle))
                .isInstanceOf(NativeBridgeException.class).hasMessageContaining("not found");
    }
}
