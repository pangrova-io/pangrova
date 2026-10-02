package org.pangrova.worker.execution;

import org.pangrova.worker.bootstrap.phase.ActualNativeTaskFixture;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.ResourceLock;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real JNI cleanup must remain idempotent under concurrent Worker shutdown. */
@Tag("native-required")
@ResourceLock(ActualNativeTaskFixture.RUNTIME_LOCK)
class NativeTaskExecutorConcurrentCloseTest {

    @Test
    void concurrentCloseCallsAreIdempotent() throws Exception {
        try (var fixture = new ActualNativeTaskFixture()) {
            long handle = fixture.createPending(fixture.valuesRequest(false));
            fixture.stopTransports();
            var ready = new CountDownLatch(10);
            var go = new CountDownLatch(1);
            var done = new CountDownLatch(10);
            var failure = new AtomicReference<Throwable>();
            for (int index = 0; index < 10; index++) {
                new Thread(() -> {
                    ready.countDown();
                    try {
                        if (!go.await(5, TimeUnit.SECONDS)) {
                            throw new IllegalStateException("Concurrent close start timed out");
                        }
                        fixture.executor().close();
                    } catch (Throwable error) {
                        failure.compareAndSet(null, error);
                    } finally {
                        done.countDown();
                    }
                }, "close-thread-" + index).start();
            }
            assertThat(ready.await(5, TimeUnit.SECONDS)).isTrue();
            go.countDown();
            assertThat(done.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(failure.get()).isNull();
            NativeTaskExecutorReleaseRaceTest.assertNativeHandleGone(handle);
        }
    }

    @Test
    void createTaskAfterCloseThrowsIllegalState() {
        try (var fixture = new ActualNativeTaskFixture()) {
            var request = fixture.valuesRequest(false);
            fixture.stopTransports();
            fixture.executor().close();
            assertThatThrownBy(() -> fixture.executor().createTask(request.toByteArray()))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("closed");
        }
    }
}
