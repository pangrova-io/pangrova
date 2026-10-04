package org.pangrova.transport;

import org.junit.jupiter.api.Test;
import org.pangrova.common.concurrent.ManagedExecutorFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Production owner-state and real executor scheduling only; not a simulated database sink. */
class ResultTransportLifecycleTest {
    @Test
    void actualShutdownExecutorRejectionReturnsOnlyTheUnstartedReaderClaim() throws Exception {
        var lifecycle = new ResultTransportLifecycle();
        var executor = ManagedExecutorFactory.newSingleThreadExecutor("result-rejected-reader");
        executor.shutdown();
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        try (var operation = lifecycle.enter()) {
            assertThatThrownBy(() -> lifecycle.read(executor, () -> true))
                    .isInstanceOf(java.util.concurrent.RejectedExecutionException.class);
        }
        assertThat(lifecycle.beginClose()).isTrue();
        lifecycle.closeStreams();
        lifecycle.closeClients();
        lifecycle.endClose(true);
    }

    @Test
    void cancelledRunningReaderRetainsOwnerUntilItsActualCallableExits() throws Exception {
        var lifecycle = new ResultTransportLifecycle();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var executor = ManagedExecutorFactory.newSingleThreadExecutor("result-owned-reader");
        try {
            var operation = lifecycle.enter();
            var read = lifecycle.read(executor, () -> {
                entered.countDown();
                awaitActualExit(release);
                return true;
            });
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(read.cancel(true)).isTrue();
            operation.close();
            assertThatThrownBy(lifecycle::beginClose).isInstanceOf(TransportConnectionException.class)
                    .hasMessageContaining("readers are still owned");
            release.countDown();
            executor.shutdown();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            assertThat(lifecycle.beginClose()).isTrue();
            lifecycle.closeClients();
            lifecycle.endClose(true);
            assertThat(lifecycle.beginClose()).isFalse();
        } finally {
            release.countDown();
            executor.shutdown();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void cancelledQueuedReaderReleasesOnlyTheNeverStartedClaim() throws Exception {
        var lifecycle = new ResultTransportLifecycle();
        var executor = ManagedExecutorFactory.newSingleThreadExecutor("result-queued-reader");
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var calls = new AtomicInteger();
        try {
            executor.execute(() -> { entered.countDown(); awaitActualExit(release); });
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            var operation = lifecycle.enter();
            var queued = lifecycle.read(executor, () -> { calls.incrementAndGet(); return true; });
            assertThat(queued.cancel(false)).isTrue();
            operation.close();
            assertThat(lifecycle.beginClose()).isTrue();
            lifecycle.closeClients();
            lifecycle.endClose(true);
            release.countDown();
            executor.shutdown();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            assertThat(calls).hasValue(0);
            assertThatThrownBy(lifecycle::enter).isInstanceOf(TransportConnectionException.class);
        } finally {
            release.countDown();
            executor.shutdown();
            assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static void awaitActualExit(CountDownLatch release) {
        boolean interrupted = false;
        try {
            for (;;) {
                try {
                    if (!release.await(5, TimeUnit.SECONDS)) throw new AssertionError("Actual reader owner was not released");
                    return;
                } catch (InterruptedException retryUntilReleased) { interrupted = true; }
            }
        } finally { if (interrupted) Thread.currentThread().interrupt(); }
    }
}
