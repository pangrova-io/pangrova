package org.pangrova.coordinator.artifact;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Control lifecycle proof; this is not a distributed Worker end-to-end test. */
class ArtifactPollLifecycleTest {
    @Test
    void concurrentPollReturnsAndShutdownWaitsForOwnerBeforeCleanup() throws Exception {
        var lifecycle = new ArtifactPollLifecycle();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var cleanups = new AtomicInteger();
        var executor = Executors.newFixedThreadPool(2, task -> new Thread(task, "artifact-poll-test"));
        try {
            var polling = executor.submit(() -> lifecycle.poll(() -> gatedPoll(entered, release)));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(lifecycle.poll(() -> { throw new AssertionError("Concurrent reconciliation"); })).isZero();
            var closing = executor.submit(() -> lifecycle.close(cleanups::incrementAndGet));
            assertThatThrownBy(() -> closing.get(100, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
            assertThat(cleanups).hasValue(0);
            release.countDown();
            assertThat(polling.get(5, TimeUnit.SECONDS)).isEqualTo(7);
            closing.get(5, TimeUnit.SECONDS);
            lifecycle.close(cleanups::incrementAndGet);
            assertThat(cleanups).hasValue(1);
            assertThat(lifecycle.poll(() -> { throw new AssertionError("Closed reconciliation"); })).isZero();
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void failedPollReleasesOwnershipWithoutHidingFailure() {
        var lifecycle = new ArtifactPollLifecycle();
        assertThatThrownBy(() -> lifecycle.poll(() -> { throw new IllegalStateException("PG unavailable"); }))
                .isInstanceOf(IllegalStateException.class).hasMessage("PG unavailable");
        assertThat(lifecycle.poll(() -> 3)).isEqualTo(3);
        lifecycle.close(() -> { });
    }

    private static int gatedPoll(CountDownLatch entered, CountDownLatch release) {
        entered.countDown();
        try {
            if (!release.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Poll gate timed out");
            }
            return 7;
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Poll gate interrupted", failure);
        }
    }
}
