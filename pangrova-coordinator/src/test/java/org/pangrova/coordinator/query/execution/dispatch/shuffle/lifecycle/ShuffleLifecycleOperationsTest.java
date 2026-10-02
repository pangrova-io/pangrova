package org.pangrova.coordinator.query.execution.dispatch.shuffle.lifecycle;

import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Queue ownership and shutdown proof, not a distributed execution test. */
class ShuffleLifecycleOperationsTest {
    @Test
    void serializesExactExecutionWithoutMonitorsOrCrossExecutionHashCollisionBlocking() throws Exception {
        var operations = new ShuffleLifecycleOperations();
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var executor = Executors.newFixedThreadPool(2, task -> new Thread(task, "shuffle-operation-test"));
        assertThat("Aa".hashCode()).isEqualTo("BB".hashCode());
        try {
            var first = executor.submit(() -> operations.execute("Aa", () -> gated(entered, release)));
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(operations.execute("BB", () -> 2)).isEqualTo(2);
            var second = executor.submit(() -> operations.execute("Aa", () -> 3));
            assertThatThrownBy(() -> second.get(100, TimeUnit.MILLISECONDS)).isInstanceOf(TimeoutException.class);
            release.countDown();
            assertThat(first.get(5, TimeUnit.SECONDS)).isEqualTo(1);
            assertThat(second.get(5, TimeUnit.SECONDS)).isEqualTo(3);
            operations.awaitIdle();
            assertThatThrownBy(() -> operations.execute("Aa", () -> 4))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("closed");
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void exceptionsDoNotPoisonFollowingOperations() {
        var operations = new ShuffleLifecycleOperations();
        assertThatThrownBy(() -> operations.execute("exact", () -> { throw new IllegalStateException("RPC failed"); }))
                .isInstanceOf(IllegalStateException.class).hasMessage("RPC failed");
        assertThat(operations.execute("exact", () -> 2)).isEqualTo(2);
        operations.awaitIdle();
    }

    private static int gated(CountDownLatch entered, CountDownLatch release) {
        var info = ManagementFactory.getThreadMXBean().getThreadInfo(
                new long[] {Thread.currentThread().getId()}, true, true)[0];
        assertThat(info.getLockedMonitors()).isEmpty();
        entered.countDown();
        try {
            if (!release.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Operation gate timed out");
            }
            return 1;
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Operation interrupted", failure);
        }
    }
}
