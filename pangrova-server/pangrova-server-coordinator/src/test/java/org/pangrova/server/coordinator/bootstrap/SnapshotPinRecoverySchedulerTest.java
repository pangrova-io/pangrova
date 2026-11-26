package org.pangrova.server.coordinator.bootstrap;

import org.junit.jupiter.api.Test;
import org.pangrova.common.concurrent.ManagedExecutorFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

class SnapshotPinRecoverySchedulerTest {

    @Test
    void inactiveEpochDoesNotStartRecovery() throws Exception {
        var executor = ManagedExecutorFactory.newSingleThreadScheduledExecutor("pin-recovery-test");
        var calls = new AtomicInteger();
        try (var scheduler = new SnapshotPinRecoveryScheduler(executor, () -> false,
                guard -> calls.incrementAndGet())) {
            scheduler.start();
            // A barrier behind the initial recovery cycle establishes that it ran.
            executor.submit(() -> {}).get(5, TimeUnit.SECONDS);
            assertThat(calls.get()).isZero();
        }
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void demotionStopsNewWorkWithoutInterruptingCurrentCycle() throws Exception {
        var executor = ManagedExecutorFactory.newSingleThreadScheduledExecutor("pin-recovery-test");
        var active = new AtomicBoolean(true);
        var entered = new CountDownLatch(1);
        var finish = new CountDownLatch(1);
        var guard = new AtomicReference<BooleanSupplier>();
        var interrupted = new AtomicBoolean(false);
        var scheduler = new SnapshotPinRecoveryScheduler(executor, active::get, stillLeader -> {
            guard.set(stillLeader);
            entered.countDown();
            try {
                finish.await();
            } catch (InterruptedException failure) {
                interrupted.set(true);
                Thread.currentThread().interrupt();
            }
        });
        try {
            scheduler.start();
            assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(guard.get().getAsBoolean()).isTrue();
            active.set(false);
            assertThat(guard.get().getAsBoolean()).isFalse();
            scheduler.close();
            active.set(true);
            assertThat(guard.get().getAsBoolean()).isFalse();
        } finally {
            finish.countDown();
            scheduler.close();
        }
        assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
        assertThat(interrupted.get()).isFalse();
    }
}
