package org.pangrova.server.coordinator.bootstrap;

import org.junit.jupiter.api.Test;
import org.pangrova.common.concurrency.ManagedExecutorFactory;

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
    void latePinPressureObservationCannotRepopulateEndedEpoch() throws Exception {
        var executor = ManagedExecutorFactory.newSingleThreadScheduledExecutor("pin-pressure-test");
        var active = new AtomicBoolean(true);
        var entered = new CountDownLatch(1);
        var finish = new CountDownLatch(1);
        var observed = new AtomicReference<java.util.function.Supplier<java.util.OptionalDouble>>();
        var provider = new org.pangrova.observability.noop.NoOpMetricsProvider() {
            @Override
            public org.pangrova.observability.api.metrics.ObservableDoubleGauge createObservedGauge(
                    String name, String description, String unit,
                    org.pangrova.observability.api.metrics.MetricAttributes attributes,
                    java.util.function.Supplier<java.util.OptionalDouble> sample) {
                if (name.endsWith("sample_age_seconds")) {
                    observed.set(sample);
                }
                return () -> {};
            }
        };
        try (var metrics = new org.pangrova.coordinator.indexing.lifecycle.NativeSnapshotPinPressureMetrics(provider)) {
            metrics.beginEpoch(7);
            var scheduler = new SnapshotPinRecoveryScheduler(executor, active::get, stillLeader -> {
                entered.countDown();
                try {
                    finish.await();
                    // Simulate a DB result that was already in flight when the term ended.
                    var states = java.util.List.of(
                            org.pangrova.persistence.generation.api.enums.SnapshotPinState.INTENDED,
                            org.pangrova.persistence.generation.api.enums.SnapshotPinState.PINNED,
                            org.pangrova.persistence.generation.api.enums.SnapshotPinState.RELEASING);
                    var sample = new org.pangrova.persistence.generation.api.job.SnapshotPinPressure(
                            java.time.Instant.now(), states.stream().map(state ->
                                    new org.pangrova.persistence.generation.api.job.SnapshotPinPressure.StateCount(
                                            state, 0, false, java.util.Optional.empty())).toList());
                    metrics.record(7, java.util.Optional.of(sample));
                } catch (InterruptedException failure) {
                    Thread.currentThread().interrupt();
                }
            });
            try {
                scheduler.start();
                assertThat(entered.await(5, TimeUnit.SECONDS)).isTrue();
                active.set(false);
                metrics.endEpoch();
                scheduler.close();
                metrics.beginEpoch(8);
                finish.countDown();
                assertThat(executor.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
                assertThat(observed.get().get()).isEmpty();
            } finally {
                finish.countDown();
                scheduler.close();
            }
        }
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
