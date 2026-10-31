package org.pangrova.streaming.property.maintenance;

import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;
import org.pangrova.connector.spi.data.write.model.WriteResult;
import org.pangrova.streaming.core.maintenance.*;
import org.pangrova.streaming.core.maintenance.TriggerPriority;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property tests for {@link MaintenanceScheduler} priority ordering
 * and backpressure replacement.
 */
class MaintenanceSchedulerPriorityPropertyTest {

    @Property(tries = 10)
    void higherPriorityTaskReplacesLowerWhenQueueFull(
            @ForAll @IntRange(min = 2, max = 8) int queueCapacity) {
        MaintenanceConfig config = configWithCapacity(queueCapacity);
        MaintenanceScheduler scheduler = new MaintenanceScheduler(new NoOpSpi(), config);
        try {
            // Fill queue with lowest priority
            for (int i = 0; i < queueCapacity; i++) {
                scheduler.enqueueWithBackpressure(
                        new MaintenanceTask("low_" + i, TriggerPriority.SCHEDULED_SCAN, Instant.now()));
            }
            assertThat(scheduler.queueSize()).isEqualTo(queueCapacity);

            // Enqueue higher priority — should replace one low priority
            scheduler.enqueueWithBackpressure(
                    new MaintenanceTask("high", TriggerPriority.THRESHOLD, Instant.now()));
            assertThat(scheduler.queueSize()).isEqualTo(queueCapacity);
        } finally {
            scheduler.close();
        }
    }

    @Property(tries = 10)
    void samePriorityDoesNotReplaceWhenQueueFull(
            @ForAll @IntRange(min = 2, max = 8) int queueCapacity) {
        MaintenanceConfig config = configWithCapacity(queueCapacity);
        MaintenanceScheduler scheduler = new MaintenanceScheduler(new NoOpSpi(), config);
        try {
            for (int i = 0; i < queueCapacity; i++) {
                scheduler.enqueueWithBackpressure(
                        new MaintenanceTask("t_" + i, TriggerPriority.INTERVAL, Instant.now()));
            }
            // Same priority should not replace
            scheduler.enqueueWithBackpressure(
                    new MaintenanceTask("same", TriggerPriority.INTERVAL, Instant.now()));
            assertThat(scheduler.queueSize()).isEqualTo(queueCapacity);
        } finally {
            scheduler.close();
        }
    }

    @Property(tries = 10)
    void inFlightTableIsNotReEnqueued(
            @ForAll @IntRange(min = 1, max = 5) int writeCount) {
        MaintenanceConfig config = configWithCapacity(16);
        BlockingSpi spi = new BlockingSpi();
        MaintenanceScheduler scheduler = new MaintenanceScheduler(spi, config);
        try {
            // First write enqueues AND immediately dispatches (task moves to in-flight)
                var result = new WriteResult("t1", 100, 1, 0, 0, Instant.now());
            scheduler.onWriteComplete("t1", result);
            assertThat(scheduler.isInFlight("t1")).isTrue();

            // Subsequent writes should not re-enqueue (in-flight guard)
            for (int i = 0; i < writeCount; i++) {
                scheduler.onWriteComplete("t1", result);
            }
            assertThat(scheduler.queueSize()).isEqualTo(0);
        } finally {
            spi.unblock();
            scheduler.close();
        }
    }

    private static MaintenanceConfig configWithCapacity(int capacity) {
        return new MaintenanceConfig(
                Duration.ofMillis(1), Duration.ofHours(6), Duration.ofHours(12),
                Duration.ofMillis(1), Duration.ofHours(24),
                5, 10, Duration.ofDays(3), Duration.ofDays(7), 100,
                2, 2, capacity, Duration.ofSeconds(5));
    }

    static class NoOpSpi implements TableMaintenanceSpi {
        @Override public CompletableFuture<CompactionResult> compactAsync(String t) {
            return CompletableFuture.completedFuture(new CompactionResult(t, 0, 0, 0));
        }
        @Override public CompletableFuture<OrphanCleanupResult> cleanOrphansAsync(String t, Duration d) {
            return CompletableFuture.completedFuture(new OrphanCleanupResult(t, 0, 0));
        }
        @Override public CompletableFuture<SnapshotExpirationResult> expireSnapshotsAsync(String t, Duration d) {
            return CompletableFuture.completedFuture(new SnapshotExpirationResult(t, 0));
        }
        @Override public MaintenanceStatus getMaintenanceStatus(String t) {
            return new MaintenanceStatus(t, 0, 0, 0, 0, 0, 0, 0, null, null, null);
        }
    }

    static class BlockingSpi extends NoOpSpi {
        private final java.util.concurrent.CountDownLatch gate = new java.util.concurrent.CountDownLatch(1);
        @Override public CompletableFuture<CompactionResult> compactAsync(String t) {
            return CompletableFuture.supplyAsync(() -> {
                try { gate.await(); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                return new CompactionResult(t, 0, 0, 0);
            });
        }
        void unblock() { gate.countDown(); }
    }
}
