package org.pangrova.streaming.maintenance;

import org.pangrova.streaming.enums.TriggerPriority;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies F-STREAM-09: when a maintenance task times out, the underlying
 * CompletableFuture is cancelled and the per-table in-flight slot is freed
 * so the table can be re-scheduled.
 */
class MaintenanceTaskTimeoutTest {

    private TimeoutTrackingSpi spi;
    private MaintenanceScheduler scheduler;

    @BeforeEach
    void setUp() {
        spi = new TimeoutTrackingSpi();
        // Very short timeout (50ms) so tests complete quickly
        MaintenanceConfig config = new MaintenanceConfig(
            Duration.ofMillis(1),   // compactionInterval
            Duration.ofHours(6),    // orphanCleanupInterval
            Duration.ofHours(12),   // snapshotExpirationInterval
            Duration.ofMillis(1),   // compactionScanInterval
            Duration.ofHours(24),   // orphanCleanupScanInterval
            5,                      // minFilesToCompact
            10,                     // maxEqualityDeleteFiles
            Duration.ofDays(3),     // orphanFileAge
            Duration.ofDays(7),     // snapshotRetention
            100,                    // snapshotRetainCount
            2,                      // threads
            2,                      // maxConcurrentTasks
            4,                      // taskQueueCapacity
            Duration.ofMillis(50)   // taskTimeout — very short for test
        );
        scheduler = new MaintenanceScheduler(spi, config);
    }

    @AfterEach
    void tearDown() {
        scheduler.close();
    }

    @Test
    void timeoutCancelsUnderlyingFutureAndFreesInFlightSlot() throws Exception {
        // Arrange: make compaction never complete on its own
        spi.blockForever();

        // Enqueue and process a task
        scheduler.enqueueWithBackpressure(
            new MaintenanceTask("timeout_table", TriggerPriority.THRESHOLD, Instant.now()));
        scheduler.processTaskQueue();

        // The table should be in-flight initially
        assertThat(scheduler.isInFlight("timeout_table")).isTrue();

        // Wait for the timeout to fire and the executeTask method to finish cleanup.
        // The timeout is 50ms; give generous time for the async processing.
        boolean freed = waitUntil(() -> !scheduler.isInFlight("timeout_table"), 3000);
        assertThat(freed)
            .as("in-flight slot must be freed after task timeout")
            .isTrue();

        // Verify the underlying future was cancelled
        assertThat(spi.wasCancelled())
            .as("underlying CompletableFuture must be cancelled on timeout")
            .isTrue();
    }

    @Test
    void tableCanBeRescheduledAfterTimeout() throws Exception {
        // Arrange: first compaction blocks forever (will timeout)
        spi.blockForever();

        scheduler.enqueueWithBackpressure(
            new MaintenanceTask("resched_table", TriggerPriority.THRESHOLD, Instant.now()));
        scheduler.processTaskQueue();

        // Wait for timeout to clear in-flight
        boolean freed = waitUntil(() -> !scheduler.isInFlight("resched_table"), 3000);
        assertThat(freed).isTrue();

        // Now allow the next compaction to complete immediately
        spi.allowCompletion();

        // Enqueue the same table again — should succeed because in-flight is cleared
        scheduler.enqueueWithBackpressure(
            new MaintenanceTask("resched_table", TriggerPriority.INTERVAL, Instant.now()));
        assertThat(scheduler.queueSize())
            .as("table must be re-enqueueable after timeout clears in-flight")
            .isEqualTo(1);

        // Process — should execute without issues
        scheduler.processTaskQueue();

        // Wait for completion
        boolean done = waitUntil(() -> !scheduler.isInFlight("resched_table"), 3000);
        assertThat(done).isTrue();
    }

    /**
     * Polls a condition until it becomes true or timeout elapses.
     */
    private boolean waitUntil(java.util.function.BooleanSupplier condition, long timeoutMs)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (!condition.getAsBoolean()) {
            if (System.currentTimeMillis() >= deadline) {
                return false;
            }
            Thread.sleep(10);
        }
        return true;
    }

    // --- SPI stub that tracks cancellation ---

    private static class TimeoutTrackingSpi implements TableMaintenanceSpi {

        private volatile boolean blockForever;
        private volatile CompletableFuture<CompactionResult> lastIssuedFuture;
        private final AtomicBoolean cancelled = new AtomicBoolean(false);
        private final CountDownLatch completionGate = new CountDownLatch(1);

        void blockForever() {
            blockForever = true;
        }

        void allowCompletion() {
            blockForever = false;
            completionGate.countDown();
        }

        boolean wasCancelled() {
            CompletableFuture<CompactionResult> f = lastIssuedFuture;
            return f != null && (f.isCancelled() || cancelled.get());
        }

        @Override
        public CompletableFuture<CompactionResult> compactAsync(String tableName) {
            if (!blockForever) {
                CompletableFuture<CompactionResult> f = CompletableFuture.completedFuture(
                    new CompactionResult(tableName, 5, 1, 1000L));
                lastIssuedFuture = f;
                return f;
            }
            // Create a future that will never complete on its own.
            // We track cancellation by observing the future's state.
            CompletableFuture<CompactionResult> future = new CompletableFuture<>() {
                @Override
                public boolean cancel(boolean mayInterruptIfRunning) {
                    cancelled.set(true);
                    return super.cancel(mayInterruptIfRunning);
                }
            };
            lastIssuedFuture = future;
            return future;
        }

        @Override
        public CompletableFuture<OrphanCleanupResult> cleanOrphansAsync(String tableName, Duration olderThan) {
            return CompletableFuture.completedFuture(
                new OrphanCleanupResult(tableName, 0, 0L));
        }

        @Override
        public CompletableFuture<SnapshotExpirationResult> expireSnapshotsAsync(String tableName, Duration olderThan) {
            return CompletableFuture.completedFuture(
                new SnapshotExpirationResult(tableName, 0));
        }

        @Override
        public MaintenanceStatus getMaintenanceStatus(String tableName) {
            return new MaintenanceStatus(
                tableName, 0, 10, 500L, 3, 0, 0L, 0,
                null, null, null);
        }
    }
}
