package org.pangrova.streaming.core.maintenance;

import org.pangrova.spi.connector.data.write.model.WriteResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies F-STREAM-08: when a write-triggered (INTERVAL) maintenance task is
 * enqueued via {@link MaintenanceScheduler#onWriteComplete}, it is dispatched
 * immediately rather than waiting for the next periodic scan cycle.
 */
class MaintenanceImmediateDispatchTest {

    private ImmediateDispatchSpi spi;
    private MaintenanceScheduler scheduler;

    @BeforeEach
    void setUp() {
        spi = new ImmediateDispatchSpi();
        // Use a very long scan interval so the periodic scan never fires during the test.
        // If the task executes, it must be because onWriteComplete triggered immediate dispatch.
        MaintenanceConfig config = new MaintenanceConfig(
            Duration.ofMillis(1),       // compactionInterval (very short cooldown)
            Duration.ofHours(6),        // orphanCleanupInterval
            Duration.ofHours(12),       // snapshotExpirationInterval
            Duration.ofHours(24),       // compactionScanInterval — deliberately huge
            Duration.ofHours(24),       // orphanCleanupScanInterval
            5,                          // minFilesToCompact
            10,                         // maxEqualityDeleteFiles
            Duration.ofDays(3),         // orphanFileAge
            Duration.ofDays(7),         // snapshotRetention
            100,                        // snapshotRetainCount
            2,                          // threads
            2,                          // maxConcurrentTasks
            8,                          // taskQueueCapacity
            Duration.ofSeconds(5)       // taskTimeout
        );
        scheduler = new MaintenanceScheduler(spi, config);
    }

    @AfterEach
    void tearDown() {
        scheduler.close();
    }

    @Test
    void onWriteCompleteTriggersImmediateExecution() throws Exception {
        // Act: call onWriteComplete — no periodic scan is started, no manual processTaskQueue
        WriteResult result = new WriteResult("imm_table", 100L, 1, 0, 0, Instant.now());
        scheduler.onWriteComplete("imm_table", result);

        // Assert: the task should be executed promptly (within 2 seconds)
        boolean executed = spi.executionLatch.await(2, TimeUnit.SECONDS);
        assertThat(executed)
            .as("INTERVAL task must be executed immediately on enqueue, not wait for periodic scan")
            .isTrue();
        assertThat(spi.compactionCount.get()).isEqualTo(1);
    }

    @Test
    void onWriteCompleteExecutesMultipleTablesImmediately() throws Exception {
        // Enqueue maintenance for two different tables
        WriteResult result1 = new WriteResult("table_x", 50L, 1, 0, 0, Instant.now());
        WriteResult result2 = new WriteResult("table_y", 60L, 1, 0, 0, Instant.now());

        scheduler.onWriteComplete("table_x", result1);
        scheduler.onWriteComplete("table_y", result2);

        // Both should execute promptly
        boolean bothDone = spi.multiLatch.await(2, TimeUnit.SECONDS);
        assertThat(bothDone)
            .as("Both INTERVAL tasks must be dispatched immediately")
            .isTrue();
        assertThat(spi.compactionCount.get()).isEqualTo(2);
    }

    // --- SPI stub that tracks immediate execution ---

    private static class ImmediateDispatchSpi implements TableMaintenanceSpi {

        final CountDownLatch executionLatch = new CountDownLatch(1);
        final CountDownLatch multiLatch = new CountDownLatch(2);
        final AtomicInteger compactionCount = new AtomicInteger(0);

        @Override
        public CompletableFuture<CompactionResult> compactAsync(String tableName) {
            compactionCount.incrementAndGet();
            executionLatch.countDown();
            multiLatch.countDown();
            return CompletableFuture.completedFuture(
                new CompactionResult(tableName, 5, 1, 1000L));
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
