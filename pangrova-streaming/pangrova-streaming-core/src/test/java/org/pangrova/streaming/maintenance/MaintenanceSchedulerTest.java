package org.pangrova.streaming.maintenance;

import org.pangrova.common.concurrent.ThreadSafe;
import org.pangrova.connector.api.write.WriteResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link MaintenanceScheduler} concurrency safety and core logic.
 */
class MaintenanceSchedulerTest {

    private StubTableMaintenanceSpi spi;
    private MaintenanceConfig config;
    private MaintenanceScheduler scheduler;

    @BeforeEach
    void setUp() {
        spi = new StubTableMaintenanceSpi();
        config = new MaintenanceConfig(
            Duration.ofMillis(1),   // compactionInterval (very short for tests)
            Duration.ofHours(6),
            Duration.ofHours(12),
            Duration.ofMillis(1),   // compactionScanInterval
            Duration.ofHours(24),
            5,                      // minFilesToCompact
            10,                     // maxEqualityDeleteFiles
            Duration.ofDays(3),
            Duration.ofDays(7),
            100,
            2,                      // threads
            2,                      // maxConcurrentTasks
            4,                      // taskQueueCapacity
            Duration.ofSeconds(5)   // taskTimeout
        );
        scheduler = new MaintenanceScheduler(spi, config);
    }

    @AfterEach
    void tearDown() {
        scheduler.close();
    }

    @Test
    void classIsAnnotatedThreadSafe() {
        assertThat(MaintenanceScheduler.class.isAnnotationPresent(ThreadSafe.class))
            .as("MaintenanceScheduler must be annotated @ThreadSafe")
            .isTrue();
    }

    @Test
    void onWriteCompleteEnqueuesTaskWhenIntervalElapsed() {
        // Block SPI so the dispatched task stays in-flight and is observable
        spi.blockCompaction();
        WriteResult result = stubWriteResult();
        scheduler.onWriteComplete("table_a", result);
        // onWriteComplete enqueues AND immediately processes the queue,
        // so the task should now be in-flight (not sitting in the queue).
        assertThat(scheduler.isInFlight("table_a")).isTrue();
        assertThat(scheduler.queueSize()).isEqualTo(0);
        spi.unblockCompaction();
    }

    @Test
    void onWriteCompleteSkipsWhenInFlight() {
        // Arrange: block the compaction so the task stays in-flight
        spi.blockCompaction();
        WriteResult result = stubWriteResult();
        scheduler.onWriteComplete("table_a", result);
        scheduler.processTaskQueue();

        // The table is now in-flight
        assertThat(scheduler.isInFlight("table_a")).isTrue();

        // Act: try to enqueue again
        scheduler.onWriteComplete("table_a", result);

        // Assert: queue should be empty (second enqueue was skipped)
        assertThat(scheduler.queueSize()).isEqualTo(0);

        spi.unblockCompaction();
    }

    @Test
    void scanAllTablesEnqueuesBasedOnPriority() {
        spi.setStatus("table_a", new MaintenanceStatus(
            "table_a", 10, 50, 1000L, 5, 15, 500L, 0,
            null, null, null));

        scheduler.scanAllTablesWithPriority(List.of("table_a"));
        assertThat(scheduler.queueSize()).isEqualTo(1);
    }

    @Test
    void processTaskQueueRespectsMaxConcurrency() throws Exception {
        spi.blockCompaction();

        // Enqueue 3 tasks
        scheduler.enqueueWithBackpressure(
            new MaintenanceTask("t1", TriggerPriority.THRESHOLD, Instant.now()));
        scheduler.enqueueWithBackpressure(
            new MaintenanceTask("t2", TriggerPriority.INTERVAL, Instant.now()));
        scheduler.enqueueWithBackpressure(
            new MaintenanceTask("t3", TriggerPriority.SCHEDULED_SCAN, Instant.now()));

        scheduler.processTaskQueue();

        // maxConcurrentTasks=2, so only 2 should be in-flight, 1 remains in queue
        int inFlightCount = 0;
        for (String name : List.of("t1", "t2", "t3")) {
            if (scheduler.isInFlight(name)) {
                inFlightCount++;
            }
        }
        assertThat(inFlightCount).isEqualTo(2);
        assertThat(scheduler.queueSize()).isEqualTo(1);

        spi.unblockCompaction();
    }

    @Test
    void enqueueWithBackpressureReplacesLowestPriority() {
        MaintenanceConfig smallQueueConfig = new MaintenanceConfig(
            Duration.ofMillis(1), Duration.ofHours(6), Duration.ofHours(12),
            Duration.ofMillis(1), Duration.ofHours(24),
            5, 10, Duration.ofDays(3), Duration.ofDays(7), 100,
            2, 2, 2, Duration.ofSeconds(5));
        MaintenanceScheduler smallScheduler = new MaintenanceScheduler(spi, smallQueueConfig);

        try {
            // Fill queue to capacity
            smallScheduler.enqueueWithBackpressure(
                new MaintenanceTask("t1", TriggerPriority.SCHEDULED_SCAN, Instant.now()));
            smallScheduler.enqueueWithBackpressure(
                new MaintenanceTask("t2", TriggerPriority.SCHEDULED_SCAN, Instant.now()));
            assertThat(smallScheduler.queueSize()).isEqualTo(2);

            // Enqueue higher priority task — should replace one SCHEDULED_SCAN
            smallScheduler.enqueueWithBackpressure(
                new MaintenanceTask("t3", TriggerPriority.THRESHOLD, Instant.now()));
            assertThat(smallScheduler.queueSize()).isEqualTo(2);
        } finally {
            smallScheduler.close();
        }
    }

    @Test
    void concurrentOnWriteCompleteDoesNotCorruptState() throws Exception {
        // Block compaction so tasks stay in-flight or in queue (observable state)
        spi.blockCompaction();
        // Use a config with large enough queue and concurrency for all tasks
        MaintenanceConfig largeQueueConfig = new MaintenanceConfig(
            Duration.ofMillis(1), Duration.ofHours(6), Duration.ofHours(12),
            Duration.ofMillis(1), Duration.ofHours(24),
            5, 10, Duration.ofDays(3), Duration.ofDays(7), 100,
            2, 16, 16, Duration.ofSeconds(5));
        MaintenanceScheduler largeScheduler = new MaintenanceScheduler(spi, largeQueueConfig);

        try {
            int threadCount = 8;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch startLatch = new CountDownLatch(1);
            CountDownLatch doneLatch = new CountDownLatch(threadCount);

            WriteResult result = stubWriteResult();

            // Each thread writes to a different table
            for (int i = 0; i < threadCount; i++) {
                int idx = i;
                executor.submit(() -> {
                    try {
                        startLatch.await();
                        largeScheduler.onWriteComplete("table_" + idx, result);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        doneLatch.countDown();
                    }
                });
            }

            startLatch.countDown();
            doneLatch.await(5, TimeUnit.SECONDS);
            executor.shutdown();

            // All 8 distinct tables should be dispatched without corruption.
            // With immediate dispatch, tasks are either in-flight or still queued.
            int total = largeScheduler.queueSize();
            for (int i = 0; i < threadCount; i++) {
                if (largeScheduler.isInFlight("table_" + i)) {
                    total++;
                }
            }
            assertThat(total).isEqualTo(threadCount);
        } finally {
            spi.unblockCompaction();
            largeScheduler.close();
        }
    }

    @Test
    void scheduledDueTriggersThePolicyCycle() throws Exception {
        spi.scheduledDue = true;

        scheduler.scanAllTablesWithPriority(List.of("table_due"));
        scheduler.processTaskQueue();

        assertThat(spi.scheduledCycle.await(5, TimeUnit.SECONDS)).isTrue();
    }

    @Test
    void scheduledNotDueDoesNotEnqueueWithoutThresholdPressure() {
        spi.scheduledDue = false;

        scheduler.scanAllTablesWithPriority(List.of("table_idle"));

        assertThat(scheduler.queueSize()).isZero();
    }

    @Test
    void periodicScanStartStopIsRepeatableAcrossLeaderFlaps() {
        MaintenanceConfig longScan = new MaintenanceConfig(
            Duration.ofMillis(1), Duration.ofHours(6), Duration.ofHours(12),
            Duration.ofHours(1), Duration.ofHours(24),
            5, 10, Duration.ofDays(3), Duration.ofDays(7), 100,
            2, 2, 4, Duration.ofSeconds(5));
        MaintenanceScheduler leaderGated = new MaintenanceScheduler(spi, longScan);
        try {
            leaderGated.startPeriodicScan(List::of);
            assertThat(leaderGated.isPeriodicScanRunning()).isTrue();

            leaderGated.stopPeriodicScan();
            assertThat(leaderGated.isPeriodicScanRunning()).isFalse();

            leaderGated.startPeriodicScan(List::of);
            assertThat(leaderGated.isPeriodicScanRunning()).isTrue();
        } finally {
            leaderGated.close();
        }
    }

    @Test
    void onWriteCompleteSkipsDuplicateWhenInFlight() {
        // Arrange: block compaction so the task stays in-flight
        spi.blockCompaction();
        WriteResult result = stubWriteResult();

        // onWriteComplete now enqueues AND processes, so the task moves to in-flight
        scheduler.onWriteComplete("dup_table", result);
        assertThat(scheduler.isInFlight("dup_table")).isTrue();
        assertThat(scheduler.queueSize()).isEqualTo(0);

        // Second onWriteComplete for same table should be skipped (in-flight guard)
        scheduler.onWriteComplete("dup_table", result);
        assertThat(scheduler.queueSize()).isEqualTo(0);

        spi.unblockCompaction();
    }

    private static WriteResult stubWriteResult() {
        return new WriteResult("test_table", 100L, 1, 0, 0, Instant.now());
    }

    // --- Stub SPI ---

    private static class StubTableMaintenanceSpi implements TableMaintenanceSpi {

        private final ConcurrentHashMap<String, MaintenanceStatus> statuses = new ConcurrentHashMap<>();
        private final CountDownLatch scheduledCycle = new CountDownLatch(1);
        private volatile CountDownLatch compactionGate;
        private volatile boolean scheduledDue;

        void blockCompaction() {
            compactionGate = new CountDownLatch(1);
        }

        void unblockCompaction() {
            CountDownLatch gate = compactionGate;
            if (gate != null) {
                gate.countDown();
            }
        }

        void setStatus(String tableName, MaintenanceStatus status) {
            statuses.put(tableName, status);
        }

        @Override
        public CompletableFuture<CompactionResult> compactAsync(String tableName) {
            CountDownLatch gate = compactionGate;
            if (gate != null) {
                return CompletableFuture.supplyAsync(() -> {
                    try {
                        gate.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    return new CompactionResult(tableName, 5, 1, 1000L);
                });
            }
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
            return statuses.getOrDefault(tableName, new MaintenanceStatus(
                tableName, 0, 10, 500L, 3, 0, 0L, 0,
                null, null, null));
        }

        @Override
        public boolean isScheduledMaintenanceDue(String tableName) {
            return scheduledDue;
        }

        @Override
        public CompletableFuture<Void> runScheduledMaintenanceAsync(String tableName) {
            if (!scheduledDue) {
                return compactAsync(tableName).thenApply(ignored -> null);
            }
            scheduledCycle.countDown();
            return CompletableFuture.completedFuture(null);
        }
    }
}
