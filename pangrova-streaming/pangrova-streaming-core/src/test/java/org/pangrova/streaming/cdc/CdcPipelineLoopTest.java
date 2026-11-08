package org.pangrova.streaming.cdc;

import org.pangrova.streaming.api.commit.SourceId;
import org.pangrova.common.concurrent.BackoffStrategy;
import org.pangrova.connector.api.write.WriteResult;
import org.pangrova.streaming.api.cdc.CdcEnvelope;
import org.pangrova.streaming.api.cdc.CdcOperation;
import org.pangrova.streaming.api.cdc.CdcRecordConverter;
import org.pangrova.streaming.api.source.SourceRecord;
import org.pangrova.streaming.api.cdc.CdcSourceReader;
import org.pangrova.streaming.api.cdc.CdcWriteResult;
import org.pangrova.streaming.api.cdc.CdcWriteSink;
import org.pangrova.streaming.api.checkpoint.TopicPartitionOffset;
import org.pangrova.streaming.api.watermark.WatermarkStore;
import org.pangrova.streaming.maintenance.CompactionResult;
import org.pangrova.streaming.maintenance.MaintenanceConfig;
import org.pangrova.streaming.maintenance.MaintenanceScheduler;
import org.pangrova.streaming.maintenance.MaintenanceStatus;
import org.pangrova.streaming.maintenance.OrphanCleanupResult;
import org.pangrova.streaming.maintenance.SnapshotExpirationResult;
import org.pangrova.streaming.maintenance.TableMaintenanceSpi;
import org.pangrova.streaming.watermark.WatermarkTracker;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link CdcPipelineLoop}.
 *
 * <p>Uses real stubs/fakes — no mock frameworks.</p>
 */
class CdcPipelineLoopTest {

    private static final String STREAM_NAME = "test-stream";
    private static final String TARGET_TABLE = "catalog.schema.orders";

    private CdcPipelineConfig defaultConfig() {
        return new CdcPipelineConfig(
            STREAM_NAME, new SourceId(STREAM_NAME), TARGET_TABLE,
            100,
            Duration.ofMillis(50), 10, 3, 5, Duration.ofSeconds(5));
    }

    // --- Fakes ---

    static class FakeSourceReader implements CdcSourceReader {
        private final List<List<SourceRecord>> batches = new CopyOnWriteArrayList<>();
        private final AtomicInteger pollIndex = new AtomicInteger(0);
        private final List<Map<TopicPartitionOffset, Long>> committedOffsets = new CopyOnWriteArrayList<>();

        void addBatch(List<SourceRecord> batch) {
            batches.add(batch);
        }

        @Override
        public List<SourceRecord> pollBatch(int maxRecords, Duration timeout) {
            int idx = pollIndex.getAndIncrement();
            if (idx < batches.size()) {
                return batches.get(idx);
            }
            return List.of();
        }

        @Override
        public void commitOffsets(Map<TopicPartitionOffset, Long> offsets) {
            committedOffsets.add(Map.copyOf(offsets));
        }

        @Override
        public void seekToOffsets(Map<TopicPartitionOffset, Long> offsets) {}

        @Override
        public void close() {}
    }

    static class FakeConverter implements CdcRecordConverter {
        @Override
        public String formatId() { return "fake-json"; }

        @Override
        public CdcEnvelope deserialize(byte[] payload) {
            return new CdcEnvelope(
                CdcOperation.INSERT, TARGET_TABLE, null, null,
                Instant.now(), 1, Map.of());
        }

        @Override
        public List<CdcEnvelope> convertBatch(List<byte[]> payloads,
                                               BiConsumer<byte[], Exception> deadLetter) {
            List<CdcEnvelope> results = new ArrayList<>();
            for (byte[] p : payloads) {
                results.add(deserialize(p));
            }
            return results;
        }
    }

    static class FakeWriteSink implements CdcWriteSink {
        private final AtomicInteger writeCount = new AtomicInteger(0);
        private volatile boolean failOnWrite = false;

        @Override
        public CdcWriteResult writeBatch(List<CdcEnvelope> envelopes) {
            if (failOnWrite) {
                throw new RuntimeException("Simulated write failure");
            }
            writeCount.incrementAndGet();
            return new CdcWriteResult(envelopes.size(), envelopes.size() * 256L, 1, TARGET_TABLE);
        }

        @Override
        public void close() {}
    }

    static class FakeMaintenanceScheduler extends MaintenanceScheduler {
        private final List<String> notifiedTables = new CopyOnWriteArrayList<>();

        FakeMaintenanceScheduler() {
            super(new StubMaintenanceSpi(), MaintenanceConfig.defaults());
        }

        @Override
        public void onWriteComplete(String tableName, WriteResult result) {
            notifiedTables.add(tableName);
        }
    }

    static class StubMaintenanceSpi implements TableMaintenanceSpi {
        @Override
        public CompletableFuture<CompactionResult> compactAsync(String tableName) {
            return CompletableFuture.completedFuture(new CompactionResult(tableName, 0, 0, 0L));
        }

        @Override
        public CompletableFuture<OrphanCleanupResult> cleanOrphansAsync(String tableName, Duration olderThan) {
            return CompletableFuture.completedFuture(new OrphanCleanupResult(tableName, 0, 0L));
        }

        @Override
        public CompletableFuture<SnapshotExpirationResult> expireSnapshotsAsync(String tableName, Duration olderThan) {
            return CompletableFuture.completedFuture(new SnapshotExpirationResult(tableName, 0));
        }

        @Override
        public MaintenanceStatus getMaintenanceStatus(String tableName) {
            return new MaintenanceStatus(tableName, 0, 0, 0L, 0, 0, 0L, 0, null, null, null);
        }
    }

    static class FakeWatermarkStore implements WatermarkStore {
        @Override
        public void save(String tableName, Instant watermark) {}

        @Override
        public Optional<Instant> load(String tableName) { return Optional.empty(); }
    }

    private CdcPipelineLoopContext buildDeps(
            FakeSourceReader reader, CdcWriteSink writeSink,
            CdcQuotaEnforcer quotaEnforcer, FakeMaintenanceScheduler maintenance,
            CdcPipelineListener listener) {
        return new CdcPipelineLoopContext(
            reader,
            new FakeConverter(),
            writeSink,
            quotaEnforcer,
            new WatermarkTracker(new FakeWatermarkStore()),
            maintenance,
            new CdcPipelineRuntimeHooks(listener, BackoffStrategy.retry(10, 100), null));
    }

    private SourceRecord record(String topic, int partition, long offset) {
        return new SourceRecord(topic, partition, offset,
            new byte[]{1}, new byte[]{2, 3}, System.currentTimeMillis());
    }

    // --- Tests ---

    @Test
    void successfulCycleWritesAndCommitsOffsets() throws Exception {
        FakeSourceReader reader = new FakeSourceReader();
        reader.addBatch(List.of(record("t", 0, 0), record("t", 0, 1)));

        FakeWriteSink writeSink = new FakeWriteSink();
        FakeMaintenanceScheduler maintenance = new FakeMaintenanceScheduler();
        CdcQuotaEnforcer quota = new CdcQuotaEnforcer(10, 1_000_000);

        CdcPipelineLoop loop = new CdcPipelineLoop(
            defaultConfig(),
            buildDeps(reader, writeSink, quota, maintenance, e -> {}));

        Thread t = new Thread(loop);
        t.start();
        t.join(2000);
        loop.stop();
        t.join(500);

        assertEquals(1, writeSink.writeCount.get(), "should write one batch");
        assertEquals(1, reader.committedOffsets.size(), "should commit offsets once");
        Map<TopicPartitionOffset, Long> committed = reader.committedOffsets.get(0);
        assertEquals(2L, committed.get(new TopicPartitionOffset("t", 0)),
            "committed offset should be max+1");
        assertEquals(1, maintenance.notifiedTables.size(),
            "maintenance should be notified once");
    }

    @Test
    void quotaPausePreventsPolling() throws Exception {
        FakeSourceReader reader = new FakeSourceReader();
        reader.addBatch(List.of(record("t", 0, 0)));

        FakeWriteSink writeSink = new FakeWriteSink();
        FakeMaintenanceScheduler maintenance = new FakeMaintenanceScheduler();
        // quota already exceeded
        CdcQuotaEnforcer quota = new CdcQuotaEnforcer(1, 100);
        quota.onWriteStart(200);

        CdcPipelineLoop loop = new CdcPipelineLoop(
            defaultConfig(),
            buildDeps(reader, writeSink, quota, maintenance, e -> {}));

        Thread t = new Thread(loop);
        t.start();
        Thread.sleep(100);
        loop.stop();
        t.join(1000);

        assertEquals(0, writeSink.writeCount.get(),
            "should not write when quota is exceeded");
    }

    @Test
    void consecutiveFailuresTransitionToFailed() throws Exception {
        FakeSourceReader reader = new FakeSourceReader();
        for (int i = 0; i < 5; i++) {
            reader.addBatch(List.of(record("t", 0, i)));
        }

        FakeWriteSink writeSink = new FakeWriteSink();
        writeSink.failOnWrite = true;
        FakeMaintenanceScheduler maintenance = new FakeMaintenanceScheduler();
        CdcQuotaEnforcer quota = new CdcQuotaEnforcer(10, 1_000_000);

        AtomicBoolean failedCalled = new AtomicBoolean(false);
        CdcPipelineListener listener = e -> failedCalled.set(true);

        CdcPipelineConfig cfg = new CdcPipelineConfig(
            STREAM_NAME, new SourceId(STREAM_NAME), TARGET_TABLE,
            100,
            Duration.ofMillis(10), 5, 3, 5, Duration.ofSeconds(5));

        CdcPipelineLoop loop = new CdcPipelineLoop(
            cfg,
            buildDeps(reader, writeSink, quota, maintenance, listener));

        Thread t = new Thread(loop);
        t.start();
        t.join(5000);

        assertFalse(loop.isRunning(), "loop should have stopped after consecutive failures");
        assertTrue(failedCalled.get(), "listener.onFailed should have been called");
    }

    @Test
    void stopMethodHaltsLoop() throws Exception {
        FakeSourceReader reader = new FakeSourceReader();
        for (int i = 0; i < 100; i++) {
            reader.addBatch(List.of(record("t", 0, i)));
        }

        FakeWriteSink writeSink = new FakeWriteSink();
        FakeMaintenanceScheduler maintenance = new FakeMaintenanceScheduler();
        CdcQuotaEnforcer quota = new CdcQuotaEnforcer(10, 1_000_000);

        CdcPipelineLoop loop = new CdcPipelineLoop(
            defaultConfig(),
            buildDeps(reader, writeSink, quota, maintenance, e -> {}));

        Thread t = new Thread(loop);
        t.start();
        Thread.sleep(50);
        loop.stop();
        t.join(2000);

        assertFalse(loop.isRunning(), "loop should be stopped");
    }

    @Test
    void quotaEnforcerCountersBalancedOnSuccess() throws Exception {
        FakeSourceReader reader = new FakeSourceReader();
        reader.addBatch(List.of(record("t", 0, 0)));

        FakeWriteSink writeSink = new FakeWriteSink();
        FakeMaintenanceScheduler maintenance = new FakeMaintenanceScheduler();
        CdcQuotaEnforcer quota = new CdcQuotaEnforcer(10, 1_000_000);

        CdcPipelineLoop loop = new CdcPipelineLoop(
            defaultConfig(),
            buildDeps(reader, writeSink, quota, maintenance, e -> {}));

        Thread t = new Thread(loop);
        t.start();
        t.join(2000);
        loop.stop();
        t.join(500);

        assertEquals(0, quota.activeWrites(),
            "activeWrites should be zero after completion");
        assertEquals(0, quota.pendingBytes(),
            "pendingBytes should be zero after completion");
    }

    @Test
    void quotaEnforcerCountersBalancedOnWriteFailure() throws Exception {
        FakeSourceReader reader = new FakeSourceReader();
        reader.addBatch(List.of(record("t", 0, 0)));

        FakeWriteSink writeSink = new FakeWriteSink();
        writeSink.failOnWrite = true;
        FakeMaintenanceScheduler maintenance = new FakeMaintenanceScheduler();
        CdcQuotaEnforcer quota = new CdcQuotaEnforcer(10, 1_000_000);

        CdcPipelineConfig cfg = new CdcPipelineConfig(
            STREAM_NAME, new SourceId(STREAM_NAME), TARGET_TABLE,
            100,
            Duration.ofMillis(10), 5, 1, 5, Duration.ofSeconds(5));

        CdcPipelineLoop loop = new CdcPipelineLoop(
            cfg,
            buildDeps(reader, writeSink, quota, maintenance, e -> {}));

        Thread t = new Thread(loop);
        t.start();
        t.join(3000);

        assertEquals(0, quota.activeWrites(),
            "activeWrites should be zero after write failure");
        assertEquals(0, quota.pendingBytes(),
            "pendingBytes should be zero after write failure");
    }

    @Test
    void emptyBatchDoesNotWriteOrCommit() throws Exception {
        FakeSourceReader reader = new FakeSourceReader();
        // No batches added — pollBatch returns empty

        FakeWriteSink writeSink = new FakeWriteSink();
        FakeMaintenanceScheduler maintenance = new FakeMaintenanceScheduler();
        CdcQuotaEnforcer quota = new CdcQuotaEnforcer(10, 1_000_000);

        CdcPipelineLoop loop = new CdcPipelineLoop(
            defaultConfig(),
            buildDeps(reader, writeSink, quota, maintenance, e -> {}));

        Thread t = new Thread(loop);
        t.start();
        t.join(500);
        loop.stop();
        t.join(500);

        assertEquals(0, writeSink.writeCount.get(), "should not write for empty batch");
        assertTrue(reader.committedOffsets.isEmpty(), "should not commit for empty batch");
    }

    @Test
    void stopInterruptsPipelineThreadForPromptShutdown() throws Exception {
        // Source reader that blocks for a long time on poll, simulating a slow poll/write
        CdcSourceReader blockingReader = new CdcSourceReader() {
            private final AtomicBoolean polled = new AtomicBoolean(false);

            @Override
            public List<SourceRecord> pollBatch(int maxRecords, Duration timeout) {
                polled.set(true);
                try {
                    // Block for a very long time — only interrupt can break this promptly
                    Thread.sleep(30_000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return List.of();
            }

            @Override
            public void commitOffsets(Map<TopicPartitionOffset, Long> offsets) {}

            @Override
            public void seekToOffsets(Map<TopicPartitionOffset, Long> offsets) {}

            @Override
            public void close() {}
        };

        FakeMaintenanceScheduler maintenance = new FakeMaintenanceScheduler();
        CdcQuotaEnforcer quota = new CdcQuotaEnforcer(10, 1_000_000);

        CdcPipelineLoopContext deps = new CdcPipelineLoopContext(
            blockingReader, new FakeConverter(), new FakeWriteSink(), quota,
            new WatermarkTracker(new FakeWatermarkStore()),
            maintenance, new CdcPipelineRuntimeHooks(e -> {}, BackoffStrategy.retry(10, 100), null));

        CdcPipelineLoop loop = new CdcPipelineLoop(defaultConfig(), deps);

        Thread t = new Thread(loop);
        t.start();
        // Wait for the pipeline thread to enter the blocking poll
        Thread.sleep(200);

        long stopStart = System.nanoTime();
        loop.stop();
        t.join(2000);
        long elapsed = Duration.ofNanos(System.nanoTime() - stopStart).toMillis();

        assertFalse(t.isAlive(), "pipeline thread should have terminated promptly");
        assertFalse(loop.isRunning(), "loop should be stopped");
        assertTrue(elapsed < 2000,
            "stop() should complete promptly via interrupt, not wait for poll timeout; took " + elapsed + "ms");
    }

    @Test
    void successAfterFailureResetsConsecutiveCount() throws Exception {
        FakeSourceReader reader = new FakeSourceReader();
        for (int i = 0; i < 5; i++) {
            reader.addBatch(List.of(record("t", 0, i)));
        }

        AtomicInteger writeAttempts = new AtomicInteger(0);
        CdcWriteSink conditionalSink = new CdcWriteSink() {
            @Override
            public CdcWriteResult writeBatch(List<CdcEnvelope> envelopes) {
                int attempt = writeAttempts.incrementAndGet();
                if (attempt <= 2) {
                    throw new RuntimeException("Transient failure #" + attempt);
                }
                return new CdcWriteResult(envelopes.size(), 256, 1, TARGET_TABLE);
            }

            @Override
            public void close() {}
        };

        FakeMaintenanceScheduler maintenance = new FakeMaintenanceScheduler();
        CdcQuotaEnforcer quota = new CdcQuotaEnforcer(10, 1_000_000);
        AtomicBoolean failedCalled = new AtomicBoolean(false);

        CdcPipelineConfig cfg = new CdcPipelineConfig(
            STREAM_NAME, new SourceId(STREAM_NAME), TARGET_TABLE,
            100,
            Duration.ofMillis(10), 5, 5, 5, Duration.ofSeconds(5));

        CdcPipelineLoopContext deps = new CdcPipelineLoopContext(
            reader, new FakeConverter(), conditionalSink, quota,
            new WatermarkTracker(new FakeWatermarkStore()),
            maintenance, new CdcPipelineRuntimeHooks(
                e -> failedCalled.set(true), BackoffStrategy.retry(5, 50), null));

        CdcPipelineLoop loop = new CdcPipelineLoop(cfg, deps);

        Thread t = new Thread(loop);
        t.start();
        t.join(5000);
        loop.stop();
        t.join(500);

        assertFalse(failedCalled.get(),
            "should not call onFailed since success resets counter");
        assertTrue(writeAttempts.get() >= 3,
            "should have attempted at least 3 writes");
    }

    @Test
    void stoppingCompletedPipelineDoesNotInterruptTheNextExecutorTask() throws Exception {
        var reader = new FakeSourceReader() {
            @Override public List<SourceRecord> pollBatch(int maxRecords, Duration timeout) {
                throw new IllegalStateException("source failed");
            }
        };
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var executor = Executors.newSingleThreadExecutor();
        try (var maintenance = new FakeMaintenanceScheduler()) {
            var config = new CdcPipelineConfig(STREAM_NAME, new SourceId(STREAM_NAME), TARGET_TABLE,
                    100, Duration.ofMillis(50), 10, 1, 5, Duration.ofSeconds(5));
            var loop = new CdcPipelineLoop(config, buildDeps(reader, new FakeWriteSink(),
                    new CdcQuotaEnforcer(10, 1_000_000), maintenance, failure -> { }));
            executor.submit(loop).get(10, TimeUnit.SECONDS);
            var nextTask = executor.submit(() -> {
                entered.countDown();
                try {
                    assertTrue(release.await(10, TimeUnit.SECONDS));
                    return Thread.currentThread().isInterrupted();
                } catch (InterruptedException interrupted) {
                    return true;
                }
            });
            assertTrue(entered.await(10, TimeUnit.SECONDS));
            loop.stop();
            release.countDown();
            assertFalse(nextTask.get(10, TimeUnit.SECONDS), "completed pipeline must release its thread reference");
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void cancelledBeforeRunStillClosesSinkWhenReaderCloseFails() {
        var readerCloses = new AtomicInteger();
        var sinkCloses = new AtomicInteger();
        var reader = new FakeSourceReader() {
            @Override public List<SourceRecord> pollBatch(int maxRecords, Duration timeout) {
                throw new AssertionError("cancelled pipeline must not poll");
            }
            @Override public void close() {
                readerCloses.incrementAndGet();
                throw new IllegalStateException("reader close failed");
            }
        };
        var sink = new FakeWriteSink() {
            @Override public void close() { sinkCloses.incrementAndGet(); }
        };
        try (var maintenance = new FakeMaintenanceScheduler()) {
            var loop = new CdcPipelineLoop(defaultConfig(), buildDeps(reader, sink,
                    new CdcQuotaEnforcer(10, 1_000_000), maintenance, failure -> { }));
            loop.stop();
            loop.run();
            assertEquals(1, readerCloses.get());
            assertEquals(1, sinkCloses.get());
        }
    }
}
