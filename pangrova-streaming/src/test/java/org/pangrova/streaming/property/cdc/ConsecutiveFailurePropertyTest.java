package org.pangrova.streaming.property.cdc;

import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;
import org.pangrova.common.concurrent.BackoffStrategy;
import org.pangrova.spi.connector.data.write.model.WriteResult;
import org.pangrova.spi.streaming.*;
import org.pangrova.streaming.cdc.*;
import org.pangrova.streaming.maintenance.*;
import org.pangrova.streaming.watermark.WatermarkTracker;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 12: Consecutive failures transition to FAILED.
 *
 * <p>Validates: Requirements 10.6</p>
 *
 * <p>Verifies that N consecutive failures trigger onFailed(), and that
 * a success resets the consecutive failure counter.</p>
 */
class ConsecutiveFailurePropertyTest {

    private static final String STREAM = "fail-stream";
    private static final String TABLE = "catalog.schema.orders";

    /**
     * Validates: Requirements 10.6
     *
     * When all writes fail and the count reaches maxConsecutiveFailures,
     * onFailed() is called and the loop stops.
     */
    @Property(tries = 10)
    void consecutiveFailuresTriggersOnFailed(
            @ForAll @IntRange(min = 1, max = 10) int maxFailures) {

        AtomicBoolean failedCalled = new AtomicBoolean(false);
        AlwaysFailingWriteSink writeSink = new AlwaysFailingWriteSink();
        // Provide enough batches to exceed maxFailures
        UnlimitedSourceReader reader = new UnlimitedSourceReader();

        CdcPipelineConfig config = new CdcPipelineConfig(
                STREAM, TABLE, 100, Duration.ofMillis(10), 5, maxFailures, 5, Duration.ofSeconds(5));

        CdcPipelineLoop loop = new CdcPipelineLoop(config, buildDeps(reader, writeSink, e -> failedCalled.set(true)));

        Thread t = new Thread(loop);
        t.start();
        try { t.join(5000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        loop.stop();
        try { t.join(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        assertThat(failedCalled.get())
                .as("onFailed must be called after %d consecutive failures", maxFailures)
                .isTrue();
        assertThat(loop.isRunning()).isFalse();
    }

    /**
     * Validates: Requirements 10.6
     *
     * A success after (maxFailures - 1) failures resets the counter,
     * so onFailed() is NOT called.
     */
    @Property(tries = 10)
    void successResetsConsecutiveFailureCounter(
            @ForAll @IntRange(min = 2, max = 8) int maxFailures) {

        int failBeforeSuccess = maxFailures - 1;
        AtomicBoolean failedCalled = new AtomicBoolean(false);
        FailThenSucceedWriteSink writeSink = new FailThenSucceedWriteSink(failBeforeSuccess);
        // Provide enough batches for failures + success + a few more
        LimitedSourceReader reader = new LimitedSourceReader(failBeforeSuccess + 3);

        CdcPipelineConfig config = new CdcPipelineConfig(
                STREAM, TABLE, 100, Duration.ofMillis(10), 5, maxFailures, 5, Duration.ofSeconds(5));

        CdcPipelineLoop loop = new CdcPipelineLoop(config, buildDeps(reader, writeSink, e -> failedCalled.set(true)));

        Thread t = new Thread(loop);
        t.start();
        try { t.join(5000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        loop.stop();
        try { t.join(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        assertThat(failedCalled.get())
                .as("onFailed must NOT be called when success resets the counter")
                .isFalse();
    }

    // --- Fakes ---

    static class UnlimitedSourceReader implements CdcSourceReader {
        private final AtomicInteger idx = new AtomicInteger(0);

        @Override
        public List<SourceRecord> pollBatch(int maxRecords, Duration timeout) {
            int i = idx.getAndIncrement();
            return List.of(new SourceRecord("t", 0, i, new byte[]{1}, new byte[]{2}, System.currentTimeMillis()));
        }

        @Override public void commitOffsets(Map<TopicPartitionOffset, Long> offsets) {}
        @Override public void seekToOffsets(Map<TopicPartitionOffset, Long> offsets) {}
        @Override public void close() {}
    }

    static class LimitedSourceReader implements CdcSourceReader {
        private final AtomicInteger idx = new AtomicInteger(0);
        private final int limit;

        LimitedSourceReader(int limit) { this.limit = limit; }

        @Override
        public List<SourceRecord> pollBatch(int maxRecords, Duration timeout) {
            int i = idx.getAndIncrement();
            if (i < limit) {
                return List.of(new SourceRecord("t", 0, i, new byte[]{1}, new byte[]{2}, System.currentTimeMillis()));
            }
            return List.of();
        }

        @Override public void commitOffsets(Map<TopicPartitionOffset, Long> offsets) {}
        @Override public void seekToOffsets(Map<TopicPartitionOffset, Long> offsets) {}
        @Override public void close() {}
    }

    static class AlwaysFailingWriteSink implements CdcWriteSink {
        @Override
        public CdcWriteResult writeBatch(List<CdcEnvelope> envelopes) {
            throw new RuntimeException("Simulated persistent failure");
        }
        @Override public void close() {}
    }

    static class FailThenSucceedWriteSink implements CdcWriteSink {
        private final AtomicInteger attempts = new AtomicInteger(0);
        private final int failCount;

        FailThenSucceedWriteSink(int failCount) { this.failCount = failCount; }

        @Override
        public CdcWriteResult writeBatch(List<CdcEnvelope> envelopes) {
            if (attempts.getAndIncrement() < failCount) {
                throw new RuntimeException("Transient failure #" + attempts.get());
            }
            return new CdcWriteResult(envelopes.size(), envelopes.size() * 256L, 1, TABLE);
        }
        @Override public void close() {}
    }

    private static CdcPipelineLoopContext buildDeps(
            CdcSourceReader reader, CdcWriteSink writeSink, CdcPipelineListener listener) {
        return new CdcPipelineLoopContext(
                reader,
                new StubConverter(),
                writeSink,
                new CdcQuotaEnforcer(10, 1_000_000),
                new WatermarkTracker(new InMemoryWatermarkStore()),
                new StubMaintenanceScheduler(),
                new CdcPipelineRuntimeHooks(listener, BackoffStrategy.retry(1, 10), null));
    }

    static class StubConverter implements CdcRecordConverter {
        @Override public String formatId() { return "stub"; }
        @Override public CdcEnvelope deserialize(byte[] payload) {
            return new CdcEnvelope(CdcOperation.INSERT, TABLE, null, null, Instant.now(), 1, Map.of());
        }
    }

    static class InMemoryWatermarkStore implements WatermarkStore {
        @Override public void save(String t, Instant w) {}
        @Override public Optional<Instant> load(String t) { return Optional.empty(); }
    }

    static class StubMaintenanceScheduler extends MaintenanceScheduler {
        StubMaintenanceScheduler() { super(new StubSpi(), MaintenanceConfig.defaults()); }
        @Override public void onWriteComplete(String tableName, WriteResult result) {}
    }

    static class StubSpi implements TableMaintenanceSpi {
        @Override public CompletableFuture<CompactionResult> compactAsync(String t) {
            return CompletableFuture.completedFuture(new CompactionResult(t, 0, 0, 0L));
        }
        @Override public CompletableFuture<OrphanCleanupResult> cleanOrphansAsync(String t, Duration d) {
            return CompletableFuture.completedFuture(new OrphanCleanupResult(t, 0, 0L));
        }
        @Override public CompletableFuture<SnapshotExpirationResult> expireSnapshotsAsync(String t, Duration d) {
            return CompletableFuture.completedFuture(new SnapshotExpirationResult(t, 0));
        }
        @Override public MaintenanceStatus getMaintenanceStatus(String t) {
            return new MaintenanceStatus(t, 0, 0, 0L, 0, 0, 0L, 0, null, null, null);
        }
    }
}
