package org.pangrova.streaming.property.cdc;

import org.pangrova.streaming.core.commit.SourceId;
import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;
import org.pangrova.common.concurrent.BackoffStrategy;
import org.pangrova.connector.spi.data.write.model.WriteResult;
import org.pangrova.streaming.api.cdc.*;
import org.pangrova.streaming.api.checkpoint.*;
import org.pangrova.streaming.api.source.*;
import org.pangrova.streaming.api.watermark.*;
import org.pangrova.streaming.core.cdc.*;
import org.pangrova.streaming.core.maintenance.*;
import org.pangrova.streaming.core.watermark.WatermarkTracker;

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

    /** The first reached failure threshold stops the same pending batch. */
    @Property(tries = 10)
    void firstFailureLimitStopsWithoutAdvancingSource(
            @ForAll @IntRange(min = 1, max = 10) int maxFailures,
            @ForAll @IntRange(min = 1, max = 10) int maxRetries) throws Exception {
        var failed = new java.util.concurrent.CountDownLatch(1);
        var writeSink = new AlwaysFailingWriteSink();
        var reader = new LimitedSourceReader(2);
        var config = new CdcPipelineConfig(
                STREAM, new SourceId(STREAM), TABLE,
                100, Duration.ofMillis(10), 5, maxFailures, maxRetries, Duration.ofSeconds(5));
        var loop = new CdcPipelineLoop(config, buildDeps(reader, writeSink, e -> failed.countDown()));
        Thread thread = new Thread(loop, "cdc-failure-limit-property");
        thread.start();
        try {
            assertThat(failed.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        } finally {
            loop.stop();
            thread.join(5000);
        }
        assertThat(thread.isAlive()).isFalse();
        assertThat(loop.isRunning()).isFalse();
        assertThat(writeSink.attempts.get()).isEqualTo(Math.min(maxFailures, maxRetries));
        assertThat(reader.idx.get()).isEqualTo(1);
        assertThat(reader.commits.get()).isZero();
    }

    /** Success resets both counters, allowing the next batch the same bounded retries. */
    @Property(tries = 10)
    void successResetsConsecutiveFailureCounter(
            @ForAll @IntRange(min = 2, max = 8) int maxFailures,
            @ForAll @IntRange(min = 2, max = 8) int maxRetries) throws Exception {
        int failBeforeSuccess = Math.min(maxFailures, maxRetries) - 1;
        var failedCalled = new AtomicBoolean();
        var writeSink = new FailThenSucceedWriteSink(failBeforeSuccess);
        var reader = new LimitedSourceReader(2);
        var config = new CdcPipelineConfig(
                STREAM, new SourceId(STREAM), TABLE,
                100, Duration.ofMillis(10), 5, maxFailures, maxRetries, Duration.ofSeconds(5));
        var loop = new CdcPipelineLoop(config, buildDeps(reader, writeSink, e -> failedCalled.set(true)));
        Thread thread = new Thread(loop, "cdc-failure-reset-property");
        thread.start();
        try {
            assertThat(reader.acknowledged.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        } finally {
            loop.stop();
            thread.join(5000);
        }
        assertThat(thread.isAlive()).isFalse();
        assertThat(failedCalled.get()).isFalse();
        assertThat(writeSink.attempts.get()).isEqualTo(2 * (failBeforeSuccess + 1));
        assertThat(reader.commits.get()).isEqualTo(2);
    }

    // --- Fakes ---

    static class LimitedSourceReader implements CdcSourceReader {
        private final AtomicInteger idx = new AtomicInteger(0);
        private final int limit;
        private final AtomicInteger commits = new AtomicInteger();
        private final java.util.concurrent.CountDownLatch acknowledged;

        LimitedSourceReader(int limit) {
            this.limit = limit;
            acknowledged = new java.util.concurrent.CountDownLatch(limit);
        }

        @Override
        public List<SourceRecord> pollBatch(int maxRecords, Duration timeout) {
            int i = idx.getAndIncrement();
            if (i < limit) {
                return List.of(new SourceRecord("t", 0, i, new byte[]{1}, new byte[]{2}, System.currentTimeMillis()));
            }
            return List.of();
        }

        @Override public void commitOffsets(Map<TopicPartitionOffset, Long> offsets) {
            commits.incrementAndGet();
            acknowledged.countDown();
        }
        @Override public void seekToOffsets(Map<TopicPartitionOffset, Long> offsets) {}
        @Override public void close() {}
    }

    static class AlwaysFailingWriteSink implements CdcWriteSink {
        private final AtomicInteger attempts = new AtomicInteger();
        @Override
        public CdcWriteResult writeBatch(List<CdcEnvelope> envelopes) {
            attempts.incrementAndGet();
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
            if (attempts.getAndIncrement() % (failCount + 1) < failCount) {
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
