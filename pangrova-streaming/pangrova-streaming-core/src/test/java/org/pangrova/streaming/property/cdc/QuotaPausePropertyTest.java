package org.pangrova.streaming.property.cdc;

import org.pangrova.streaming.api.commit.SourceId;
import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;
import net.jqwik.api.constraints.LongRange;
import org.pangrova.common.concurrent.BackoffStrategy;
import org.pangrova.connector.api.write.WriteResult;
import org.pangrova.streaming.api.cdc.*;
import org.pangrova.streaming.api.checkpoint.*;
import org.pangrova.streaming.api.source.*;
import org.pangrova.streaming.api.watermark.*;
import org.pangrova.streaming.cdc.*;
import org.pangrova.streaming.maintenance.*;
import org.pangrova.streaming.watermark.WatermarkTracker;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 11: Quota enforcement pauses polling.
 *
 * <p>Validates: Requirements 10.4, 17.1, 17.2</p>
 *
 * <p>Verifies that when {@code CdcQuotaEnforcer.shouldPause()} returns true,
 * the CdcPipelineLoop does not call pollBatch on the source reader.</p>
 */
class QuotaPausePropertyTest {

    private static final String STREAM = "quota-stream";
    private static final String TABLE = "catalog.schema.events";

    /**
     * Validates: Requirements 10.4, 17.1, 17.2
     *
     * When quota is exceeded before the loop starts, no pollBatch calls occur.
     */
    @Property(tries = 10)
    void shouldPausePreventsPolling(
            @ForAll @IntRange(min = 1, max = 5) int maxConcurrent,
            @ForAll @LongRange(min = 100, max = 10000) long maxBytes) {

        CdcQuotaEnforcer quota = new CdcQuotaEnforcer(maxConcurrent, maxBytes);
        // Exceed the byte limit so shouldPause() returns true
        quota.onWriteStart(maxBytes + 1);

        assertThat(quota.shouldPause()).isTrue();

        PollCountingSourceReader reader = new PollCountingSourceReader();
        CountingWriteSink writeSink = new CountingWriteSink();

        CdcPipelineConfig config = new CdcPipelineConfig(
                STREAM, new SourceId(STREAM), TABLE,
                100, Duration.ofMillis(10), 5, 3, 5, Duration.ofSeconds(5));

        CdcPipelineLoop loop = new CdcPipelineLoop(config, buildDeps(reader, writeSink, quota));

        Thread t = new Thread(loop);
        t.start();
        try { Thread.sleep(80); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        loop.stop();
        try { t.join(1000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        assertThat(reader.pollCount.get())
                .as("pollBatch must not be called while quota is exceeded")
                .isZero();
        assertThat(writeSink.writeCount.get())
                .as("writeBatch must not be called while quota is exceeded")
                .isZero();
    }

    /**
     * Validates: Requirements 10.4, 17.1
     *
     * When quota is exceeded via concurrent writes, polling is paused.
     */
    @Property(tries = 10)
    void concurrentWriteLimitPausesPolling(
            @ForAll @IntRange(min = 1, max = 3) int maxConcurrent) {

        CdcQuotaEnforcer quota = new CdcQuotaEnforcer(maxConcurrent, Long.MAX_VALUE);
        // Saturate the concurrent write slots
        for (int i = 0; i < maxConcurrent; i++) {
            quota.onWriteStart(1);
        }
        assertThat(quota.shouldPause()).isTrue();

        PollCountingSourceReader reader = new PollCountingSourceReader();
        CountingWriteSink writeSink = new CountingWriteSink();

        CdcPipelineConfig config = new CdcPipelineConfig(
                STREAM, new SourceId(STREAM), TABLE,
                100, Duration.ofMillis(10), 5, 3, 5, Duration.ofSeconds(5));

        CdcPipelineLoop loop = new CdcPipelineLoop(config, buildDeps(reader, writeSink, quota));

        Thread t = new Thread(loop);
        t.start();
        try { Thread.sleep(80); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        loop.stop();
        try { t.join(1000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        assertThat(reader.pollCount.get()).isZero();
    }

    /**
     * Validates: Requirements 17.2
     *
     * When quota is not exceeded, polling proceeds normally.
     */
    @Property(tries = 10)
    void noQuotaExceededAllowsPolling(
            @ForAll @IntRange(min = 1, max = 5) int batchCount) {

        CdcQuotaEnforcer quota = new CdcQuotaEnforcer(10, 1_000_000);
        assertThat(quota.shouldPause()).isFalse();

        PollCountingSourceReader reader = new PollCountingSourceReader(batchCount);
        CountingWriteSink writeSink = new CountingWriteSink();

        CdcPipelineConfig config = new CdcPipelineConfig(
                STREAM, new SourceId(STREAM), TABLE,
                100, Duration.ofMillis(10), 5, 3, 5, Duration.ofSeconds(5));

        CdcPipelineLoop loop = new CdcPipelineLoop(config, buildDeps(reader, writeSink, quota));

        Thread t = new Thread(loop);
        t.start();
        try { t.join(3000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        loop.stop();
        try { t.join(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        assertThat(reader.pollCount.get())
                .as("pollBatch should be called when quota is not exceeded")
                .isGreaterThanOrEqualTo(batchCount);
    }

    // --- Fakes ---

    static class PollCountingSourceReader implements CdcSourceReader {
        final AtomicInteger pollCount = new AtomicInteger(0);
        private final int batchesToReturn;

        PollCountingSourceReader() { this(0); }
        PollCountingSourceReader(int batchesToReturn) { this.batchesToReturn = batchesToReturn; }

        @Override
        public List<SourceRecord> pollBatch(int maxRecords, Duration timeout) {
            int idx = pollCount.getAndIncrement();
            if (idx < batchesToReturn) {
                return List.of(new SourceRecord("t", 0, idx, new byte[]{1}, new byte[]{2}, System.currentTimeMillis()));
            }
            return List.of();
        }

        @Override public void commitOffsets(Map<TopicPartitionOffset, Long> offsets) {}
        @Override public void seekToOffsets(Map<TopicPartitionOffset, Long> offsets) {}
        @Override public void close() {}
    }

    static class CountingWriteSink implements CdcWriteSink {
        final AtomicInteger writeCount = new AtomicInteger(0);

        @Override
        public CdcWriteResult writeBatch(List<CdcEnvelope> envelopes) {
            writeCount.incrementAndGet();
            return new CdcWriteResult(envelopes.size(), envelopes.size() * 256L, 1, TABLE);
        }

        @Override public void close() {}
    }

    private static CdcPipelineLoopContext buildDeps(
            CdcSourceReader reader, CdcWriteSink writeSink, CdcQuotaEnforcer quota) {
        return new CdcPipelineLoopContext(
                reader,
                new StubConverter(),
                writeSink,
                quota,
                new WatermarkTracker(new InMemoryWatermarkStore()),
                new StubMaintenanceScheduler(),
                new CdcPipelineRuntimeHooks(e -> {}, BackoffStrategy.retry(10, 100), null));
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
