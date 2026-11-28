package org.pangrova.streaming.property.cdc;

import org.pangrova.observability.noop.NoOpMetricsProvider;

import org.pangrova.streaming.api.enums.CdcOperation;

import org.pangrova.streaming.api.commit.SourceId;
import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;
import org.pangrova.common.concurrency.BackoffStrategy;
import org.pangrova.streaming.api.cdc.*;
import org.pangrova.streaming.api.checkpoint.*;
import org.pangrova.streaming.api.source.*;
import org.pangrova.streaming.api.watermark.*;
import org.pangrova.connector.api.write.WriteResult;
import org.pangrova.streaming.cdc.*;
import org.pangrova.streaming.maintenance.*;
import org.pangrova.streaming.watermark.WatermarkTracker;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 10: Write-then-commit-offset ordering.
 *
 * <p>Validates: Requirements 10.3, 11.1</p>
 *
 * <p>Verifies that offset is only committed AFTER a successful write.
 * Uses a fake CdcSourceReader that records call order and a fake CdcWriteSink.</p>
 */
class WriteCommitOrderingPropertyTest {

    private static final String STREAM = "order-stream";
    private static final String TABLE = "catalog.schema.orders";

    /**
     * Validates: Requirements 10.3, 11.1
     *
     * For any batch of records, the call sequence must be:
     * writeBatch → commitOffsets (never commit before write).
     */
    @Property(tries = 10)
    void offsetCommittedOnlyAfterSuccessfulWrite(
            @ForAll @IntRange(min = 1, max = 10) int batchSize) {

        List<String> callLog = new CopyOnWriteArrayList<>();
        OrderTrackingSourceReader reader = new OrderTrackingSourceReader(callLog, batchSize);
        OrderTrackingWriteSink writeSink = new OrderTrackingWriteSink(callLog);

        CdcPipelineConfig config = new CdcPipelineConfig(
                STREAM, new SourceId(STREAM), TABLE,
                100, Duration.ofMillis(50), 10, 3, 5, Duration.ofSeconds(5));

        CdcPipelineLoop loop = new CdcPipelineLoop(config, buildDeps(reader, writeSink));
        reader.setLoop(loop);

        Thread t = new Thread(loop);
        t.start();
        try { t.join(3000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        loop.stop();
        try { t.join(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        // Verify ordering: every commitOffsets must be preceded by a writeBatch
        int writeCount = 0;
        int commitCount = 0;
        for (String call : callLog) {
            if ("writeBatch".equals(call)) {
                writeCount++;
            } else if ("commitOffsets".equals(call)) {
                commitCount++;
                assertThat(writeCount)
                        .as("commitOffsets at position must have at least as many preceding writeBatch calls")
                        .isGreaterThanOrEqualTo(commitCount);
            }
        }
        // At least one write+commit cycle should have occurred
        assertThat(writeCount).isGreaterThanOrEqualTo(1);
        assertThat(commitCount).isGreaterThanOrEqualTo(1);
    }

    /**
     * Validates: Requirements 10.3
     *
     * When write fails, commitOffsets must NOT be called for that batch.
     */
    @Property(tries = 10)
    void noCommitWhenWriteFails(
            @ForAll @IntRange(min = 1, max = 5) int batchCount) {

        List<String> callLog = new CopyOnWriteArrayList<>();
        OrderTrackingSourceReader reader = new OrderTrackingSourceReader(callLog, batchCount);
        FailingWriteSink writeSink = new FailingWriteSink(callLog);

        CdcPipelineConfig config = new CdcPipelineConfig(
                STREAM, new SourceId(STREAM), TABLE,
                100, Duration.ofMillis(10), 10, 1, 5, Duration.ofSeconds(5));

        CdcPipelineLoop loop = new CdcPipelineLoop(config, buildDeps(reader, writeSink));

        Thread t = new Thread(loop);
        t.start();
        try { t.join(3000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        loop.stop();
        try { t.join(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        // No commitOffsets should appear after a failed write
        assertThat(callLog.stream().filter("commitOffsets"::equals).count())
                .as("commitOffsets must not be called when all writes fail")
                .isZero();
    }

    // --- Fakes ---

    static class OrderTrackingSourceReader implements CdcSourceReader {
        private final List<String> callLog;
        private final AtomicInteger pollIndex = new AtomicInteger(0);
        private final int batchCount;
        private volatile CdcPipelineLoop loop;

        OrderTrackingSourceReader(List<String> callLog, int batchCount) {
            this.callLog = callLog;
            this.batchCount = batchCount;
        }

        void setLoop(CdcPipelineLoop loop) { this.loop = loop; }

        @Override
        public List<SourceRecord> pollBatch(int maxRecords, Duration timeout) {
            int idx = pollIndex.getAndIncrement();
            if (idx < batchCount) {
                return List.of(new SourceRecord("t", 0, idx, new byte[]{1}, new byte[]{2}, System.currentTimeMillis()));
            }
            if (loop != null) { loop.stop(); }
            return List.of();
        }

        @Override
        public void commitOffsets(Map<TopicPartitionOffset, Long> offsets) {
            callLog.add("commitOffsets");
        }

        @Override public void seekToOffsets(Map<TopicPartitionOffset, Long> offsets) {}
        @Override public void close() {}
    }

    static class OrderTrackingWriteSink implements CdcWriteSink {
        private final List<String> callLog;

        OrderTrackingWriteSink(List<String> callLog) {
            this.callLog = callLog;
        }

        @Override
        public CdcWriteResult writeBatch(List<CdcEnvelope> envelopes) {
            callLog.add("writeBatch");
            return new CdcWriteResult(envelopes.size(), envelopes.size() * 256L, 1, TABLE);
        }

        @Override public void close() {}
    }

    static class FailingWriteSink implements CdcWriteSink {
        private final List<String> callLog;

        FailingWriteSink(List<String> callLog) {
            this.callLog = callLog;
        }

        @Override
        public CdcWriteResult writeBatch(List<CdcEnvelope> envelopes) {
            callLog.add("writeBatch");
            throw new RuntimeException("Simulated write failure");
        }

        @Override public void close() {}
    }

    private static CdcPipelineLoopContext buildDeps(CdcSourceReader reader, CdcWriteSink writeSink) {
        return new CdcPipelineLoopContext(
                reader,
                new StubConverter(),
                writeSink,
                new CdcQuotaEnforcer(10, 1_000_000),
                new WatermarkTracker(new InMemoryWatermarkStore()),
                new StubMaintenanceScheduler(),
                new CdcPipelineRuntimeHooks(new NoOpMetricsProvider(), e -> {}, BackoffStrategy.retry(10, 100), null));
    }

    static class StubConverter implements CdcRecordConverter {
        @Override public String formatId() { return "stub"; }
        @Override public CdcEnvelope deserialize(byte[] payload) {
            return new CdcEnvelope(CdcOperation.INSERT, TABLE, null, null, Instant.now(), 1, Map.of());
        }
    }

    static class InMemoryWatermarkStore implements WatermarkStore {
        @Override public Instant advance(String t, Instant w) { return w; }
        @Override public Optional<Instant> load(String t) { return Optional.empty(); }
    }

    static class StubMaintenanceScheduler extends MaintenanceScheduler {
        StubMaintenanceScheduler() {
            super(new StubMaintenanceSpi(), MaintenanceConfig.defaults());
        }
        @Override public void onWriteComplete(String tableName, WriteResult result) {}
    }

    static class StubMaintenanceSpi implements TableMaintenance {
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
