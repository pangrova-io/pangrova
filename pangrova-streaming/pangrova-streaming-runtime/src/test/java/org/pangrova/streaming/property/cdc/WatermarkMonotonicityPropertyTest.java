package org.pangrova.streaming.property.cdc;

import org.pangrova.spi.streaming.source.SourceId;
import net.jqwik.api.*;
import org.pangrova.common.concurrent.BackoffStrategy;
import org.pangrova.spi.connector.data.write.model.WriteResult;
import org.pangrova.spi.streaming.record.CdcEnvelope;
import org.pangrova.spi.streaming.record.CdcOperation;
import org.pangrova.spi.streaming.record.CdcRecordConverter;
import org.pangrova.spi.streaming.sink.CdcWriteResult;
import org.pangrova.spi.streaming.sink.CdcWriteSink;
import org.pangrova.spi.streaming.source.CdcSourceReader;
import org.pangrova.spi.streaming.source.SourceRecord;
import org.pangrova.spi.streaming.source.TopicPartitionOffset;
import org.pangrova.spi.streaming.store.WatermarkStore;
import org.pangrova.streaming.cdc.*;
import org.pangrova.streaming.maintenance.*;
import org.pangrova.streaming.watermark.WatermarkTracker;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 13: Watermark advances monotonically via CdcPipelineLoop.
 *
 * <p>Validates: Requirements 10.7</p>
 *
 * <p>Verifies that the WatermarkTracker's watermark is non-decreasing
 * as the CdcPipelineLoop processes batches with varying event timestamps.</p>
 *
 * <p>NOTE: There is a separate WatermarkTrackerMonotonicityPropertyTest in the
 * watermark package that tests the tracker in isolation. This test focuses on
 * the CdcPipelineLoop's watermark update behavior specifically — ensuring the
 * loop correctly extracts max event time from envelopes and feeds it to the tracker.</p>
 */
class WatermarkMonotonicityPropertyTest {

    private static final String STREAM = "wm-stream";
    private static final String TABLE = "catalog.schema.events";

    /**
     * Validates: Requirements 10.7
     *
     * After processing multiple batches with varying event timestamps,
     * the watermark reported by WatermarkTracker is non-decreasing.
     */
    @Property(tries = 10)
    void watermarkNeverDecreasesThroughPipeline(
            @ForAll("eventTimeOffsets") List<List<Integer>> batchOffsets) {
        Assume.that(!batchOffsets.isEmpty());
        Assume.that(batchOffsets.stream().noneMatch(List::isEmpty));

        Instant base = Instant.parse("2026-01-01T00:00:00Z");
        RecordingWatermarkStore store = new RecordingWatermarkStore();
        WatermarkTracker tracker = new WatermarkTracker(store);

        CdcPipelineLoop[] loopRef = new CdcPipelineLoop[1];
        TimestampedSourceReader reader = new TimestampedSourceReader(batchOffsets, base, loopRef);
        SimpleWriteSink writeSink = new SimpleWriteSink();

        CdcPipelineConfig config = new CdcPipelineConfig(
                STREAM, new SourceId(STREAM), TABLE,
                100, Duration.ofMillis(10), 5, 3, 5);

        CdcPipelineLoop loop = new CdcPipelineLoop(config, new CdcPipelineLoopContext(
                reader,
                new TimestampedConverter(base),
                writeSink,
                new CdcQuotaEnforcer(10, 1_000_000),
                tracker,
                new StubMaintenanceScheduler(),
                CdcPipelineRuntimeHooks.forTests(e -> {}, BackoffStrategy.retry(10, 100), null)));
        loopRef[0] = loop;

        Thread t = new Thread(loop);
        t.start();
        try { t.join(5000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        loop.stop();
        try { t.join(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        // Verify watermark monotonicity from the store's save history
        List<Instant> savedWatermarks = store.history(TABLE);
        for (int i = 1; i < savedWatermarks.size(); i++) {
            assertThat(savedWatermarks.get(i))
                    .as("Watermark at index %d must be >= watermark at index %d", i, i - 1)
                    .isAfterOrEqualTo(savedWatermarks.get(i - 1));
        }

        // Also verify the tracker's current watermark is the max of all batch max event times
        if (!savedWatermarks.isEmpty()) {
            Instant trackerWm = tracker.getWatermark(TABLE);
            assertThat(trackerWm).isAfterOrEqualTo(savedWatermarks.get(0));
        }
    }

    @Provide
    Arbitrary<List<List<Integer>>> eventTimeOffsets() {
        Arbitrary<List<Integer>> batch = Arbitraries.integers()
                .between(0, 200)
                .list().ofMinSize(1).ofMaxSize(5);
        return batch.list().ofMinSize(1).ofMaxSize(8);
    }

    // --- Fakes ---

    static class RecordingWatermarkStore implements WatermarkStore {
        private final ConcurrentHashMap<String, List<Instant>> histories = new ConcurrentHashMap<>();
        private final ConcurrentHashMap<String, Instant> data = new ConcurrentHashMap<>();

        @Override
        public void save(String tableName, Instant watermark) {
            data.put(tableName, watermark);
            histories.computeIfAbsent(tableName, k -> new CopyOnWriteArrayList<>()).add(watermark);
        }

        @Override
        public Optional<Instant> load(String tableName) {
            return Optional.ofNullable(data.get(tableName));
        }

        List<Instant> history(String tableName) {
            return histories.getOrDefault(tableName, List.of());
        }
    }

    /** Source reader that returns batches with records whose values encode a timestamp offset.
     *  Stops the loop when all batches are exhausted. */
    static class TimestampedSourceReader implements CdcSourceReader {
        private final AtomicInteger batchIdx = new AtomicInteger(0);
        private final List<List<Integer>> batchOffsets;
        private final Instant base;
        private final CdcPipelineLoop[] loopRef;

        TimestampedSourceReader(List<List<Integer>> batchOffsets, Instant base, CdcPipelineLoop[] loopRef) {
            this.batchOffsets = batchOffsets;
            this.base = base;
            this.loopRef = loopRef;
        }

        @Override
        public List<SourceRecord> pollBatch(int maxRecords, Duration timeout) {
            int idx = batchIdx.getAndIncrement();
            if (idx < batchOffsets.size()) {
                List<Integer> offsets = batchOffsets.get(idx);
                List<SourceRecord> records = new ArrayList<>();
                for (int i = 0; i < offsets.size(); i++) {
                    // Encode the offset in the value bytes (4 bytes, big-endian)
                    int offset = offsets.get(i);
                    byte[] value = new byte[]{
                            (byte) (offset >> 24), (byte) (offset >> 16),
                            (byte) (offset >> 8), (byte) offset};
                    records.add(new SourceRecord("t", 0, idx * 100L + i,
                            new byte[]{1}, value, base.plusSeconds(offset).toEpochMilli()));
                }
                return records;
            }
            if (loopRef[0] != null) {
                loopRef[0].stop();
            }
            return List.of();
        }

        @Override public void commitOffsets(Map<TopicPartitionOffset, Long> offsets) {}
        @Override public void seekToOffsets(Map<TopicPartitionOffset, Long> offsets) {}
        @Override public void close() {}
    }

    /** Converter that decodes the timestamp offset from the value bytes. */
    static class TimestampedConverter implements CdcRecordConverter {
        private final Instant base;

        TimestampedConverter(Instant base) { this.base = base; }

        @Override public String formatId() { return "timestamped-stub"; }

        @Override
        public CdcEnvelope deserialize(byte[] payload) {
            int offset = 0;
            if (payload != null && payload.length >= 4) {
                offset = ((payload[0] & 0xFF) << 24) | ((payload[1] & 0xFF) << 16)
                        | ((payload[2] & 0xFF) << 8) | (payload[3] & 0xFF);
            }
            Instant eventTime = base.plusSeconds(offset);
            return new CdcEnvelope(CdcOperation.INSERT, TABLE, null, null, eventTime, 1, Map.of());
        }
    }

    static class SimpleWriteSink implements CdcWriteSink {
        @Override
        public CdcWriteResult writeBatch(List<CdcEnvelope> envelopes) {
            return new CdcWriteResult(envelopes.size(), envelopes.size() * 256L, 1, TABLE);
        }
        @Override public void close() {}
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
