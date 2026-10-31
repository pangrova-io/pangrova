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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 21: Maintenance notification on batch write.
 *
 * <p>Validates: Requirements 14.1, 14.3</p>
 *
 * <p>Verifies that MaintenanceScheduler.onWriteComplete is called after each
 * successful batch write by the CdcPipelineLoop.</p>
 */
class MaintenanceNotificationPropertyTest {

    private static final String STREAM = "maint-stream";
    private static final String TABLE = "catalog.schema.orders";

    /**
     * Validates: Requirements 14.1, 14.3
     *
     * For N successful batch writes, MaintenanceScheduler.onWriteComplete
     * is called exactly N times with the correct table name.
     */
    @Property(tries = 10)
    void maintenanceNotifiedOncePerSuccessfulWrite(
            @ForAll @IntRange(min = 1, max = 10) int batchCount) {

        NotifyingMaintenanceScheduler maintenance = new NotifyingMaintenanceScheduler();
        SimpleWriteSink writeSink = new SimpleWriteSink();

        CdcPipelineConfig config = new CdcPipelineConfig(
                STREAM, new SourceId(STREAM), TABLE,
                100, Duration.ofMillis(10), 5, 3, 5, Duration.ofSeconds(5));

        CdcPipelineLoop[] loopRef = new CdcPipelineLoop[1];
        LimitedSourceReader reader = new LimitedSourceReader(batchCount, loopRef);

        CdcPipelineLoop loop = new CdcPipelineLoop(config, new CdcPipelineLoopContext(
                reader,
                new StubConverter(),
                writeSink,
                new CdcQuotaEnforcer(10, 1_000_000),
                new WatermarkTracker(new InMemoryWatermarkStore()),
                maintenance,
                new CdcPipelineRuntimeHooks(e -> {}, BackoffStrategy.retry(10, 100), null)));
        loopRef[0] = loop;

        Thread t = new Thread(loop);
        t.start();
        try { t.join(5000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        loop.stop();
        try { t.join(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        assertThat(maintenance.notifiedTables.size())
                .as("onWriteComplete should be called once per successful batch write")
                .isEqualTo(batchCount);

        for (String notifiedTable : maintenance.notifiedTables) {
            assertThat(notifiedTable)
                    .as("onWriteComplete must be called with the correct table name")
                    .isEqualTo(TABLE);
        }
    }

    /**
     * Validates: Requirements 14.1
     *
     * The WriteResult passed to onWriteComplete contains valid metadata:
     * non-negative recordCount and dataFilesWritten.
     */
    @Property(tries = 10)
    void maintenanceReceivesValidWriteResult(
            @ForAll @IntRange(min = 1, max = 5) int batchCount) {

        NotifyingMaintenanceScheduler maintenance = new NotifyingMaintenanceScheduler();
        SimpleWriteSink writeSink = new SimpleWriteSink();

        CdcPipelineConfig config = new CdcPipelineConfig(
                STREAM, new SourceId(STREAM), TABLE,
                100, Duration.ofMillis(10), 5, 3, 5, Duration.ofSeconds(5));

        CdcPipelineLoop[] loopRef = new CdcPipelineLoop[1];
        LimitedSourceReader reader = new LimitedSourceReader(batchCount, loopRef);

        CdcPipelineLoop loop = new CdcPipelineLoop(config, new CdcPipelineLoopContext(
                reader,
                new StubConverter(),
                writeSink,
                new CdcQuotaEnforcer(10, 1_000_000),
                new WatermarkTracker(new InMemoryWatermarkStore()),
                maintenance,
                new CdcPipelineRuntimeHooks(e -> {}, BackoffStrategy.retry(10, 100), null)));
        loopRef[0] = loop;

        Thread t = new Thread(loop);
        t.start();
        try { t.join(5000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        loop.stop();
        try { t.join(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        for (WriteResult result : maintenance.notifiedResults) {
            assertThat(result.recordCount())
                    .as("recordCount must be non-negative")
                    .isGreaterThanOrEqualTo(0);
            assertThat(result.dataFilesWritten())
                    .as("dataFilesWritten must be non-negative")
                    .isGreaterThanOrEqualTo(0);
            assertThat(result.tableName())
                    .as("tableName must match the target table")
                    .isEqualTo(TABLE);
        }
    }

    /**
     * Validates: Requirements 14.1
     *
     * When a write fails, MaintenanceScheduler.onWriteComplete is NOT called
     * for that failed batch.
     */
    @Property(tries = 10)
    void noMaintenanceNotificationOnWriteFailure(
            @ForAll @IntRange(min = 1, max = 5) int batchCount) {

        NotifyingMaintenanceScheduler maintenance = new NotifyingMaintenanceScheduler();
        FailingWriteSink writeSink = new FailingWriteSink();

        CdcPipelineConfig config = new CdcPipelineConfig(
                STREAM, new SourceId(STREAM), TABLE,
                100, Duration.ofMillis(10), 5, 1, 5, Duration.ofSeconds(5));

        CdcPipelineLoop[] loopRef = new CdcPipelineLoop[1];
        LimitedSourceReader reader = new LimitedSourceReader(batchCount, loopRef);

        CdcPipelineLoop loop = new CdcPipelineLoop(config, new CdcPipelineLoopContext(
                reader,
                new StubConverter(),
                writeSink,
                new CdcQuotaEnforcer(10, 1_000_000),
                new WatermarkTracker(new InMemoryWatermarkStore()),
                maintenance,
                new CdcPipelineRuntimeHooks(e -> {}, BackoffStrategy.retry(1, 10), null)));
        loopRef[0] = loop;

        Thread t = new Thread(loop);
        t.start();
        try { t.join(3000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        loop.stop();
        try { t.join(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        assertThat(maintenance.notifiedTables)
                .as("onWriteComplete must not be called when writes fail")
                .isEmpty();
    }

    // --- Fakes ---

    static class NotifyingMaintenanceScheduler extends MaintenanceScheduler {
        final List<String> notifiedTables = new CopyOnWriteArrayList<>();
        final List<WriteResult> notifiedResults = new CopyOnWriteArrayList<>();

        NotifyingMaintenanceScheduler() { super(new StubSpi(), MaintenanceConfig.defaults()); }

        @Override
        public void onWriteComplete(String tableName, WriteResult result) {
            notifiedTables.add(tableName);
            notifiedResults.add(result);
        }
    }

    static class LimitedSourceReader implements CdcSourceReader {
        private final AtomicInteger idx = new AtomicInteger(0);
        private final int limit;
        private final CdcPipelineLoop[] loopRef;

        LimitedSourceReader(int limit, CdcPipelineLoop[] loopRef) {
            this.limit = limit;
            this.loopRef = loopRef;
        }

        @Override
        public List<SourceRecord> pollBatch(int maxRecords, Duration timeout) {
            int i = idx.getAndIncrement();
            if (i < limit) {
                return List.of(new SourceRecord("t", 0, i, new byte[]{1}, new byte[]{2}, System.currentTimeMillis()));
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

    static class SimpleWriteSink implements CdcWriteSink {
        @Override
        public CdcWriteResult writeBatch(List<CdcEnvelope> envelopes) {
            return new CdcWriteResult(envelopes.size(), envelopes.size() * 256L, 1, TABLE);
        }
        @Override public void close() {}
    }

    static class FailingWriteSink implements CdcWriteSink {
        @Override
        public CdcWriteResult writeBatch(List<CdcEnvelope> envelopes) {
            throw new RuntimeException("Simulated write failure");
        }
        @Override public void close() {}
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
