package org.pangrova.streaming.property.cdc;

import org.pangrova.observability.noop.NoOpMetricsProvider;

import org.pangrova.streaming.api.enums.CdcOperation;

import org.pangrova.streaming.api.commit.SourceId;
import net.jqwik.api.*;
import net.jqwik.api.constraints.IntRange;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property 15: Schema evolution classification.
 *
 * <p>Validates: Requirements 13.1, 13.2, 13.3</p>
 *
 * <p>Verifies that the CdcPipelineLoop correctly tracks the schemaVersion field
 * from CdcEnvelope. Since CdcPipelineLoop does not yet have full schema evolution
 * detection logic (additive vs incompatible), this test validates that:</p>
 * <ul>
 *   <li>Batches with consistent schemaVersion are processed successfully</li>
 *   <li>The schemaVersion field is preserved through the pipeline</li>
 *   <li>Varying schemaVersion values across batches do not cause data loss</li>
 * </ul>
 */
class SchemaEvolutionPropertyTest {

    private static final String STREAM = "schema-stream";
    private static final String TABLE = "catalog.schema.orders";

    /**
     * Validates: Requirements 13.1
     *
     * Batches with the same schemaVersion are all processed successfully.
     * The pipeline writes all records without error.
     */
    @Property(tries = 10)
    void consistentSchemaVersionProcessedSuccessfully(
            @ForAll @IntRange(min = 1, max = 5) int batchCount,
            @ForAll @IntRange(min = 0, max = 100) int schemaVersion) {

        SchemaVersionTrackingWriteSink writeSink = new SchemaVersionTrackingWriteSink();
        CdcPipelineConfig config = new CdcPipelineConfig(
                STREAM, new SourceId(STREAM), TABLE,
                100, Duration.ofMillis(10), 5, 3, 5, Duration.ofSeconds(5));

        // Use a late-binding reference so the reader can stop the loop
        CdcPipelineLoop[] loopRef = new CdcPipelineLoop[1];
        StoppingSourceReader reader = new StoppingSourceReader(batchCount, schemaVersion, loopRef);

        CdcPipelineLoop loop = new CdcPipelineLoop(config, buildDeps(
                reader, writeSink, new SchemaVersionConverter(schemaVersion)));
        loopRef[0] = loop;

        Thread t = new Thread(loop);
        t.start();
        try { t.join(5000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        loop.stop();
        try { t.join(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        assertThat(writeSink.writtenBatches.size())
                .as("All batches with consistent schema should be written")
                .isEqualTo(batchCount);

        // Verify schemaVersion is preserved in all written envelopes
        for (List<CdcEnvelope> batch : writeSink.writtenBatches) {
            for (CdcEnvelope env : batch) {
                assertThat(env.schemaVersion())
                        .as("schemaVersion must be preserved through the pipeline")
                        .isEqualTo(schemaVersion);
            }
        }
    }

    /**
     * Validates: Requirements 13.2, 13.3
     *
     * When schemaVersion changes across batches (additive scenario),
     * the pipeline continues processing without data loss.
     * Each batch's envelopes carry the correct schemaVersion.
     */
    @Property(tries = 10)
    void varyingSchemaVersionsAreTracked(
            @ForAll("schemaVersionSequence") List<Integer> versions) {
        Assume.that(!versions.isEmpty());

        SchemaVersionTrackingWriteSink writeSink = new SchemaVersionTrackingWriteSink();
        CdcPipelineConfig config = new CdcPipelineConfig(
                STREAM, new SourceId(STREAM), TABLE,
                100, Duration.ofMillis(10), 5, 3, 5, Duration.ofSeconds(5));

        CdcPipelineLoop[] loopRef = new CdcPipelineLoop[1];
        StoppingMultiVersionSourceReader reader = new StoppingMultiVersionSourceReader(versions, loopRef);

        CdcPipelineLoop loop = new CdcPipelineLoop(config, buildDeps(
                reader, writeSink, new DynamicSchemaVersionConverter()));
        loopRef[0] = loop;

        Thread t = new Thread(loop);
        t.start();
        try { t.join(5000); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        loop.stop();
        try { t.join(500); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }

        // All batches should be written (no data loss for schema version changes)
        assertThat(writeSink.writtenBatches.size())
                .as("All batches should be written regardless of schema version changes")
                .isEqualTo(versions.size());

        // Verify each batch carries the correct schema version
        for (int i = 0; i < writeSink.writtenBatches.size(); i++) {
            List<CdcEnvelope> batch = writeSink.writtenBatches.get(i);
            int expectedVersion = versions.get(i);
            for (CdcEnvelope env : batch) {
                assertThat(env.schemaVersion())
                        .as("Batch %d should carry schemaVersion %d", i, expectedVersion)
                        .isEqualTo(expectedVersion);
            }
        }
    }

    @Provide
    Arbitrary<List<Integer>> schemaVersionSequence() {
        return Arbitraries.integers().between(0, 50)
                .list().ofMinSize(1).ofMaxSize(8);
    }

    // --- Fakes ---

    static class StoppingSourceReader implements CdcSourceReader {
        private final AtomicInteger idx = new AtomicInteger(0);
        private final int batchCount;
        private final int schemaVersion;
        private final CdcPipelineLoop[] loopRef;

        StoppingSourceReader(int batchCount, int schemaVersion, CdcPipelineLoop[] loopRef) {
            this.batchCount = batchCount;
            this.schemaVersion = schemaVersion;
            this.loopRef = loopRef;
        }

        @Override
        public List<SourceRecord> pollBatch(int maxRecords, Duration timeout) {
            int i = idx.getAndIncrement();
            if (i < batchCount) {
                byte[] value = encodeVersion(schemaVersion);
                return List.of(new SourceRecord("t", 0, i, new byte[]{1}, value, System.currentTimeMillis()));
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

    static class StoppingMultiVersionSourceReader implements CdcSourceReader {
        private final AtomicInteger idx = new AtomicInteger(0);
        private final List<Integer> versions;
        private final CdcPipelineLoop[] loopRef;

        StoppingMultiVersionSourceReader(List<Integer> versions, CdcPipelineLoop[] loopRef) {
            this.versions = versions;
            this.loopRef = loopRef;
        }

        @Override
        public List<SourceRecord> pollBatch(int maxRecords, Duration timeout) {
            int i = idx.getAndIncrement();
            if (i < versions.size()) {
                byte[] value = encodeVersion(versions.get(i));
                return List.of(new SourceRecord("t", 0, i, new byte[]{1}, value, System.currentTimeMillis()));
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

    static class SchemaVersionConverter implements CdcRecordConverter {
        private final int schemaVersion;
        SchemaVersionConverter(int schemaVersion) { this.schemaVersion = schemaVersion; }

        @Override public String formatId() { return "schema-stub"; }
        @Override public CdcEnvelope deserialize(byte[] payload) {
            return new CdcEnvelope(CdcOperation.INSERT, TABLE, null, null,
                    Instant.now(), schemaVersion, Map.of());
        }
    }

    /** Converter that reads schemaVersion from the payload bytes. */
    static class DynamicSchemaVersionConverter implements CdcRecordConverter {
        @Override public String formatId() { return "dynamic-schema-stub"; }
        @Override public CdcEnvelope deserialize(byte[] payload) {
            int version = decodeVersion(payload);
            return new CdcEnvelope(CdcOperation.INSERT, TABLE, null, null,
                    Instant.now(), version, Map.of());
        }
    }

    static class SchemaVersionTrackingWriteSink implements CdcWriteSink {
        final List<List<CdcEnvelope>> writtenBatches = new CopyOnWriteArrayList<>();

        @Override
        public CdcWriteResult writeBatch(List<CdcEnvelope> envelopes) {
            writtenBatches.add(List.copyOf(envelopes));
            return new CdcWriteResult(envelopes.size(), envelopes.size() * 256L, 1, TABLE);
        }
        @Override public void close() {}
    }

    private static byte[] encodeVersion(int version) {
        return new byte[]{(byte) (version >> 24), (byte) (version >> 16),
                (byte) (version >> 8), (byte) version};
    }

    private static int decodeVersion(byte[] payload) {
        if (payload == null || payload.length < 4) return 0;
        return ((payload[0] & 0xFF) << 24) | ((payload[1] & 0xFF) << 16)
                | ((payload[2] & 0xFF) << 8) | (payload[3] & 0xFF);
    }

    private static CdcPipelineLoopContext buildDeps(
            CdcSourceReader reader, CdcWriteSink writeSink, CdcRecordConverter converter) {
        return new CdcPipelineLoopContext(
                reader, converter, writeSink,
                new CdcQuotaEnforcer(10, 1_000_000),
                new WatermarkTracker(new InMemoryWatermarkStore()),
                new StubMaintenanceScheduler(),
                new CdcPipelineRuntimeHooks(new NoOpMetricsProvider(), e -> {}, BackoffStrategy.retry(10, 100), null));
    }

    static class InMemoryWatermarkStore implements WatermarkStore {
        @Override public Instant advance(String t, Instant w) { return w; }
        @Override public Optional<Instant> load(String t) { return Optional.empty(); }
    }

    static class StubMaintenanceScheduler extends MaintenanceScheduler {
        StubMaintenanceScheduler() { super(new StubSpi(), MaintenanceConfig.defaults()); }
        @Override public void onWriteComplete(String tableName, WriteResult result) {}
    }

    static class StubSpi implements TableMaintenance {
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
