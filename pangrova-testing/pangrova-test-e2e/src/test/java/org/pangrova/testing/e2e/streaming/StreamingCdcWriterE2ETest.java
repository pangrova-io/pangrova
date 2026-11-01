package org.pangrova.testing.e2e.streaming;

import org.junit.jupiter.api.Test;
import org.pangrova.common.concurrent.BackoffStrategy;
import org.pangrova.spi.streaming.record.CdcEnvelope;
import org.pangrova.spi.streaming.record.CdcOperation;
import org.pangrova.spi.streaming.source.CdcSourceReader;
import org.pangrova.spi.streaming.sink.CdcWriteResult;
import org.pangrova.spi.streaming.sink.CdcWriteSink;
import org.pangrova.spi.streaming.source.SourceRecord;
import org.pangrova.spi.streaming.source.SourceId;
import org.pangrova.spi.streaming.source.TopicPartitionOffset;
import org.pangrova.streaming.cdc.CdcPipelineConfig;
import org.pangrova.streaming.cdc.CdcPipelineLoop;
import org.pangrova.streaming.cdc.CdcPipelineLoopContext;
import org.pangrova.streaming.cdc.CdcPipelineRuntimeHooks;
import org.pangrova.streaming.cdc.CdcQuotaEnforcer;
import org.pangrova.streaming.format.DebeziumJsonConverter;
import org.pangrova.streaming.maintenance.CompactionResult;
import org.pangrova.streaming.maintenance.MaintenanceConfig;
import org.pangrova.streaming.maintenance.MaintenanceScheduler;
import org.pangrova.streaming.maintenance.MaintenanceStatus;
import org.pangrova.streaming.maintenance.OrphanCleanupResult;
import org.pangrova.streaming.maintenance.SnapshotExpirationResult;
import org.pangrova.streaming.maintenance.TableMaintenanceSpi;
import org.pangrova.streaming.watermark.InMemoryWatermarkStore;
import org.pangrova.streaming.watermark.WatermarkTracker;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * End-to-end coverage for the production CDC path: Debezium JSON → converter →
 * {@link CdcPipelineLoop} → {@link CdcWriteSink} → offset commit.
 */
class StreamingCdcWriterE2ETest {

    private static final String TARGET = "catalog.schema.orders";
    private static final String STREAM = "cdc-orders";

    @Test
    void debeziumConverterParsesMessage() {
        CdcEnvelope envelope = new DebeziumJsonConverter().deserialize(
                """
                {"payload":{"op":"c","before":null,"after":{"id":1,"name":"alice"},\
                "ts_ms":1234567890,"source":{"db":"mydb","table":"users"}}}"""
                        .getBytes(StandardCharsets.UTF_8));

        assertThat(envelope.operation()).isEqualTo(CdcOperation.INSERT);
        assertThat(envelope.after()).isNotNull();
        assertThat(envelope.after().get("id")).isEqualTo(1);
        assertThat(envelope.after().get("name")).isEqualTo("alice");
        assertThat(envelope.before()).isNull();
        assertThat(envelope.tableName()).isEqualTo("mydb.users");
    }

    @Test
    void pipelineWritesParsedBatchAndCommitsOffsets() throws Exception {
        byte[] payload = """
                {"payload":{"op":"u","before":{"id":1,"name":"old"},\
                "after":{"id":1,"name":"new"},"ts_ms":1234567890,\
                "source":{"db":"mydb","table":"users"}}}"""
                .getBytes(StandardCharsets.UTF_8);
        SourceRecord record = new SourceRecord("orders", 0, 42, new byte[]{1}, payload,
                System.currentTimeMillis());

        RecordingSource reader = new RecordingSource(record);
        RecordingSink sink = new RecordingSink();
        try (MaintenanceScheduler maintenance = new MaintenanceScheduler(
                new NoOpMaintenance(), MaintenanceConfig.defaults())) {
            CdcPipelineConfig config = new CdcPipelineConfig(
                    STREAM, new SourceId(STREAM), TARGET,
                    10, Duration.ofMillis(50), 10, 3, 5);
            CdcPipelineLoopContext deps = new CdcPipelineLoopContext(
                    reader, new DebeziumJsonConverter(), sink,
                    new CdcQuotaEnforcer(4, 1_000_000),
                    new WatermarkTracker(new InMemoryWatermarkStore()),
                    maintenance,
                    CdcPipelineRuntimeHooks.forTests(e -> { }, BackoffStrategy.retry(10, 100), null));
            CdcPipelineLoop loop = new CdcPipelineLoop(config, deps);
            Thread thread = new Thread(loop);
            thread.start();
            try {
                assertThat(reader.awaitCommit()).isTrue();
            } finally {
                loop.stop();
                thread.join(5_000);
            }
        }

        assertThat(sink.envelopes).hasSize(1);
        assertThat(sink.envelopes.get(0).operation()).isEqualTo(CdcOperation.UPDATE);
        assertThat(sink.envelopes.get(0).after().get("name")).isEqualTo("new");
        assertThat(reader.committed.get(0))
                .containsEntry(new TopicPartitionOffset("orders", 0), 43L);
    }

    private static final class RecordingSource implements CdcSourceReader {
        private final List<SourceRecord> batch;
        private final AtomicInteger polls = new AtomicInteger();
        private final List<Map<TopicPartitionOffset, Long>> committed = new CopyOnWriteArrayList<>();

        private RecordingSource(SourceRecord record) {
            this.batch = List.of(record);
        }

        @Override
        public List<SourceRecord> pollBatch(int maxRecords, Duration timeout) {
            return polls.getAndIncrement() == 0 ? batch : List.of();
        }

        @Override
        public void commitOffsets(Map<TopicPartitionOffset, Long> offsets) {
            committed.add(Map.copyOf(offsets));
        }

        @Override
        public void seekToOffsets(Map<TopicPartitionOffset, Long> offsets) { }

        @Override
        public void close() { }

        private boolean awaitCommit() throws InterruptedException {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (committed.isEmpty() && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            return !committed.isEmpty();
        }
    }

    private static final class RecordingSink implements CdcWriteSink {
        private final List<CdcEnvelope> envelopes = new CopyOnWriteArrayList<>();

        @Override
        public CdcWriteResult writeBatch(List<CdcEnvelope> batch) {
            envelopes.addAll(batch);
            return new CdcWriteResult(batch.size(), batch.size() * 128L, 1, TARGET);
        }

        @Override
        public void close() { }
    }

    private static final class NoOpMaintenance implements TableMaintenanceSpi {
        @Override
        public CompletableFuture<CompactionResult> compactAsync(String tableName) {
            return CompletableFuture.completedFuture(new CompactionResult(tableName, 0, 0, 0));
        }

        @Override
        public CompletableFuture<OrphanCleanupResult> cleanOrphansAsync(
                String tableName, Duration olderThan) {
            return CompletableFuture.completedFuture(new OrphanCleanupResult(tableName, 0, 0));
        }

        @Override
        public CompletableFuture<SnapshotExpirationResult> expireSnapshotsAsync(
                String tableName, Duration olderThan) {
            return CompletableFuture.completedFuture(new SnapshotExpirationResult(tableName, 0));
        }

        @Override
        public MaintenanceStatus getMaintenanceStatus(String tableName) {
            return new MaintenanceStatus(tableName, 0, 0, 0, 0, 0, 0, 0, null, null, null);
        }
    }
}
