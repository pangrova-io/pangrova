package org.pangrova.coordinator.streaming;

import org.pangrova.testing.testkit.constants.TestObservability;

import org.pangrova.streaming.coordinator.StreamConfig;
import org.pangrova.streaming.coordinator.StreamScheduler;
import org.pangrova.streaming.coordinator.StreamSchedulerContext;
import org.pangrova.streaming.api.enums.StreamLifecycleState;
import org.pangrova.streaming.api.checkpoint.StreamOffsetStore;
import org.pangrova.streaming.api.checkpoint.StreamState;
import org.pangrova.streaming.api.checkpoint.StreamStateStore;
import org.pangrova.streaming.api.cdc.CdcEnvelope;
import org.pangrova.streaming.api.cdc.CdcRecordConverter;
import org.pangrova.streaming.api.source.CdcSourceReader;
import org.pangrova.streaming.api.cdc.CdcWriteResult;
import org.pangrova.streaming.api.cdc.CdcWriteSink;
import org.pangrova.streaming.api.source.SourceRecord;
import org.pangrova.streaming.api.checkpoint.TopicPartitionOffset;
import org.pangrova.streaming.api.watermark.WatermarkStore;
import org.pangrova.streaming.cdc.CdcRecordConverterRegistry;
import org.pangrova.streaming.maintenance.CompactionResult;
import org.pangrova.streaming.maintenance.MaintenanceConfig;
import org.pangrova.streaming.maintenance.MaintenanceScheduler;
import org.pangrova.streaming.maintenance.MaintenanceStatus;
import org.pangrova.streaming.maintenance.OrphanCleanupResult;
import org.pangrova.streaming.maintenance.SnapshotExpirationResult;
import org.pangrova.streaming.maintenance.TableMaintenanceSpi;
import org.pangrova.streaming.watermark.WatermarkTracker;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for BUG-D22 fix: {@link StreamScheduler#stop(String)} and
 * {@link StreamScheduler#stopAll()} must call
 * {@link WatermarkTracker#removeTable(String)} to prevent memory leaks.
 */
class StreamSchedulerWatermarkCleanupTest extends org.pangrova.testing.testkit.ContainerTestBase {

    private WatermarkTracker watermarkTracker;
    private StreamScheduler scheduler;

    @BeforeEach
    void setUp() throws Exception {
        watermarkTracker = new WatermarkTracker(new InMemoryWatermarkStore());

        StreamConfig config = StreamConfig.withDefaults();
        CdcRecordConverterRegistry converterRegistry =
                new CdcRecordConverterRegistry(List.of(new NoOpConverter()));

        MaintenanceScheduler maintenanceScheduler = new MaintenanceScheduler(
                new NoOpTableMaintenance(), MaintenanceConfig.defaults());

        StreamSchedulerContext deps = new StreamSchedulerContext(
                TestObservability.METRICS, new InMemoryStreamStateStore(),
                new InMemoryStreamOffsetStore(),
                commitCoordinator(),
                converterRegistry,
                watermarkTracker,
                maintenanceScheduler,
                config,
                state -> new NoOpSourceReader(),
                state -> new NoOpWriteSink());

        scheduler = new StreamScheduler(deps, "coordinator-test", Executors.newFixedThreadPool(2));
    }

    @AfterEach
    void tearDown() {
        scheduler.close();
    }

    @Test
    @DisplayName("stop() calls WatermarkTracker.removeTable for the stream's target table")
    void stopRemovesWatermarkState() {
        String targetTable = "catalog.db.orders";
        StreamState state = buildStreamState("stream-1", targetTable);

        // Pre-populate watermark state to simulate an active pipeline
        watermarkTracker.updateWatermark(targetTable, Instant.parse("2026-03-29T10:00:00Z"));
        assertThat(watermarkTracker.getWatermark(targetTable)).isNotEqualTo(Instant.MIN);

        scheduler.start(state);
        assertThat(scheduler.isActive("stream-1")).isTrue();

        scheduler.stop("stream-1");

        assertThat(scheduler.isActive("stream-1")).isFalse();
        // BUG-D22 fix: watermark state must be cleaned up
        assertThat(watermarkTracker.getWatermark(targetTable)).isEqualTo(Instant.MIN);
    }

    @Test
    @DisplayName("stopAll() calls WatermarkTracker.removeTable for all active streams")
    void stopAllRemovesAllWatermarkState() {
        String target1 = "catalog.db.orders";
        String target2 = "catalog.db.users";

        watermarkTracker.updateWatermark(target1, Instant.parse("2026-03-29T10:00:00Z"));
        watermarkTracker.updateWatermark(target2, Instant.parse("2026-03-29T11:00:00Z"));

        scheduler.start(buildStreamState("stream-1", target1));
        scheduler.start(buildStreamState("stream-2", target2));
        assertThat(scheduler.activeCount()).isEqualTo(2);

        scheduler.stopAll();

        assertThat(scheduler.activeCount()).isZero();
        assertThat(watermarkTracker.getWatermark(target1)).isEqualTo(Instant.MIN);
        assertThat(watermarkTracker.getWatermark(target2)).isEqualTo(Instant.MIN);
    }

    @Test
    @DisplayName("stop() on unknown stream is a no-op (no exception)")
    void stopUnknownStreamIsNoOp() {
        scheduler.stop("nonexistent");
        // No exception thrown — pass
    }

    private static StreamState buildStreamState(String streamName, String targetTable) {
        return StreamState.builder().sourceProvider("kafka")
                .streamName(streamName)
                .tenantId("tenant-1")
                .lifecycleState(StreamLifecycleState.RUNNING)
                .targetTable(targetTable)
                .sourceProperties(Map.of("bootstrap.servers", "localhost:9092", "topic", "cdc-topic"))
                .sourceFormat("debezium-json")
                .streamProperties(Map.of())
                .creatorIdentity("admin")
                .coordinatorId("")
                .epoch(1)
                .schemaVersion(1)
                .createdAt(Instant.now())
                .updatedAt(Instant.now())
                .build();
    }

    // --- Minimal stubs (no mock frameworks per project rules) ---

    static class InMemoryWatermarkStore implements WatermarkStore {
        private final ConcurrentHashMap<String, Instant> data = new ConcurrentHashMap<>();

        @Override
        public Instant advance(String tableName, Instant watermark) {
            return data.merge(tableName, watermark, (a, b) -> a.isAfter(b) ? a : b);
        }

        @Override
        public Optional<Instant> load(String tableName) {
            return Optional.ofNullable(data.get(tableName));
        }
    }

    static class InMemoryStreamStateStore implements StreamStateStore {
        @Override public boolean createState(StreamState state) { return true; }
        @Override public void saveStateIfEpoch(StreamState state, long epoch) {}
        @Override public void saveState(StreamState state) {}
        @Override public Optional<StreamState> loadState(String name) { return Optional.empty(); }
        @Override public List<StreamState> listByTenant(String tenantId) { return List.of(); }
        @Override public List<StreamState> listByState(StreamLifecycleState s) { return List.of(); }
        @Override public List<StreamState> listAll() { return List.of(); }
        @Override public boolean delete(StreamState state) { return false; }
    }

    static class InMemoryStreamOffsetStore implements StreamOffsetStore {
        @Override public void saveOffsets(String name, Map<TopicPartitionOffset, Long> offsets) {}
        @Override public Map<TopicPartitionOffset, Long> loadOffsets(String name) { return Map.of(); }
        @Override public void deleteOffsets(String name) {}
    }

    static class NoOpConverter implements CdcRecordConverter {
        @Override public String formatId() { return "debezium-json"; }
        @Override public CdcEnvelope deserialize(byte[] payload) { return null; }
    }

    static class NoOpSourceReader implements CdcSourceReader {
        @Override public List<SourceRecord> pollBatch(int max, Duration timeout) { return List.of(); }
        @Override public void commitOffsets(Map<TopicPartitionOffset, Long> offsets) {}
        @Override public void seekToOffsets(Map<TopicPartitionOffset, Long> offsets) {}
        @Override public void close() {}
    }

    static class NoOpWriteSink implements CdcWriteSink {
        @Override public CdcWriteResult writeBatch(List<CdcEnvelope> envelopes) {
            return new CdcWriteResult(0, 0L, 0, "");
        }
        @Override public void close() {}
    }

    static class NoOpTableMaintenance implements TableMaintenanceSpi {
        @Override public java.util.concurrent.CompletableFuture<CompactionResult> compactAsync(String table) {
            return java.util.concurrent.CompletableFuture.completedFuture(
                    new CompactionResult(table, 0, 0, 0L));
        }
        @Override public java.util.concurrent.CompletableFuture<OrphanCleanupResult> cleanOrphansAsync(
                String table, Duration olderThan) {
            return java.util.concurrent.CompletableFuture.completedFuture(
                    new OrphanCleanupResult(table, 0, 0L));
        }
        @Override public java.util.concurrent.CompletableFuture<SnapshotExpirationResult> expireSnapshotsAsync(
                String table, Duration olderThan) {
            return java.util.concurrent.CompletableFuture.completedFuture(
                    new SnapshotExpirationResult(table, 0));
        }
        @Override public MaintenanceStatus getMaintenanceStatus(String table) {
            return new MaintenanceStatus(table, 0, 0, 0L, 0, 0, 0L, 0L, null, null, null);
        }
    }

    private static org.pangrova.streaming.commit.CdcCommitCoordinator commitCoordinator() throws Exception {
        var dataSource = org.pangrova.coordinator.testinfra.SharedPostgresContainer
                .migratedDataSource("stream_scheduler");
        return new org.pangrova.streaming.commit.TwoPhaseCommitCoordinator(
                new org.pangrova.persistence.streaming.postgresql.JdbcCdcCommitStateStore(dataSource));
    }

}
