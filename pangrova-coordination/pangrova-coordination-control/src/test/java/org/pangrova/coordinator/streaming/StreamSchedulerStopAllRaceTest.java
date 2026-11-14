package org.pangrova.coordinator.streaming;

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
 * Unit tests for BUG-D16 fix: {@link StreamScheduler#stopAll()} must set a
 * {@code paused} flag to prevent concurrent {@link StreamScheduler#start}
 * from adding new pipelines between the stop loop and clear().
 * {@link StreamScheduler#resume()} clears the flag for re-election.
 */
class StreamSchedulerStopAllRaceTest extends org.pangrova.testing.testkit.ContainerTestBase {

    private StreamScheduler scheduler;

    @BeforeEach
    void setUp() throws Exception {
        StreamConfig config = StreamConfig.withDefaults();
        CdcRecordConverterRegistry converterRegistry =
                new CdcRecordConverterRegistry(List.of(new NoOpConverter()));
        MaintenanceScheduler maintenanceScheduler = new MaintenanceScheduler(
                new NoOpTableMaintenance(), MaintenanceConfig.defaults());

        StreamSchedulerContext deps = new StreamSchedulerContext(
                new NoOpStreamStateStore(),
                new NoOpStreamOffsetStore(),
                commitCoordinator(),
                converterRegistry,
                new WatermarkTracker(new InMemoryWatermarkStore()),
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
    @DisplayName("start() is rejected after stopAll() sets paused flag (BUG-D16)")
    void startRejectedAfterStopAll() {
        StreamState state = buildStreamState("stream-1", "catalog.db.orders");
        scheduler.start(state);
        assertThat(scheduler.isActive("stream-1")).isTrue();

        scheduler.stopAll();
        assertThat(scheduler.activeCount()).isZero();

        // After stopAll, start should be rejected because paused=true
        StreamState newState = buildStreamState("stream-2", "catalog.db.users");
        scheduler.start(newState);
        assertThat(scheduler.isActive("stream-2")).isFalse();
    }

    @Test
    @DisplayName("resume() clears paused flag, allowing start() again")
    void resumeAllowsStartAfterStopAll() {
        scheduler.stopAll();

        // Paused — start should be rejected
        StreamState state = buildStreamState("stream-1", "catalog.db.orders");
        scheduler.start(state);
        assertThat(scheduler.isActive("stream-1")).isFalse();

        // Resume — start should work
        scheduler.resume();
        scheduler.start(state);
        assertThat(scheduler.isActive("stream-1")).isTrue();
    }

    @Test
    @DisplayName("close() still works after stopAll()")
    void closeAfterStopAll() {
        scheduler.stopAll();
        // close() should not throw even when paused
        scheduler.close();
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

    // --- Minimal stubs (reused from StreamSchedulerWatermarkCleanupTest pattern) ---

    static class InMemoryWatermarkStore implements WatermarkStore {
        private final ConcurrentHashMap<String, Instant> data = new ConcurrentHashMap<>();
        @Override public Instant advance(String t, Instant w) { return data.merge(t, w, (a, b) -> a.isAfter(b) ? a : b); }
        @Override public Optional<Instant> load(String t) { return Optional.ofNullable(data.get(t)); }
    }

    static class NoOpStreamStateStore implements StreamStateStore {
        @Override public boolean createState(StreamState state) { return true; }
        @Override public void saveStateIfEpoch(StreamState state, long epoch) {}
        @Override public void saveState(StreamState s) {}
        @Override public Optional<StreamState> loadState(String n) { return Optional.empty(); }
        @Override public List<StreamState> listByTenant(String t) { return List.of(); }
        @Override public List<StreamState> listByState(StreamLifecycleState s) { return List.of(); }
        @Override public List<StreamState> listAll() { return List.of(); }
        @Override public boolean delete(StreamState state) { return false; }
    }

    static class NoOpStreamOffsetStore implements StreamOffsetStore {
        @Override public void saveOffsets(String n, Map<TopicPartitionOffset, Long> o) {}
        @Override public Map<TopicPartitionOffset, Long> loadOffsets(String n) { return Map.of(); }
        @Override public void deleteOffsets(String n) {}
    }

    static class NoOpConverter implements CdcRecordConverter {
        @Override public String formatId() { return "debezium-json"; }
        @Override public CdcEnvelope deserialize(byte[] p) { return null; }
    }

    static class NoOpSourceReader implements CdcSourceReader {
        @Override public List<SourceRecord> pollBatch(int m, Duration t) { return List.of(); }
        @Override public void commitOffsets(Map<TopicPartitionOffset, Long> o) {}
        @Override public void seekToOffsets(Map<TopicPartitionOffset, Long> o) {}
        @Override public void close() {}
    }

    static class NoOpWriteSink implements CdcWriteSink {
        @Override public CdcWriteResult writeBatch(List<CdcEnvelope> e) {
            return new CdcWriteResult(0, 0L, 0, "");
        }
        @Override public void close() {}
    }

    static class NoOpTableMaintenance implements TableMaintenanceSpi {
        @Override public java.util.concurrent.CompletableFuture<CompactionResult> compactAsync(String t) {
            return java.util.concurrent.CompletableFuture.completedFuture(new CompactionResult(t, 0, 0, 0L));
        }
        @Override public java.util.concurrent.CompletableFuture<OrphanCleanupResult> cleanOrphansAsync(String t, Duration o) {
            return java.util.concurrent.CompletableFuture.completedFuture(new OrphanCleanupResult(t, 0, 0L));
        }
        @Override public java.util.concurrent.CompletableFuture<SnapshotExpirationResult> expireSnapshotsAsync(String t, Duration o) {
            return java.util.concurrent.CompletableFuture.completedFuture(new SnapshotExpirationResult(t, 0));
        }
        @Override public MaintenanceStatus getMaintenanceStatus(String t) {
            return new MaintenanceStatus(t, 0, 0, 0L, 0, 0, 0L, 0L, null, null, null);
        }
    }

    private static org.pangrova.streaming.commit.CdcCommitCoordinator commitCoordinator() throws Exception {
        var dataSource = org.pangrova.coordinator.testinfra.SharedPostgresContainer
                .migratedDataSource("stream_scheduler");
        return new org.pangrova.streaming.commit.TwoPhaseCommitCoordinator(
                new org.pangrova.persistence.streaming.postgresql.JdbcCdcCommitStateStore(dataSource));
    }

}
