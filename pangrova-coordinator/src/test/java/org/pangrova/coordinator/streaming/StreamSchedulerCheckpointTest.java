package org.pangrova.coordinator.streaming;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.spi.streaming.store.StreamLifecycleState;
import org.pangrova.spi.streaming.store.StreamState;
import org.pangrova.spi.streaming.record.CdcEnvelope;
import org.pangrova.spi.streaming.record.CdcOperation;
import org.pangrova.spi.streaming.record.CdcRecordConverter;
import org.pangrova.spi.streaming.source.SourceRecord;
import org.pangrova.spi.streaming.source.TopicPartitionOffset;
import org.pangrova.streaming.cdc.CdcRecordConverterRegistry;
import org.pangrova.streaming.jdbc.JdbcCdcCommitStateStore;
import org.pangrova.spi.streaming.source.SourceId;
import org.pangrova.streaming.cdc.commit.TwoPhaseCommitCoordinator;
import org.pangrova.streaming.maintenance.MaintenanceConfig;
import org.pangrova.streaming.maintenance.MaintenanceScheduler;
import org.pangrova.streaming.watermark.WatermarkTracker;
import org.pangrova.testing.ContainerTestBase;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class StreamSchedulerCheckpointTest extends ContainerTestBase {
    @Test
    void scheduledPipelinePersistsAllOffsetsBeforeAcknowledgingSource() throws Exception {
        var store = new JdbcCdcCommitStateStore(SharedPostgresContainer.migratedDataSource("scheduler_checkpoint"));
        var coordinator = new TwoPhaseCommitCoordinator(store);
        var old = coordinator.prepare(new SourceId("ingestion"),
                new org.pangrova.spi.streaming.commit.CdcBatchCheckpoint(List.of(
                        new org.pangrova.spi.streaming.source.OffsetRange("orders", 0, 0, 100))));
        store.compareAndSetState(old.id(), org.pangrova.spi.streaming.commit.CdcCommitState.PREPARED,
                org.pangrova.spi.streaming.commit.CdcCommitState.COMMITTING);
        coordinator.commit(old);
        var expected = Map.of(new TopicPartitionOffset("orders", 0), 11L,
                new TopicPartitionOffset("payments", 2), 21L);
        var acknowledgement = new CountDownLatch(1);
        var failure = new AtomicReference<Throwable>();
        var polled = new AtomicBoolean();
        var restored = new AtomicReference<Map<TopicPartitionOffset, Long>>(Map.of());
        var source = new StreamSchedulerWatermarkCleanupTest.NoOpSourceReader() {
            @Override
            public void seekToOffsets(Map<TopicPartitionOffset, Long> offsets) {
                restored.set(offsets);
            }
            @Override
            public List<SourceRecord> pollBatch(int max, Duration timeout) {
                if (polled.compareAndSet(false, true)) {
                    return List.of(new SourceRecord("orders", 0, 10, null, new byte[0], 0),
                            new SourceRecord("payments", 2, 20, null, new byte[0], 0));
                }
                return List.of();
            }

            @Override
            public void commitOffsets(Map<TopicPartitionOffset, Long> offsets) {
                try {
                    assertThat(offsets).isEqualTo(expected);
                    assertThat(coordinator.findLastCommittedOffsets(new SourceId("new-incarnation")))
                            .isEqualTo(expected);
                } catch (Throwable problem) {
                    failure.set(problem);
                } finally {
                    acknowledgement.countDown();
                }
            }
        };
        CdcRecordConverter converter = new CdcRecordConverter() {
            @Override public String formatId() { return "test"; }
            @Override public CdcEnvelope deserialize(byte[] payload) {
                return new CdcEnvelope(CdcOperation.INSERT, "catalog.db.target", null, null,
                        Instant.EPOCH, 1, Map.of());
            }
        };
        var context = new StreamSchedulerContext(
                new StreamSchedulerWatermarkCleanupTest.InMemoryStreamStateStore(),
                new StreamSchedulerWatermarkCleanupTest.InMemoryStreamOffsetStore(), coordinator,
                new CdcRecordConverterRegistry(List.of(converter)),
                new WatermarkTracker(new StreamSchedulerWatermarkCleanupTest.InMemoryWatermarkStore()),
                new MaintenanceScheduler(new StreamSchedulerWatermarkCleanupTest.NoOpTableMaintenance(),
                        MaintenanceConfig.defaults()), StreamConfig.withDefaults(),
                state -> source, state -> new StreamSchedulerWatermarkCleanupTest.NoOpWriteSink());
        try (var scheduler = new StreamScheduler(context, "coordinator-test")) {
            scheduler.start(StreamState.builder().streamName("ingestion").tenantId("tenant")
                    .checkpointId("new-incarnation")
                    .lifecycleState(StreamLifecycleState.RUNNING).targetTable("catalog.db.target")
                    .sourceProperties(Map.of("topic", "orders")).sourceFormat("test")
                    .streamProperties(Map.of()).creatorIdentity("admin").coordinatorId("")
                    .epoch(1).schemaVersion(1).createdAt(Instant.EPOCH).updatedAt(Instant.EPOCH).build());
            assertThat(acknowledgement.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(failure.get()).isNull();
            assertThat(restored.get()).isEmpty();
            assertThat(coordinator.findLastCommittedOffsets(new SourceId("ingestion")))
                    .containsEntry(new TopicPartitionOffset("orders", 0), 101L);
        }
    }
}
