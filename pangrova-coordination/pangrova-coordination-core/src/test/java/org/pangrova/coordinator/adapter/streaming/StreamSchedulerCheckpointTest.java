package org.pangrova.coordinator.adapter.streaming;

import org.pangrova.streaming.adapter.coordinator.StreamConfig;
import org.pangrova.streaming.adapter.coordinator.StreamScheduler;
import org.pangrova.streaming.adapter.coordinator.StreamSchedulerContext;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.streaming.api.checkpoint.StreamLifecycleState;
import org.pangrova.streaming.api.checkpoint.StreamState;
import org.pangrova.streaming.api.cdc.CdcEnvelope;
import org.pangrova.streaming.api.cdc.CdcOperation;
import org.pangrova.streaming.api.cdc.CdcRecordConverter;
import org.pangrova.streaming.api.source.SourceRecord;
import org.pangrova.streaming.api.checkpoint.TopicPartitionOffset;
import org.pangrova.streaming.core.cdc.CdcRecordConverterRegistry;
import org.pangrova.streaming.core.commit.JdbcCdcCommitStateStore;
import org.pangrova.streaming.core.commit.SourceId;
import org.pangrova.streaming.core.commit.TwoPhaseCommitCoordinator;
import org.pangrova.streaming.core.maintenance.MaintenanceConfig;
import org.pangrova.streaming.core.maintenance.MaintenanceScheduler;
import org.pangrova.streaming.core.watermark.WatermarkTracker;
import org.pangrova.testing.testkit.ContainerTestBase;

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
                new org.pangrova.streaming.core.commit.CdcBatchCheckpoint(List.of(
                        new org.pangrova.streaming.core.commit.OffsetRange("orders", 0, 0, 100))));
        store.compareAndSetState(old.id(), org.pangrova.streaming.core.commit.CdcCommitState.PREPARED,
                org.pangrova.streaming.core.commit.CdcCommitState.COMMITTING);
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
