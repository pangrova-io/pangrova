package org.pangrova.coordinator.bootstrap;

import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.streaming.StreamConfig;
import org.pangrova.coordinator.streaming.StreamDdlContext;
import org.pangrova.coordinator.streaming.StreamDdlExecutor;
import org.pangrova.coordinator.streaming.StreamScheduler;
import org.pangrova.coordinator.streaming.StreamSchedulerContext;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.streaming.jdbc.JdbcStreamOffsetStore;
import org.pangrova.streaming.jdbc.JdbcStreamStateStore;
import org.pangrova.spi.streaming.store.StreamLifecycleState;
import org.pangrova.spi.streaming.store.StreamState;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.statement.stream.AlterStreamAction;
import org.pangrova.parser.ast.statement.stream.AlterStreamStatement;
import org.pangrova.parser.ast.statement.stream.CreateStreamStatement;
import org.pangrova.spi.streaming.record.CdcEnvelope;
import org.pangrova.spi.streaming.source.CdcSourceReader;
import org.pangrova.spi.streaming.sink.CdcWriteResult;
import org.pangrova.spi.streaming.sink.CdcWriteSink;
import org.pangrova.spi.streaming.source.SourceRecord;
import org.pangrova.spi.streaming.source.TopicPartitionOffset;
import org.pangrova.streaming.cdc.CdcRecordConverterRegistry;
import org.pangrova.streaming.jdbc.JdbcCdcCommitStateStore;
import org.pangrova.streaming.cdc.commit.TwoPhaseCommitCoordinator;
import org.pangrova.streaming.format.DebeziumJsonConverter;
import org.pangrova.streaming.maintenance.MaintenanceConfig;
import org.pangrova.streaming.maintenance.MaintenanceScheduler;
import org.pangrova.streaming.watermark.InMemoryWatermarkStore;
import org.pangrova.streaming.watermark.WatermarkTracker;
import org.pangrova.testing.ContainerTestBase;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class StreamingBootstrapLeadershipTest extends ContainerTestBase {
    @Test
    void reElectionResumesPersistedStreamsWithANewOwnerAttempt() throws Exception {
        var dataSource = SharedPostgresContainer.migratedDataSource("stream_leadership");
        var states = new JdbcStreamStateStore(dataSource);
        var offsets = new JdbcStreamOffsetStore(dataSource);
        var state = StreamState.builder().streamName("orders").tenantId("tenant")
                .targetTable("catalog.db.orders").sourceFormat("debezium-json")
                .sourceProperties(Map.of())
                .lifecycleState(StreamLifecycleState.RUNNING)
                .createdAt(Instant.EPOCH).updatedAt(Instant.EPOCH).build();
        assertThat(states.createState(state)).isTrue();
        var readers = List.of(new BlockingReader(), new BlockingReader(), new BlockingReader(), new BlockingReader());
        var created = new AtomicInteger();
        var config = StreamConfig.withDefaults();
        try (var maintenance = new MaintenanceScheduler(AbsentTableMaintenance.instance(), MaintenanceConfig.defaults())) {
            var scheduler = new StreamScheduler(new StreamSchedulerContext(states, offsets,
                    new TwoPhaseCommitCoordinator(new JdbcCdcCommitStateStore(dataSource)),
                    new CdcRecordConverterRegistry(List.of(new DebeziumJsonConverter())),
                    new WatermarkTracker(new InMemoryWatermarkStore()), maintenance, config,
                    s -> readers.get(created.getAndIncrement()), s -> new UnusedSink()), "coordinator-a");
            var ddl = new StreamDdlExecutor(new StreamDdlContext(states, offsets, scheduler, config));
            var followerScheduler = new StreamScheduler(new StreamSchedulerContext(states, offsets,
                    new TwoPhaseCommitCoordinator(new JdbcCdcCommitStateStore(dataSource)),
                    new CdcRecordConverterRegistry(List.of(new DebeziumJsonConverter())),
                    new WatermarkTracker(new InMemoryWatermarkStore()), maintenance, config,
                    s -> { throw new AssertionError("follower must not create a source"); },
                    s -> { throw new AssertionError("follower must not create a sink"); }), "coordinator-b");
            var followerDdl = new StreamDdlExecutor(new StreamDdlContext(states, offsets, followerScheduler, config));
            try (var bootstrap = new StreamingBootstrap(ddl, scheduler);
                    var follower = new StreamingBootstrap(followerDdl, followerScheduler)) {
                scheduler.start(state);
                followerScheduler.start(state);
                assertThat(created.get()).isZero();
                assertThat(states.loadState("orders").orElseThrow().epoch()).isZero();
                bootstrap.onLeaderElected();
                assertThat(readers.get(0).polled.await(10, TimeUnit.SECONDS)).isTrue();
                assertThat(states.loadState("orders").orElseThrow().epoch()).isEqualTo(1);
                bootstrap.onLeadershipLost();
                assertThat(readers.get(0).closed.await(10, TimeUnit.SECONDS)).isTrue();
                assertThat(scheduler.activeCount()).isZero();
                bootstrap.onLeaderElected();
                assertThat(readers.get(1).polled.await(10, TimeUnit.SECONDS)).isTrue();
                var resumed = states.loadState("orders").orElseThrow();
                assertThat(resumed.epoch()).isEqualTo(2);
                assertThat(resumed.coordinatorId()).isEqualTo("coordinator-a");
                assertThat(resumed.checkpointId()).isEqualTo(state.checkpointId());
                assertThat(resumed.lifecycleState()).isEqualTo(StreamLifecycleState.RUNNING);
                assertThat(scheduler.activeCount()).isEqualTo(1);

                // Execute DDL on a different coordinator. Only the elected coordinator polls the source.
                follower.getDdlExecutor().alter(new AlterStreamStatement(Identifier.of("orders"),
                        new AlterStreamAction.Suspend()), "tenant");
                assertThat(readers.get(1).closed.await(10, TimeUnit.SECONDS)).isTrue();
                assertThat(states.loadState("orders").orElseThrow().lifecycleState())
                        .isEqualTo(StreamLifecycleState.SUSPENDED);
                follower.getDdlExecutor().alter(new AlterStreamStatement(Identifier.of("orders"),
                        new AlterStreamAction.Resume()), "tenant");
                assertThat(readers.get(2).polled.await(10, TimeUnit.SECONDS)).isTrue();
                assertThat(states.loadState("orders").orElseThrow().coordinatorId()).isEqualTo("coordinator-a");
                assertThat(created.get()).isEqualTo(3);
                assertThat(followerScheduler.activeCount()).isZero();
                follower.getDdlExecutor().create(new CreateStreamStatement(Identifier.of("new-orders"), false,
                        Map.of("bootstrap.servers", "unused:9092", "topic", "orders"), "debezium-json",
                        Identifier.of("catalog", "db", "orders"), Map.of()), "tenant", "creator");
                assertThat(readers.get(3).polled.await(10, TimeUnit.SECONDS)).isTrue();
                var createdRemotely = states.loadState("new-orders").orElseThrow();
                assertThat(createdRemotely.targetTable()).isEqualTo("catalog.db.orders");
                assertThat(createdRemotely.coordinatorId()).isEqualTo("coordinator-a");
                assertThat(followerScheduler.activeCount()).isZero();
            }
            assertThat(readers.get(2).closed.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(readers.get(3).closed.await(10, TimeUnit.SECONDS)).isTrue();
        }
    }

    private static final class BlockingReader implements CdcSourceReader {
        private final CountDownLatch polled = new CountDownLatch(1);
        private final CountDownLatch closed = new CountDownLatch(1);
        @Override public List<SourceRecord> pollBatch(int max, Duration timeout) {
            polled.countDown();
            try {
                new CountDownLatch(1).await(timeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException stopped) {
                Thread.currentThread().interrupt();
            }
            return List.of();
        }
        @Override public void commitOffsets(Map<TopicPartitionOffset, Long> offsets) {
            throw new AssertionError("empty source must not acknowledge records");
        }
        @Override public void seekToOffsets(Map<TopicPartitionOffset, Long> offsets) {
            throw new AssertionError("new source has no checkpoint");
        }
        @Override public void close() { closed.countDown(); }
    }

    private static final class UnusedSink implements CdcWriteSink {
        @Override public CdcWriteResult writeBatch(List<CdcEnvelope> envelopes) {
            throw new AssertionError("empty source must not publish");
        }
        @Override public void close() { }
    }
}
