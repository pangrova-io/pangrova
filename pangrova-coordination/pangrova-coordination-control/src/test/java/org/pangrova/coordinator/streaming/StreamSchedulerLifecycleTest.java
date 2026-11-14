package org.pangrova.coordinator.streaming;

import org.pangrova.streaming.coordinator.StreamConfig;
import org.pangrova.streaming.coordinator.StreamDdlContext;
import org.pangrova.streaming.coordinator.StreamDdlExecutor;
import org.pangrova.streaming.coordinator.StreamScheduler;
import org.pangrova.streaming.coordinator.StreamSchedulerContext;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.pangrova.streaming.coordinator.CdcSourceRegistry;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.persistence.streaming.postgresql.JdbcStreamOffsetStore;
import org.pangrova.persistence.streaming.postgresql.JdbcStreamStateStore;
import org.pangrova.streaming.api.enums.StreamLifecycleState;
import org.pangrova.streaming.api.checkpoint.StreamState;
import org.pangrova.common.exception.StaleEpochException;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.statement.stream.AlterStreamAction;
import org.pangrova.parser.api.ast.statement.stream.AlterStreamStatement;
import org.pangrova.parser.api.ast.statement.stream.CreateStreamStatement;
import org.pangrova.parser.api.ast.statement.stream.DropStreamStatement;
import org.pangrova.streaming.api.source.CdcSourceReader;
import org.pangrova.streaming.api.cdc.CdcWriteSink;
import org.pangrova.streaming.api.source.SourceRecord;
import org.pangrova.streaming.cdc.CdcRecordConverterRegistry;
import org.pangrova.persistence.streaming.postgresql.JdbcCdcCommitStateStore;
import org.pangrova.streaming.commit.TwoPhaseCommitCoordinator;
import org.pangrova.streaming.maintenance.MaintenanceConfig;
import org.pangrova.streaming.maintenance.MaintenanceScheduler;
import org.pangrova.streaming.watermark.WatermarkTracker;
import org.pangrova.testing.testkit.ContainerTestBase;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class StreamSchedulerLifecycleTest extends ContainerTestBase {
    private DataSource dataSource;
    private JdbcStreamStateStore states;
    private StreamState state;
    private MaintenanceScheduler maintenance;

    @BeforeEach
    void setUp() throws Exception {
        dataSource = SharedPostgresContainer.migratedDataSource("stream_lifecycle");
        states = new JdbcStreamStateStore(dataSource);
        state = StreamState.builder().sourceProvider("kafka").streamName("orders-stream").tenantId("tenant")
                .lifecycleState(StreamLifecycleState.RUNNING).targetTable("catalog.db.orders")
                .sourceProperties(Map.of()).sourceFormat("debezium-json")
                .streamProperties(Map.of("max_consecutive_failures", "1"))
                .createdAt(Instant.EPOCH).updatedAt(Instant.EPOCH).build();
        assertThat(states.createState(state)).isTrue();
        maintenance = new MaintenanceScheduler(new StreamSchedulerStopAllRaceTest.NoOpTableMaintenance(),
                MaintenanceConfig.defaults());
    }

    @AfterEach
    void tearDown() {
        if (maintenance != null) maintenance.close();
    }

    @Test
    void unknownFormatIsRejectedBeforePersistenceOrSourceAllocation() {
        try (var scheduler = new StreamScheduler(context(s -> {
            throw new AssertionError("An unsupported format must not open a source reader");
        }, s -> {
            throw new AssertionError("An unsupported format must not open a sink");
        }), "coordinator", new QueuedExecutor())) {
            var ddl = new StreamDdlExecutor(new StreamDdlContext(states, new JdbcStreamOffsetStore(dataSource),
                    scheduler, StreamConfig.withDefaults(),
                    CdcSourceRegistry.discover(getClass().getClassLoader())));
            var create = new CreateStreamStatement(Identifier.of("unsupported"), false, "kafka",
                    Map.of("bootstrap.servers", "unused:9092", "topic", "orders"), "debezium-avro",
                    Identifier.of("orders"), Map.of());
            assertThatThrownBy(() -> ddl.create(create, "tenant", "user"))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("debezium-avro");
            var unknownSource = new CreateStreamStatement(Identifier.of("unsupported"), false, "audit_feed",
                    Map.of("channel", "orders"), "debezium-json", Identifier.of("orders"), Map.of());
            assertThatThrownBy(() -> ddl.create(unknownSource, "tenant", "user"))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("audit_feed");
            var invalidKafka = new CreateStreamStatement(Identifier.of("unsupported"), false, "kafka",
                    Map.of("topic", "orders"), "debezium-json", Identifier.of("orders"), Map.of());
            assertThatThrownBy(() -> ddl.create(invalidKafka, "tenant", "user"))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("bootstrap.servers");
            assertThat(states.loadState("unsupported")).isEmpty();

            var persisted = state.toBuilder().streamName("unsupported").checkpointId("stream:unsupported")
                    .sourceFormat("debezium-avro").build();
            assertThat(states.createState(persisted)).isTrue();
            assertThatThrownBy(() -> scheduler.start(persisted))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("debezium-avro");
            assertThat(states.loadState("unsupported").orElseThrow().lifecycleState()).isEqualTo(StreamLifecycleState.FAILED);
            assertThat(scheduler.activeCount()).isZero();
        }
    }

    @Test
    void sinkFactoryFailureClosesTheReaderAndPersistsFailure() {
        var reader = new TrackingReader();
        try (var scheduler = new StreamScheduler(context(s -> reader, s -> {
            throw new IllegalStateException("sink unavailable");
        }), "coordinator", new QueuedExecutor())) {
            assertThatThrownBy(() -> scheduler.start(state)).hasMessage("sink unavailable");
            assertThat(reader.closes.get()).isEqualTo(1);
            assertThat(scheduler.activeCount()).isZero();
            var failed = states.loadState(state.streamName()).orElseThrow();
            assertThat(failed.lifecycleState()).isEqualTo(StreamLifecycleState.FAILED);
            assertThat(failed.coordinatorId()).isEqualTo("coordinator");
            assertThat(failed.epoch()).isEqualTo(2);
        }
    }

    @Test
    void duplicateStartDoesNotReclaimOrAllocateAndQueuedStopClosesExactlyOnce() {
        var reader = new TrackingReader();
        var sink = new TrackingSink();
        var creates = new AtomicInteger();
        var executor = new QueuedExecutor();
        try (var scheduler = new StreamScheduler(context(s -> {
            creates.incrementAndGet();
            return reader;
        }, s -> sink), "coordinator", executor)) {
            scheduler.start(state);
            scheduler.start(states.loadState(state.streamName()).orElseThrow());
            assertThat(creates.get()).isEqualTo(1);
            assertThat(states.loadState(state.streamName()).orElseThrow().epoch()).isEqualTo(1);
            scheduler.stop(state.streamName());
            executor.runQueued();
            assertThat(reader.polls.get()).isZero();
            assertThat(reader.closes.get()).isEqualTo(1);
            assertThat(sink.closes.get()).isEqualTo(1);
            assertThat(scheduler.activeCount()).isZero();
        }
        assertThat(reader.closes.get()).isEqualTo(1);
        assertThat(sink.closes.get()).isEqualTo(1);
    }

    @Test
    void executorRejectionClosesBothResourcesAndRemovesTheSlot() {
        var reader = new TrackingReader();
        var sink = new TrackingSink();
        var executor = new QueuedExecutor();
        executor.shutdown();
        try (var scheduler = new StreamScheduler(context(s -> reader, s -> sink), "coordinator", executor)) {
            assertThatThrownBy(() -> scheduler.start(state)).isInstanceOf(RejectedExecutionException.class);
            assertThat(reader.closes.get()).isEqualTo(1);
            assertThat(sink.closes.get()).isEqualTo(1);
            assertThat(scheduler.activeCount()).isZero();
            assertThat(states.loadState(state.streamName()).orElseThrow().lifecycleState())
                    .isEqualTo(StreamLifecycleState.FAILED);
        }
    }

    @Test
    void aFactoryFinishingAfterLeadershipLossCannotReplaceTheNewAttempt() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CountDownLatch(1);
        var oldReader = new TrackingReader();
        var newReader = new TrackingReader();
        var created = new AtomicInteger();
        var executor = new QueuedExecutor();
        var caller = Executors.newSingleThreadExecutor();
        try (var scheduler = new StreamScheduler(context(s -> {
            if (created.getAndIncrement() == 0) {
                entered.countDown();
                await(release);
                return oldReader;
            }
            return newReader;
        }, s -> new TrackingSink()), "coordinator", executor)) {
            var first = caller.submit(() -> scheduler.start(state));
            assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
            scheduler.stopAll();
            scheduler.resume();
            scheduler.start(states.loadState(state.streamName()).orElseThrow());
            release.countDown();
            first.get(10, TimeUnit.SECONDS);
            assertThat(scheduler.isActive(state.streamName())).isTrue();
            assertThat(states.loadState(state.streamName()).orElseThrow().epoch()).isEqualTo(2);
            assertThat(oldReader.closes.get()).isEqualTo(1);
            assertThat(newReader.closes.get()).isZero();
            assertThat(executor.tasks).hasSize(1);
        } finally {
            release.countDown();
            caller.shutdownNow();
        }
        assertThat(newReader.closes.get()).isEqualTo(1);
    }

    @Test
    void runningPipelineClosesBothResourcesOnItsOwnThread() throws Exception {
        var reader = new TrackingReader();
        var sink = new TrackingSink();
        try (var scheduler = new StreamScheduler(context(s -> reader, s -> sink), "coordinator")) {
            scheduler.start(state);
            assertThat(reader.entered.await(10, TimeUnit.SECONDS)).isTrue();
            scheduler.stop(state.streamName());
            assertThat(sink.closed.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(reader.closeThread).isSameAs(reader.pollThread);
            assertThat(sink.closeThread).isSameAs(reader.pollThread);
            assertThat(reader.closes.get()).isEqualTo(1);
            assertThat(sink.closes.get()).isEqualTo(1);
        }
    }

    @Test
    void lateFailureAndCompletionOfStoppedAttemptCannotRemoveItsReplacement() throws Exception {
        var entered = new CountDownLatch(1);
        var release = new CompletableFuture<Void>();
        var newReader = new TrackingReader();
        var created = new AtomicInteger();
        var oldReader = new StreamSchedulerStopAllRaceTest.NoOpSourceReader() {
            @Override public List<SourceRecord> pollBatch(int max, Duration timeout) {
                entered.countDown();
                // Model an external client that finishes only after cancellation has returned.
                release.join();
                throw new IllegalStateException("old attempt failed late");
            }
        };
        try (var scheduler = new StreamScheduler(context(
                s -> created.getAndIncrement() == 0 ? oldReader : newReader,
                s -> new TrackingSink()), "coordinator", Executors.newSingleThreadExecutor())) {
            scheduler.start(state);
            assertThat(entered.await(10, TimeUnit.SECONDS)).isTrue();
            scheduler.stop(state.streamName());
            scheduler.start(states.loadState(state.streamName()).orElseThrow());
            release.complete(null);
            // The single executor thread cannot poll the replacement until the old run exits.
            assertThat(newReader.entered.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(scheduler.isActive(state.streamName())).isTrue();
            var current = states.loadState(state.streamName()).orElseThrow();
            assertThat(current.epoch()).isEqualTo(2);
            assertThat(current.lifecycleState()).isEqualTo(StreamLifecycleState.RUNNING);
            assertThat(current.errorMessage()).isNull();
        } finally {
            release.complete(null);
        }
    }

    @Test
    void suspendFencesTheRunningAttemptBeforeClosingItsResources() {
        verifyDdlFencesBeforeStop(false);
    }

    @Test
    void dropFencesTheRunningAttemptBeforeClosingItsResources() {
        verifyDdlFencesBeforeStop(true);
    }

    @Test
    void configurationChangeRestartsThePipelineAndFencesItsOldCallbacks() {
        var attempts = new ArrayList<StreamState>();
        var readers = List.of(new TrackingReader(), new TrackingReader());
        try (var scheduler = new StreamScheduler(context(claimed -> {
            attempts.add(claimed);
            return readers.get(attempts.size() - 1);
        }, s -> new TrackingSink()), "coordinator", new QueuedExecutor())) {
            scheduler.start(state);
            var ddl = new StreamDdlExecutor(new StreamDdlContext(states,
                    new JdbcStreamOffsetStore(dataSource), scheduler, StreamConfig.withDefaults(),
                    CdcSourceRegistry.discover(getClass().getClassLoader())));
            ddl.alter(new AlterStreamStatement(Identifier.of(state.streamName()),
                    new AlterStreamAction.SetProperties(Map.of("batch_size", "5"))), state.tenantId());
            assertThat(attempts).hasSize(2);
            assertThat(readers.get(0).closes.get()).isEqualTo(1);
            assertThat(attempts.get(1).streamProperties()).containsEntry("batch_size", "5");
            assertThat(attempts.get(1).epoch()).isEqualTo(3);
            assertThatThrownBy(() -> states.saveState(attempts.get(0).withError("old failure")))
                    .isInstanceOf(StaleEpochException.class);
            assertThat(states.loadState(state.streamName()).orElseThrow().streamProperties())
                    .containsEntry("batch_size", "5");
        }
    }

    private void verifyDdlFencesBeforeStop(boolean drop) {
        var closes = new AtomicInteger();
        try (var scheduler = new StreamScheduler(context(claimed ->
                new StreamSchedulerStopAllRaceTest.NoOpSourceReader() {
                    @Override public void close() {
                        closes.incrementAndGet();
                        var persisted = states.loadState(state.streamName()).orElseThrow();
                        assertThat(persisted.epoch()).isEqualTo(claimed.epoch() + 1);
                        assertThat(persisted.lifecycleState()).isEqualTo(drop
                                ? StreamLifecycleState.DROPPING : StreamLifecycleState.SUSPENDED);
                        assertThatThrownBy(() -> states.saveState(claimed.withError("late failure")))
                                .isInstanceOf(StaleEpochException.class);
                    }
                }, s -> new TrackingSink()), "coordinator", new QueuedExecutor())) {
            scheduler.start(state);
            var ddl = new StreamDdlExecutor(new StreamDdlContext(states,
                    new JdbcStreamOffsetStore(dataSource), scheduler, StreamConfig.withDefaults(),
                    CdcSourceRegistry.discover(getClass().getClassLoader())));
            if (drop) {
                ddl.drop(new DropStreamStatement(Identifier.of(state.streamName()), false), state.tenantId());
                assertThat(states.loadState(state.streamName())).isEmpty();
            } else {
                ddl.alter(new AlterStreamStatement(Identifier.of(state.streamName()),
                        new AlterStreamAction.Suspend()), state.tenantId());
                assertThat(states.loadState(state.streamName()).orElseThrow().lifecycleState())
                        .isEqualTo(StreamLifecycleState.SUSPENDED);
            }
            assertThat(closes.get()).isEqualTo(1);
            assertThat(scheduler.activeCount()).isZero();
        }
    }

    private StreamSchedulerContext context(Function<StreamState, CdcSourceReader> readers,
            Function<StreamState, CdcWriteSink> sinks) {
        return new StreamSchedulerContext(states, new JdbcStreamOffsetStore(dataSource),
                new TwoPhaseCommitCoordinator(new JdbcCdcCommitStateStore(dataSource)),
                new CdcRecordConverterRegistry(List.of(new StreamSchedulerStopAllRaceTest.NoOpConverter())),
                new WatermarkTracker(new StreamSchedulerStopAllRaceTest.InMemoryWatermarkStore()),
                maintenance, StreamConfig.withDefaults(), readers, sinks);
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("test barrier timed out");
            }
        } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(failure);
        }
    }

    private static final class TrackingReader extends StreamSchedulerStopAllRaceTest.NoOpSourceReader {
        final AtomicInteger polls = new AtomicInteger();
        final AtomicInteger closes = new AtomicInteger();
        final CountDownLatch entered = new CountDownLatch(1);
        volatile Thread pollThread;
        volatile Thread closeThread;
        @Override public List<SourceRecord> pollBatch(int max, Duration timeout) {
            polls.incrementAndGet();
            pollThread = Thread.currentThread();
            entered.countDown();
            await(new CountDownLatch(1));
            return List.of();
        }
        @Override public void close() {
            closeThread = Thread.currentThread();
            closes.incrementAndGet();
        }
    }

    private static final class TrackingSink extends StreamSchedulerStopAllRaceTest.NoOpWriteSink {
        final AtomicInteger closes = new AtomicInteger();
        final CountDownLatch closed = new CountDownLatch(1);
        volatile Thread closeThread;
        @Override public void close() {
            closeThread = Thread.currentThread();
            closes.incrementAndGet();
            closed.countDown();
        }
    }

    private static final class QueuedExecutor extends AbstractExecutorService {
        final List<Runnable> tasks = new ArrayList<>();
        private boolean shutdown;
        @Override public void execute(Runnable task) {
            if (shutdown) throw new RejectedExecutionException("test executor rejected");
            tasks.add(task);
        }
        void runQueued() { List.copyOf(tasks).forEach(Runnable::run); tasks.clear(); }
        @Override public void shutdown() { shutdown = true; }
        @Override public List<Runnable> shutdownNow() {
            shutdown = true;
            var abandoned = List.copyOf(tasks);
            tasks.clear();
            return abandoned;
        }
        @Override public boolean isShutdown() { return shutdown; }
        @Override public boolean isTerminated() { return shutdown && tasks.isEmpty(); }
        @Override public boolean awaitTermination(long timeout, TimeUnit unit) { return isTerminated(); }
    }
}
