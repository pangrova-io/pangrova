package org.pangrova.persistence.streaming.postgresql;

import org.pangrova.testing.testkit.constants.TestObservability;

import org.pangrova.common.concurrent.BackoffStrategy;
import org.pangrova.streaming.api.cdc.CdcEnvelope;
import org.pangrova.streaming.api.enums.CdcOperation;
import org.pangrova.streaming.api.cdc.CdcRecordConverter;
import org.pangrova.streaming.api.source.CdcSourceReader;
import org.pangrova.streaming.api.cdc.CdcWriteResult;
import org.pangrova.streaming.api.cdc.CdcWriteSink;
import org.pangrova.streaming.api.source.SourceRecord;
import org.pangrova.streaming.api.checkpoint.TopicPartitionOffset;
import org.pangrova.streaming.api.watermark.WatermarkStore;
import org.pangrova.streaming.cdc.CdcPipelineConfig;
import org.pangrova.streaming.cdc.CdcPipelineListener;
import org.pangrova.streaming.cdc.CdcPipelineLoop;
import org.pangrova.streaming.cdc.CdcPipelineLoopContext;
import org.pangrova.streaming.cdc.CdcPipelineRuntimeHooks;
import org.pangrova.streaming.cdc.CdcQuotaEnforcer;
import org.pangrova.streaming.commit.CdcCommitCoordinator;
import org.pangrova.streaming.api.enums.CdcCommitState;
import org.pangrova.persistence.streaming.postgresql.JdbcCdcCommitStateStore;
import org.pangrova.streaming.api.commit.OffsetRange;
import org.pangrova.streaming.api.commit.CdcBatchCheckpoint;
import org.pangrova.streaming.api.commit.PrepareToken;
import org.pangrova.streaming.api.commit.SourceId;
import org.pangrova.streaming.commit.TwoPhaseCommitCoordinator;
import org.pangrova.streaming.maintenance.CompactionResult;
import org.pangrova.streaming.maintenance.MaintenanceConfig;
import org.pangrova.streaming.maintenance.MaintenanceScheduler;
import org.pangrova.streaming.maintenance.MaintenanceStatus;
import org.pangrova.streaming.maintenance.OrphanCleanupResult;
import org.pangrova.streaming.maintenance.SnapshotExpirationResult;
import org.pangrova.streaming.maintenance.TableMaintenance;
import org.pangrova.streaming.watermark.WatermarkTracker;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for CDC checkpoint and replay behavior via the commit coordinator wiring
 * in {@link CdcPipelineLoop}.
 *
 * <p>Verifies:</p>
 * <ul>
 *   <li>Successful cycle: publication and durable checkpoint precede source acknowledgement</li>
 *   <li>Dedup on replay: re-polling the same offsets after a simulated crash skips the write</li>
 *   <li>Offset recovery from JDBC: pipeline seeks to last committed offsets on startup</li>
 *   <li>Publication failure remains ambiguous and cannot advance source offsets</li>
 *   <li>At-least-once fallback when no coordinator is provided</li>
 * </ul>
 */
@Execution(ExecutionMode.SAME_THREAD)
class CdcPipelineExactlyOnceTest extends org.pangrova.testing.testkit.ContainerTestBase {

    private static final String STREAM_NAME = "cdc-orders";
    private static final String TARGET_TABLE = "catalog.schema.orders";

    private static javax.sql.DataSource dataSource;
    private JdbcCdcCommitStateStore stateStore;
    private TwoPhaseCommitCoordinator coordinator;

    @BeforeAll
    static void initializeDatabase() throws Exception {
        dataSource = MigratedPostgres.create();
    }

    @BeforeEach
    void setUp() throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("TRUNCATE cdc_commit_state");
        }
        stateStore = new JdbcCdcCommitStateStore(dataSource);
        coordinator = new TwoPhaseCommitCoordinator(stateStore, Duration.ofMinutes(5));
    }

    private CdcPipelineConfig defaultConfig() {
        return new CdcPipelineConfig(
            STREAM_NAME, new SourceId(STREAM_NAME), TARGET_TABLE,
            100,
            Duration.ofMillis(50), 10, 3, 5, Duration.ofSeconds(5));
    }

    // --- Fakes ---

    static class FakeSourceReader implements CdcSourceReader {
        private final List<List<SourceRecord>> batches = new CopyOnWriteArrayList<>();
        private final AtomicInteger pollIndex = new AtomicInteger(0);
        final List<Map<TopicPartitionOffset, Long>> committedOffsets = new CopyOnWriteArrayList<>();
        final List<Map<TopicPartitionOffset, Long>> seekedOffsets = new CopyOnWriteArrayList<>();

        void addBatch(List<SourceRecord> batch) {
            batches.add(batch);
        }

        @Override
        public List<SourceRecord> pollBatch(int maxRecords, Duration timeout) {
            int idx = pollIndex.getAndIncrement();
            if (idx < batches.size()) {
                return batches.get(idx);
            }
            return List.of();
        }

        @Override
        public void commitOffsets(Map<TopicPartitionOffset, Long> offsets) {
            committedOffsets.add(Map.copyOf(offsets));
        }

        @Override
        public void seekToOffsets(Map<TopicPartitionOffset, Long> offsets) {
            seekedOffsets.add(Map.copyOf(offsets));
        }

        @Override
        public void close() {}
    }

    static class FakeConverter implements CdcRecordConverter {
        @Override
        public String formatId() { return "fake-json"; }

        @Override
        public CdcEnvelope deserialize(byte[] payload) {
            return new CdcEnvelope(
                CdcOperation.INSERT, TARGET_TABLE, null, null,
                Instant.now(), 1, Map.of());
        }

        @Override
        public List<CdcEnvelope> convertBatch(List<byte[]> payloads,
                                               BiConsumer<byte[], Exception> deadLetter) {
            List<CdcEnvelope> results = new ArrayList<>();
            for (byte[] p : payloads) {
                results.add(deserialize(p));
            }
            return results;
        }
    }

    static class FakeWriteSink implements CdcWriteSink {
        final AtomicInteger writeCount = new AtomicInteger(0);
        volatile boolean failOnWrite = false;

        @Override
        public CdcWriteResult writeBatch(List<CdcEnvelope> envelopes) {
            if (failOnWrite) {
                throw new RuntimeException("Simulated write failure");
            }
            writeCount.incrementAndGet();
            return new CdcWriteResult(envelopes.size(), envelopes.size() * 256L, 1, TARGET_TABLE);
        }

        @Override
        public void close() {}
    }

    static class FakeWatermarkStore implements WatermarkStore {
        @Override
        public Instant advance(String tableName, Instant watermark) { return watermark; }

        @Override
        public Optional<Instant> load(String tableName) { return Optional.empty(); }
    }

    static class StubMaintenanceSpi implements TableMaintenance {
        @Override
        public CompletableFuture<CompactionResult> compactAsync(String tableName) {
            return CompletableFuture.completedFuture(new CompactionResult(tableName, 0, 0, 0L));
        }

        @Override
        public CompletableFuture<OrphanCleanupResult> cleanOrphansAsync(String tableName, Duration olderThan) {
            return CompletableFuture.completedFuture(new OrphanCleanupResult(tableName, 0, 0L));
        }

        @Override
        public CompletableFuture<SnapshotExpirationResult> expireSnapshotsAsync(String tableName, Duration olderThan) {
            return CompletableFuture.completedFuture(new SnapshotExpirationResult(tableName, 0));
        }

        @Override
        public MaintenanceStatus getMaintenanceStatus(String tableName) {
            return new MaintenanceStatus(tableName, 0, 0, 0L, 0, 0, 0L, 0, null, null, null);
        }
    }

    private CdcPipelineLoopContext buildDeps(FakeSourceReader reader, FakeWriteSink writeSink,
                                              CdcPipelineListener listener,
                                              CdcCommitCoordinator commitCoordinator) {
        return new CdcPipelineLoopContext(
            reader,
            new FakeConverter(),
            writeSink,
            new CdcQuotaEnforcer(10, 1_000_000),
            new WatermarkTracker(new FakeWatermarkStore()),
            new MaintenanceScheduler(new StubMaintenanceSpi(), MaintenanceConfig.defaults()),
            new CdcPipelineRuntimeHooks(TestObservability.METRICS, listener, BackoffStrategy.retry(10, 100), commitCoordinator));
    }

    private SourceRecord record(String topic, int partition, long offset) {
        return new SourceRecord(topic, partition, offset,
            new byte[]{1}, new byte[]{2, 3}, System.currentTimeMillis());
    }

    // --- Tests ---

    @Test
    void exactlyOnceCycleWritesAndCommitsWithCoordinator() throws Exception {
        FakeSourceReader reader = new FakeSourceReader();
        reader.addBatch(List.of(record("t", 0, 0), record("t", 0, 1)));

        FakeWriteSink writeSink = new FakeWriteSink();

        CdcPipelineLoop loop = new CdcPipelineLoop(
            defaultConfig(),
            buildDeps(reader, writeSink, e -> {}, coordinator));

        Thread t = new Thread(loop);
        t.start();
        t.join(3000);
        loop.stop();
        t.join(500);

        // Write should have happened once
        assertEquals(1, writeSink.writeCount.get(), "should write one batch");
        // Offsets should be committed (by OffsetCommitParticipant or dedup path)
        assertFalse(reader.committedOffsets.isEmpty(), "offsets should be committed");

        // JDBC state store should have a COMMITTED record
        SourceId source = new SourceId(STREAM_NAME);
        Map<TopicPartitionOffset, Long> lastOffsets = stateStore.findLastCommittedOffsets(source);
        assertFalse(lastOffsets.isEmpty(), "JDBC should have committed offsets");
        // End offset for partition 0 should be 1 (offsets 0,1), next-to-consume = 2
        assertEquals(2L, lastOffsets.get(new TopicPartitionOffset("t", 0)), "JDBC should store endOffset+1");
    }

    @Test
    void replayAfterCrashSkipsWriteViaDedup() throws Exception {
        // Simulate: first run writes and commits to JDBC, but Kafka offset was NOT committed
        // (crash between JDBC COMMITTED and Kafka commit). On restart, same records are polled.

        // Phase 1: first run
        FakeSourceReader reader1 = new FakeSourceReader();
        reader1.addBatch(List.of(record("t", 0, 10), record("t", 0, 11)));

        FakeWriteSink writeSink1 = new FakeWriteSink();

        CdcPipelineLoop loop1 = new CdcPipelineLoop(
            defaultConfig(),
            buildDeps(reader1, writeSink1, e -> {}, coordinator));

        Thread t1 = new Thread(loop1);
        t1.start();
        t1.join(3000);
        loop1.stop();
        t1.join(500);

        assertEquals(1, writeSink1.writeCount.get(), "first run should write");

        // Phase 2: simulate restart — same offsets polled again (Kafka offset not committed)
        FakeSourceReader reader2 = new FakeSourceReader();
        reader2.addBatch(List.of(record("t", 0, 10), record("t", 0, 11)));

        FakeWriteSink writeSink2 = new FakeWriteSink();

        CdcPipelineLoop loop2 = new CdcPipelineLoop(
            defaultConfig(),
            buildDeps(reader2, writeSink2, e -> {}, coordinator));

        Thread t2 = new Thread(loop2);
        t2.start();
        t2.join(3000);
        loop2.stop();
        t2.join(500);

        // Second run should NOT write (dedup by coordinator)
        assertEquals(0, writeSink2.writeCount.get(),
            "replay should skip write due to dedup");
        // But offsets should still be committed to Kafka
        assertFalse(reader2.committedOffsets.isEmpty(),
            "replay should still commit Kafka offsets");
    }

    @Test
    void startupRestoresOffsetsFromJdbcStateStore() throws Exception {
        // Pre-populate JDBC with committed offsets
        SourceId source = new SourceId(STREAM_NAME);
        CdcBatchCheckpoint range = checkpoint(0, 0, 49);
        PrepareToken token = coordinator.prepare(source, range);
        assertTrue(stateStore.compareAndSetState(
                token.id(), CdcCommitState.PREPARED, CdcCommitState.COMMITTING));
        coordinator.commit(token);

        FakeSourceReader reader = new FakeSourceReader();
        // No batches — we just want to verify seek was called

        FakeWriteSink writeSink = new FakeWriteSink();

        // No explicit initial offsets — pipeline should query JDBC
        CdcPipelineLoop loop = new CdcPipelineLoop(
            defaultConfig(),
            buildDeps(reader, writeSink, e -> {}, coordinator));

        Thread t = new Thread(loop);
        t.start();
        Thread.sleep(200);
        loop.stop();
        t.join(2000);

        // Verify seek was called with JDBC offsets
        assertFalse(reader.seekedOffsets.isEmpty(),
            "pipeline should seek to JDBC-stored offsets on startup");
        Map<TopicPartitionOffset, Long> seeked = reader.seekedOffsets.get(0);
        // JDBC stores endOffset+1 = 50
        assertEquals(50L, seeked.get(new TopicPartitionOffset("orders", 0)),
            "should seek to endOffset+1 from JDBC");
    }

    @Test
    void writeFailureTriggersAbortAndRollback() throws Exception {
        FakeSourceReader reader = new FakeSourceReader();
        reader.addBatch(List.of(record("t", 0, 0)));
        reader.addBatch(List.of(record("t", 0, 0))); // retry same batch
        reader.addBatch(List.of(record("t", 0, 0))); // retry same batch

        FakeWriteSink writeSink = new FakeWriteSink();
        writeSink.failOnWrite = true;

        AtomicBoolean failedCalled = new AtomicBoolean(false);

        CdcPipelineConfig cfg = new CdcPipelineConfig(
            STREAM_NAME, new SourceId(STREAM_NAME), TARGET_TABLE,
            100,
            Duration.ofMillis(10), 5, 3, 3, Duration.ofSeconds(5));

        CdcPipelineLoop loop = new CdcPipelineLoop(
            cfg,
            buildDeps(reader, writeSink, e -> failedCalled.set(true), coordinator));

        Thread t = new Thread(loop);
        t.start();
        t.join(5000);

        // Write failed, so no offsets should be committed
        assertTrue(reader.committedOffsets.isEmpty(),
            "no offsets should be committed when write fails");

        // JDBC should NOT have any COMMITTED records
        SourceId source = new SourceId(STREAM_NAME);
        Map<TopicPartitionOffset, Long> lastOffsets = stateStore.findLastCommittedOffsets(source);
        assertTrue(lastOffsets.isEmpty(),
            "JDBC should not have committed records after write failure");
    }

    @Test
    void atLeastOnceFallbackWhenNoCoordinatorProvided() throws Exception {
        FakeSourceReader reader = new FakeSourceReader();
        reader.addBatch(List.of(record("t", 0, 0), record("t", 0, 1)));

        FakeWriteSink writeSink = new FakeWriteSink();

        // No coordinator — runtime hooks carry a null commit coordinator
        CdcPipelineLoopContext deps = new CdcPipelineLoopContext(
            reader,
            new FakeConverter(),
            writeSink,
            new CdcQuotaEnforcer(10, 1_000_000),
            new WatermarkTracker(new FakeWatermarkStore()),
            new MaintenanceScheduler(new StubMaintenanceSpi(), MaintenanceConfig.defaults()),
            new CdcPipelineRuntimeHooks(TestObservability.METRICS, e -> {}, BackoffStrategy.retry(10, 100), null));

        assertNull(deps.commitCoordinator(),
            "null coordinator in runtime hooks should surface as null commit coordinator");

        CdcPipelineLoop loop = new CdcPipelineLoop(defaultConfig(), deps);

        Thread t = new Thread(loop);
        t.start();
        t.join(3000);
        loop.stop();
        t.join(500);

        // Write and offset commit should work normally (at-least-once)
        assertEquals(1, writeSink.writeCount.get(), "should write one batch");
        assertFalse(reader.committedOffsets.isEmpty(), "should commit offsets");
    }

    @Test
    void acknowledgementFailureRetriesSameBatchAfterDurableCheckpointWithoutRepublishing() throws Exception {
        var acknowledged = new java.util.concurrent.CountDownLatch(1);
        var attempts = new AtomicInteger();
        var batch = List.of(record("orders", 0, 10), record("orders", 1, 20), record("payments", 0, 30));
        var expected = CdcBatchCheckpoint.fromRecords(batch).nextOffsets();
        var reader = new FakeSourceReader() {
            @Override
            public void commitOffsets(Map<TopicPartitionOffset, Long> offsets) {
                assertEquals(expected, coordinator.findLastCommittedOffsets(new SourceId(STREAM_NAME)),
                        "all offsets must already be durable before source acknowledgement");
                if (attempts.incrementAndGet() == 1) {
                    throw new IllegalStateException("source acknowledgement interrupted");
                }
                super.commitOffsets(offsets);
                acknowledged.countDown();
            }
        };
        reader.addBatch(batch);
        var sink = new FakeWriteSink();
        var loop = new CdcPipelineLoop(defaultConfig(), buildDeps(reader, sink, e -> {}, coordinator));
        Thread thread = new Thread(loop, "cdc-checkpoint-ack-test");
        thread.start();
        try {
            assertTrue(acknowledged.await(10, java.util.concurrent.TimeUnit.SECONDS));
        } finally {
            loop.stop();
            thread.join(5000);
        }
        assertFalse(thread.isAlive());
        assertEquals(1, sink.writeCount.get(), "acknowledgement retry must not republish rows");
        assertEquals(2, attempts.get());
        assertEquals(expected, new TwoPhaseCommitCoordinator(new JdbcCdcCommitStateStore(dataSource))
                .findLastCommittedOffsets(new SourceId(STREAM_NAME)));
    }

    @Test
    void failedWriteCannotBeSkippedByPollingTheNextBatch() throws Exception {
        var acknowledged = new java.util.concurrent.CountDownLatch(2);
        var reader = new FakeSourceReader() {
            @Override
            public void commitOffsets(Map<TopicPartitionOffset, Long> offsets) {
                super.commitOffsets(offsets);
                acknowledged.countDown();
            }
        };
        reader.addBatch(List.of(record("orders", 0, 10)));
        reader.addBatch(List.of(record("orders", 0, 11)));
        var attempts = new AtomicInteger();
        var sink = new FakeWriteSink() {
            @Override
            public CdcWriteSink.StagedWrite stage(List<CdcEnvelope> envelopes) {
                if (attempts.incrementAndGet() == 1) {
                    throw new IllegalStateException("transient staging failure");
                }
                return super.stage(envelopes);
            }
        };
        var loop = new CdcPipelineLoop(defaultConfig(), buildDeps(reader, sink, e -> {}, coordinator));
        Thread thread = new Thread(loop, "cdc-checkpoint-retry-test");
        thread.start();
        try {
            assertTrue(acknowledged.await(10, java.util.concurrent.TimeUnit.SECONDS));
        } finally {
            loop.stop();
            thread.join(5000);
        }
        assertFalse(thread.isAlive());
        assertEquals(3, attempts.get());
        assertEquals(2, sink.writeCount.get());
        assertEquals(List.of(Map.of(new TopicPartitionOffset("orders", 0), 11L),
                Map.of(new TopicPartitionOffset("orders", 0), 12L)), reader.committedOffsets);
    }

    @Test
    void startupUsesExactDurableTopicsInsteadOfStaleInitialOffsets() throws Exception {
        var checkpoint = new CdcBatchCheckpoint(List.of(new OffsetRange("orders", 0, 10, 19),
                new OffsetRange("payments", 0, 20, 29)));
        var token = coordinator.prepare(new SourceId(STREAM_NAME), checkpoint);
        assertTrue(stateStore.compareAndSetState(token.id(), CdcCommitState.PREPARED, CdcCommitState.COMMITTING));
        coordinator.commit(token);
        var restored = new java.util.concurrent.CountDownLatch(1);
        var reader = new FakeSourceReader() {
            @Override
            public void seekToOffsets(Map<TopicPartitionOffset, Long> offsets) {
                super.seekToOffsets(offsets);
                restored.countDown();
            }
        };
        var loop = new CdcPipelineLoop(defaultConfig(), buildDeps(reader, new FakeWriteSink(), e -> {}, coordinator),
                Map.of(new TopicPartitionOffset("orders", 0), 999L));
        Thread thread = new Thread(loop, "cdc-checkpoint-restore-test");
        thread.start();
        try {
            assertTrue(restored.await(10, java.util.concurrent.TimeUnit.SECONDS));
        } finally {
            loop.stop();
            thread.join(5000);
        }
        assertFalse(thread.isAlive());
        assertEquals(List.of(checkpoint.nextOffsets()), reader.seekedOffsets);
    }

    @Test
    void ambiguousRecoveryReportsFailureBeforePollingAndClosesTheReader() {
        var token = coordinator.prepare(new SourceId(STREAM_NAME), checkpoint(0, 10, 19));
        assertTrue(stateStore.compareAndSetState(token.id(), CdcCommitState.PREPARED, CdcCommitState.COMMITTING));
        var polled = new AtomicBoolean();
        var closed = new AtomicBoolean();
        var failure = new java.util.concurrent.atomic.AtomicReference<Exception>();
        var reader = new FakeSourceReader() {
            @Override
            public List<SourceRecord> pollBatch(int maxRecords, Duration timeout) {
                polled.set(true);
                return List.of();
            }

            @Override
            public void close() {
                closed.set(true);
            }
        };
        var loop = new CdcPipelineLoop(defaultConfig(), buildDeps(reader, new FakeWriteSink(),
                failure::set, coordinator));
        loop.run();
        assertFalse(loop.isRunning());
        assertFalse(polled.get());
        assertTrue(closed.get());
        assertNotNull(failure.get());
        assertTrue(failure.get().getMessage().contains("ambiguous"));
        assertTrue(coordinator.findLastCommittedOffsets(new SourceId(STREAM_NAME)).isEmpty());
    }

    private static CdcBatchCheckpoint checkpoint(int partition, long start, long end) {
        return new CdcBatchCheckpoint(List.of(new OffsetRange("orders", partition, start, end)));
    }

}
