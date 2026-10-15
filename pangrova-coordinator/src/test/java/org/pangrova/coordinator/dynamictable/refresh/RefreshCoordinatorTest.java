package org.pangrova.coordinator.dynamictable.refresh;

import org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease;
import org.pangrova.coordinator.dynamictable.DynamicTableConfig;
import org.pangrova.coordinator.dynamictable.DynamicTableRefreshDispatchFunction;
import org.pangrova.coordinator.dynamictable.NativeRefreshOutcome;
import org.pangrova.coordinator.dynamictable.detect.ChangeResult;
import org.pangrova.coordinator.dynamictable.detect.IncrementalScanPlan;
import org.pangrova.coordinator.dynamictable.detect.SchemaEvolutionException;
import org.pangrova.coordinator.dynamictable.eval.IncrementalResult;
import org.pangrova.coordinator.dynamictable.refresh.lock.RefreshLockManager;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshCoordinator;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshCoordinatorContext;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshLockCoordinator;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshPipeline;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshResult;
import org.pangrova.coordinator.dynamictable.write.DynamicTableWriter;
import org.pangrova.metadata.dynamictable.DynamicTableLifecycleState;
import org.pangrova.metadata.dynamictable.DynamicTableState;
import org.pangrova.metadata.dynamictable.InMemoryDynamicTableStateStore;
import org.pangrova.metadata.dynamictable.InMemoryRefreshHistoryStore;
import org.pangrova.metadata.dynamictable.RefreshHistoryEntry;
import org.pangrova.metadata.dynamictable.DynamicTableRefreshMode;
import org.pangrova.metadata.dynamictable.RefreshStatus;
import org.pangrova.metadata.dynamictable.TriggerType;
import org.pangrova.planner.cache.CachedResult;
import org.pangrova.planner.cache.L2CacheStore;
import org.pangrova.planner.cache.QueryResultCache;
import org.pangrova.spi.connector.data.write.model.TableFormat;
import org.pangrova.spi.connector.data.write.TableWriter;
import org.pangrova.spi.connector.data.write.model.WriteContext;
import org.pangrova.spi.connector.data.write.model.WriteMode;
import org.pangrova.spi.connector.data.write.model.WriteRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link RefreshCoordinator}.
 *
 * <p>Uses real {@link InMemoryDynamicTableStateStore} and
 * {@link InMemoryRefreshHistoryStore}, plus hand-written test
 * implementations for pipeline components — no mock frameworks.</p>
 *
 * <p><b>Validates: Requirements 3, 4, 7, 10, 13, 14, 18, 20, 23</b></p>
 */
class RefreshCoordinatorTest {

    private InMemoryDynamicTableStateStore stateStore;
    private InMemoryRefreshHistoryStore historyStore;
    private RefreshLockManager lockManager;
    private DynamicTableConfig config;

    @BeforeEach
    void setUp() {
        stateStore = new InMemoryDynamicTableStateStore();
        historyStore = new InMemoryRefreshHistoryStore();
        lockManager = new RefreshLockManager(new InMemoryDistributedLease());
        config = DynamicTableConfig.defaults();
    }

    // ── Normal refresh full flow ────────────────────────────────────

    @Test
    void normalRefresh_detectEvaluateWriteCommit() {
        DynamicTableState state = activeState("sales_summary");
        stateStore.saveState(state);

        RefreshCoordinator coordinator = buildCoordinator(
                snapshotPipeline(incrementalChanges(), evaluationWithRows(5)),
                recordingWriter(5));

        RefreshResult result = coordinator.executeRefresh(state, TriggerType.SCHEDULED);

        assertThat(result.status()).isEqualTo(RefreshStatus.SUCCESS);
        assertThat(result.processedRows()).isEqualTo(5);
        assertThat(result.bytesWritten()).isGreaterThan(0);
        assertThat(result.newCheckpoint()).isNotNull();
        assertThat(result.durationMs()).isGreaterThanOrEqualTo(0);
    }

    @Test
    void normalRefresh_commitsNewCheckpointToStateStore() {
        DynamicTableState state = activeState("orders");
        stateStore.saveState(state);

        RefreshCoordinator coordinator = buildCoordinator(
                snapshotPipeline(incrementalChanges(), evaluationWithRows(3)),
                recordingWriter(3));

        RefreshResult result = coordinator.executeRefresh(state, TriggerType.MANUAL);

        DynamicTableState committed = stateStore.loadState("orders").orElseThrow();
        assertThat(committed.checkpoint()).isEqualTo(result.newCheckpoint());
        assertThat(committed.consecutiveFailures()).isZero();
    }

    @Test
    void nativeRefresh_advancesCheckpointOnlyAfterConfirmedCommit() {
        DynamicTableState state = activeState("native_committed_dt");
        stateStore.saveState(state);
        AtomicInteger dispatches = new AtomicInteger();
        RefreshCoordinator coordinator = buildNativeCoordinator((current, checkpoint) -> {
            dispatches.incrementAndGet();
            return NativeRefreshOutcome.success("task-committed", 7, 896, checkpoint);
        });

        RefreshResult result = coordinator.executeRefresh(state, TriggerType.MANUAL);

        assertThat(result.status()).isEqualTo(RefreshStatus.SUCCESS);
        assertThat(result.processedRows()).isEqualTo(7);
        assertThat(result.bytesWritten()).isEqualTo(896);
        assertThat(result.newCheckpoint()).isEqualTo("200");
        assertThat(stateStore.loadState(state.dtName()).orElseThrow().checkpoint()).isEqualTo("200");
        assertThat(dispatches).hasValue(1);
    }

    @Test
    void nativeRefresh_failedCommitDoesNotAdvanceCheckpoint() {
        DynamicTableState state = activeState("native_failed_dt");
        stateStore.saveState(state);
        RefreshCoordinator coordinator = buildNativeCoordinator((current, checkpoint) ->
                NativeRefreshOutcome.failure("task-failed", "target commit failed"));

        RefreshResult result = coordinator.executeRefresh(state, TriggerType.MANUAL);

        assertThat(result.status()).isEqualTo(RefreshStatus.FAILURE);
        assertThat(result.newCheckpoint()).isNull();
        assertThat(stateStore.loadState(state.dtName()).orElseThrow().checkpoint()).isEqualTo("100");
    }

    @Test
    void nativeRefresh_wrongCheckpointDoesNotAdvanceCheckpoint() {
        DynamicTableState state = activeState("native_wrong_checkpoint_dt");
        stateStore.saveState(state);
        RefreshCoordinator coordinator = buildNativeCoordinator((current, checkpoint) ->
                NativeRefreshOutcome.success("task-wrong", 7, 896, "201"));

        RefreshResult result = coordinator.executeRefresh(state, TriggerType.MANUAL);

        assertThat(result.status()).isEqualTo(RefreshStatus.FAILURE);
        assertThat(stateStore.loadState(state.dtName()).orElseThrow().checkpoint()).isEqualTo("100");
    }

    @Test
    void nativeRefresh_missingCompletionDoesNotAdvanceCheckpoint() {
        DynamicTableState state = activeState("native_missing_completion_dt");
        stateStore.saveState(state);
        RefreshCoordinator coordinator = buildNativeCoordinator((current, checkpoint) -> null);

        RefreshResult result = coordinator.executeRefresh(state, TriggerType.MANUAL);

        assertThat(result.status()).isEqualTo(RefreshStatus.FAILURE);
        assertThat(stateStore.loadState(state.dtName()).orElseThrow().checkpoint()).isEqualTo("100");
    }

    // ── No-op skip ──────────────────────────────────────────────────

    @Test
    void noChangesDetected_returnsNoOp() {
        DynamicTableState state = activeState("empty_dt");
        stateStore.saveState(state);

        RefreshCoordinator coordinator = buildCoordinator(
                snapshotPipeline(ChangeResult.empty(), IncrementalResult.empty()),
                recordingWriter(0));

        RefreshResult result = coordinator.executeRefresh(state, TriggerType.SCHEDULED);

        assertThat(result.status()).isEqualTo(RefreshStatus.NO_OP);
        assertThat(result.processedRows()).isZero();
    }

    // ── Schema evolution suspend (R14) ──────────────────────────────

    @Test
    void schemaEvolution_suspendsState() {
        DynamicTableState state = activeState("evolving_dt");
        stateStore.saveState(state);

        var pipeline = new RefreshPipeline.SnapshotRefreshPipeline(
                new SchemaEvolutionDetector(),
                new FixedEvaluator(IncrementalResult.empty()));
        RefreshCoordinator coordinator = buildCoordinator(pipeline, recordingWriter(0));

        RefreshResult result = coordinator.executeRefresh(state, TriggerType.SCHEDULED);

        assertThat(result.status()).isEqualTo(RefreshStatus.FAILURE);
        DynamicTableState updated = stateStore.loadState("evolving_dt").orElseThrow();
        assertThat(updated.state()).isEqualTo(DynamicTableLifecycleState.SUSPENDED);
    }

    // ── Epoch expired reject (R13 AC4) ──────────────────────────────

    @Test
    void epochMismatch_rejectsRefresh() {
        DynamicTableState state = activeState("epoch_dt");
        // Save state with a different epoch to simulate leader change
        DynamicTableState staleState = new DynamicTableState(
                "epoch_dt", "tenant-1", DynamicTableLifecycleState.ACTIVE, DynamicTableRefreshMode.SNAPSHOT,
                Duration.ofMinutes(5), "100", "SNAPSHOT", 1, null,
                "admin", "coord-1", 99L, null, 0, 0,
                Instant.now(), Instant.now());
        stateStore.saveState(staleState);

        RefreshCoordinator coordinator = buildCoordinator(
                snapshotPipeline(incrementalChanges(), evaluationWithRows(2)),
                recordingWriter(2));

        RefreshResult result = coordinator.executeRefresh(state, TriggerType.SCHEDULED);

        assertThat(result.status()).isEqualTo(RefreshStatus.FAILURE);
    }

    // ── CDC detect failure (R20 AC4) ────────────────────────────────

    @Test
    void cdcDetectFailure_recordsFailureAndIncrementsConsecutiveFailures() {
        DynamicTableState state = cdcState("cdc_dt");
        stateStore.saveState(state);

        var pipeline = new RefreshPipeline.CdcRefreshPipeline(
                new FailingDetector(),
                new FixedEvaluator(IncrementalResult.empty()));
        RefreshCoordinator coordinator = buildCoordinator(pipeline, recordingWriter(0));

        RefreshResult result = coordinator.executeRefresh(state, TriggerType.SCHEDULED);

        assertThat(result.status()).isEqualTo(RefreshStatus.FAILURE);
        DynamicTableState updated = stateStore.loadState("cdc_dt").orElseThrow();
        assertThat(updated.consecutiveFailures()).isEqualTo(1);
    }

    // ── History recording (R23) ─────────────────────────────────────

    @Test
    void successfulRefresh_recordsHistory() {
        DynamicTableState state = activeState("history_dt");
        stateStore.saveState(state);

        RefreshCoordinator coordinator = buildCoordinator(
                snapshotPipeline(incrementalChanges(), evaluationWithRows(10)),
                recordingWriter(10));

        coordinator.executeRefresh(state, TriggerType.MANUAL);

        List<RefreshHistoryEntry> history = historyStore.query(
                "history_dt", Instant.EPOCH, Instant.now().plusSeconds(60), 10);
        assertThat(history).hasSize(1);
        RefreshHistoryEntry entry = history.get(0);
        assertThat(entry.dtName()).isEqualTo("history_dt");
        assertThat(entry.status()).isEqualTo(RefreshStatus.SUCCESS);
        assertThat(entry.triggerType()).isEqualTo(TriggerType.MANUAL);
        assertThat(entry.processedRows()).isEqualTo(10);
    }

    @Test
    void noOpRefresh_recordsHistoryWithNoOpStatus() {
        DynamicTableState state = activeState("noop_history");
        stateStore.saveState(state);

        RefreshCoordinator coordinator = buildCoordinator(
                snapshotPipeline(ChangeResult.empty(), IncrementalResult.empty()),
                recordingWriter(0));

        coordinator.executeRefresh(state, TriggerType.SCHEDULED);

        List<RefreshHistoryEntry> history = historyStore.query(
                "noop_history", Instant.EPOCH, Instant.now().plusSeconds(60), 10);
        assertThat(history).hasSize(1);
        assertThat(history.get(0).status()).isEqualTo(RefreshStatus.NO_OP);
    }

    @Test
    void failedRefresh_recordsHistoryWithFailureStatus() {
        DynamicTableState state = activeState("fail_history");
        stateStore.saveState(state);

        var pipeline = new RefreshPipeline.SnapshotRefreshPipeline(
                new FailingDetector(),
                new FixedEvaluator(IncrementalResult.empty()));
        RefreshCoordinator coordinator = buildCoordinator(pipeline, recordingWriter(0));

        coordinator.executeRefresh(state, TriggerType.SCHEDULED);

        List<RefreshHistoryEntry> history = historyStore.query(
                "fail_history", Instant.EPOCH, Instant.now().plusSeconds(60), 10);
        assertThat(history).hasSize(1);
        assertThat(history.get(0).status()).isEqualTo(RefreshStatus.FAILURE);
    }

    // ── Lock contention ─────────────────────────────────────────────

    @Test
    void lockAlreadyHeld_returnsNoOp() {
        DynamicTableState state = activeState("locked_dt");
        stateStore.saveState(state);
        lockManager.tryAcquire("locked_dt", "other-coord", Duration.ofMinutes(10));

        RefreshCoordinator coordinator = buildCoordinator(
                snapshotPipeline(incrementalChanges(), evaluationWithRows(5)),
                recordingWriter(5));

        RefreshResult result = coordinator.executeRefresh(state, TriggerType.SCHEDULED);

        assertThat(result.status()).isEqualTo(RefreshStatus.NO_OP);
    }

    // ── BUG-D03: Lock renewal during execution ─────────────────────

    @Test
    void longRefresh_renewsLockPeriodically() throws Exception {
        DynamicTableState state = activeState("renewal_dt");
        stateStore.saveState(state);

        // Use short config: targetLag=300ms, multiplier=3 → lockTimeout=900ms,
        // renewal interval=300ms.
        DynamicTableConfig fastConfig = new DynamicTableConfig(
                4, 2, 100, Duration.ofMillis(300), 1000L, 1000L, 300_000L,
                5, 10, 3.0, 300_000L, 1000, 30, 20);

        AtomicInteger renewalCount = new AtomicInteger(0);
        CountDownLatch renewalFired = new CountDownLatch(1);

        // Delegating lease that counts renewals
        var delegate = new org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease();
        var countingLease = new CountingRenewalLease(delegate, renewalCount, renewalFired);
        var countingLockManager = new RefreshLockManager(countingLease);

        // Slow detector that waits for at least one renewal to fire
        var slowDetector = new SlowDetector(renewalFired);

        var pipeline = new RefreshPipeline.SnapshotRefreshPipeline(
                slowDetector, new FixedEvaluator(evaluationWithRows(2)));

        ScheduledExecutorService scheduler = createTestScheduler();
        try {
            var deps = new RefreshCoordinatorContext(stateStore, historyStore, recordingWriter(2));
            var coordinator = new RefreshCoordinator(deps, pipeline, new RefreshLockCoordinator(countingLockManager, scheduler), fastConfig);

            DynamicTableState fastState = new DynamicTableState(
                    "renewal_dt", "tenant-1", DynamicTableLifecycleState.ACTIVE,
                    DynamicTableRefreshMode.SNAPSHOT, Duration.ofMillis(300),
                    "100", "SNAPSHOT", 1, null, "admin", "coord-1", 1L,
                    null, 0, 0, Instant.now(), Instant.now());
            stateStore.saveState(fastState);

            RefreshResult result = coordinator.executeRefresh(fastState, TriggerType.SCHEDULED);

            assertThat(result.status()).isEqualTo(RefreshStatus.SUCCESS);
            assertThat(renewalCount.get()).isGreaterThanOrEqualTo(1);
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void lockRenewalFails_abortsBeforeWrite() {
        DynamicTableState state = activeState("abort_dt");
        stateStore.saveState(state);

        // Lease where renewal always fails
        var delegate = new org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease();
        var failingLease = new FailingRenewalLease(delegate);
        var failingRenewalLockManager = new RefreshLockManager(failingLease);

        // Track whether writer.write() was called
        AtomicBoolean writeCalled = new AtomicBoolean(false);
        var trackingWriter = new DynamicTableWriter(
                new WriteTrackingTableWriter(writeCalled), noOpCache());

        // Use very short config so renewal fires quickly
        DynamicTableConfig fastConfig = new DynamicTableConfig(
                4, 2, 100, Duration.ofMillis(100), 1000L, 1000L, 300_000L,
                5, 10, 3.0, 300_000L, 1000, 30, 20);

        // Slow detector that gives time for renewal to fail
        var slowDetector = new SleepingDetector(200);

        var pipeline = new RefreshPipeline.SnapshotRefreshPipeline(
                slowDetector, new FixedEvaluator(evaluationWithRows(3)));

        ScheduledExecutorService scheduler = createTestScheduler();
        try {
            var deps = new RefreshCoordinatorContext(stateStore, historyStore, trackingWriter);
            var coordinator = new RefreshCoordinator(deps, pipeline, new RefreshLockCoordinator(failingRenewalLockManager, scheduler), fastConfig);

            DynamicTableState fastState = new DynamicTableState(
                    "abort_dt", "tenant-1", DynamicTableLifecycleState.ACTIVE,
                    DynamicTableRefreshMode.SNAPSHOT, Duration.ofMillis(100),
                    "100", "SNAPSHOT", 1, null, "admin", "coord-1", 1L,
                    null, 0, 0, Instant.now(), Instant.now());
            stateStore.saveState(fastState);

            RefreshResult result = coordinator.executeRefresh(fastState, TriggerType.SCHEDULED);

            assertThat(result.status()).isEqualTo(RefreshStatus.FAILURE);
            assertThat(writeCalled.get()).isFalse();
        } finally {
            scheduler.shutdownNow();
        }
    }

    @Test
    void renewalFutureCancelledAfterRefreshCompletes() throws Exception {
        DynamicTableState state = activeState("cancel_renewal_dt");
        stateStore.saveState(state);

        AtomicInteger renewalCount = new AtomicInteger(0);
        var delegate = new org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease();
        var countingLease = new CountingRenewalLease(delegate, renewalCount, new CountDownLatch(1));
        var countingLockManager = new RefreshLockManager(countingLease);

        ScheduledExecutorService scheduler = createTestScheduler();
        try {
            var deps = new RefreshCoordinatorContext(stateStore, historyStore, recordingWriter(2));
            var coordinator = new RefreshCoordinator(deps,
                    snapshotPipeline(incrementalChanges(), evaluationWithRows(2)),
                    new RefreshLockCoordinator(countingLockManager, scheduler), config);

            coordinator.executeRefresh(state, TriggerType.SCHEDULED);

            // After refresh completes, wait a bit and verify no more renewals fire
            int countAfterRefresh = renewalCount.get();
            Thread.sleep(200);
            assertThat(renewalCount.get()).isEqualTo(countAfterRefresh);
        } finally {
            scheduler.shutdownNow();
        }
    }

    private static ScheduledExecutorService createTestScheduler() {
        var scheduler = new ScheduledThreadPoolExecutor(1, r -> {
            Thread t = new Thread(r, "test-renewal-scheduler");
            t.setDaemon(true);
            return t;
        });
        scheduler.setRemoveOnCancelPolicy(true);
        return scheduler;
    }

    // ── Write failure increments consecutive failures ────────────────

    @Test
    void writeFailure_incrementsConsecutiveFailures() {
        DynamicTableState state = activeState("write_fail_dt");
        stateStore.saveState(state);

        RefreshCoordinator coordinator = buildCoordinator(
                snapshotPipeline(incrementalChanges(), evaluationWithRows(3)),
                failingWriter());

        RefreshResult result = coordinator.executeRefresh(state, TriggerType.SCHEDULED);

        assertThat(result.status()).isEqualTo(RefreshStatus.FAILURE);
        DynamicTableState updated = stateStore.loadState("write_fail_dt").orElseThrow();
        assertThat(updated.consecutiveFailures()).isEqualTo(1);
    }

    // ── Cascade trigger type ────────────────────────────────────────

    @Test
    void cascadeTrigger_recordedInHistory() {
        DynamicTableState state = activeState("cascade_dt");
        stateStore.saveState(state);

        RefreshCoordinator coordinator = buildCoordinator(
                snapshotPipeline(incrementalChanges(), evaluationWithRows(1)),
                recordingWriter(1));

        coordinator.executeRefresh(state, TriggerType.CASCADE);

        List<RefreshHistoryEntry> history = historyStore.query(
                "cascade_dt", Instant.EPOCH, Instant.now().plusSeconds(60), 10);
        assertThat(history).hasSize(1);
        assertThat(history.get(0).triggerType()).isEqualTo(TriggerType.CASCADE);
    }

    // ── Helper: build coordinator ───────────────────────────────────

    private RefreshCoordinator buildCoordinator(RefreshPipeline pipeline,
                                                DynamicTableWriter writer) {
        var deps = new RefreshCoordinatorContext(stateStore, historyStore, writer);
        return new RefreshCoordinator(deps, pipeline, new RefreshLockCoordinator(lockManager), config);
    }

    private RefreshCoordinator buildNativeCoordinator(DynamicTableRefreshDispatchFunction dispatch) {
        var deps = new RefreshCoordinatorContext(stateStore, historyStore,
                failingWriter(), null, null, null, dispatch);
        return new RefreshCoordinator(deps,
                snapshotPipeline(incrementalChanges(), IncrementalResult.empty()),
                new RefreshLockCoordinator(lockManager), config);
    }

    // ── Helper: state factories ─────────────────────────────────────

    private static DynamicTableState activeState(String dtName) {
        return new DynamicTableState(
                dtName, "tenant-1", DynamicTableLifecycleState.ACTIVE, DynamicTableRefreshMode.SNAPSHOT,
                Duration.ofMinutes(5), "100", "SNAPSHOT", 1, null,
                "admin", "coord-1", 1L, null, 0, 0,
                Instant.now(), Instant.now());
    }

    private static DynamicTableState cdcState(String dtName) {
        return new DynamicTableState(
                dtName, "tenant-1", DynamicTableLifecycleState.ACTIVE, DynamicTableRefreshMode.CDC,
                Duration.ofMinutes(5), "offset-100", "CDC_OFFSET", 1, null,
                "admin", "coord-1", 1L, null, 0, 0,
                Instant.now(), Instant.now());
    }

    // ── Helper: pipeline factories ──────────────────────────────────

    private static RefreshPipeline.SnapshotRefreshPipeline snapshotPipeline(
            ChangeResult changes, IncrementalResult evalResult) {
        return new RefreshPipeline.SnapshotRefreshPipeline(
                new FixedDetector(changes), new FixedEvaluator(evalResult));
    }

    private static ChangeResult incrementalChanges() {
        return ChangeResult.incremental(
                new IncrementalScanPlan(List.of("file1.parquet"), List.of(), 100, 200));
    }

    private static IncrementalResult evaluationWithRows(int rows) {
        List<Map<String, Object>> records = new ArrayList<>();
        for (int i = 0; i < rows; i++) {
            records.add(Map.of("id", i, "value", i * 10));
        }
        return new IncrementalResult(records, rows, 0);
    }

    // ── Helper: writer factories ────────────────────────────────────

    private static DynamicTableWriter recordingWriter(int expectedRows) {
        return new DynamicTableWriter(
                new RecordingTableWriter(expectedRows), noOpCache());
    }

    private static DynamicTableWriter failingWriter() {
        return new DynamicTableWriter(new FailingTableWriter(), noOpCache());
    }

    private static QueryResultCache noOpCache() {
        return new QueryResultCache(new NoOpL2CacheStore());
    }

    // ── Test implementations ────────────────────────────────────────

    /** Detector that returns a fixed ChangeResult. */
    static final class FixedDetector implements org.pangrova.coordinator.dynamictable.detect.ChangeDetector {
        private final ChangeResult result;

        FixedDetector(ChangeResult result) {
            this.result = result;
        }

        @Override
        public ChangeResult detect(DynamicTableState state) {
            return result;
        }
    }

    /** Detector that throws SchemaEvolutionException. */
    static final class SchemaEvolutionDetector
            implements org.pangrova.coordinator.dynamictable.detect.ChangeDetector {
        @Override
        public ChangeResult detect(DynamicTableState state) {
            throw new SchemaEvolutionException(1, 2, state.dtName());
        }
    }

    /** Detector that always fails with a RuntimeException. */
    static final class FailingDetector
            implements org.pangrova.coordinator.dynamictable.detect.ChangeDetector {
        @Override
        public ChangeResult detect(DynamicTableState state) {
            throw new RuntimeException("CDC connection failed");
        }
    }

    /** Evaluator that returns a fixed IncrementalResult. */
    static final class FixedEvaluator
            implements org.pangrova.coordinator.dynamictable.eval.IncrementalEvaluator {
        private final IncrementalResult result;

        FixedEvaluator(IncrementalResult result) {
            this.result = result;
        }

        @Override
        public IncrementalResult evaluate(DynamicTableState state, ChangeResult changes) {
            return result;
        }
    }

    /** TableWriter that records calls and returns a fixed result. */
    static final class RecordingTableWriter implements TableWriter {
        private final int rowCount;

        RecordingTableWriter(int rowCount) {
            this.rowCount = rowCount;
        }

        @Override
        public org.pangrova.spi.connector.data.write.model.WriteResult write(
                String tableName, List<WriteRecord> records,
                WriteMode writeMode, WriteContext context) {
            return new org.pangrova.spi.connector.data.write.model.WriteResult(
                    tableName, rowCount, 1, 0, 0, Instant.now());
        }

        @Override
        public Optional<String> readCommitProperty(String tableName, String key) {
            return Optional.empty();
        }

        @Override
        public TableFormat format() {
            return TableFormat.ICEBERG;
        }

        @Override
        public void close() {}
    }

    /** TableWriter that always throws. */
    static final class FailingTableWriter implements TableWriter {
        @Override
        public org.pangrova.spi.connector.data.write.model.WriteResult write(
                String tableName, List<WriteRecord> records,
                WriteMode writeMode, WriteContext context) {
            throw new RuntimeException("write I/O error");
        }

        @Override
        public Optional<String> readCommitProperty(String tableName, String key) {
            return Optional.empty();
        }

        @Override
        public TableFormat format() {
            return TableFormat.ICEBERG;
        }

        @Override
        public void close() {}
    }

    /** No-op L2CacheStore for test construction. */
    static final class NoOpL2CacheStore implements L2CacheStore {
        @Override
        public Optional<CachedResult> get(String key) {
            return Optional.empty();
        }

        @Override
        public void put(String key, CachedResult value) {}

        @Override
        public void remove(String key) {}

        @Override
        public void removeByTable(String tableName) {}

        @Override
        public void removeByUser(String userId) {}
    }

    // ── BUG-D03 test doubles ────────────────────────────────────────

    /** Delegating lease that counts successful renewals. */
    static final class CountingRenewalLease implements org.pangrova.spi.coordination.DistributedLease {
        private final org.pangrova.spi.coordination.DistributedLease delegate;
        private final AtomicInteger renewalCount;
        private final CountDownLatch renewalFired;

        CountingRenewalLease(org.pangrova.spi.coordination.DistributedLease delegate,
                             AtomicInteger renewalCount, CountDownLatch renewalFired) {
            this.delegate = delegate;
            this.renewalCount = renewalCount;
            this.renewalFired = renewalFired;
        }

        @Override
        public Optional<org.pangrova.spi.coordination.LeaseEntry> tryAcquire(
                String leaseKey, String holderId, Duration duration) {
            return delegate.tryAcquire(leaseKey, holderId, duration);
        }

        @Override
        public boolean tryRenew(
                String leaseKey, String holderId, long epoch, Duration duration) {
            boolean result = delegate.tryRenew(leaseKey, holderId, epoch, duration);
            if (result) {
                renewalCount.incrementAndGet();
                renewalFired.countDown();
            }
            return result;
        }

        @Override
        public boolean release(String leaseKey, String holderId, long epoch) {
            return delegate.release(leaseKey, holderId, epoch);
        }

        @Override
        public Optional<org.pangrova.spi.coordination.LeaseEntry> currentHolder(String leaseKey) {
            return delegate.currentHolder(leaseKey);
        }

    }

    /** Delegating lease where tryRenew always returns false. */
    static final class FailingRenewalLease implements org.pangrova.spi.coordination.DistributedLease {
        private final org.pangrova.spi.coordination.DistributedLease delegate;

        FailingRenewalLease(org.pangrova.spi.coordination.DistributedLease delegate) {
            this.delegate = delegate;
        }

        @Override
        public Optional<org.pangrova.spi.coordination.LeaseEntry> tryAcquire(
                String leaseKey, String holderId, Duration duration) {
            return delegate.tryAcquire(leaseKey, holderId, duration);
        }

        @Override
        public boolean tryRenew(
                String leaseKey, String holderId, long epoch, Duration duration) {
            return false;
        }

        @Override
        public boolean release(String leaseKey, String holderId, long epoch) {
            return delegate.release(leaseKey, holderId, epoch);
        }

        @Override
        public Optional<org.pangrova.spi.coordination.LeaseEntry> currentHolder(String leaseKey) {
            return delegate.currentHolder(leaseKey);
        }

    }

    /** Detector that waits for a latch before returning changes. */
    static final class SlowDetector implements org.pangrova.coordinator.dynamictable.detect.ChangeDetector {
        private final CountDownLatch latch;

        SlowDetector(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public ChangeResult detect(DynamicTableState state) {
            try {
                latch.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return ChangeResult.incremental(
                    new IncrementalScanPlan(List.of("file1.parquet"), List.of(), 100, 200));
        }
    }

    /** Detector that sleeps for a fixed duration before returning changes. */
    static final class SleepingDetector implements org.pangrova.coordinator.dynamictable.detect.ChangeDetector {
        private final long sleepMs;

        SleepingDetector(long sleepMs) {
            this.sleepMs = sleepMs;
        }

        @Override
        public ChangeResult detect(DynamicTableState state) {
            try {
                Thread.sleep(sleepMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return ChangeResult.incremental(
                    new IncrementalScanPlan(List.of("file1.parquet"), List.of(), 100, 200));
        }
    }

    /** TableWriter that tracks whether write() was called. */
    static final class WriteTrackingTableWriter implements TableWriter {
        private final AtomicBoolean writeCalled;

        WriteTrackingTableWriter(AtomicBoolean writeCalled) {
            this.writeCalled = writeCalled;
        }

        @Override
        public org.pangrova.spi.connector.data.write.model.WriteResult write(
                String tableName, List<WriteRecord> records,
                WriteMode writeMode, WriteContext context) {
            writeCalled.set(true);
            return new org.pangrova.spi.connector.data.write.model.WriteResult(
                    tableName, records.size(), 1, 0, 0, Instant.now());
        }

        @Override
        public Optional<String> readCommitProperty(String tableName, String key) {
            return Optional.empty();
        }

        @Override
        public TableFormat format() {
            return TableFormat.ICEBERG;
        }

        @Override
        public void close() {}
    }
}
