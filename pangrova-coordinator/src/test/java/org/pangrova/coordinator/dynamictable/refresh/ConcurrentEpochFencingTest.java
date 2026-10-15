package org.pangrova.coordinator.dynamictable.refresh;

import org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease;
import org.pangrova.coordinator.dynamictable.DynamicTableConfig;
import org.pangrova.coordinator.dynamictable.detect.ChangeDetector;
import org.pangrova.coordinator.dynamictable.detect.ChangeResult;
import org.pangrova.coordinator.dynamictable.detect.IncrementalScanPlan;
import org.pangrova.coordinator.dynamictable.eval.IncrementalEvaluator;
import org.pangrova.coordinator.dynamictable.eval.IncrementalResult;
import org.pangrova.coordinator.dynamictable.refresh.lock.RefreshLockManager;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshCoordinator;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshCoordinatorContext;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshLockCoordinator;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshPipeline;
import org.pangrova.coordinator.dynamictable.refresh.pipeline.RefreshResult;
import org.pangrova.coordinator.dynamictable.write.DynamicTableWriter;
import org.pangrova.metadata.dynamictable.DynamicTableLifecycleState;
import org.pangrova.metadata.dynamictable.DynamicTableRefreshMode;
import org.pangrova.metadata.dynamictable.DynamicTableState;
import org.pangrova.metadata.dynamictable.InMemoryDynamicTableStateStore;
import org.pangrova.metadata.dynamictable.InMemoryRefreshHistoryStore;
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
import org.pangrova.spi.connector.data.write.model.WriteResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies epoch-based fencing prevents two coordinators from concurrently
 * committing refresh results for the same Dynamic Table.
 *
 * <p>Uses real {@link InMemoryDynamicTableStateStore} with CAS-enabled
 * {@code saveStateIfEpoch}. No mock frameworks.</p>
 */
class ConcurrentEpochFencingTest {

    private InMemoryDynamicTableStateStore stateStore;
    private DynamicTableConfig config;

    @BeforeEach
    void setUp() {
        stateStore = new InMemoryDynamicTableStateStore();
        config = DynamicTableConfig.defaults();
    }

    @Test
    void twoCoordinators_onlyOneCommitsSuccessfully() throws Exception {
        DynamicTableState state = activeState("shared_dt", 1L);
        stateStore.saveState(state);

        // Both coordinators read the same state snapshot (epoch=1)
        DynamicTableState stateA = stateStore.loadState("shared_dt").orElseThrow();

        // Coordinator B represents a new leader after failover — it bumps the epoch
        DynamicTableState stateB = stateA.toBuilder().epoch(2L).updatedAt(Instant.now()).build();

        CountDownLatch writerAStarted = new CountDownLatch(1);
        CountDownLatch writerACanFinish = new CountDownLatch(1);

        // Coordinator A: old leader, slow writer (epoch=1)
        RefreshCoordinator coordA = buildCoordinator(
                snapshotPipeline(incrementalChanges(), evaluationWithRows(5)),
                writerWith(new SlowTableWriter(3, writerAStarted, writerACanFinish)));

        // Coordinator B: new leader, fast writer (epoch=2)
        RefreshCoordinator coordB = buildCoordinator(
                snapshotPipeline(incrementalChanges(), evaluationWithRows(3)),
                writerWith(new CountingTableWriter(3)));

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            // Start old leader A (will block in writer)
            Future<RefreshResult> futureA = executor.submit(
                    () -> coordA.executeRefresh(stateA, TriggerType.SCHEDULED));

            assertThat(writerAStarted.await(5, TimeUnit.SECONDS)).isTrue();

            // New leader B saves its epoch=2 state and runs refresh
            stateStore.saveState(stateB);
            Future<RefreshResult> futureB = executor.submit(
                    () -> coordB.executeRefresh(stateB, TriggerType.SCHEDULED));
            RefreshResult resultB = futureB.get(5, TimeUnit.SECONDS);

            // Now let A finish — its commit should fail (epoch changed from 1 to 2)
            writerACanFinish.countDown();
            RefreshResult resultA = futureA.get(5, TimeUnit.SECONDS);

            // B (new leader) succeeds, A (old leader) fails
            assertThat(resultB.status()).isEqualTo(RefreshStatus.SUCCESS);
            assertThat(resultA.status()).isEqualTo(RefreshStatus.FAILURE);

            DynamicTableState finalState = stateStore.loadState("shared_dt").orElseThrow();
            assertThat(finalState.epoch()).isEqualTo(2L);
            assertThat(finalState.totalRefreshCount()).isEqualTo(1);
        } finally {
            writerACanFinish.countDown();
            executor.shutdownNow();
            executor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void staleEpoch_secondCoordinatorGetsFailure() {
        DynamicTableState state = activeState("fenced_dt", 1L);
        stateStore.saveState(state);

        DynamicTableState snapshot = stateStore.loadState("fenced_dt").orElseThrow();

        // Another coordinator advances the epoch
        DynamicTableState advanced = snapshot.toBuilder().epoch(2L).updatedAt(Instant.now()).build();
        stateStore.saveState(advanced);

        RefreshCoordinator coordinator = buildCoordinator(
                snapshotPipeline(incrementalChanges(), evaluationWithRows(3)),
                writerWith(new CountingTableWriter(3)));

        RefreshResult result = coordinator.executeRefresh(snapshot, TriggerType.SCHEDULED);

        assertThat(result.status()).isEqualTo(RefreshStatus.FAILURE);
    }

    @Test
    void sameEpoch_commitSucceeds() {
        DynamicTableState state = activeState("ok_dt", 1L);
        stateStore.saveState(state);

        DynamicTableState snapshot = stateStore.loadState("ok_dt").orElseThrow();

        RefreshCoordinator coordinator = buildCoordinator(
                snapshotPipeline(incrementalChanges(), evaluationWithRows(5)),
                writerWith(new CountingTableWriter(5)));

        RefreshResult result = coordinator.executeRefresh(snapshot, TriggerType.SCHEDULED);

        assertThat(result.status()).isEqualTo(RefreshStatus.SUCCESS);
        assertThat(result.processedRows()).isEqualTo(5);

        DynamicTableState committed = stateStore.loadState("ok_dt").orElseThrow();
        assertThat(committed.totalRefreshCount()).isEqualTo(1);
    }

    // ── Helpers ─────────────────────────────────────────────────────

    private RefreshCoordinator buildCoordinator(RefreshPipeline pipeline,
                                                DynamicTableWriter writer) {
        var deps = new RefreshCoordinatorContext(
                stateStore, new InMemoryRefreshHistoryStore(), writer);
        return new RefreshCoordinator(deps, pipeline, new RefreshLockCoordinator(new RefreshLockManager(new InMemoryDistributedLease())), config);
    }

    private static DynamicTableState activeState(String dtName, long epoch) {
        return new DynamicTableState(
                dtName, "tenant-1", DynamicTableLifecycleState.ACTIVE,
                DynamicTableRefreshMode.SNAPSHOT,
                Duration.ofMinutes(5), "100", "SNAPSHOT", 1, null,
                "admin", "coord-1", epoch, null, 0, 0,
                Instant.now(), Instant.now());
    }

    private static RefreshPipeline.SnapshotRefreshPipeline snapshotPipeline(
            ChangeResult changes, IncrementalResult evalResult) {
        return new RefreshPipeline.SnapshotRefreshPipeline(
                state -> changes, (state, ch) -> evalResult);
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

    private static DynamicTableWriter writerWith(TableWriter tableWriter) {
        return new DynamicTableWriter(tableWriter, noOpCache());
    }

    private static QueryResultCache noOpCache() {
        return new QueryResultCache(new NoOpL2CacheStore());
    }

    // ── Test implementations ────────────────────────────────────────

    static class CountingTableWriter implements TableWriter {
        private final int rowCount;
        CountingTableWriter(int rowCount) { this.rowCount = rowCount; }

        @Override
        public WriteResult write(String tableName, List<WriteRecord> records,
                                 WriteMode mode, WriteContext ctx) {
            return new WriteResult(tableName, rowCount, 1, 0, 0, Instant.now());
        }

        @Override
        public Optional<String> readCommitProperty(String tableName, String key) {
            return Optional.empty();
        }

        @Override
        public TableFormat format() { return TableFormat.ICEBERG; }

        @Override
        public void close() {}
    }

    static final class SlowTableWriter implements TableWriter {
        private final int rowCount;
        private final CountDownLatch started;
        private final CountDownLatch canFinish;

        SlowTableWriter(int rowCount, CountDownLatch started, CountDownLatch canFinish) {
            this.rowCount = rowCount;
            this.started = started;
            this.canFinish = canFinish;
        }

        @Override
        public WriteResult write(String tableName, List<WriteRecord> records,
                                 WriteMode mode, WriteContext ctx) {
            started.countDown();
            try {
                canFinish.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted waiting for latch", e);
            }
            return new WriteResult(tableName, rowCount, 1, 0, 0, Instant.now());
        }

        @Override
        public Optional<String> readCommitProperty(String tableName, String key) {
            return Optional.empty();
        }

        @Override
        public TableFormat format() { return TableFormat.ICEBERG; }

        @Override
        public void close() {}
    }

    static final class NoOpL2CacheStore implements L2CacheStore {
        @Override public Optional<CachedResult> get(String key) { return Optional.empty(); }
        @Override public void put(String key, CachedResult value) {}
        @Override public void remove(String key) {}
        @Override public void removeByTable(String tableName) {}
        @Override public void removeByUser(String userId) {}
    }
}
