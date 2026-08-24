package org.pangrova.coordinator.dynamictable.refresh;

import org.pangrova.coordinator.coordination.inmemory.InMemoryDistributedLease;
import org.pangrova.coordinator.dynamictable.DynamicTableConfig;
import org.pangrova.coordinator.dynamictable.detect.ChangeResult;
import org.pangrova.coordinator.dynamictable.detect.IncrementalScanPlan;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for F-CHAIN-STREAM-06 fix: atomic checkpoint in dynamic table refresh.
 *
 * <p>Validates:
 * <ul>
 *   <li>Checkpoint is embedded atomically in table commit properties</li>
 *   <li>Idempotent recovery: if checkpoint is already committed in table, write is skipped</li>
 *   <li>State store is still updated as secondary persistence</li>
 *   <li>Normal refresh flow still works with checkpoint embedding</li>
 * </ul>
 */
class RefreshAtomicCheckpointTest {

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

    // ── F-CHAIN-STREAM-06: Atomic checkpoint in commit properties ───

    @Test
    void refresh_embedsCheckpointInCommitProperties() {
        DynamicTableState state = activeState("atomic_dt");
        stateStore.saveState(state);

        var capturingWriter = new CommitPropertyCapturingTableWriter();
        var dtWriter = new DynamicTableWriter(capturingWriter, noOpCache());

        RefreshCoordinator coordinator = buildCoordinator(
                snapshotPipeline(incrementalChanges(), evaluationWithRows(3)),
                dtWriter);

        RefreshResult result = coordinator.executeRefresh(state, TriggerType.SCHEDULED);

        assertThat(result.status()).isEqualTo(RefreshStatus.SUCCESS);
        // Verify that commit properties contain the checkpoint
        assertThat(capturingWriter.lastCommitProperties)
                .containsKey("pangrova.dt.source-checkpoint");
        assertThat(capturingWriter.lastCommitProperties.get("pangrova.dt.source-checkpoint"))
                .isEqualTo("200");  // extractCheckpoint from IncrementalScanPlan toSnapshot
    }

    @Test
    void refresh_embedsCheckpointTypeInCommitProperties() {
        DynamicTableState state = activeState("typed_dt");
        stateStore.saveState(state);

        var capturingWriter = new CommitPropertyCapturingTableWriter();
        var dtWriter = new DynamicTableWriter(capturingWriter, noOpCache());

        RefreshCoordinator coordinator = buildCoordinator(
                snapshotPipeline(incrementalChanges(), evaluationWithRows(2)),
                dtWriter);

        RefreshResult result = coordinator.executeRefresh(state, TriggerType.SCHEDULED);

        assertThat(result.status()).isEqualTo(RefreshStatus.SUCCESS);
        assertThat(capturingWriter.lastCommitProperties)
                .containsKey("pangrova.dt.checkpoint-type");
        assertThat(capturingWriter.lastCommitProperties.get("pangrova.dt.checkpoint-type"))
                .isEqualTo("SNAPSHOT");
    }

    // ── F-CHAIN-STREAM-06: Idempotent recovery (skip already-committed) ───

    @Test
    void refresh_checkpointAlreadyCommittedInTable_skipsWrite() {
        DynamicTableState state = activeState("idempotent_dt");
        stateStore.saveState(state);

        // Table writer reports that checkpoint "200" is already committed
        var writeCounter = new AtomicInteger(0);
        var alreadyCommittedWriter = new AlreadyCommittedTableWriter("200", writeCounter);
        var dtWriter = new DynamicTableWriter(alreadyCommittedWriter, noOpCache());

        RefreshCoordinator coordinator = buildCoordinator(
                snapshotPipeline(incrementalChanges(), evaluationWithRows(5)),
                dtWriter);

        RefreshResult result = coordinator.executeRefresh(state, TriggerType.SCHEDULED);

        // Should succeed (commit state store) but skip the actual write
        assertThat(result.status()).isEqualTo(RefreshStatus.SUCCESS);
        assertThat(writeCounter.get()).isZero(); // no write was performed
        assertThat(result.newCheckpoint()).isEqualTo("200");

        // State store should still be updated
        DynamicTableState updated = stateStore.loadState("idempotent_dt").orElseThrow();
        assertThat(updated.checkpoint()).isEqualTo("200");
    }

    @Test
    void refresh_differentCheckpointInTable_proceedsWithWrite() {
        DynamicTableState state = activeState("proceed_dt");
        stateStore.saveState(state);

        // Table writer reports that checkpoint "150" is committed (different from "200")
        var writeCounter = new AtomicInteger(0);
        var writer = new AlreadyCommittedTableWriter("150", writeCounter);
        var dtWriter = new DynamicTableWriter(writer, noOpCache());

        RefreshCoordinator coordinator = buildCoordinator(
                snapshotPipeline(incrementalChanges(), evaluationWithRows(3)),
                dtWriter);

        RefreshResult result = coordinator.executeRefresh(state, TriggerType.SCHEDULED);

        assertThat(result.status()).isEqualTo(RefreshStatus.SUCCESS);
        assertThat(writeCounter.get()).isEqualTo(1); // write was performed
    }

    @Test
    void refresh_readCheckpointFails_proceedsWithWrite() {
        DynamicTableState state = activeState("fallback_dt");
        stateStore.saveState(state);

        // Table writer throws on readLastCommitProperty
        var writeCounter = new AtomicInteger(0);
        var failingReader = new FailingReadButWriteableTableWriter(writeCounter);
        var dtWriter = new DynamicTableWriter(failingReader, noOpCache());

        RefreshCoordinator coordinator = buildCoordinator(
                snapshotPipeline(incrementalChanges(), evaluationWithRows(2)),
                dtWriter);

        RefreshResult result = coordinator.executeRefresh(state, TriggerType.SCHEDULED);

        // Should proceed with write despite read failure
        assertThat(result.status()).isEqualTo(RefreshStatus.SUCCESS);
        assertThat(writeCounter.get()).isEqualTo(1);
    }

    // ── Helpers ────────────────────────────────────────────────────

    private RefreshCoordinator buildCoordinator(RefreshPipeline pipeline,
                                                DynamicTableWriter writer) {
        var deps = new RefreshCoordinatorContext(stateStore, historyStore, writer);
        return new RefreshCoordinator(deps, pipeline,
                new RefreshLockCoordinator(lockManager), config);
    }

    private static DynamicTableState activeState(String dtName) {
        return new DynamicTableState(
                dtName, "tenant-1", DynamicTableLifecycleState.ACTIVE, DynamicTableRefreshMode.SNAPSHOT,
                Duration.ofMinutes(5), "100", "SNAPSHOT", 1, null,
                "admin", "coord-1", 1L, null, 0, 0,
                Instant.now(), Instant.now());
    }

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

    private static QueryResultCache noOpCache() {
        return new QueryResultCache(new NoOpL2CacheStore());
    }

    // ── Test implementations ────────────────────────────────────────

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

    static final class FixedEvaluator implements org.pangrova.coordinator.dynamictable.eval.IncrementalEvaluator {
        private final IncrementalResult result;

        FixedEvaluator(IncrementalResult result) {
            this.result = result;
        }

        @Override
        public IncrementalResult evaluate(DynamicTableState state, ChangeResult changes) {
            return result;
        }
    }

    /**
     * Captures commit properties passed via WriteContext.
     */
    static final class CommitPropertyCapturingTableWriter implements TableWriter {
        Map<String, String> lastCommitProperties = Map.of();

        @Override
        public WriteResult write(String tableName, List<WriteRecord> records,
                                 WriteMode writeMode, WriteContext context) {
            lastCommitProperties = context.commitProperties();
            return new WriteResult(tableName, records.size(), 1, 0, 0, Instant.now());
        }

        @Override
        public TableFormat format() {
            return TableFormat.ICEBERG;
        }

        @Override
        public void close() {}
    }

    /**
     * Reports a specific checkpoint as already committed; counts write calls.
     */
    static final class AlreadyCommittedTableWriter implements TableWriter {
        private final String committedCheckpoint;
        private final AtomicInteger writeCount;

        AlreadyCommittedTableWriter(String committedCheckpoint, AtomicInteger writeCount) {
            this.committedCheckpoint = committedCheckpoint;
            this.writeCount = writeCount;
        }

        @Override
        public WriteResult write(String tableName, List<WriteRecord> records,
                                 WriteMode writeMode, WriteContext context) {
            writeCount.incrementAndGet();
            return new WriteResult(tableName, records.size(), 1, 0, 0, Instant.now());
        }

        @Override
        public Optional<String> readCommitProperty(String tableName, String key) {
            if ("pangrova.dt.source-checkpoint".equals(key)) {
                return Optional.of(committedCheckpoint);
            }
            return Optional.empty();
        }

        @Override
        public TableFormat format() {
            return TableFormat.ICEBERG;
        }

        @Override
        public void close() {}
    }

    /**
     * Counts write calls.
     */
    static final class FailingReadButWriteableTableWriter implements TableWriter {
        private final AtomicInteger writeCount;

        FailingReadButWriteableTableWriter(AtomicInteger writeCount) {
            this.writeCount = writeCount;
        }

        @Override
        public WriteResult write(String tableName, List<WriteRecord> records,
                                 WriteMode writeMode, WriteContext context) {
            writeCount.incrementAndGet();
            return new WriteResult(tableName, records.size(), 1, 0, 0, Instant.now());
        }

        @Override
        public Optional<String> readCommitProperty(String tableName, String key) {
            throw new RuntimeException("simulated read failure");
        }

        @Override
        public TableFormat format() {
            return TableFormat.ICEBERG;
        }

        @Override
        public void close() {}
    }

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
}
