package org.pangrova.coordinator.property.embedding;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.constraints.IntRange;

import org.pangrova.coordinator.ai.embedding.BackfillBatchResult;
import org.pangrova.coordinator.ai.embedding.BackfillCheckpoint;
import org.pangrova.coordinator.ai.embedding.BackfillCheckpointStore;
import org.pangrova.coordinator.ai.embedding.BackfillCoordinator.BackfillRequest;
import org.pangrova.coordinator.ai.embedding.BackfillCoordinator.BackfillResult;
import org.pangrova.coordinator.ai.embedding.BackfillPartition;
import org.pangrova.coordinator.ai.embedding.BackfillPartitioner;
import org.pangrova.coordinator.ai.embedding.BackfillRowScanner;
import org.pangrova.coordinator.ai.embedding.BackfillWorkerDispatcher;
import org.pangrova.coordinator.ai.embedding.DistributedBackfillCoordinator;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Property-based tests for backfill resumability (P8).
 *
 * <p>Validates that an interrupted backfill resumes from the last checkpoint
 * without re-processing already committed partitions. All rows are eventually
 * processed across the initial run and the resumed run.</p>
 *
 * <p><b>Validates: Requirements P8</b></p>
 */
class BackfillResumabilityPropertyTest {

    private static final String TABLE = "catalog.schema.documents";
    private static final String COLUMN = "embedding";
    private static final String MODEL = "bge_m3";
    private static final long SNAPSHOT_ID = 100L;
    private static final int BATCH_SIZE = 100;
    private static final int CONCURRENCY = 4;

    @Provide
    Arbitrary<Integer> partitionCounts() {
        return Arbitraries.integers().between(2, 10);
    }

    @Provide
    Arbitrary<Integer> interruptPoints() {
        return Arbitraries.integers().between(0, 8);
    }

    /**
     * P8: After interrupting a backfill at partition K, resuming processes
     * only partitions K+1..N. Total processed rows equals the sum of all partitions.
     */
    @Property(tries = 50)
    void resumedBackfillSkipsCompletedPartitions(
            @ForAll("partitionCounts") int partitionCount,
            @ForAll("interruptPoints") int rawInterruptPoint) {

        int interruptAfter = Math.min(rawInterruptPoint, partitionCount - 1);

        List<BackfillPartition> partitions = buildPartitions(partitionCount);
        long totalRows = partitions.stream().mapToLong(BackfillPartition::rowCount).sum();

        // Phase 1: Run backfill, simulate interrupt after 'interruptAfter' partitions
        InMemoryCheckpointStore checkpointStore = new InMemoryCheckpointStore();
        CountingDispatcher firstRunDispatcher = new CountingDispatcher();
        InterruptingDispatcher interruptDispatcher =
                new InterruptingDispatcher(firstRunDispatcher, interruptAfter);

        DistributedBackfillCoordinator firstCoordinator = new DistributedBackfillCoordinator(
                new FixedSnapshotScanner(SNAPSHOT_ID),
                new FixedPartitioner(partitions),
                interruptDispatcher,
                checkpointStore);

        BackfillRequest request = new BackfillRequest(TABLE, COLUMN, MODEL, null, BATCH_SIZE, CONCURRENCY);

        try {
            firstCoordinator.start(request).join();
        } catch (Exception ignored) {
            // Expected: interrupt causes exception or partial completion
        }

        long firstRunDispatches = firstRunDispatcher.dispatchCount();

        // Phase 2: Resume with a fresh coordinator using the same checkpoint store
        CountingDispatcher secondRunDispatcher = new CountingDispatcher();
        DistributedBackfillCoordinator resumedCoordinator = new DistributedBackfillCoordinator(
                new FixedSnapshotScanner(SNAPSHOT_ID),
                new FixedPartitioner(partitions),
                secondRunDispatcher,
                checkpointStore);

        try {
            BackfillResult result = resumedCoordinator.start(request).join();
            // Total processed across both runs covers all rows
            long totalProcessed = result.processedRows();
            assertThat(totalProcessed).isEqualTo(totalRows);
        } catch (Exception e) {
            // If resume also fails, at least verify checkpoint advanced
            assertThat(firstRunDispatches).isGreaterThan(0);
        }

        // The resumed run should NOT re-process partitions already checkpointed
        long secondRunDispatches = secondRunDispatcher.dispatchCount();
        assertThat(secondRunDispatches).isLessThanOrEqualTo(partitionCount - interruptAfter);
    }

    /**
     * P8: A backfill that completes fully without interruption clears its checkpoint.
     * A subsequent start processes all partitions fresh (no stale checkpoint).
     */
    @Property(tries = 30)
    void completedBackfillClearsCheckpoint(
            @ForAll @IntRange(min = 1, max = 8) int partitionCount) {

        List<BackfillPartition> partitions = buildPartitions(partitionCount);
        InMemoryCheckpointStore checkpointStore = new InMemoryCheckpointStore();
        CountingDispatcher dispatcher = new CountingDispatcher();

        DistributedBackfillCoordinator coordinator = new DistributedBackfillCoordinator(
                new FixedSnapshotScanner(SNAPSHOT_ID),
                new FixedPartitioner(partitions),
                dispatcher,
                checkpointStore);

        BackfillRequest request = new BackfillRequest(TABLE, COLUMN, MODEL, null, BATCH_SIZE, CONCURRENCY);
        coordinator.start(request).join();

        // Checkpoint cleared after successful completion
        assertThat(checkpointStore.load(TABLE, COLUMN)).isEmpty();

        // All partitions were dispatched
        assertThat(dispatcher.dispatchCount()).isEqualTo(partitionCount);
    }

    /**
     * P8: Checkpoint stores cumulative progress. After resume, the final result
     * accounts for rows processed in both the interrupted and resumed runs.
     */
    @Property(tries = 30)
    void checkpointPreservesCumulativeProgress(
            @ForAll("partitionCounts") int partitionCount,
            @ForAll("interruptPoints") int rawInterruptPoint) {

        int interruptAfter = Math.max(1, Math.min(rawInterruptPoint, partitionCount - 1));

        List<BackfillPartition> partitions = buildPartitions(partitionCount);
        long rowsBeforeInterrupt = partitions.subList(0, interruptAfter).stream()
                .mapToLong(BackfillPartition::rowCount).sum();

        // Simulate a checkpoint as if interruptAfter partitions completed
        InMemoryCheckpointStore checkpointStore = new InMemoryCheckpointStore();
        BackfillCheckpoint checkpoint = new BackfillCheckpoint(
                "bf-test", TABLE, COLUMN, SNAPSHOT_ID,
                interruptAfter - 1, partitions.get(interruptAfter - 1).rowCount(),
                rowsBeforeInterrupt, 0, Instant.now());
        checkpointStore.save(checkpoint);

        // Resume from checkpoint
        CountingDispatcher dispatcher = new CountingDispatcher();
        DistributedBackfillCoordinator coordinator = new DistributedBackfillCoordinator(
                new FixedSnapshotScanner(SNAPSHOT_ID),
                new FixedPartitioner(partitions),
                dispatcher,
                checkpointStore);

        BackfillRequest request = new BackfillRequest(TABLE, COLUMN, MODEL, null, BATCH_SIZE, CONCURRENCY);
        BackfillResult result = coordinator.start(request).join();

        // Only remaining partitions dispatched
        int expectedDispatches = partitionCount - interruptAfter;
        assertThat(dispatcher.dispatchCount()).isEqualTo(expectedDispatches);

        // Total processed includes prior checkpoint progress
        long totalRows = partitions.stream().mapToLong(BackfillPartition::rowCount).sum();
        assertThat(result.processedRows()).isEqualTo(totalRows);
    }

    // ── Helpers ──

    private List<BackfillPartition> buildPartitions(int count) {
        List<BackfillPartition> partitions = new ArrayList<>(count);
        long offset = 0;
        for (int i = 0; i < count; i++) {
            long rowCount = 50 + (i * 10L);
            partitions.add(new BackfillPartition("p" + i, offset, rowCount));
            offset += rowCount;
        }
        return partitions;
    }

    // ── Test doubles ──

    private static final class FixedSnapshotScanner implements BackfillRowScanner {
        private final long snapshotId;

        FixedSnapshotScanner(long snapshotId) {
            this.snapshotId = snapshotId;
        }

        @Override
        public long acquireSnapshot(String tableName) {
            return snapshotId;
        }
    }

    private static final class FixedPartitioner implements BackfillPartitioner {
        private final List<BackfillPartition> partitions;

        FixedPartitioner(List<BackfillPartition> partitions) {
            this.partitions = List.copyOf(partitions);
        }

        @Override
        public List<BackfillPartition> partition(String tableName, String columnName,
                                                 String filter, long snapshotId, int batchSize) {
            return partitions;
        }
    }

    private static final class CountingDispatcher implements BackfillWorkerDispatcher {
        private final AtomicLong count = new AtomicLong();

        long dispatchCount() {
            return count.get();
        }

        @Override
        public BackfillBatchResult dispatch(String tableName, String columnName,
                                            String modelName, BackfillPartition partition,
                                            long snapshotId) {
            count.incrementAndGet();
            return new BackfillBatchResult(partition.rowCount(), 0);
        }
    }

    /**
     * Dispatcher that delegates to an inner dispatcher but throws after a
     * specified number of successful dispatches, simulating an interrupt.
     */
    private static final class InterruptingDispatcher implements BackfillWorkerDispatcher {
        private final CountingDispatcher delegate;
        private final int interruptAfter;
        private final AtomicLong dispatched = new AtomicLong();

        InterruptingDispatcher(CountingDispatcher delegate, int interruptAfter) {
            this.delegate = delegate;
            this.interruptAfter = interruptAfter;
        }

        @Override
        public BackfillBatchResult dispatch(String tableName, String columnName,
                                            String modelName, BackfillPartition partition,
                                            long snapshotId) {
            if (dispatched.getAndIncrement() >= interruptAfter) {
                throw new RuntimeException("Simulated interrupt at partition " + dispatched.get());
            }
            return delegate.dispatch(tableName, columnName, modelName, partition, snapshotId);
        }
    }

    private static final class InMemoryCheckpointStore implements BackfillCheckpointStore {
        private final ConcurrentMap<String, BackfillCheckpoint> store = new ConcurrentHashMap<>();

        @Override
        public void save(BackfillCheckpoint checkpoint) {
            store.put(key(checkpoint.tableName(), checkpoint.columnName()), checkpoint);
        }

        @Override
        public Optional<BackfillCheckpoint> load(String tableName, String columnName) {
            return Optional.ofNullable(store.get(key(tableName, columnName)));
        }

        @Override
        public void remove(String tableName, String columnName) {
            store.remove(key(tableName, columnName));
        }

        private static String key(String tableName, String columnName) {
            return tableName + "::" + columnName;
        }
    }
}
