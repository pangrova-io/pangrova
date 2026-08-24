/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.coordinator.ai.embedding;

import org.pangrova.coordinator.ai.embedding.BackfillCoordinator.BackfillRequest;
import org.pangrova.coordinator.ai.embedding.BackfillCoordinator.BackfillResult;
import org.pangrova.coordinator.ai.embedding.BackfillCoordinator.BackfillState;
import org.pangrova.coordinator.ai.embedding.BackfillCoordinator.BackfillStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DistributedBackfillCoordinatorTest {

    private InMemoryCheckpointStore checkpointStore;
    private FixedSnapshotScanner rowScanner;
    private FixedPartitioner partitioner;
    private CountingDispatcher dispatcher;
    private DistributedBackfillCoordinator coordinator;

    @BeforeEach
    void setUp() {
        checkpointStore = new InMemoryCheckpointStore();
        rowScanner = new FixedSnapshotScanner(42L);
        partitioner = new FixedPartitioner();
        dispatcher = new CountingDispatcher();
        coordinator = new DistributedBackfillCoordinator(
                rowScanner, partitioner, dispatcher, checkpointStore);
    }

    @Test
    void startProcessesAllPartitionsAndReturnsResult() throws Exception {
        partitioner.addPartition(new BackfillPartition("p1", 0, 100));
        partitioner.addPartition(new BackfillPartition("p2", 100, 50));

        BackfillRequest request = new BackfillRequest("db.table1", "embedding", "bge_m3", null, 100, 4);
        BackfillResult result = coordinator.start(request).get();

        assertThat(result.tableName()).isEqualTo("db.table1");
        assertThat(result.columnName()).isEqualTo("embedding");
        assertThat(result.totalRows()).isEqualTo(150);
        assertThat(result.processedRows()).isEqualTo(150);
        assertThat(result.failedRows()).isEqualTo(0);
        assertThat(result.cancelled()).isFalse();
        assertThat(result.snapshotId()).isEqualTo(42L);
        assertThat(dispatcher.dispatchCount()).isEqualTo(2);
    }

    @Test
    void startWithFailedRowsReportsCorrectCounts() throws Exception {
        partitioner.addPartition(new BackfillPartition("p1", 0, 100));
        dispatcher.setFailedRowsPerBatch(5);

        BackfillRequest request = new BackfillRequest("db.table1", "embedding", "bge_m3", null, 100, 4);
        BackfillResult result = coordinator.start(request).get();

        assertThat(result.processedRows()).isEqualTo(95);
        assertThat(result.failedRows()).isEqualTo(5);
    }

    @Test
    void cancelStopsProcessingNewPartitions() throws Exception {
        partitioner.addPartition(new BackfillPartition("p1", 0, 100));
        partitioner.addPartition(new BackfillPartition("p2", 100, 100));
        partitioner.addPartition(new BackfillPartition("p3", 200, 100));
        dispatcher.setDelayMs(50);

        BackfillRequest request = new BackfillRequest("db.table1", "embedding", "bge_m3", null, 100, 1);
        var future = coordinator.start(request);

        Thread.sleep(30);
        boolean cancelled = coordinator.cancel("db.table1", "embedding");
        assertThat(cancelled).isTrue();

        BackfillResult result = future.get();
        assertThat(result.cancelled()).isTrue();
        assertThat(dispatcher.dispatchCount()).isLessThan(3);
    }

    @Test
    void getStatusReturnsActiveJobStatus() throws Exception {
        partitioner.addPartition(new BackfillPartition("p1", 0, 100));
        dispatcher.setDelayMs(100);

        BackfillRequest request = new BackfillRequest("db.table1", "embedding", "bge_m3", null, 100, 1);
        coordinator.start(request);

        Thread.sleep(30);
        Optional<BackfillStatus> status = coordinator.getStatus("db.table1");
        assertThat(status).isPresent();
        assertThat(status.get().tableName()).isEqualTo("db.table1");
    }

    @Test
    void getStatusReturnsEmptyForUnknownTable() {
        Optional<BackfillStatus> status = coordinator.getStatus("unknown.table");
        assertThat(status).isEmpty();
    }

    @Test
    void duplicateStartThrowsBackfillAlreadyRunning() {
        partitioner.addPartition(new BackfillPartition("p1", 0, 100));
        dispatcher.setDelayMs(200);

        BackfillRequest request = new BackfillRequest("db.table1", "embedding", "bge_m3", null, 100, 1);
        coordinator.start(request);

        assertThatThrownBy(() -> coordinator.start(request))
                .isInstanceOf(BackfillAlreadyRunningException.class);
    }

    @Test
    void resumeFromCheckpointSkipsCompletedPartitions() throws Exception {
        BackfillCheckpoint checkpoint = new BackfillCheckpoint(
                "bf-123", "db.table1", "embedding", 42L, 0, 100, 100, 0, Instant.now());
        checkpointStore.save(checkpoint);

        partitioner.addPartition(new BackfillPartition("p1", 0, 100));
        partitioner.addPartition(new BackfillPartition("p2", 100, 50));

        BackfillRequest request = new BackfillRequest("db.table1", "embedding", "bge_m3", null, 100, 4);
        BackfillResult result = coordinator.start(request).get();

        assertThat(dispatcher.dispatchCount()).isEqualTo(1);
        assertThat(result.processedRows()).isEqualTo(150);
    }

    @Test
    void checkpointRemovedOnCompletion() throws Exception {
        partitioner.addPartition(new BackfillPartition("p1", 0, 100));

        BackfillRequest request = new BackfillRequest("db.table1", "embedding", "bge_m3", null, 100, 4);
        coordinator.start(request).get();

        assertThat(checkpointStore.load("db.table1", "embedding")).isEmpty();
    }

    @Test
    void cancelOnNonExistentJobReturnsFalse() {
        boolean result = coordinator.cancel("nonexistent", "col");
        assertThat(result).isFalse();
    }

    @Test
    void constructorRejectsNullDependencies() {
        assertThatThrownBy(() -> new DistributedBackfillCoordinator(null, partitioner, dispatcher, checkpointStore))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DistributedBackfillCoordinator(rowScanner, null, dispatcher, checkpointStore))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DistributedBackfillCoordinator(rowScanner, partitioner, null, checkpointStore))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new DistributedBackfillCoordinator(rowScanner, partitioner, dispatcher, null))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void backfillRequestRejectsInvalidInputs() {
        assertThatThrownBy(() -> new BackfillRequest(null, "col", "model", null, 100, 4))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BackfillRequest("table", "", "model", null, 100, 4))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BackfillRequest("table", "col", "model", null, 0, 4))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new BackfillRequest("table", "col", "model", null, 100, -1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    // ── Test doubles (in-process, no mocking frameworks) ──

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
        private final List<BackfillPartition> partitions = new ArrayList<>();

        void addPartition(BackfillPartition partition) {
            partitions.add(partition);
        }

        @Override
        public List<BackfillPartition> partition(String tableName, String columnName,
                                                 String filter, long snapshotId, int batchSize) {
            return List.copyOf(partitions);
        }
    }

    private static final class CountingDispatcher implements BackfillWorkerDispatcher {
        private final AtomicLong count = new AtomicLong();
        private volatile long failedRowsPerBatch;
        private volatile long delayMs;

        void setFailedRowsPerBatch(long failed) {
            this.failedRowsPerBatch = failed;
        }

        void setDelayMs(long ms) {
            this.delayMs = ms;
        }

        long dispatchCount() {
            return count.get();
        }

        @Override
        public BackfillBatchResult dispatch(String tableName, String columnName,
                                            String modelName, BackfillPartition partition,
                                            long snapshotId) {
            if (delayMs > 0) {
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            count.incrementAndGet();
            long processed = partition.rowCount() - failedRowsPerBatch;
            return new BackfillBatchResult(processed, failedRowsPerBatch);
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
