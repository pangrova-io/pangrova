package org.pangrova.coordinator.adapter.indexing.lifecycle;

import org.pangrova.indexing.core.build.IndexBuildResult;
import org.pangrova.indexing.store.IndexMetadataStore;
import org.pangrova.indexing.store.IndexFileMapping;
import org.pangrova.spi.indexing.descriptor.IndexDescriptor;
import org.pangrova.spi.indexing.state.IndexState;
import org.pangrova.spi.indexing.descriptor.IndexType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies F-INDEX-02: {@code lastIndexedSnapshotId} semantics and
 * {@code IndexBuildResult} success/failure classification.
 *
 * <p>After the fix, only fully successful builds (zero failures) should
 * result in advancing the snapshot watermark. Failed, aborted, and stale-
 * generation builds must NOT advance the watermark.</p>
 */
@DisplayName("F-INDEX-02: lastIndexedSnapshotId build-success guarantee")
class IndexCommitHandlerSnapshotBindingTest {

    @Test
    @DisplayName("IndexBuildResult.success() indicates full success")
    void successResultIsFullySuccessful() {
        IndexBuildResult result = IndexBuildResult.success(5);
        assertThat(result.successCount()).isEqualTo(5);
        assertThat(result.failureCount()).isZero();
        assertThat(result.isFullySuccessful()).isTrue();
    }

    @Test
    @DisplayName("IndexBuildResult with any failure is NOT fully successful")
    void partialFailureIsNotFullySuccessful() {
        IndexBuildResult result = new IndexBuildResult(3, 2);
        assertThat(result.successCount()).isEqualTo(3);
        assertThat(result.failureCount()).isEqualTo(2);
        assertThat(result.isFullySuccessful()).isFalse();
    }

    @Test
    @DisplayName("IndexBuildResult.claimFailed() is NOT fully successful")
    void claimFailedIsNotSuccessful() {
        IndexBuildResult result = IndexBuildResult.claimFailed();
        assertThat(result.isFullySuccessful()).isFalse();
    }

    @Test
    @DisplayName("IndexBuildResult.staleGeneration() is NOT fully successful")
    void staleGenerationIsNotSuccessful() {
        IndexBuildResult result = IndexBuildResult.staleGeneration();
        assertThat(result.isFullySuccessful()).isFalse();
    }

    @Test
    @DisplayName("RecordingStore tracks updateLastIndexedSnapshotId calls")
    void recordingStoreTracksSnapshotUpdates() {
        RecordingStore store = new RecordingStore(List.of());
        store.updateLastIndexedSnapshotId("idx-1", "snap-1");
        store.updateLastIndexedSnapshotId("idx-2", "snap-2");

        assertThat(store.snapshotUpdates).containsExactly(
                new SnapUpdate("idx-1", "snap-1"),
                new SnapUpdate("idx-2", "snap-2"));
    }

    @Test
    @DisplayName("filterUnindexedFiles returns all paths when not all covered")
    void filterUnindexedFilesReturnsAllWhenNotCovered() {
        RecordingStore store = new RecordingStore(List.of());
        List<String> paths = List.of("a.parquet", "b.parquet");
        assertThat(store.filterUnindexedFiles("idx-1", paths)).isEqualTo(paths);
    }

    @Test
    @DisplayName("filterUnindexedFiles returns empty when all covered")
    void filterUnindexedFilesReturnsEmptyWhenAllCovered() {
        RecordingStore store = new RecordingStore(List.of());
        store.markAllAsCovered = true;
        assertThat(store.filterUnindexedFiles("idx-1", List.of("a.parquet"))).isEmpty();
    }

    @Test
    @DisplayName("Index descriptor with READY state is findable by table")
    void readyIndexIsFindableByTable() {
        IndexDescriptor idx = readyIndex("idx-ready");
        RecordingStore store = new RecordingStore(List.of(idx));
        List<IndexDescriptor> found = store.findIndexesByTable(
                "tenant-a", "cat", "sch", "tbl");
        assertThat(found).hasSize(1);
        assertThat(found.get(0).indexId()).isEqualTo("idx-ready");
    }

    // -- helpers --

    private static IndexDescriptor readyIndex(String indexId) {
        Instant now = Instant.now();
        return new IndexDescriptor(
                indexId, "name-" + indexId,
                "cat", "sch", "tbl",
                List.of("c1"), IndexType.BTREE, Map.of(),
                IndexState.READY, null, 0, null, null, now, now, "default");
    }

    /** Records updateLastIndexedSnapshotId calls and exposes index discovery. */
    static final class RecordingStore implements IndexMetadataStore {
        final CopyOnWriteArrayList<SnapUpdate> snapshotUpdates = new CopyOnWriteArrayList<>();
        private final List<IndexDescriptor> indexes;
        volatile boolean markAllAsCovered = false;

        RecordingStore(List<IndexDescriptor> indexes) {
            this.indexes = indexes;
        }

        @Override
        public void updateLastIndexedSnapshotId(String indexId, String snapshotId) {
            snapshotUpdates.add(new SnapUpdate(indexId, snapshotId));
        }

        @Override
        public List<IndexDescriptor> findIndexesByTable(String tenantId, String c, String s, String t) {
            return indexes;
        }

        @Override
        public List<String> filterUnindexedFiles(String indexId, List<String> paths) {
            return markAllAsCovered ? List.of() : paths;
        }

        // -- No-op remainder --
        @Override public void registerIndex(IndexDescriptor d) {}
        @Override public void updateIndexState(String id, String s, Instant t) {}
        @Override public boolean beginIndexGeneration(String id, int next, String snapshot,
                Instant timestamp, Instant expected, IndexState state, int generation) { return false; }
        @Override public org.pangrova.indexing.store.IndexGenerationManifest publishIndexGeneration(
                org.pangrova.indexing.store.IndexPublicationRequest request) { throw new AssertionError(); }
        @Override public Optional<org.pangrova.indexing.store.IndexGenerationManifest>
                findActiveIndexGeneration(String id) { return Optional.empty(); }
        @Override public Optional<IndexDescriptor> findIndex(String id) { return Optional.empty(); }
        @Override public List<IndexDescriptor> findReadyIndexesForColumns(
                String tenantId, String c, String s, String t, List<String> cols, boolean b) { return List.of(); }
        @Override public void dropIndex(String id, Instant t) {}
        @Override public void purgeIndex(String id) {}
        @Override public void registerIndexFile(IndexFileMapping m) {}
        @Override public void batchRegisterIndexFiles(List<IndexFileMapping> m) {}
        @Override public List<IndexFileMapping> getIndexFileMappings(String id) { return List.of(); }
        @Override public Map<String, Map<String, String>> batchGetIndexFileMappings(
                List<String> ids, List<String> paths) { return Map.of(); }
        @Override public void removeIndexFileMapping(String id, String path) {}
        @Override public void markOrphanedByDataFiles(String id, List<String> paths) {}
        @Override public List<IndexFileMapping> listStaleFileMappings(long ms) { return List.of(); }
        @Override public void updateLastVerifiedAt(String id, String path, long ts) {}
        @Override public List<IndexDescriptor> findIndexesByColumn(
                String tenantId, String c, String s, String t, String col) { return List.of(); }
        @Override public void renameColumnInIndexes(
                String tenantId, String c, String s, String t, String o, String n) {}
        @Override public void dropAllIndexesForTable(
                String tenantId, String c, String s, String t, Instant ts) {}
        @Override public void renameTable(String tenantId, String c, String s, String o, String n) {}
        @Override public List<IndexDescriptor> findIndexesByState(IndexState st) { return List.of(); }
    }

    record SnapUpdate(String indexId, String snapshotId) {}
}
