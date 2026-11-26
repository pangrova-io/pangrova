package org.pangrova.coordinator.indexing.lifecycle.gc;

import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;

import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

import org.pangrova.coordinator.indexing.connector.ConnectorAccessorResolver;
import org.pangrova.indexing.config.IndexingConfig;
import org.pangrova.indexing.storage.LocalFileSystemAccessor;
import org.pangrova.indexing.storage.IndexStorageManager;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;
import org.pangrova.persistence.indexing.api.metadata.IndexFileMapping;
import org.pangrova.persistence.catalog.api.catalog.CatalogDeploymentRepository;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.connector.api.index.SnapshotPinProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies index versioning with generation fencing in GC:
 * GC keeps generation N-1 (rollback), deletes generation N-2 and older.
 */
@DisplayName("Index GC Generation Fencing")
class IndexGcGenerationFencingTest {

    @TempDir Path files;
    private IndexGarbageCollector gc;

    @AfterEach
    void tearDown() {
        if (gc != null) {
            gc.close();
        }
    }

    @Test
    @DisplayName("GC skips mappings from current generation (N)")
    void gcSkipsCurrentGeneration() {
        var store = new GenerationTrackingStore();
        // Index at generation 3
        IndexDescriptor descriptor = buildDescriptor("idx-1", 3, IndexState.READY);
        store.addDescriptor(descriptor);
        // Mapping at generation 3 (current) — should be skipped
        store.addStaleMapping(buildMapping("idx-1", "/data/f1.parquet", "/idx/f1.idx", 3));

        gc = buildGc(store);
        GcResult result = gc.runGc();

        assertThat(result.orphanedCount()).isZero();
        assertThat(store.removedMappings).isEmpty();
    }

    @Test
    @DisplayName("GC skips mappings from generation N-1 (rollback)")
    void gcSkipsRollbackGeneration() {
        var store = new GenerationTrackingStore();
        // Index at generation 3
        IndexDescriptor descriptor = buildDescriptor("idx-1", 3, IndexState.READY);
        store.addDescriptor(descriptor);
        // Mapping at generation 2 (N-1) — should be kept for rollback
        store.addStaleMapping(buildMapping("idx-1", "/data/f2.parquet", "/idx/f2.idx", 2));

        gc = buildGc(store);
        GcResult result = gc.runGc();

        assertThat(result.orphanedCount()).isZero();
        assertThat(store.removedMappings).isEmpty();
    }

    @Test
    @DisplayName("GC deletes mappings from generation N-2 and older")
    void gcDeletesOldGenerations() {
        var store = new GenerationTrackingStore();
        // Index at generation 3
        IndexDescriptor descriptor = buildDescriptor("idx-1", 3, IndexState.READY);
        store.addDescriptor(descriptor);
        // Mapping at generation 1 (N-2) — should be deleted
        store.addStaleMapping(buildMapping("idx-1", "/data/old.parquet", "/idx/old.idx", 1));

        gc = buildGc(store);
        GcResult result = gc.runGc();

        assertThat(result.orphanedCount()).isEqualTo(1);
        assertThat(store.removedMappings).hasSize(1);
        assertThat(store.removedMappings.get(0)).isEqualTo("idx-1:/data/old.parquet");
    }

    @Test
    @DisplayName("GC deletes generation 0 when current is 3")
    void gcDeletesVeryOldGeneration() {
        var store = new GenerationTrackingStore();
        IndexDescriptor descriptor = buildDescriptor("idx-1", 3, IndexState.READY);
        store.addDescriptor(descriptor);
        // Generation 0 — well below N-2 threshold
        store.addStaleMapping(buildMapping("idx-1", "/data/ancient.parquet", "/idx/ancient.idx", 0));

        gc = buildGc(store);
        GcResult result = gc.runGc();

        assertThat(result.orphanedCount()).isEqualTo(1);
        assertThat(store.removedMappings).contains("idx-1:/data/ancient.parquet");
    }

    @Test
    @DisplayName("GC skips BUILDING state indexes regardless of generation")
    void gcSkipsBuildingState() {
        var store = new GenerationTrackingStore();
        IndexDescriptor descriptor = buildDescriptor("idx-1", 3, IndexState.BUILDING);
        store.addDescriptor(descriptor);
        // Old generation but BUILDING — should be skipped
        store.addStaleMapping(buildMapping("idx-1", "/data/building.parquet", "/idx/building.idx", 0));

        gc = buildGc(store);
        GcResult result = gc.runGc();

        assertThat(result.orphanedCount()).isZero();
        assertThat(store.removedMappings).isEmpty();
    }

    @Test
    @DisplayName("Mixed generations: only N-2 and older are deleted")
    void gcMixedGenerations() {
        var store = new GenerationTrackingStore();
        IndexDescriptor descriptor = buildDescriptor("idx-1", 4, IndexState.READY);
        store.addDescriptor(descriptor);

        // Gen 4 (current) — keep
        store.addStaleMapping(buildMapping("idx-1", "/data/gen4.parquet", "/idx/gen4.idx", 4));
        // Gen 3 (N-1) — keep for rollback
        store.addStaleMapping(buildMapping("idx-1", "/data/gen3.parquet", "/idx/gen3.idx", 3));
        // Gen 2 (N-2) — delete
        store.addStaleMapping(buildMapping("idx-1", "/data/gen2.parquet", "/idx/gen2.idx", 2));
        // Gen 1 (N-3) — delete
        store.addStaleMapping(buildMapping("idx-1", "/data/gen1.parquet", "/idx/gen1.idx", 1));

        gc = buildGc(store);
        GcResult result = gc.runGc();

        assertThat(result.orphanedCount()).isEqualTo(2);
        assertThat(store.removedMappings)
                .containsExactlyInAnyOrder("idx-1:/data/gen2.parquet", "idx-1:/data/gen1.parquet");
    }

    @Test
    void oldGenerationDeletionPreservesCurrentFileForSameSource() throws Exception {
        var store = new GenerationTrackingStore();
        store.addDescriptor(buildDescriptor("idx-1", 3, IndexState.READY));
        Path old = Files.writeString(files.resolve("g1.idx"), "old");
        Path current = Files.writeString(files.resolve("g3.idx"), "current");
        store.addStaleMapping(buildMapping("idx-1", "/data/same.parquet", old.toString(), 1));
        store.addStaleMapping(buildMapping("idx-1", "/data/same.parquet", current.toString(), 3));
        gc = buildGc(store);

        assertThat(gc.runGc().orphanedCount()).isEqualTo(1);
        assertThat(Files.exists(old)).isFalse();
        assertThat(Files.readString(current)).isEqualTo("current");
        assertThat(store.getIndexFileMappings("idx-1"))
                .extracting(IndexFileMapping::generation).containsExactly(3);
    }

    @Test
    void legacySharedPathIsNeverDeletedWhileCurrentGenerationUsesIt() throws Exception {
        var store = new GenerationTrackingStore();
        store.addDescriptor(buildDescriptor("idx-1", 3, IndexState.READY));
        Path shared = Files.writeString(files.resolve("v1-shared.idx"), "serving");
        store.addStaleMapping(buildMapping("idx-1", "/data/same.parquet", shared.toString(), 1));
        store.addStaleMapping(buildMapping("idx-1", "/data/same.parquet", shared.toString(), 3));
        gc = buildGc(store);

        assertThat(gc.runGc().orphanedCount()).isEqualTo(1);
        assertThat(Files.readString(shared)).isEqualTo("serving");
        assertThat(store.getIndexFileMappings("idx-1"))
                .extracting(IndexFileMapping::generation).containsExactly(3);
    }

    @Test
    void removesAnIndexCohortWithoutReloadingAllItsMappingsPerOrphan() throws Exception {
        var store = new GenerationTrackingStore();
        store.addDescriptor(buildDescriptor("idx-1", 4, IndexState.READY));
        Path current = Files.writeString(files.resolve("current.idx"), "serving");
        store.addStaleMapping(buildMapping("idx-1", "/data/current.parquet", current.toString(), 4));
        var obsolete = new ArrayList<Path>();
        for (int index = 0; index < 100; index++) {
            Path file = Files.writeString(files.resolve("orphan-" + index + ".idx"), "obsolete");
            obsolete.add(file);
            store.addStaleMapping(buildMapping("idx-1", "/data/orphan-" + index + ".parquet", file.toString(), 2));
        }
        gc = buildGc(store);

        assertThat(gc.runGc().orphanedCount()).isEqualTo(100);
        assertThat(obsolete).noneMatch(Files::exists);
        assertThat(Files.readString(current)).isEqualTo("serving");
        assertThat(store.mappingReadCalls).as("complete index mapping reloads").isZero();
    }

    @Test
    void stateFenceFailureKeepsMappingAndNeverDeletesTheFile() throws Exception {
        var store = new GenerationTrackingStore();
        store.addDescriptor(buildDescriptor("idx-1", 3, IndexState.READY));
        Path file = Files.writeString(files.resolve("fenced.idx"), "keep");
        store.addStaleMapping(buildMapping("idx-1", "/data/fenced.parquet", file.toString(), 1));
        store.rejectMappingRemoval = true;
        gc = buildGc(store);

        assertThat(gc.runGc().orphanedCount()).isZero();
        assertThat(Files.readString(file)).isEqualTo("keep");
        assertThat(store.getIndexFileMappings("idx-1")).hasSize(1);
    }

    @Test
    void failedFileDeletionStillFencesStaleMapping() {
        var store = new GenerationTrackingStore();
        store.addDescriptor(buildDescriptor("idx-1", 3, IndexState.READY));
        store.addStaleMapping(buildMapping("idx-1", "/data/leaked.parquet", "/idx/leaked.idx", 1));
        var config = new IndexingConfig("/tmp/test-gc-gen", false, null, null);
        gc = new IndexGarbageCollector(new IndexGcContext(store, new IndexStorageManager(),
                descriptor -> new FailingDeleteAccessor(), config, null), null);

        assertThat(gc.runGc().orphanedCount()).isEqualTo(1);
        assertThat(store.getIndexFileMappings("idx-1")).isEmpty();
    }

    @Test
    void failedSnapshotUnpinKeepsDroppedIndexForRetry() {
        var store = new GenerationTrackingStore();
        store.addDescriptor(new IndexDescriptor(
                "idx-pinned", "test-index", "cat", "sch", "tbl", List.of("col1"),
                IndexType.BITMAP, Map.of(), IndexState.DROPPED, null, 0,
                "42", "42", Instant.now().minusSeconds(172800),
                Instant.now().minusSeconds(172800), "default"));
        var pin = new RetryablePinProvider();
        gc = buildGc(store, pin);

        assertThat(gc.runGc().purgedCount()).isZero();
        assertThat(store.purgedIndexes).isEmpty();

        pin.fail = false;
        assertThat(gc.runGc().purgedCount()).isEqualTo(1);
        assertThat(store.purgedIndexes).containsExactly("idx-pinned");
        assertThat(pin.unpins).isEqualTo(2);
    }

    @Test
    void failedFileDeletionKeepsDroppedIndexForRetry() {
        var store = new GenerationTrackingStore();
        store.addDescriptor(new IndexDescriptor(
                "idx-delete", "test-index", "cat", "sch", "tbl", List.of("col1"),
                IndexType.BITMAP, Map.of(), IndexState.DROPPED, null, 0,
                null, null, Instant.now().minusSeconds(172800),
                Instant.now().minusSeconds(172800), "default"));
        store.addStaleMapping(buildMapping("idx-delete", "/data/old.parquet", "/idx/old.idx", 0));
        gc = new IndexGarbageCollector(new IndexGcContext(
                store, new IndexStorageManager(), descriptor -> {
                    throw new IllegalStateException("storage unavailable");
                }, new IndexingConfig("/tmp/test-gc-gen", false, null, null), null), null);

        assertThat(gc.runGc().purgedCount()).isZero();
        assertThat(store.purgedIndexes).isEmpty();
        assertThat(store.findIndex("idx-delete")).isPresent();
    }

    // ── Helpers ──

    private IndexGarbageCollector buildGc(GenerationTrackingStore store) {
        return buildGc(store, null);
    }

    private IndexGarbageCollector buildGc(GenerationTrackingStore store, SnapshotPinProvider pin) {
        var config = new IndexingConfig("/tmp/test-gc-gen", false, null, null);
        var deps = new IndexGcContext(store, new IndexStorageManager(),
                descriptor -> new LocalFileSystemAccessor(), config, pin);
        return new IndexGarbageCollector(deps, null);
    }

    private static final class RetryablePinProvider implements SnapshotPinProvider {
        private boolean fail = true;
        private int unpins;

        @Override public void pinSnapshot(String catalog, String schema, String table,
                                          long snapshot, String indexId) {}
        @Override public void unpinSnapshot(String catalog, String schema, String table,
                                            long snapshot, String indexId) {
            unpins++;
            if (fail) throw new IllegalStateException("pin store unavailable");
        }
        @Override public List<Long> getPinnedSnapshots(String catalog, String schema, String table) {
            return List.of();
        }
        @Override public List<Long> getPinnedSnapshotsForIndex(
                String catalog, String schema, String table, String indexId) {
            return List.of(42L);
        }
    }

    private static ConnectorAccessorResolver buildResolver() {
        var persistence = new StubCatalogDeployments();
        var registry = new CatalogRegistry(memoryTableStores(), persistence, desc -> null, null, null);
        registry.initialize();
        return new ConnectorAccessorResolver(registry);
    }

    private static IndexDescriptor buildDescriptor(String indexId, int generation, IndexState state) {
        return new IndexDescriptor(
                indexId, "test-index", "cat", "sch", "tbl",
                List.of("col1"), IndexType.BITMAP, Map.of(),
                state, null, generation, null, null,
                Instant.now().minusSeconds(3600), Instant.now(), "default");
    }

    private static IndexFileMapping buildMapping(String indexId, String dataFile,
                                                  String indexFile, int generation) {
        return new IndexFileMapping(
                indexId, dataFile, indexFile,
                100L, 1024L, "a".repeat(64), generation, "snap-1", 1L,
                Instant.now().minusSeconds(7200), 0L);
    }

    private static final class FailingDeleteAccessor implements org.pangrova.connector.api.index.FileSystemAccessor {
        @Override public java.io.InputStream openInputStream(String path) {
            throw new UnsupportedOperationException();
        }
        @Override public java.io.OutputStream openOutputStream(String path) {
            throw new UnsupportedOperationException();
        }
        @Override public void deleteFile(String path) throws java.io.IOException {
            throw new java.io.IOException("storage unavailable");
        }
        @Override public boolean exists(String path) { return true; }
        @Override public long fileSize(String path) { throw new UnsupportedOperationException(); }
        @Override public List<String> listFiles(String directoryPath) { return List.of(); }
        @Override public void close() {}
    }

    /**
     * In-memory IndexMetadataStore that tracks generation-based GC behavior.
     * Stale mappings are returned directly; removal is tracked for assertions.
     */
    static final class GenerationTrackingStore implements IndexMetadataStore {
        int mappingReadCalls;
        private final Map<String, IndexDescriptor> descriptors = new HashMap<>();
        private final List<IndexFileMapping> staleMappings = new ArrayList<>();
        final CopyOnWriteArrayList<String> removedMappings = new CopyOnWriteArrayList<>();
        boolean rejectMappingRemoval;
        final CopyOnWriteArrayList<String> purgedIndexes = new CopyOnWriteArrayList<>();

        void addDescriptor(IndexDescriptor d) { descriptors.put(d.indexId(), d); }
        void addStaleMapping(IndexFileMapping m) { staleMappings.add(m); }

        @Override public void withIndexLifecycleLock(String indexId, Runnable action) {
            action.run();
        }

        @Override public void registerIndex(IndexDescriptor d) {}
        @Override public void updateIndexState(String id, String s, Instant t) {}
        @Override public boolean beginIndexGeneration(String id, int next, String snapshot,
                Instant timestamp, Instant expected, IndexState state, int generation) { return false; }
        @Override public org.pangrova.persistence.indexing.api.metadata.IndexGenerationManifest publishIndexGeneration(
                org.pangrova.persistence.indexing.api.metadata.IndexPublicationRequest request) { throw new AssertionError(); }
        @Override public Optional<org.pangrova.persistence.indexing.api.metadata.IndexGenerationManifest>
                findActiveIndexGeneration(String id) { return Optional.empty(); }
        @Override public Optional<IndexDescriptor> findIndex(String id) {
            return Optional.ofNullable(descriptors.get(id));
        }
        @Override public List<IndexDescriptor> findIndexesByTable(String tenantId, String c, String s, String t) { return List.of(); }
        @Override public List<IndexDescriptor> findReadyIndexesForColumns(
                String tenantId, String c, String s, String t, List<String> cols, boolean includeStale) { return List.of(); }
        @Override public void dropIndex(String id, Instant t) {}
        @Override public void purgeIndex(String id) { purgedIndexes.add(id); descriptors.remove(id); }
        @Override public void batchRegisterIndexFiles(String owner, List<IndexFileMapping> m) {}
        @Override public Set<String> getIndexedDataFilePaths(String id, int generation) {
            return staleMappings.stream()
                    .filter(mapping -> id.equals(mapping.indexId()) && mapping.generation() == generation)
                    .map(IndexFileMapping::dataFilePath).collect(java.util.stream.Collectors.toUnmodifiableSet());
        }
        @Override public List<IndexFileMapping> getIndexFileMappings(String id) {
            mappingReadCalls++;
            return staleMappings.stream().filter(mapping -> id.equals(mapping.indexId())).toList();
        }
        @Override public Map<String, Map<String, String>> batchGetIndexFileMappings(
                List<String> ids, List<String> paths) { return Map.of(); }
        @Override public void removeIndexFileMapping(String id, String path) {
            removedMappings.add(id + ":" + path);
        }
        @Override
        public OrphanRemoval removeOrphanMappings(String indexId, IndexState expectedState,
                                                  int expectedGeneration, List<IndexFileMapping> mappings) {
            IndexDescriptor desc = descriptors.get(indexId);
            if (rejectMappingRemoval || desc == null
                    || desc.state() != expectedState || desc.generation() != expectedGeneration) {
                return new OrphanRemoval(0, java.util.Set.of());
            }
            int removed = 0;
            var paths = new java.util.HashSet<String>();
            for (var mapping : mappings) {
                if (staleMappings.remove(mapping)) {
                    removedMappings.add(mapping.indexId() + ":" + mapping.dataFilePath());
                    paths.add(mapping.indexFilePath());
                    removed++;
                }
            }
            staleMappings.stream().filter(mapping -> indexId.equals(mapping.indexId()))
                    .map(IndexFileMapping::indexFilePath).forEach(paths::remove);
            return new OrphanRemoval(removed, paths);
        }
        @Override public List<String> filterUnindexedFiles(String id, List<String> paths) { return List.of(); }
        @Override public void markOrphanedByDataFiles(String id, List<String> paths) {}
        @Override public List<IndexFileMapping> listStaleFileMappings(long ms) {
            return List.copyOf(staleMappings);
        }
        @Override public List<IndexFileMapping> listStaleFileMappings(long ms, Instant createdBefore) {
            return List.copyOf(staleMappings);
        }
        @Override public void updateLastVerifiedAt(String id, String path, long ts) {}
        @Override public List<IndexDescriptor> findIndexesByColumn(
                String tenantId, String c, String s, String t, String col) { return List.of(); }
        @Override public void renameColumnInIndexes(
                String tenantId, String c, String s, String t, String old, String n) {}
        @Override public void dropAllIndexesForTable(String tenantId, String c, String s, String t, Instant ts) {}
        @Override public void renameTable(String tenantId, String c, String s, String old, String n) {}
        @Override public List<IndexDescriptor> findIndexesByState(IndexState state) {
            return descriptors.values().stream().filter(d -> d.state() == state).toList();
        }
        @Override public void updateLastIndexedSnapshotId(String id, String snap) {}
        @Override public Map<String, IndexDescriptor> findIndexesByIds(List<String> indexIds) {
            Map<String, IndexDescriptor> result = new HashMap<>();
            for (String id : indexIds) {
                IndexDescriptor d = descriptors.get(id);
                if (d != null) result.put(id, d);
            }
            return result;
        }
    }

    /** Minimal CatalogDeploymentRepository for CatalogRegistry initialization. */
    static final class StubCatalogDeployments implements CatalogDeploymentRepository {
        @Override
        public java.util.Optional<org.pangrova.metadata.api.catalog.CatalogDescriptor>
                findCatalogDescriptorRevision(String name, String incarnationId, long revision) {
            throw new UnsupportedOperationException("This fixture does not store deployment history");
        }

        @Override public void saveCatalogDescriptor(CatalogDescriptor d) {}
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String n) { return Optional.empty(); }
        @Override public boolean deleteCatalogDescriptor(CatalogDeploymentIdentity expected) { throw new UnsupportedOperationException("Read-only catalog fixture"); }
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.of(); }

    }
}
