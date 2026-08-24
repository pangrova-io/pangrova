package org.pangrova.coordinator.index.lifecycle;

import org.pangrova.coordinator.index.ddl.ConnectorAccessorResolver;
import org.pangrova.indexing.build.IndexBuildContext;
import org.pangrova.indexing.build.IndexBuildCoordinator;
import org.pangrova.indexing.config.ConsistencyConfig;
import org.pangrova.indexing.config.IndexingConfig;
import org.pangrova.indexing.config.LifecycleConfig;
import org.pangrova.indexing.storage.IndexStorageManager;
import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.index.IndexMetadataStore;
import org.pangrova.metadata.index.model.IndexFileMapping;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.metadata.model.TableMetadata;
import org.pangrova.metadata.store.MetadataStore;
import org.pangrova.metadata.sync.SyncResult;
import org.pangrova.spi.index.IndexDescriptor;
import org.pangrova.spi.index.IndexState;
import org.pangrova.spi.index.IndexType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@link IndexConsistencyChecker} treats STALE as a transient
 * (recoverable) state rather than a terminal one: each cycle scans STALE
 * indexes and drives them back to READY when auto-rebuild is enabled.
 *
 * <p>Regression for architecture-remediation #161 — previously the checker only
 * paginated READY indexes, so STALE (e.g. parked by {@link StuckBuildRecovery})
 * had no automatic rebuild path despite the documented contract.</p>
 */
@DisplayName("IndexConsistencyChecker STALE rebuild")
class IndexConsistencyCheckerStaleRebuildTest {

    private IndexConsistencyChecker checker;

    @AfterEach
    void tearDown() {
        if (checker != null) {
            checker.close();
        }
    }

    @Test
    @DisplayName("STALE index is automatically rebuilt back to READY in a cycle")
    void staleIndexRebuiltToReady() {
        var store = new RecoveryTrackingStore();
        store.addDescriptor(buildDescriptor("idx-1", IndexState.STALE));

        checker = buildChecker(store, true);
        checker.checkAll();

        assertThat(store.findIndex("idx-1")).isPresent();
        assertThat(store.findIndex("idx-1").get().state()).isEqualTo(IndexState.READY);
        // It transitioned through BUILDING rather than jumping straight to READY.
        assertThat(store.transitions("idx-1"))
                .containsExactly(IndexState.BUILDING, IndexState.READY);
    }

    @Test
    @DisplayName("STALE index is left untouched when auto-rebuild is disabled")
    void staleIndexLeftWhenAutoRebuildDisabled() {
        var store = new RecoveryTrackingStore();
        store.addDescriptor(buildDescriptor("idx-1", IndexState.STALE));

        checker = buildChecker(store, false);
        checker.checkAll();

        assertThat(store.findIndex("idx-1").get().state()).isEqualTo(IndexState.STALE);
        assertThat(store.transitions("idx-1")).isEmpty();
    }

    // ── Helpers ──

    private IndexConsistencyChecker buildChecker(RecoveryTrackingStore store, boolean autoRebuild) {
        var lifecycle = new LifecycleConfig(
                null, new ConsistencyConfig(30, 0.8, autoRebuild, 2), null);
        var config = new IndexingConfig("/tmp/test-stale-rebuild", false, null, lifecycle);
        var buildCoordinator = new IndexBuildCoordinator(
                new IndexBuildContext(config, null, new IndexStorageManager(), null), null);
        var resolver = buildResolver();
        var deps = new org.pangrova.coordinator.index.lifecycle.IndexConsistencyContext(
                store, buildCoordinator, resolver, config);
        return new IndexConsistencyChecker(deps, null);
    }

    private static ConnectorAccessorResolver buildResolver() {
        var registry = new CatalogRegistry(new StubMetadataStore(), desc -> null, null, null);
        registry.initialize();
        return new ConnectorAccessorResolver(registry);
    }

    private static IndexDescriptor buildDescriptor(String indexId, IndexState state) {
        return new IndexDescriptor(
                indexId, "test-index", "cat", "sch", "tbl",
                List.of("col1"), IndexType.BITMAP, Map.of(),
                state, state == IndexState.STALE ? "data-change" : null, 0, null, null,
                Instant.now().minusSeconds(3600), Instant.now().minusSeconds(1800), "default");
    }

    /**
     * In-memory IndexMetadataStore that applies optimistic state transitions and
     * records the transition history per index for assertions.
     */
    static final class RecoveryTrackingStore implements IndexMetadataStore {
        private final Map<String, IndexDescriptor> descriptors = new HashMap<>();
        private final Map<String, List<IndexState>> transitions = new HashMap<>();

        void addDescriptor(IndexDescriptor d) {
            descriptors.put(d.indexId(), d);
        }

        List<IndexState> transitions(String indexId) {
            return transitions.getOrDefault(indexId, List.of());
        }

        private boolean apply(String indexId, String newState, Instant timestamp,
                              Instant expectedUpdatedAt, IndexState expectedState,
                              Integer expectedGeneration) {
            IndexDescriptor d = descriptors.get(indexId);
            if (d == null) {
                return false;
            }
            if (expectedUpdatedAt != null && !d.updatedAt().equals(expectedUpdatedAt)) {
                return false;
            }
            if (expectedState != null && d.state() != expectedState) {
                return false;
            }
            if (expectedGeneration != null && d.generation() != expectedGeneration) {
                return false;
            }
            IndexState target = IndexState.valueOf(newState);
            descriptors.put(indexId, d.withState(target, timestamp));
            transitions.computeIfAbsent(indexId, k -> new ArrayList<>()).add(target);
            return true;
        }

        @Override
        public void updateIndexState(String indexId, String newState, Instant timestamp) {
            apply(indexId, newState, timestamp, null, null, null);
        }

        @Override
        public boolean updateIndexState(String indexId, String newState,
                                        Instant timestamp, Instant expectedUpdatedAt) {
            return apply(indexId, newState, timestamp, expectedUpdatedAt, null, null);
        }

        @Override
        public boolean updateIndexState(String indexId, String newState, Instant timestamp,
                                        Instant expectedUpdatedAt, String expectedState,
                                        int expectedGeneration) {
            return apply(indexId, newState, timestamp, expectedUpdatedAt,
                    IndexState.valueOf(expectedState), expectedGeneration);
        }

        @Override
        public Optional<IndexDescriptor> findIndex(String indexId) {
            return Optional.ofNullable(descriptors.get(indexId));
        }

        @Override
        public List<IndexDescriptor> findIndexesByState(IndexState state) {
            return descriptors.values().stream().filter(d -> d.state() == state).toList();
        }

        @Override
        public List<IndexDescriptor> findIndexesByStatePaginated(IndexState state, int offset, int limit) {
            List<IndexDescriptor> all = findIndexesByState(state);
            if (offset >= all.size()) {
                return List.of();
            }
            return all.subList(offset, Math.min(offset + limit, all.size()));
        }

        @Override
        public List<IndexFileMapping> getIndexFileMappings(String indexId) {
            return List.of();
        }

        // ── Unused interface methods ──
        @Override public void registerIndex(IndexDescriptor d) {}
        @Override public List<IndexDescriptor> findIndexesByTable(String c, String s, String t) { return List.of(); }
        @Override public List<IndexDescriptor> findReadyIndexesForColumns(
                String c, String s, String t, List<String> cols, boolean includeStale) { return List.of(); }
        @Override public void dropIndex(String id, Instant t) {}
        @Override public void purgeIndex(String id) {}
        @Override public void registerIndexFile(IndexFileMapping m) {}
        @Override public void batchRegisterIndexFiles(List<IndexFileMapping> m) {}
        @Override public Map<String, Map<String, String>> batchGetIndexFileMappings(
                List<String> ids, List<String> paths) { return Map.of(); }
        @Override public void removeIndexFileMapping(String id, String path) {}
        @Override public List<String> filterUnindexedFiles(String id, List<String> paths) { return List.of(); }
        @Override public void markOrphanedByDataFiles(String id, List<String> paths) {}
        @Override public List<IndexFileMapping> listStaleFileMappings(long ms) { return List.of(); }
        @Override public void updateLastVerifiedAt(String id, String path, long ts) {}
        @Override public List<IndexDescriptor> findIndexesByColumn(
                String c, String s, String t, String col) { return List.of(); }
        @Override public void renameColumnInIndexes(String c, String s, String t, String o, String n) {}
        @Override public void dropAllIndexesForTable(String c, String s, String t, Instant ts) {}
        @Override public void renameTable(String c, String s, String o, String n) {}
        @Override public void updateLastIndexedSnapshotId(String id, String snap) {}
    }

    /** Minimal MetadataStore for CatalogRegistry initialization. */
    static final class StubMetadataStore implements MetadataStore {
        @Override public void saveTable(TableMetadata m) {}
        @Override public Optional<TableMetadata> loadTable(String c, String s, String t) { return Optional.empty(); }
        @Override public void deleteTable(String c, String s, String t) {}
        @Override public List<QualifiedTableRef> searchTables(String p) { return List.of(); }
        @Override public void saveCatalogDescriptor(CatalogDescriptor d) {}
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String n) { return Optional.empty(); }
        @Override public void deleteCatalogDescriptor(String n) {}
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.of(); }
        @Override public Optional<Instant> getLastSyncTime(String c) { return Optional.empty(); }
        @Override public void updateSyncTime(String c, Instant t) {}
        @Override public void recordSyncHistory(SyncResult r) {}
        @Override public void saveSchemas(String c, List<String> s) {}
    }
}
