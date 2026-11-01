package org.pangrova.coordinator.indexing.lifecycle.gc;

import org.pangrova.coordinator.indexing.lifecycle.MaintenanceLeaderElection;
import org.pangrova.coordinator.indexing.ddl.ConnectorAccessorResolver;
import org.pangrova.indexing.config.IndexingConfig;
import org.pangrova.indexing.storage.IndexStorageManager;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.indexing.store.IndexMetadataStore;
import org.pangrova.indexing.store.IndexFileMapping;
import org.pangrova.metadata.api.catalog.MetadataStore;
import org.pangrova.persistence.api.coordination.DistributedLease;
import org.pangrova.spi.coordination.LeaseEntry;
import org.pangrova.spi.indexing.descriptor.IndexDescriptor;
import org.pangrova.spi.indexing.state.IndexState;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.catalog.SyncResult;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@link IndexGarbageCollector#runGc()} is leader-gated:
 * when the maintenance lease is not held, GC is a no-op.
 */
@DisplayName("IndexGarbageCollector Leader Gate")
class IndexGarbageCollectorLeaderGateTest {

    private IndexGarbageCollector gc;

    @AfterEach
    void tearDown() {
        if (gc != null) {
            gc.close();
        }
    }

    @Test
    @DisplayName("runGc() is no-op when lease not held")
    void runGcSkipsWhenNotLeader() {
        var env = createEnv(false);
        gc = env.gc;

        GcResult result = gc.runGc();

        assertThat(result.orphanedCount()).isZero();
        assertThat(result.purgedCount()).isZero();
        assertThat(env.store.listStaleCallCount.get())
                .as("runGc must not query stale mappings when not leader")
                .isZero();
        assertThat(env.store.findByStateCallCount.get())
                .as("runGc must not query dropped indexes when not leader")
                .isZero();
    }

    @Test
    @DisplayName("runGc() executes when leader")
    void runGcExecutesWhenLeader() {
        var env = createEnv(true);
        gc = env.gc;

        GcResult result = gc.runGc();

        assertThat(env.store.listStaleCallCount.get())
                .as("runGc must query stale mappings when leader")
                .isPositive();
        assertThat(env.store.findByStateCallCount.get())
                .as("runGc must query dropped indexes when leader")
                .isPositive();
    }

    @Test
    @DisplayName("runGc() without leader election always executes")
    void runGcWithoutLeaderElectionAlwaysExecutes() {
        var store = new TrackingIndexMetadataStore();
        var config = new IndexingConfig("/tmp/test-idx", false, null, null);
        var resolver = buildResolver();
        var deps = new IndexGcContext(store, new IndexStorageManager(),
                descriptor -> new org.pangrova.indexing.fs.LocalFileSystemAccessor(), config);
        gc = new IndexGarbageCollector(deps, null, null);

        gc.runGc();

        assertThat(store.listStaleCallCount.get())
                .as("runGc must execute when no leader election is configured")
                .isPositive();
    }

    // ── Test infrastructure ─────────────────────────────────────────────

    private TestEnv createEnv(boolean isLeader) {
        var store = new TrackingIndexMetadataStore();
        var lease = new ControllableLease(isLeader);
        var leaderElection = new MaintenanceLeaderElection(lease, "test-coord");
        var config = new IndexingConfig("/tmp/test-idx", false, null, null);
        var resolver = buildResolver();
        var deps = new IndexGcContext(store, new IndexStorageManager(),
                descriptor -> new org.pangrova.indexing.fs.LocalFileSystemAccessor(), config);
        var gc = new IndexGarbageCollector(deps, null, leaderElection);
        return new TestEnv(gc, store);
    }

    private static ConnectorAccessorResolver buildResolver() {
        var persistence = new StubMetadataStore();
        var registry = new CatalogRegistry(persistence, desc -> null, null, null);
        registry.initialize();
        return new ConnectorAccessorResolver(registry);
    }

    record TestEnv(IndexGarbageCollector gc, TrackingIndexMetadataStore store) {}

    static final class ControllableLease implements DistributedLease {
        private final boolean allowAcquire;

        ControllableLease(boolean allowAcquire) {
            this.allowAcquire = allowAcquire;
        }

        @Override
        public Optional<LeaseEntry> tryAcquire(String k, String h, Duration d) {
            return allowAcquire
                    ? Optional.of(new LeaseEntry(k, h, 1, Instant.now(), Instant.now().plus(d)))
                    : Optional.empty();
        }

        @Override
        public boolean tryRenew(String k, String h, long e, Duration d) {
            return allowAcquire;
        }

        @Override
        public boolean release(String k, String h, long e) { return true; }

        @Override
        public Optional<LeaseEntry> currentHolder(String k) {
            return Optional.empty();
        }

    }

    /** Metadata store that tracks method calls for assertion. */
    static final class TrackingIndexMetadataStore implements IndexMetadataStore {
        final AtomicInteger listStaleCallCount = new AtomicInteger();
        final AtomicInteger findByStateCallCount = new AtomicInteger();

        @Override public void registerIndex(IndexDescriptor d) {}
        @Override public void updateIndexState(String id, String s, Instant t) {}
        @Override public boolean beginIndexGeneration(String id, int next, String snapshot,
                Instant timestamp, Instant expected, IndexState state, int generation) { return false; }
        @Override public org.pangrova.indexing.store.IndexGenerationManifest publishIndexGeneration(
                org.pangrova.indexing.store.IndexPublicationRequest request) { throw new AssertionError(); }
        @Override public Optional<org.pangrova.indexing.store.IndexGenerationManifest>
                findActiveIndexGeneration(String id) { return Optional.empty(); }
        @Override public Optional<IndexDescriptor> findIndex(String id) { return Optional.empty(); }
        @Override public List<IndexDescriptor> findIndexesByTable(String tenantId, String c, String s, String t) { return List.of(); }
        @Override public List<IndexDescriptor> findReadyIndexesForColumns(
                String tenantId, String c, String s, String t, List<String> cols, boolean includeStale) { return List.of(); }
        @Override public void dropIndex(String id, Instant t) {}
        @Override public void purgeIndex(String id) {}
        @Override public void registerIndexFile(IndexFileMapping m) {}
        @Override public void batchRegisterIndexFiles(List<IndexFileMapping> m) {}
        @Override public List<IndexFileMapping> getIndexFileMappings(String id) { return List.of(); }
        @Override public Map<String, Map<String, String>> batchGetIndexFileMappings(
                List<String> ids, List<String> paths) { return Map.of(); }
        @Override public void removeIndexFileMapping(String id, String path) {}
        @Override public List<String> filterUnindexedFiles(String id, List<String> paths) { return List.of(); }
        @Override public void markOrphanedByDataFiles(String id, List<String> paths) {}

        @Override
        public List<IndexFileMapping> listStaleFileMappings(long ms) {
            listStaleCallCount.incrementAndGet();
            return List.of();
        }

        @Override
        public List<IndexFileMapping> listStaleFileMappings(long ms, Instant createdBefore) {
            listStaleCallCount.incrementAndGet();
            return List.of();
        }

        @Override public void updateLastVerifiedAt(String id, String path, long ts) {}
        @Override public List<IndexDescriptor> findIndexesByColumn(
                String tenantId, String c, String s, String t, String col) { return List.of(); }
        @Override public void renameColumnInIndexes(
                String tenantId, String c, String s, String t, String old, String n) {}
        @Override public void dropAllIndexesForTable(String tenantId, String c, String s, String t, Instant ts) {}
        @Override public void renameTable(String tenantId, String c, String s, String old, String n) {}

        @Override
        public List<IndexDescriptor> findIndexesByState(IndexState state) {
            findByStateCallCount.incrementAndGet();
            return List.of();
        }

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
