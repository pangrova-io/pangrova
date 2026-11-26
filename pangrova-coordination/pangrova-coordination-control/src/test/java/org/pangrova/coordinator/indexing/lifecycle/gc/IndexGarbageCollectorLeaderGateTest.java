package org.pangrova.coordinator.indexing.lifecycle.gc;

import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;

import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

import org.pangrova.coordinator.indexing.lifecycle.MaintenanceLeaderElection;
import org.pangrova.coordinator.indexing.connector.ConnectorAccessorResolver;
import org.pangrova.indexing.config.IndexingConfig;
import org.pangrova.indexing.storage.IndexStorageManager;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;
import org.pangrova.persistence.indexing.api.metadata.IndexFileMapping;
import org.pangrova.persistence.catalog.api.catalog.CatalogDeploymentRepository;
import org.pangrova.persistence.coordination.api.lease.DistributedLease;
import org.pangrova.coordination.api.lease.LeaseEntry;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
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
                descriptor -> new org.pangrova.indexing.storage.LocalFileSystemAccessor(), config, null);
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
                descriptor -> new org.pangrova.indexing.storage.LocalFileSystemAccessor(), config, null);
        var gc = new IndexGarbageCollector(deps, null, leaderElection);
        return new TestEnv(gc, store);
    }

    private static ConnectorAccessorResolver buildResolver() {
        var persistence = new StubCatalogDeployments();
        var registry = new CatalogRegistry(memoryTableStores(), persistence, desc -> null, null, null);
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
        @Override public org.pangrova.persistence.indexing.api.metadata.IndexGenerationManifest publishIndexGeneration(
                org.pangrova.persistence.indexing.api.metadata.IndexPublicationRequest request) { throw new AssertionError(); }
        @Override public Optional<org.pangrova.persistence.indexing.api.metadata.IndexGenerationManifest>
                findActiveIndexGeneration(String id) { return Optional.empty(); }
        @Override public Optional<IndexDescriptor> findIndex(String id) { return Optional.empty(); }
        @Override public List<IndexDescriptor> findIndexesByTable(String tenantId, String c, String s, String t) { return List.of(); }
        @Override public List<IndexDescriptor> findReadyIndexesForColumns(
                String tenantId, String c, String s, String t, List<String> cols, boolean includeStale) { return List.of(); }
        @Override public void dropIndex(String id, Instant t) {}
        @Override public void purgeIndex(String id) {}
        @Override public void batchRegisterIndexFiles(String owner, List<IndexFileMapping> m) {}
        @Override public Set<String> getIndexedDataFilePaths(String id, int generation) {
            return Set.of();
        }
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
            throw new AssertionError("GC must not materialize all descriptors");
        }

        @Override
        public MaintenancePage loadMaintenancePage(IndexState state, Instant cutoff, int limit) {
            findByStateCallCount.incrementAndGet();
            return new MaintenancePage(state, 0, null, List.of(), true);
        }

        @Override
        public boolean completeMaintenancePage(MaintenancePage page) {
            return true;
        }

        @Override public void updateLastIndexedSnapshotId(String id, String snap) {}
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
