package org.pangrova.coordinator.bootstrap;

import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.core.catalog.registry.CatalogRegistry;
import org.pangrova.indexing.store.IndexMetadataStore;
import org.pangrova.indexing.store.IndexFileMapping;
import org.pangrova.metadata.api.catalog.MetadataStore;
import org.pangrova.persistence.api.coordination.DistributedLease;
import org.pangrova.spi.coordination.LeaseEntry;
import org.pangrova.spi.indexing.descriptor.IndexDescriptor;
import org.pangrova.spi.indexing.state.IndexState;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Regression smoke test for the indexing-bootstrap startup ordering defect
 * (architecture-remediation #164).
 *
 * <p>{@link IndexingBootstrap#start} resolves the {@code SnapshotPinProvider}
 * from {@code accessorResolver} during build-coordinator construction (step 6).
 * Previously {@code accessorResolver} was not assigned until step 9, so step 6
 * threw a guaranteed {@link NullPointerException}. That exception was swallowed
 * by {@link ModuleIntegrationBootstrap#startIndexing} which then set
 * {@code indexingBootstrap = null}, silently disabling the entire indexing
 * subsystem.</p>
 *
 * <p>These tests fail (NPE / null bootstrap) before the ordering fix and pass
 * afterwards.</p>
 */
@DisplayName("IndexingBootstrap start() ordering")
class IndexingBootstrapStartOrderingTest {

    @TempDir
    Path indexDir;

    private ModuleIntegrationBootstrap wiring;

    @AfterEach
    void tearDown() {
        if (wiring != null) {
            wiring.close();
            wiring = null;
        }
    }

    @Test
    @DisplayName("start() completes without throwing (no swallowed init exception)")
    void startDoesNotThrow() {
        IndexingBootstrap bootstrap = new IndexingBootstrap(
                indexDir.toString(), new NoOpLease());
        try {
            assertThatCode(() -> bootstrap.start(
                    new NoOpIndexMetadataStore(),
                    new CatalogRegistry(new EmptyMetadataStore()),
                    null,
                    null))
                    .as("start() must not raise (and the NPE must not be silently swallowed)")
                    .doesNotThrowAnyException();
            assertThat(bootstrap.getAccessorResolver()).isNotNull();
            assertThat(bootstrap.getBuildCoordinator()).isNotNull();
            assertThat(bootstrap.getSplitFilter()).isNotNull();
            assertThat(bootstrap.getDdlExecutor()).isNotNull();
        } finally {
            bootstrap.close();
        }
    }

    @Test
    @DisplayName("ModuleIntegrationBootstrap.getIndexingBootstrap() is non-null after startIndexing()")
    void indexingBootstrapNonNullAfterStart() {
        wiring = new ModuleIntegrationBootstrap();

        wiring.startIndexing(
                new NoOpIndexMetadataStore(),
                new CatalogRegistry(new EmptyMetadataStore()),
                null,
                null,
                indexDir.toString(),
                new NoOpLease());

        assertThat(wiring.getIndexingBootstrap())
                .as("indexing bootstrap must be non-null — a swallowed init NPE leaves it null")
                .isNotNull();
        assertThat(wiring.getIndexingBootstrap().getAccessorResolver()).isNotNull();
    }

    // ── Fakes ───────────────────────────────────────────────────────────

    /** DistributedLease that grants nothing — sufficient for bootstrap wiring. */
    static final class NoOpLease implements DistributedLease {
        @Override public Optional<LeaseEntry> tryAcquire(String k, String h, Duration d) {
            return Optional.empty();
        }
        @Override public boolean tryRenew(String k, String h, long e, Duration d) { return false; }
        @Override public boolean release(String k, String h, long e) { return true; }
        @Override public Optional<LeaseEntry> currentHolder(String k) { return Optional.empty(); }
    }

    /** MetadataStore with no catalogs — CatalogRegistry has nothing to load. */
    static final class EmptyMetadataStore implements MetadataStore {
        @Override public void saveTable(org.pangrova.metadata.api.object.TableMetadata m) {}
        @Override public Optional<org.pangrova.metadata.api.object.TableMetadata> loadTable(
                String c, String s, String t) { return Optional.empty(); }
        @Override public void deleteTable(String c, String s, String t) {}
        @Override public List<org.pangrova.metadata.api.object.QualifiedTableRef> searchTables(String p) {
            return List.of();
        }
        @Override public void saveCatalogDescriptor(CatalogDescriptor d) {}
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String n) {
            return Optional.empty();
        }
        @Override public void deleteCatalogDescriptor(String n) {}
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.of(); }
        @Override public Optional<Instant> getLastSyncTime(String c) { return Optional.empty(); }
        @Override public void updateSyncTime(String c, Instant t) {}
        @Override public void recordSyncHistory(org.pangrova.metadata.api.catalog.SyncResult r) {}
        @Override public void saveSchemas(String c, List<String> s) {}
    }

    /** IndexMetadataStore implementing only the abstract surface as no-ops. */
    static final class NoOpIndexMetadataStore implements IndexMetadataStore {
        @Override public void registerIndex(IndexDescriptor d) {}
        @Override public void updateIndexState(String id, String s, Instant t) {}
        @Override public boolean beginIndexGeneration(String id, int next, String snapshot,
                Instant timestamp, Instant expected, IndexState state, int generation) { return false; }
        @Override public org.pangrova.indexing.store.IndexGenerationManifest publishIndexGeneration(
                org.pangrova.indexing.store.IndexPublicationRequest request) { throw new AssertionError(); }
        @Override public Optional<org.pangrova.indexing.store.IndexGenerationManifest>
                findActiveIndexGeneration(String id) { return Optional.empty(); }
        @Override public Optional<IndexDescriptor> findIndex(String id) { return Optional.empty(); }
        @Override public List<IndexDescriptor> findIndexesByTable(String tenantId, String c, String s, String t) {
            return List.of();
        }
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
        @Override public List<String> filterUnindexedFiles(String id, List<String> paths) {
            return List.of();
        }
        @Override public void markOrphanedByDataFiles(String id, List<String> paths) {}
        @Override public List<IndexFileMapping> listStaleFileMappings(long ms) { return List.of(); }
        @Override public void updateLastVerifiedAt(String id, String path, long ts) {}
        @Override public List<IndexDescriptor> findIndexesByColumn(
                String tenantId, String c, String s, String t, String col) { return List.of(); }
        @Override public void renameColumnInIndexes(
                String tenantId, String c, String s, String t, String o, String n) {}
        @Override public void dropAllIndexesForTable(String tenantId, String c, String s, String t, Instant ts) {}
        @Override public void renameTable(String tenantId, String c, String s, String o, String n) {}
        @Override public List<IndexDescriptor> findIndexesByState(IndexState state) {
            return List.of();
        }
        @Override public void updateLastIndexedSnapshotId(String id, String snap) {}
    }
}
