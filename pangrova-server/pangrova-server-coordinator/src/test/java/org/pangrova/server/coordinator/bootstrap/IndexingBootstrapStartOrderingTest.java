package org.pangrova.server.coordinator.bootstrap;

import org.pangrova.persistence.coordination.memory.lease.InMemoryDistributedLease;
import org.pangrova.coordinator.indexing.lifecycle.MaintenanceLeaderElection;
import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;

import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

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
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
                    new NoOpIndexMetadataStore(), new UnusedBuildClaims(),
                    new CatalogRegistry(memoryTableStores(), new EmptyCatalogDeployments()),
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
    @DisplayName("startup performs fenced recovery before starting maintenance schedulers")
    void startupRecoveryRunsBeforeMaintenanceSchedulers() {
        var store = new NoOpIndexMetadataStore();
        var leader = new MaintenanceLeaderElection(new InMemoryDistributedLease(), "startup-recovery");
        IndexingBootstrap bootstrap = new IndexingBootstrap(
                indexDir.toString(), new NoOpLease(), null, null, leader);
        try {
            bootstrap.start(store, new UnusedBuildClaims(),
                    new CatalogRegistry(memoryTableStores(), new EmptyCatalogDeployments()),
                    null, null);
            assertThat(store.recoveryLookups).isOne();
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
                new NoOpIndexMetadataStore(), new UnusedBuildClaims(),
                new CatalogRegistry(memoryTableStores(), new EmptyCatalogDeployments()),
                null,
                null,
                indexDir.toString(),
                new NoOpLease());

        assertThat(wiring.getIndexingBootstrap())
                .as("indexing bootstrap must be non-null — a swallowed init NPE leaves it null")
                .isNotNull();
        assertThat(wiring.getIndexingBootstrap().getAccessorResolver()).isNotNull();
    }

    @Test
    void schemaRecoveryWaitsForElectedMaintenanceLifecycle() throws Exception {
        var store = new NoOpIndexMetadataStore();
        var leader = new MaintenanceLeaderElection(new InMemoryDistributedLease(), "schema-recovery");
        IndexingBootstrap bootstrap = new IndexingBootstrap(
                indexDir.toString(), new NoOpLease(), null, null, leader);
        try {
            bootstrap.start(store, new UnusedBuildClaims(),
                    new CatalogRegistry(memoryTableStores(), new EmptyCatalogDeployments()), null, null);
            assertThat(bootstrap.getSchemaEvolutionHandler()).isNotNull();
            assertThat(store.schemaRecoveryStarted.getCount()).isOne();
            assertThat(store.schemaDiagnosticsStarted.getCount()).isOne();
            assertThat(leader.tryAcquireOrRenew()).isTrue();
            bootstrap.onLeaderElected();
            assertThat(store.schemaRecoveryStarted.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            assertThat(store.schemaDiagnosticsStarted.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            bootstrap.onLeadershipLost();
        } finally {
            bootstrap.close();
            leader.release();
        }
    }

    @Test
    void failedStartupDoesNotPublishPartialBootstrapOrRetainLease() {
        wiring = new ModuleIntegrationBootstrap();
        var store = new NoOpIndexMetadataStore();
        store.failRecovery = true;
        var lease = new InMemoryDistributedLease();

        wiring.startIndexing(store, new UnusedBuildClaims(),
                new CatalogRegistry(memoryTableStores(), new EmptyCatalogDeployments()),
                null, null, indexDir.toString(), lease);

        assertThat(wiring.getIndexingBootstrap()).isNull();
        assertThat(lease.currentHolder("maintenance-control")).isEmpty();
    }

    private static final class UnusedBuildClaims
            implements org.pangrova.persistence.indexing.api.build.IndexBuildClaimRepository {
        @Override
        public boolean tryClaim(String index, int generation, String owner, java.time.Instant now) {
            throw new AssertionError("Startup must not acquire a build claim");
        }

        @Override
        public boolean release(String index, int generation, String owner) {
            throw new AssertionError("Startup must not release a build claim");
        }

        @Override
        public boolean heartbeat(String index, int generation, String owner, java.time.Instant now) {
            throw new AssertionError("Startup must not renew a build claim");
        }
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

    @Test
    void schemaDiagnosticReadFailureDoesNotBlockConfirmedRecovery() throws Exception {
        var store = new NoOpIndexMetadataStore();
        store.failSchemaDiagnostics = true;
        var leader = new MaintenanceLeaderElection(new InMemoryDistributedLease(), "schema-diagnostics");
        IndexingBootstrap bootstrap = new IndexingBootstrap(indexDir.toString(), new NoOpLease(), null, null, leader);
        try {
            bootstrap.start(store, new UnusedBuildClaims(),
                    new CatalogRegistry(memoryTableStores(), new EmptyCatalogDeployments()), null, null);
            assertThat(leader.tryAcquireOrRenew()).isTrue();
            bootstrap.onLeaderElected();
            assertThat(store.schemaDiagnosticsStarted.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            assertThat(store.schemaRecoveryStarted.await(5, java.util.concurrent.TimeUnit.SECONDS)).isTrue();
        } finally {
            bootstrap.close();
            leader.release();
        }
    }

    /** CatalogDeploymentRepository with no catalogs — CatalogRegistry has nothing to load. */
    static final class EmptyCatalogDeployments implements CatalogDeploymentRepository {
        @Override
        public java.util.Optional<org.pangrova.metadata.api.catalog.CatalogDescriptor>
                findCatalogDescriptorRevision(String name, String incarnationId, long revision) {
            throw new UnsupportedOperationException("This fixture does not store deployment history");
        }

        @Override public void saveCatalogDescriptor(CatalogDescriptor d) {}
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String n) {
            return Optional.empty();
        }
        @Override public boolean deleteCatalogDescriptor(CatalogDeploymentIdentity expected) { throw new UnsupportedOperationException("Read-only catalog fixture"); }
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.of(); }

    }

    /** IndexMetadataStore implementing only the abstract surface as no-ops. */
    static final class NoOpIndexMetadataStore implements IndexMetadataStore {
        private int recoveryLookups;
        private boolean failRecovery;
        private boolean failSchemaDiagnostics;
        private final java.util.concurrent.CountDownLatch schemaRecoveryStarted =
                new java.util.concurrent.CountDownLatch(1);

        private final java.util.concurrent.CountDownLatch schemaDiagnosticsStarted =
                new java.util.concurrent.CountDownLatch(1);

        @Override
        public List<org.pangrova.persistence.indexing.api.metadata.IndexSchemaEvolutionStore.IndexSchemaEvolutionRecord>
                listUnresolvedSchemaEvolution(int limit) {
            schemaDiagnosticsStarted.countDown();
            if (failSchemaDiagnostics) {
                throw new IllegalStateException("Injected diagnostic read failure");
            }
            return List.of();
        }

        @Override
        public List<org.pangrova.persistence.indexing.api.metadata.IndexSchemaEvolutionStore.IndexSchemaEvolutionRecord>
                listPendingSchemaEvolution(int limit) {
            schemaRecoveryStarted.countDown();
            return List.of();
        }

        @Override
        public List<org.pangrova.persistence.indexing.api.metadata.IndexBuildGenerations.IndexBuildCandidate>
                findIndexBuildRecoveryCandidates() {
            recoveryLookups++;
            if (failRecovery) {
                throw new IllegalStateException("Injected startup recovery failure");
            }
            return List.of();
        }

        @Override public void registerIndex(IndexDescriptor d) {}
        @Override public void updateIndexState(String id, String s, Instant t) {}
        @Override public boolean beginIndexGeneration(String id, int next, String snapshot,
                Instant timestamp, Instant expected, IndexState state, int generation) { return false; }
        @Override public org.pangrova.persistence.indexing.api.metadata.IndexGenerationManifest publishIndexGeneration(
                org.pangrova.persistence.indexing.api.metadata.IndexPublicationRequest request) { throw new AssertionError(); }
        @Override public Optional<org.pangrova.persistence.indexing.api.metadata.IndexGenerationManifest>
                findActiveIndexGeneration(String id) { return Optional.empty(); }
        @Override public Optional<IndexDescriptor> findIndex(String id) { return Optional.empty(); }
        @Override public List<IndexDescriptor> findIndexesByTable(String tenantId, String c, String s, String t) {
            return List.of();
        }
        @Override public List<IndexDescriptor> findReadyIndexesForColumns(
                String tenantId, String c, String s, String t, List<String> cols, boolean b) { return List.of(); }
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
