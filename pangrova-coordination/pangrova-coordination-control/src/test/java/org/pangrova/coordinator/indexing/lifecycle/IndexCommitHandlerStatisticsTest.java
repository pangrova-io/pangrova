package org.pangrova.coordinator.indexing.lifecycle;

import org.pangrova.indexing.codec.data.TypeSerializerRegistry;
import org.pangrova.indexing.api.strategy.IndexStrategyProvider;
import java.util.ServiceLoader;
import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;

import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;


import org.pangrova.coordinator.indexing.connector.ConnectorAccessorResolver;
import org.pangrova.indexing.strategy.IndexEngineRegistry;
import org.pangrova.indexing.build.IndexBuildContext;
import org.pangrova.indexing.build.IndexBuildCoordinator;
import org.pangrova.indexing.config.IndexingConfig;
import org.pangrova.indexing.codec.data.IndexSerializer;
import org.pangrova.indexing.storage.IndexStorageManager;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.connector.ConnectorCatalogHandle;
import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;
import org.pangrova.persistence.indexing.api.metadata.IndexFileMapping;
import org.pangrova.persistence.indexing.api.metadata.IndexGenerationFormat;
import org.pangrova.persistence.indexing.api.metadata.IndexGenerationManifest;
import org.pangrova.connector.api.plugin.Connector;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.data.ConnectorPageSourceProvider;
import org.pangrova.connector.api.enums.CommitEventType;
import org.pangrova.connector.api.enums.SnapshotRelation;
import org.pangrova.connector.api.index.SnapshotAwareMetadata;
import org.pangrova.connector.api.index.SnapshotDataFile;
import org.pangrova.connector.api.index.SnapshotFileDelta;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.ConnectorSplitManager;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.connector.api.index.statistics.DataFileStatistics;
import org.pangrova.connector.api.index.statistics.FileColumnStatistics;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that {@link IndexCommitHandler#collectFileStatistics} extracts
 * file-level statistics from the connector and persists them via
 * {@code batchInsertFileStatistics()} for L0 pruning (Req 2.AC3).
 */
class IndexCommitHandlerStatisticsTest {

    private IndexCommitHandler handler;
    private IndexBuildCoordinator buildCoordinator;

    @AfterEach
    void tearDown() {
        if (handler != null) handler.close();
        if (buildCoordinator != null) buildCoordinator.close();
    }

    @Test
    void collectsAndPersistsStatistics() {
        var env = createEnv(statsFor("f1.parquet"));
        var files = merged(CommitEventType.APPEND, List.of("f1.parquet"), List.of());

        env.handler.collectFileStatistics(env.store, files);

        assertThat(env.store.insertedStats).hasSize(1);
        assertThat(env.store.insertedStats.get(0).dataFilePath()).isEqualTo("f1.parquet");
    }

    @Test
    void unscopedMaintenanceCannotSilentlyAcceptCommit() {
        var env = createUnscopedEnv(statsFor("f1.parquet"));
        env.store.indexesForTable = List.of(new IndexDescriptor(
                "index-1", "test-index", "cat", "sch", "tbl", List.of("col1"),
                IndexType.BLOOM_FILTER, Map.of(), IndexState.READY, null, 0,
                "snap-0", "snap-0", Instant.now(), Instant.now(), "tenant-a"));
        var files = merged(CommitEventType.OVERWRITE, List.of("f1.parquet"), List.of("old.parquet"));

        assertThatThrownBy(() -> env.handler.processTableCommit(files))
                .isInstanceOf(org.pangrova.security.api.exception.AccessDeniedException.class)
                .hasMessageContaining("trusted authorization scope");
        assertThat(env.store.insertedStats).isEmpty();
        assertThat(env.store.deletedStatsPaths).isEmpty();
    }

    @Test
    void commitWithoutReadyIndexesSkipsConnectorLookup() {
        var env = createEnv(statsFor("f1.parquet"));
        var files = merged(CommitEventType.APPEND, List.of("f1.parquet"), List.of());
        var processed = new AtomicInteger();
        env.handler.registerDurableCommitProcessor(event -> processed.incrementAndGet());

        env.handler.processTableCommit(files);

        assertThat(env.store.insertedStats).isEmpty();
        assertThat(processed.get()).isEqualTo(1);
    }

    @Test
    void replayedAndSupersededCommitEventsSkipFileEnumeration() {
        var snapshots = new SupersededSnapshotMetadata();
        var env = buildTestEnv(new StatisticsConnectorMetadata(List.of(), snapshots), "sch", true);
        var timestamp = Instant.now();
        var descriptor = new IndexDescriptor(
                "index-1", "test-index", "cat", "sch", "tbl", List.of("col1"),
                IndexType.BLOOM_FILTER, Map.of(), IndexState.READY, null, 0,
                "snap-0", "snap-0", timestamp, timestamp, "tenant-a");
        env.store.indexesForTable = List.of(descriptor);
        var processed = new AtomicInteger();
        env.handler.registerDurableCommitProcessor(event -> processed.incrementAndGet());

        var format = new IndexGenerationFormat("bloom-filter", 1, 0, "a".repeat(64), "fixture");
        for (String snapshot : List.of("snap-1", "snap-2")) {
            env.store.active = new IndexGenerationManifest(descriptor.indexId(), "cat.sch.tbl", 0, snapshot,
                    "BLOOM_FILTER", format, 1, 1, 128, "b".repeat(64), 1, timestamp);
            env.handler.processTableCommit(merged(CommitEventType.APPEND, List.of(), List.of()));
        }

        assertThat(snapshots.lineageReads.get()).isOne();
        assertThat(snapshots.fileLists.get()).isZero();
        assertThat(processed.get()).isEqualTo(2);
    }

    @Test
    void multipleFilesCollectedInBatch() {
        var env = createEnv(statsFor("a.parquet", "b.parquet"));
        var files = merged(CommitEventType.APPEND,
                List.of("a.parquet", "b.parquet"), List.of());

        env.handler.collectFileStatistics(env.store, files);

        assertThat(env.store.insertedStats).hasSize(2);
        assertThat(env.store.insertedStats)
                .extracting(DataFileStatistics::dataFilePath)
                .containsExactlyInAnyOrder("a.parquet", "b.parquet");
    }

    @Test
    void emptyConnectorStatsDoesNotInsert() {
        var env = createEnv(List.of());
        var files = merged(CommitEventType.APPEND, List.of("f1.parquet"), List.of());

        env.handler.collectFileStatistics(env.store, files);

        assertThat(env.store.insertedStats).isEmpty();
    }

    @Test
    void connectorExceptionDoesNotPropagate() {
        var env = createEnvWithFailingConnector();
        var files = merged(CommitEventType.APPEND, List.of("f1.parquet"), List.of());

        // Should not throw — statistics collection is best-effort
        env.handler.collectFileStatistics(env.store, files);

        assertThat(env.store.insertedStats).isEmpty();
    }

    @Test
    void preservesCatalogSchemaTableContext() {
        var env = createEnv(statsFor("f1.parquet"), "my_sch");
        var files = new IndexCommitHandler.MergedCommitFiles(
                "tenant-a", "cat", "my_sch", "my_tbl",
                new ArrayList<>(List.of("f1.parquet")), new ArrayList<>(),
                CommitEventType.APPEND, "snap-1", null);

        env.handler.collectFileStatistics(env.store, files);

        assertThat(env.store.lastCatalog).isEqualTo("cat");
        assertThat(env.store.lastSchema).isEqualTo("my_sch");
        assertThat(env.store.lastTable).isEqualTo("my_tbl");
    }

    @Test
    void appendOverwriteCompactWithoutReadyIndexesDoNotReadSourceStatistics() {
        for (CommitEventType type : List.of(
                CommitEventType.APPEND, CommitEventType.OVERWRITE, CommitEventType.COMPACT)) {
            var env = createEnv(statsFor("f.parquet"));
            var files = merged(type, List.of("f.parquet"), List.of("old.parquet"));

            env.handler.processTableCommit(files);

            assertThat(env.store.insertedStats)
                    .as("Event type %s must not read source statistics without a READY index", type)
                    .isEmpty();
            tearDown();
        }
    }

    @Test
    void deleteDoesNotTriggerStatisticsInProcessTableCommit() {
        var env = createEnv(List.of());
        var files = merged(CommitEventType.DELETE, List.of(), List.of("removed.parquet"));

        env.handler.processTableCommit(files);

        assertThat(env.store.insertedStats).isEmpty();
    }

    @Test
    void overwriteDeletesStatisticsForRemovedFiles() {
        var env = createEnv(statsFor("new.parquet"));
        var files = merged(CommitEventType.OVERWRITE,
                List.of("new.parquet"), List.of("old1.parquet", "old2.parquet"));

        env.handler.processTableCommit(files);

        assertThat(env.store.deletedStatsPaths)
                .containsExactlyInAnyOrder("old1.parquet", "old2.parquet");
        assertThat(env.store.deleteCatalog).isEqualTo("cat");
        assertThat(env.store.deleteSchema).isEqualTo("sch");
        assertThat(env.store.deleteTable).isEqualTo("tbl");
    }

    @Test
    void compactDeletesStatisticsForRemovedFiles() {
        var env = createEnv(statsFor("merged.parquet"));
        var files = merged(CommitEventType.COMPACT,
                List.of("merged.parquet"), List.of("frag1.parquet", "frag2.parquet"));

        env.handler.processTableCommit(files);

        assertThat(env.store.deletedStatsPaths)
                .containsExactlyInAnyOrder("frag1.parquet", "frag2.parquet");
    }

    @Test
    void appendDoesNotDeleteStatistics() {
        var env = createEnv(statsFor("new.parquet"));
        var files = merged(CommitEventType.APPEND, List.of("new.parquet"), List.of());

        env.handler.processTableCommit(files);

        assertThat(env.store.deletedStatsPaths).isEmpty();
    }

    // --- helpers ---

    private static IndexCommitHandler.MergedCommitFiles merged(
            CommitEventType type, List<String> added, List<String> removed) {
        return new IndexCommitHandler.MergedCommitFiles(
                "tenant-a", "cat", "sch", "tbl",
                new ArrayList<>(added), new ArrayList<>(removed), type, "snap-1", null);
    }

    private static List<DataFileStatistics> statsFor(String... paths) {
        List<DataFileStatistics> result = new ArrayList<>();
        for (String path : paths) {
            result.add(new DataFileStatistics(path, "snap-1", 100, 1024,
                    List.of(new FileColumnStatistics("col1",
                            (Comparable<?>) 1, (Comparable<?>) 100,
                            0L, 100L, 50L, 1024L))));
        }
        return result;
    }

    private record TestEnv(
            IndexCommitHandler handler,
            RecordingMetadataStore store,
            IndexBuildCoordinator buildCoordinator) {}

    private TestEnv createEnv(List<DataFileStatistics> connectorStats) {
        return createEnv(connectorStats, "sch");
    }

    private TestEnv createEnv(List<DataFileStatistics> connectorStats, String schema) {
        return buildTestEnv(new StatisticsConnectorMetadata(connectorStats), schema, true);
    }

    private TestEnv createUnscopedEnv(List<DataFileStatistics> connectorStats) {
        return buildTestEnv(new StatisticsConnectorMetadata(connectorStats), "sch", false);
    }

    private TestEnv createEnvWithFailingConnector() {
        return buildTestEnv(new FailingConnectorMetadata(), "sch", true);
    }

    private TestEnv buildTestEnv(ConnectorMetadata connectorMetadata, String schema, boolean scoped) {
        var store = new RecordingMetadataStore();
        store.authorizedFixture = scoped;
        var connector = new MinimalConnector(connectorMetadata);
        var catDesc = new CatalogDescriptor(
                "cat", CatalogType.CONNECTOR, "test-provider",
                CatalogState.ACTIVE, Map.of(), "", Set.of());
        var catalogHandle = new ConnectorCatalogHandle(catDesc, connector);

        var persistence = new MinimalPersistenceStore(catDesc);
        var registry = new CatalogRegistry(memoryTableStores(),
                persistence, desc -> catalogHandle, null, null);
        registry.initialize();

        var resolver = new ConnectorAccessorResolver(registry);
        if (scoped) {
            // This is a connector statistics unit fixture. It binds a concrete
            // operation identity; production maintenance still requires a
            // persisted subject and current security reauthorization.
            resolver = resolver.forSession(new ConnectorSession() {
                @Override public Optional<org.pangrova.connector.api.query.ConnectorQueryAuthority>
                        getQueryAuthority() { return Optional.empty(); }
                @Override public String getUser() { return "statistics-fixture-reader"; }
                @Override public Optional<String> getCatalog() { return Optional.of("cat"); }
                @Override public Optional<String> getSchema() { return Optional.of(schema); }
                @Override public Map<String, String> getProperties() {
                    return Map.of("tenant_id", "tenant-a");
                }
            });
        }

        // Create minimal IndexBuildCoordinator (required non-null by IndexCommitContext)
        var config = new IndexingConfig("/tmp/test-index", false, null, null);
        var engineRegistry = new IndexEngineRegistry(List.of());
        var storageManager = new IndexStorageManager();
        var serializer = new IndexSerializer(TypeSerializerRegistry.fromStrategies(ServiceLoader.load(IndexStrategyProvider.class)));
        var buildCtx = new IndexBuildContext(config, engineRegistry, storageManager, serializer);
        buildCoordinator = new IndexBuildCoordinator(buildCtx, null);

        var deps = new IndexCommitContext(
                store,
                new org.pangrova.coordinator.indexing.ddl.IndexBuildServices(
                        buildCoordinator, null),
                resolver);
        handler = new IndexCommitHandler(deps, null, 8);
        return new TestEnv(handler, store, buildCoordinator);
    }

    /** Records batchInsertFileStatistics calls. */
    static final class RecordingMetadataStore implements IndexMetadataStore {
        final List<DataFileStatistics> insertedStats =
                Collections.synchronizedList(new ArrayList<>());
        final List<String> deletedStatsPaths =
                Collections.synchronizedList(new ArrayList<>());
        volatile String lastCatalog;
        volatile String lastSchema;
        volatile String lastTable;
        volatile String deleteCatalog;
        volatile String deleteSchema;
        volatile String deleteTable;
        List<IndexDescriptor> indexesForTable = List.of();
        IndexGenerationManifest active;
        boolean authorizedFixture;

        @Override public String loadAuthorizedBuildPrincipal(IndexDescriptor descriptor) {
            if (!authorizedFixture) {
                throw new org.pangrova.security.api.exception.AccessDeniedException(
                        "Index connector access requires a trusted authorization scope");
            }
            return "statistics-fixture-reader";
        }

        @Override
        public void batchInsertFileStatistics(String tenantId, String catalog, String schema,
                String table, List<DataFileStatistics> statistics) {
            lastCatalog = catalog;
            lastSchema = schema;
            lastTable = table;
            insertedStats.addAll(statistics);
        }

        @Override
        public void deleteFileStatistics(String tenantId, String catalog, String schema,
                String table, List<String> dataFilePaths) {
            deleteCatalog = catalog;
            deleteSchema = schema;
            deleteTable = table;
            deletedStatsPaths.addAll(dataFilePaths);
        }

        @Override public void registerIndex(IndexDescriptor d) {}
        @Override public void updateIndexState(String id, String s, Instant t) {}
        @Override public boolean beginIndexGeneration(String id, int next, String snapshot,
                Instant timestamp, Instant expected, IndexState state, int generation) { return false; }
        @Override public org.pangrova.persistence.indexing.api.metadata.IndexGenerationManifest publishIndexGeneration(
                org.pangrova.persistence.indexing.api.metadata.IndexPublicationRequest request) { throw new AssertionError(); }
        @Override public Optional<IndexGenerationManifest> findActiveIndexGeneration(String id) {
            return Optional.ofNullable(active).filter(manifest -> manifest.indexId().equals(id));
        }
        @Override public Optional<IndexDescriptor> findIndex(String id) { return Optional.empty(); }
        @Override public List<IndexDescriptor> findIndexesByTable(String tenantId, String c, String s, String t) { return indexesForTable; }
        @Override public List<IndexDescriptor> findReadyIndexesForColumns(String tenantId, String c, String s, String t, List<String> cols, boolean includeStale) { return List.of(); }
        @Override public void dropIndex(String id, Instant t) {}
        @Override public void purgeIndex(String id) {}
        @Override public void batchRegisterIndexFiles(String owner, List<IndexFileMapping> m) {}
        @Override public List<IndexFileMapping> getIndexFileMappings(String id) { return List.of(); }
        @Override public Map<String, Map<String, String>> batchGetIndexFileMappings(List<String> ids, List<String> paths) { return Map.of(); }
        @Override public void removeIndexFileMapping(String id, String path) {}
        @Override public List<String> filterUnindexedFiles(String id, List<String> paths) { return List.of(); }
        @Override public void markOrphanedByDataFiles(String id, List<String> paths) {}
        @Override public List<IndexFileMapping> listStaleFileMappings(long ms) { return List.of(); }
        @Override public void updateLastVerifiedAt(String id, String path, long ts) {}
        @Override public List<IndexDescriptor> findIndexesByColumn(String tenantId, String c, String s, String t, String col) { return List.of(); }
        @Override public void renameColumnInIndexes(String tenantId, String c, String s, String t, String o, String n) {}
        @Override public void dropAllIndexesForTable(String tenantId, String c, String s, String t, Instant ts) {}
        @Override public void renameTable(String tenantId, String c, String s, String o, String n) {}
        @Override public List<IndexDescriptor> findIndexesByState(IndexState state) { return List.of(); }
        @Override public void updateLastIndexedSnapshotId(String id, String snap) {}
    }

    private static final class SupersededSnapshotMetadata implements SnapshotAwareMetadata {
        private final AtomicInteger lineageReads = new AtomicInteger();
        private final AtomicInteger fileLists = new AtomicInteger();

        @Override public Optional<String> currentSnapshotId() { throw new AssertionError("Expected exact commit"); }
        @Override public Optional<CurrentSnapshotFiles> currentSnapshotFiles() {
            throw new AssertionError("Expected exact commit");
        }
        @Override public SnapshotRelation relation(String left, String right) {
            assertThat(left).isEqualTo("snap-2");
            assertThat(right).isEqualTo("snap-1");
            lineageReads.incrementAndGet();
            return SnapshotRelation.RIGHT_ANCESTOR;
        }
        @Override public List<SnapshotDataFile> listFilesAtSnapshot(String snapshotId) {
            fileLists.incrementAndGet();
            return List.of();
        }
        @Override public SnapshotFileDelta diffFiles(String from, String to) {
            throw new AssertionError("Superseded commits have no new file delta");
        }
    }

    /** ConnectorMetadata returning pre-configured file statistics. */
    static final class StatisticsConnectorMetadata implements ConnectorMetadata {
        private final List<DataFileStatistics> stats;
        private final SnapshotAwareMetadata snapshots;

        StatisticsConnectorMetadata(List<DataFileStatistics> stats) { this(stats, null); }

        StatisticsConnectorMetadata(List<DataFileStatistics> stats, SnapshotAwareMetadata snapshots) {
            this.stats = stats;
            this.snapshots = snapshots;
        }

        @Override
        public Optional<SnapshotAwareMetadata> getSnapshotAwareMetadata(String schema, String table) {
            return Optional.ofNullable(snapshots);
        }

        @Override
        public List<DataFileStatistics> getFileStatistics(
                String cat, String sch, String tbl, List<String> paths) {
            return stats;
        }

        @Override public List<String> listSchemaNames(ConnectorSession s) { return List.of(); }
        @Override public List<org.pangrova.connector.api.metadata.table.SchemaTableName> listTables(ConnectorSession s, Optional<String> schema) { return List.of(); }
        @Override public org.pangrova.connector.api.metadata.table.ConnectorTableHandle getTableHandle(ConnectorSession s, org.pangrova.connector.api.metadata.table.SchemaTableName n) { return null; }
        @Override public org.pangrova.connector.api.metadata.table.ConnectorTableMetadata getTableMetadata(ConnectorSession s, org.pangrova.connector.api.metadata.table.ConnectorTableHandle t) { return null; }
        @Override public Map<String, org.pangrova.connector.api.metadata.table.ColumnHandle> getColumnHandles(ConnectorSession s, org.pangrova.connector.api.metadata.table.ConnectorTableHandle t) { return Map.of(); }
        @Override public org.pangrova.connector.api.metadata.table.TableStatistics getTableStatistics(ConnectorSession s, org.pangrova.connector.api.metadata.table.ConnectorTableHandle t) { return org.pangrova.connector.api.metadata.table.TableStatistics.UNKNOWN; }
    }

    /** ConnectorMetadata that throws on getFileStatistics. */
    static final class FailingConnectorMetadata implements ConnectorMetadata {
        @Override
        public List<DataFileStatistics> getFileStatistics(
                String cat, String sch, String tbl, List<String> paths) {
            throw new RuntimeException("Simulated connector failure");
        }

        @Override public List<String> listSchemaNames(ConnectorSession s) { return List.of(); }
        @Override public List<org.pangrova.connector.api.metadata.table.SchemaTableName> listTables(ConnectorSession s, Optional<String> schema) { return List.of(); }
        @Override public org.pangrova.connector.api.metadata.table.ConnectorTableHandle getTableHandle(ConnectorSession s, org.pangrova.connector.api.metadata.table.SchemaTableName n) { return null; }
        @Override public org.pangrova.connector.api.metadata.table.ConnectorTableMetadata getTableMetadata(ConnectorSession s, org.pangrova.connector.api.metadata.table.ConnectorTableHandle t) { return null; }
        @Override public Map<String, org.pangrova.connector.api.metadata.table.ColumnHandle> getColumnHandles(ConnectorSession s, org.pangrova.connector.api.metadata.table.ConnectorTableHandle t) { return Map.of(); }
        @Override public org.pangrova.connector.api.metadata.table.TableStatistics getTableStatistics(ConnectorSession s, org.pangrova.connector.api.metadata.table.ConnectorTableHandle t) { return org.pangrova.connector.api.metadata.table.TableStatistics.UNKNOWN; }
    }

    /** Minimal Connector delegating to a ConnectorMetadata. */
    static final class MinimalConnector implements Connector {
        private final ConnectorMetadata metadata;
        MinimalConnector(ConnectorMetadata metadata) { this.metadata = metadata; }

        @Override public ConnectorMetadata getMetadata(ConnectorSession s) { return metadata; }
        @Override public ConnectorSplitManager getSplitManager() { return null; }
        @Override public ConnectorPageSourceProvider getPageSourceProvider() { return null; }
    }

    /** Minimal CatalogDeploymentRepository for CatalogRegistry initialization. */
    static final class MinimalPersistenceStore implements org.pangrova.persistence.catalog.api.catalog.CatalogDeploymentRepository {
        @Override
        public java.util.Optional<org.pangrova.metadata.api.catalog.CatalogDescriptor>
                findCatalogDescriptorRevision(String name, String incarnationId, long revision) {
            throw new UnsupportedOperationException("This fixture does not store deployment history");
        }

        private final CatalogDescriptor descriptor;
        MinimalPersistenceStore(CatalogDescriptor d) { this.descriptor = d; }

        @Override public void saveCatalogDescriptor(CatalogDescriptor d) {}
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String n) { return descriptor.name().equals(n) ? Optional.of(descriptor) : Optional.empty(); }
        @Override public boolean deleteCatalogDescriptor(CatalogDeploymentIdentity expected) { throw new UnsupportedOperationException("Read-only catalog fixture"); }
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.of(descriptor); }

    }
}
