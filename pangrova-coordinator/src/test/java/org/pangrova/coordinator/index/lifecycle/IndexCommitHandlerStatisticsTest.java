package org.pangrova.coordinator.index.lifecycle;

import org.pangrova.coordinator.index.ddl.ConnectorAccessorResolver;
import org.pangrova.indexing.IndexEngineRegistry;
import org.pangrova.indexing.build.IndexBuildContext;
import org.pangrova.indexing.build.IndexBuildCoordinator;
import org.pangrova.indexing.config.IndexingConfig;
import org.pangrova.indexing.serde.IndexSerializer;
import org.pangrova.indexing.serde.TypeSerializerRegistry;
import org.pangrova.indexing.storage.IndexStorageManager;
import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.catalog.CatalogState;
import org.pangrova.metadata.catalog.CatalogType;
import org.pangrova.metadata.catalog.ConnectorCatalogHandle;
import org.pangrova.metadata.index.IndexMetadataStore;
import org.pangrova.metadata.index.model.IndexFileMapping;
import org.pangrova.spi.connector.Connector;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.index.CommitEventType;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.spi.index.IndexDescriptor;
import org.pangrova.spi.index.IndexState;
import org.pangrova.spi.index.IndexType;
import org.pangrova.spi.index.statistics.DataFileStatistics;
import org.pangrova.spi.index.statistics.FileColumnStatistics;
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
                .isInstanceOf(org.pangrova.spi.security.AccessDeniedException.class)
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
        var registry = new CatalogRegistry(
                persistence, desc -> catalogHandle, null, null);
        registry.initialize();

        var resolver = new ConnectorAccessorResolver(registry);
        if (scoped) {
            // This is a connector statistics unit fixture. It binds a concrete
            // operation identity; production maintenance still requires a
            // persisted subject and current security reauthorization.
            resolver = resolver.forSession(new ConnectorSession() {
                @Override public Optional<org.pangrova.spi.connector.ConnectorQueryAuthority>
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
        var serializer = new IndexSerializer(TypeSerializerRegistry.createDefault());
        var buildCtx = new IndexBuildContext(config, engineRegistry, storageManager, serializer);
        buildCoordinator = new IndexBuildCoordinator(buildCtx, null);

        var deps = new IndexCommitContext(
                store,
                new org.pangrova.coordinator.index.ddl.IndexBuildServices(
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
        boolean authorizedFixture;

        @Override public String loadAuthorizedBuildPrincipal(IndexDescriptor descriptor) {
            if (!authorizedFixture) {
                throw new org.pangrova.spi.security.AccessDeniedException(
                        "Index connector access requires a trusted authorization scope");
            }
            return "statistics-fixture-reader";
        }

        @Override
        public void batchInsertFileStatistics(String catalog, String schema,
                String table, List<DataFileStatistics> statistics) {
            lastCatalog = catalog;
            lastSchema = schema;
            lastTable = table;
            insertedStats.addAll(statistics);
        }

        @Override
        public void deleteFileStatistics(String catalog, String schema,
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
        @Override public org.pangrova.metadata.index.model.IndexGenerationManifest publishIndexGeneration(
                org.pangrova.metadata.index.model.IndexPublicationRequest request) { throw new AssertionError(); }
        @Override public Optional<org.pangrova.metadata.index.model.IndexGenerationManifest>
                findActiveIndexGeneration(String id) { return Optional.empty(); }
        @Override public Optional<IndexDescriptor> findIndex(String id) { return Optional.empty(); }
        @Override public List<IndexDescriptor> findIndexesByTable(String tenantId, String c, String s, String t) { return indexesForTable; }
        @Override public List<IndexDescriptor> findReadyIndexesForColumns(String tenantId, String c, String s, String t, List<String> cols, boolean includeStale) { return List.of(); }
        @Override public void dropIndex(String id, Instant t) {}
        @Override public void purgeIndex(String id) {}
        @Override public void registerIndexFile(IndexFileMapping m) {}
        @Override public void batchRegisterIndexFiles(List<IndexFileMapping> m) {}
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

    /** ConnectorMetadata returning pre-configured file statistics. */
    static final class StatisticsConnectorMetadata implements ConnectorMetadata {
        private final List<DataFileStatistics> stats;
        StatisticsConnectorMetadata(List<DataFileStatistics> stats) { this.stats = stats; }

        @Override
        public List<DataFileStatistics> getFileStatistics(
                String cat, String sch, String tbl, List<String> paths) {
            return stats;
        }

        @Override public List<String> listSchemaNames(ConnectorSession s) { return List.of(); }
        @Override public List<org.pangrova.spi.connector.metadata.table.SchemaTableName> listTables(ConnectorSession s, Optional<String> schema) { return List.of(); }
        @Override public org.pangrova.spi.connector.metadata.table.ConnectorTableHandle getTableHandle(ConnectorSession s, org.pangrova.spi.connector.metadata.table.SchemaTableName n) { return null; }
        @Override public org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata getTableMetadata(ConnectorSession s, org.pangrova.spi.connector.metadata.table.ConnectorTableHandle t) { return null; }
        @Override public Map<String, org.pangrova.spi.connector.metadata.table.ColumnHandle> getColumnHandles(ConnectorSession s, org.pangrova.spi.connector.metadata.table.ConnectorTableHandle t) { return Map.of(); }
        @Override public org.pangrova.spi.connector.metadata.table.TableStatistics getTableStatistics(ConnectorSession s, org.pangrova.spi.connector.metadata.table.ConnectorTableHandle t) { return org.pangrova.spi.connector.metadata.table.TableStatistics.UNKNOWN; }
    }

    /** ConnectorMetadata that throws on getFileStatistics. */
    static final class FailingConnectorMetadata implements ConnectorMetadata {
        @Override
        public List<DataFileStatistics> getFileStatistics(
                String cat, String sch, String tbl, List<String> paths) {
            throw new RuntimeException("Simulated connector failure");
        }

        @Override public List<String> listSchemaNames(ConnectorSession s) { return List.of(); }
        @Override public List<org.pangrova.spi.connector.metadata.table.SchemaTableName> listTables(ConnectorSession s, Optional<String> schema) { return List.of(); }
        @Override public org.pangrova.spi.connector.metadata.table.ConnectorTableHandle getTableHandle(ConnectorSession s, org.pangrova.spi.connector.metadata.table.SchemaTableName n) { return null; }
        @Override public org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata getTableMetadata(ConnectorSession s, org.pangrova.spi.connector.metadata.table.ConnectorTableHandle t) { return null; }
        @Override public Map<String, org.pangrova.spi.connector.metadata.table.ColumnHandle> getColumnHandles(ConnectorSession s, org.pangrova.spi.connector.metadata.table.ConnectorTableHandle t) { return Map.of(); }
        @Override public org.pangrova.spi.connector.metadata.table.TableStatistics getTableStatistics(ConnectorSession s, org.pangrova.spi.connector.metadata.table.ConnectorTableHandle t) { return org.pangrova.spi.connector.metadata.table.TableStatistics.UNKNOWN; }
    }

    /** Minimal Connector delegating to a ConnectorMetadata. */
    static final class MinimalConnector implements Connector {
        private final ConnectorMetadata metadata;
        MinimalConnector(ConnectorMetadata metadata) { this.metadata = metadata; }

        @Override public ConnectorMetadata getMetadata(ConnectorSession s) { return metadata; }
        @Override public ConnectorSplitManager getSplitManager() { return null; }
        @Override public ConnectorPageSourceProvider getPageSourceProvider() { return null; }
    }

    /** Minimal MetadataStore for CatalogRegistry initialization. */
    static final class MinimalPersistenceStore implements org.pangrova.metadata.store.MetadataStore {
        private final CatalogDescriptor descriptor;
        MinimalPersistenceStore(CatalogDescriptor d) { this.descriptor = d; }

        @Override public void saveTable(org.pangrova.metadata.model.TableMetadata m) {}
        @Override public Optional<org.pangrova.metadata.model.TableMetadata> loadTable(String c, String s, String t) { return Optional.empty(); }
        @Override public void deleteTable(String c, String s, String t) {}
        @Override public List<org.pangrova.metadata.model.QualifiedTableRef> searchTables(String p) { return List.of(); }
        @Override public void saveCatalogDescriptor(CatalogDescriptor d) {}
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String n) { return descriptor.name().equals(n) ? Optional.of(descriptor) : Optional.empty(); }
        @Override public void deleteCatalogDescriptor(String n) {}
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.of(descriptor); }
        @Override public Optional<Instant> getLastSyncTime(String c) { return Optional.empty(); }
        @Override public void updateSyncTime(String c, Instant t) {}
        @Override public void recordSyncHistory(org.pangrova.metadata.sync.SyncResult r) {}
        @Override public void saveSchemas(String c, List<String> s) {}
    }
}
