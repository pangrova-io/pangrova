package org.pangrova.coordinator.ai.branch;

import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;

import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.api.catalog.ConnectorHandleFactory;
import org.pangrova.persistence.catalog.api.catalog.CatalogDeploymentRepository;
import org.pangrova.connector.api.plugin.Connector;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.data.ConnectorPageSourceProvider;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.ConnectorSplitManager;
import org.pangrova.connector.api.metadata.ConnectorSystemTable;
import org.pangrova.connector.api.metadata.SnapshotReference;
import org.pangrova.connector.api.enums.ConditionalBranchDropResult;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that {@link BranchTtlCleanupTask} correctly scans tables,
 * identifies expired branches, and drops them via the connector metadata API
 * (F-COORD-FEAT-03).
 */
@DisplayName("BranchTtlCleanupTask")
class BranchTtlCleanupTaskLeaderGateTest {

    private BranchTtlCleanupTask cleanupTask;

    @AfterEach
    void tearDown() {
        if (cleanupTask != null) {
            cleanupTask.close();
        }
    }

    @Test
    @DisplayName("runCleanup() drops expired branches")
    void cleanup_executesAndDropsExpiredBranches() {
        var env = createEnv();
        cleanupTask = env.task;

        BranchTtlCleanupTask.CleanupResult result = cleanupTask.runCleanup();

        assertThat(result.tablesScanned()).isEqualTo(1);
        assertThat(result.branchesDropped()).isEqualTo(1);
        assertThat(env.metadata.droppedBranches).contains("expired_branch");
    }

    @Test
    @DisplayName("runCleanup() returns zero when no tables have TTL configured")
    void cleanup_noTtlTables_noOp() {
        var metadata = new InMemoryMetadata();
        // Table without branch.ttl property
        metadata.addTable("default", "no_ttl_table", Map.of());
        var catalogRegistry = createCatalogRegistry(metadata);
        cleanupTask = new BranchTtlCleanupTask(catalogRegistry, 60);

        BranchTtlCleanupTask.CleanupResult result = cleanupTask.runCleanup();

        assertThat(result.tablesScanned()).isZero();
        assertThat(result.branchesDropped()).isZero();
        assertThat(metadata.droppedBranches).isEmpty();
    }

    @Test
    @DisplayName("runCleanup() does not drop non-expired branches")
    void cleanup_nonExpiredBranchesAreKept() {
        var metadata = new InMemoryMetadata();
        Instant oneHourAgo = Instant.now().minus(Duration.ofHours(1));
        metadata.addTable("default", "ttl_table", Map.of("branch.ttl", "7d"));
        metadata.addBranch("default", "ttl_table", "recent_branch", 200L);
        metadata.addSnapshotTimestamp(200L, oneHourAgo);
        var catalogRegistry = createCatalogRegistry(metadata);
        cleanupTask = new BranchTtlCleanupTask(catalogRegistry, 60);

        BranchTtlCleanupTask.CleanupResult result = cleanupTask.runCleanup();

        assertThat(result.tablesScanned()).isEqualTo(1);
        assertThat(result.branchesDropped()).isZero();
        assertThat(metadata.droppedBranches).isEmpty();
    }

    @Test
    @DisplayName("default constructor (no scanInterval) always runs cleanup")
    void defaultConstructor_alwaysRunsCleanup() {
        var metadata = createMetadata();
        var catalogRegistry = createCatalogRegistry(metadata);
        cleanupTask = new BranchTtlCleanupTask(catalogRegistry);

        BranchTtlCleanupTask.CleanupResult result = cleanupTask.runCleanup();

        assertThat(result.tablesScanned()).isEqualTo(1);
        assertThat(result.branchesDropped()).isEqualTo(1);
        assertThat(metadata.droppedBranches).contains("expired_branch");
    }

    @Test
    @DisplayName("constructor with custom scanInterval runs cleanup")
    void customIntervalConstructor_runsCleanup() {
        var metadata = createMetadata();
        var catalogRegistry = createCatalogRegistry(metadata);
        cleanupTask = new BranchTtlCleanupTask(catalogRegistry, 60);

        BranchTtlCleanupTask.CleanupResult result = cleanupTask.runCleanup();

        assertThat(result.tablesScanned()).isEqualTo(1);
        assertThat(result.branchesDropped()).isEqualTo(1);
        assertThat(metadata.droppedBranches).contains("expired_branch");
    }

    // ── Test infrastructure ──

    private record TestEnv(BranchTtlCleanupTask task, InMemoryMetadata metadata) {}

    private TestEnv createEnv() {
        var metadata = createMetadata();
        var catalogRegistry = createCatalogRegistry(metadata);
        var task = new BranchTtlCleanupTask(catalogRegistry, 60);
        return new TestEnv(task, metadata);
    }

    private static InMemoryMetadata createMetadata() {
        var metadata = new InMemoryMetadata();
        Instant tenDaysAgo = Instant.now().minus(Duration.ofDays(10));
        metadata.addTable("default", "ttl_table", Map.of("branch.ttl", "7d"));
        metadata.addBranch("default", "ttl_table", "expired_branch", 100L);
        metadata.addSnapshotTimestamp(100L, tenDaysAgo);
        return metadata;
    }

    private static CatalogRegistry createCatalogRegistry(InMemoryMetadata metadata) {
        Connector connector = new MetadataOnlyConnector(metadata);
        ConnectorHandleFactory chf = desc -> new ConnectorCatalogHandle(desc, connector);
        var registry = new CatalogRegistry(memoryTableStores(), new EmptyCatalogDeployments(), chf, null, null);
        registry.register(new CatalogDescriptor(
                "test_catalog", CatalogType.CONNECTOR, "test",
                CatalogState.ACTIVE, Map.of(), null, null));
        return registry;
    }

    private static final class MetadataOnlyConnector implements Connector {
        private final InMemoryMetadata metadata;

        MetadataOnlyConnector(InMemoryMetadata metadata) {
            this.metadata = metadata;
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            return metadata;
        }

        @Override
        public ConnectorSplitManager getSplitManager() {
            return null;
        }

        @Override
        public ConnectorPageSourceProvider getPageSourceProvider() {
            return null;
        }
    }

    private static final class InMemoryMetadata implements ConnectorMetadata {
        private final List<TableEntry> tables = new ArrayList<>();
        private final List<RefEntry> refs = new ArrayList<>();
        private final java.util.Map<Long, Instant> snapshotTimestamps = new java.util.HashMap<>();
        final CopyOnWriteArrayList<String> droppedBranches = new CopyOnWriteArrayList<>();

        void addTable(String schema, String table, Map<String, String> properties) {
            tables.add(new TableEntry(schema, table, properties));
        }

        void addBranch(String schema, String table, String branchName, long snapshotId) {
            refs.add(new RefEntry(schema, table, branchName, "BRANCH", snapshotId));
        }

        void addSnapshotTimestamp(long snapshotId, Instant timestamp) {
            snapshotTimestamps.put(snapshotId, timestamp);
        }

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
            return tables.stream()
                    .filter(t -> t.schema.equals(tableName.schema()) && t.table.equals(tableName.table()))
                    .findFirst()
                    .map(t -> (ConnectorTableHandle) new TestTableHandle(t.schema, t.table))
                    .orElse(null);
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
            TestTableHandle h = (TestTableHandle) table;
            return tables.stream()
                    .filter(t -> t.schema.equals(h.schema()) && t.table.equals(h.table()))
                    .findFirst()
                    .map(t -> new ConnectorTableMetadata(
                            new SchemaTableName(t.schema, t.table), List.of(), t.properties))
                    .orElseThrow();
        }

        @Override
        public java.util.Map<String, org.pangrova.connector.api.metadata.table.ColumnHandle> getColumnHandles(
                ConnectorSession session, ConnectorTableHandle table) {
            return Map.of();
        }

        @Override
        public org.pangrova.connector.api.metadata.table.TableStatistics getTableStatistics(
                ConnectorSession session, ConnectorTableHandle table) {
            return org.pangrova.connector.api.metadata.table.TableStatistics.UNKNOWN;
        }

        @Override
        public List<String> listSchemaNames(ConnectorSession session) {
            return tables.stream().map(t -> t.schema).distinct().toList();
        }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
            return tables.stream()
                    .filter(t -> schemaName.isEmpty() || schemaName.get().equals(t.schema))
                    .map(t -> new SchemaTableName(t.schema, t.table))
                    .toList();
        }

        @Override
        public List<SnapshotReference> listSnapshotReferences(
                ConnectorSession session, ConnectorTableHandle table) {
            TestTableHandle h = (TestTableHandle) table;
            return refs.stream()
                    .filter(r -> r.schema.equals(h.schema()) && r.table.equals(h.table()))
                    .map(r -> new SnapshotReference(r.name,
                            SnapshotReference.Type.valueOf(r.type), r.snapshotId))
                    .toList();
        }

        @Override
        public Optional<ConnectorSystemTable> getSystemTable(
                ConnectorSession session, ConnectorTableHandle table, String systemTableName) {
            throw new AssertionError("TTL cleanup must not scan system-table rows");
        }

        @Override
        public Optional<Instant> resolveSnapshotTimestamp(
                ConnectorSession session, String schemaName, String tableName, long snapshotId) {
            return Optional.ofNullable(snapshotTimestamps.get(snapshotId));
        }

        @Override
        public ConditionalBranchDropResult dropBranchIfSnapshotMatches(
                ConnectorSession session, ConnectorTableHandle table,
                String branchName, long expectedSnapshotId) {
            boolean removed = refs.removeIf(r -> r.name.equals(branchName)
                    && "BRANCH".equals(r.type) && r.snapshotId == expectedSnapshotId);
            if (removed) {
                droppedBranches.add(branchName);
                return ConditionalBranchDropResult.DROPPED;
            }
            return ConditionalBranchDropResult.REF_CHANGED;
        }

        @Override
        public void dropBranch(ConnectorSession session, ConnectorTableHandle table, String branchName) {
            throw new AssertionError("TTL cleanup must use conditional branch removal");
        }

        private record TableEntry(String schema, String table, Map<String, String> properties) {}
        private record RefEntry(String schema, String table, String name, String type, long snapshotId) {}
    }

    private record TestTableHandle(String schema, String table) implements ConnectorTableHandle {}

    private static final class EmptyCatalogDeployments implements CatalogDeploymentRepository {
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
