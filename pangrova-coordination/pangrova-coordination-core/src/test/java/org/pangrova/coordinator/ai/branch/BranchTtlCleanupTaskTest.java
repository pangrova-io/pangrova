/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pangrova.coordinator.ai.branch;

import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.core.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.core.catalog.handle.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.core.catalog.handle.connector.ConnectorHandleFactory;
import org.pangrova.metadata.api.catalog.MetadataStore;
import org.pangrova.metadata.api.catalog.SyncResult;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.spi.connector.plugin.Connector;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.spi.connector.metadata.ConnectorSystemTable;
import org.pangrova.spi.connector.metadata.SnapshotReference;
import org.pangrova.spi.connector.metadata.ConditionalBranchDropResult;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

class BranchTtlCleanupTaskTest {

    private InMemoryMetadata metadata;
    private CatalogRegistry catalogRegistry;
    private BranchTtlCleanupTask cleanupTask;

    @BeforeEach
    void setUp() {
        metadata = new InMemoryMetadata();
        Connector connector = new MetadataOnlyConnector(metadata);
        ConnectorHandleFactory chf = desc -> new ConnectorCatalogHandle(desc, connector);
        catalogRegistry = new CatalogRegistry(new NoOpMetadataStore(), chf, null, null);
        catalogRegistry.register(new CatalogDescriptor(
                "test_catalog", CatalogType.CONNECTOR, "test",
                CatalogState.ACTIVE, Map.of(), null, null));
        cleanupTask = new BranchTtlCleanupTask(catalogRegistry, 60);
    }

    @Test
    void parseDuration_days() {
        assertThat(BranchTtlCleanupTask.parseDuration("7d")).isEqualTo(Duration.ofDays(7));
    }

    @Test
    void parseDuration_hours() {
        assertThat(BranchTtlCleanupTask.parseDuration("12h")).isEqualTo(Duration.ofHours(12));
    }

    @Test
    void parseDuration_minutes() {
        assertThat(BranchTtlCleanupTask.parseDuration("30m")).isEqualTo(Duration.ofMinutes(30));
    }

    @Test
    void parseDuration_combined() {
        assertThat(BranchTtlCleanupTask.parseDuration("7d12h"))
                .isEqualTo(Duration.ofDays(7).plusHours(12));
    }

    @Test
    void parseDuration_nullReturnsNull() {
        assertThat(BranchTtlCleanupTask.parseDuration(null)).isNull();
    }

    @Test
    void parseDuration_blankReturnsNull() {
        assertThat(BranchTtlCleanupTask.parseDuration("  ")).isNull();
    }

    @Test
    void parseDuration_invalidReturnsNull() {
        assertThat(BranchTtlCleanupTask.parseDuration("abc")).isNull();
    }

    @Test
    void cleanup_noTtlProperty_skipsTable() {
        metadata.addTable("default", "no_ttl_table", Map.of());
        BranchTtlCleanupTask.CleanupResult result = cleanupTask.runCleanup();
        assertThat(result.tablesScanned()).isZero();
        assertThat(result.branchesDropped()).isZero();
    }

    @Test
    void cleanup_withTtl_dropsExpiredBranch() {
        Instant now = Instant.now();
        Instant tenDaysAgo = now.minus(Duration.ofDays(10));

        metadata.addTable("default", "ttl_table", Map.of("branch.ttl", "7d"));
        metadata.addBranch("default", "ttl_table", "expired_branch", 100L);
        metadata.addSnapshotTimestamp(100L, tenDaysAgo);

        BranchTtlCleanupTask.CleanupResult result = cleanupTask.runCleanup();

        assertThat(result.tablesScanned()).isEqualTo(1);
        assertThat(result.branchesDropped()).isEqualTo(1);
        assertThat(metadata.droppedBranches).contains("expired_branch");
    }

    @Test
    void cleanup_withTtl_keepsRecentBranch() {
        Instant now = Instant.now();
        Instant oneDayAgo = now.minus(Duration.ofDays(1));

        metadata.addTable("default", "ttl_table", Map.of("branch.ttl", "7d"));
        metadata.addBranch("default", "ttl_table", "recent_branch", 200L);
        metadata.addSnapshotTimestamp(200L, oneDayAgo);

        BranchTtlCleanupTask.CleanupResult result = cleanupTask.runCleanup();

        assertThat(result.tablesScanned()).isEqualTo(1);
        assertThat(result.branchesDropped()).isZero();
        assertThat(metadata.droppedBranches).isEmpty();
    }

    @Test
    void cleanup_neverDropsMainBranch() {
        Instant now = Instant.now();
        Instant tenDaysAgo = now.minus(Duration.ofDays(10));

        metadata.addTable("default", "ttl_table", Map.of("branch.ttl", "7d"));
        metadata.addBranch("default", "ttl_table", "main", 300L);
        metadata.addSnapshotTimestamp(300L, tenDaysAgo);

        BranchTtlCleanupTask.CleanupResult result = cleanupTask.runCleanup();

        assertThat(result.branchesDropped()).isZero();
        assertThat(metadata.droppedBranches).isEmpty();
    }

    @Test
    void cleanup_skipsTagRefs() {
        Instant now = Instant.now();
        Instant tenDaysAgo = now.minus(Duration.ofDays(10));

        metadata.addTable("default", "ttl_table", Map.of("branch.ttl", "7d"));
        metadata.addTag("default", "ttl_table", "old_tag", 400L);
        metadata.addSnapshotTimestamp(400L, tenDaysAgo);

        BranchTtlCleanupTask.CleanupResult result = cleanupTask.runCleanup();

        assertThat(result.branchesDropped()).isZero();
    }

    @Test
    void cleanup_unknownCommitStopsCurrentTableUntilFreshCycle() {
        addTwoExpiredBranches();
        metadata.forcedDropResult = ConditionalBranchDropResult.COMMIT_STATE_UNKNOWN;

        BranchTtlCleanupTask.CleanupResult first = cleanupTask.runCleanup();

        assertThat(first.branchesDropped()).isZero();
        assertThat(metadata.dropAttempts).isEqualTo(1);
        assertThat(metadata.droppedBranches).isEmpty();
        assertThat(metadata.refs).hasSize(2);

        metadata.forcedDropResult = null;
        BranchTtlCleanupTask.CleanupResult nextCycle = cleanupTask.runCleanup();
        assertThat(nextCycle.branchesDropped()).isEqualTo(2);
        assertThat(metadata.dropAttempts).isEqualTo(3);
    }

    @Test
    void cleanup_conflictDoesNotRetryStaleListingInSameCycle() {
        addTwoExpiredBranches();
        metadata.forcedDropResult = ConditionalBranchDropResult.RETRYABLE_CONFLICT;

        BranchTtlCleanupTask.CleanupResult result = cleanupTask.runCleanup();

        assertThat(result.branchesDropped()).isZero();
        assertThat(metadata.dropAttempts).isEqualTo(1);
        assertThat(metadata.droppedBranches).isEmpty();
        assertThat(metadata.refs).hasSize(2);
    }

    private void addTwoExpiredBranches() {
        metadata.addTable("default", "ttl_table", Map.of("branch.ttl", "7d"));
        metadata.addBranch("default", "ttl_table", "expired_a", 100L);
        metadata.addBranch("default", "ttl_table", "expired_b", 200L);
        Instant old = Instant.now().minus(Duration.ofDays(10));
        metadata.addSnapshotTimestamp(100L, old);
        metadata.addSnapshotTimestamp(200L, old);
    }

    @Test
    void close_shutsDownScheduler() {
        cleanupTask.start();
        cleanupTask.close();
        // No exception means clean shutdown
    }

    // ── Test infrastructure ──

    /**
     * Connector that only provides metadata operations for branch TTL testing.
     * Split manager and page source are not exercised by the cleanup task.
     */
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

    /**
     * In-memory ConnectorMetadata that supports table listing, table properties,
     * typed snapshot refs, timestamp resolution, and conditional branch drop.
     */
    private static final class InMemoryMetadata implements ConnectorMetadata {

        private final List<TableEntry> tables = new ArrayList<>();
        private final List<RefEntry> refs = new ArrayList<>();
        private final java.util.Map<Long, Instant> snapshotTimestamps = new java.util.HashMap<>();
        final CopyOnWriteArrayList<String> droppedBranches = new CopyOnWriteArrayList<>();
        private ConditionalBranchDropResult forcedDropResult;
        private int dropAttempts;

        void addTable(String schema, String table, Map<String, String> properties) {
            tables.add(new TableEntry(schema, table, properties));
        }

        void addBranch(String schema, String table, String branchName, long snapshotId) {
            refs.add(new RefEntry(schema, table, branchName, "BRANCH", snapshotId));
        }

        void addTag(String schema, String table, String tagName, long snapshotId) {
            refs.add(new RefEntry(schema, table, tagName, "TAG", snapshotId));
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
        public java.util.Map<String, org.pangrova.spi.connector.metadata.table.ColumnHandle> getColumnHandles(
                ConnectorSession session, ConnectorTableHandle table) {
            return Map.of();
        }

        @Override
        public org.pangrova.spi.connector.metadata.table.TableStatistics getTableStatistics(
                ConnectorSession session, ConnectorTableHandle table) {
            return org.pangrova.spi.connector.metadata.table.TableStatistics.UNKNOWN;
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
            dropAttempts++;
            if (forcedDropResult != null) {
                return forcedDropResult;
            }
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

    private static final class NoOpMetadataStore implements MetadataStore {
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
