package org.pangrova.coordinator.property.branch;

import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.pangrova.common.exception.PangrovaException;
import org.pangrova.coordinator.ai.branch.BranchDdlContext;
import org.pangrova.coordinator.ai.branch.BranchDdlExecutor;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.catalog.handle.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.api.catalog.MetadataStore;
import org.pangrova.metadata.api.catalog.SyncResult;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.statement.CreateTagStatement;
import org.pangrova.spi.connector.plugin.Connector;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.pangrova.spi.security.UserIdentity;

/**
 * Property-based tests for tag immutability (P4).
 *
 * <p>Validates that a tag always resolves to the same snapshot and cannot be
 * modified after creation. Once a tag is created, attempting to create another
 * tag with the same name fails with an error, preserving the original tag's
 * snapshot reference.</p>
 *
 * <p><b>Validates: Requirements P4</b></p>
 */
class TagImmutabilityPropertyTest {

    private static final String CATALOG = "test_catalog";
    private static final String SCHEMA = "test_schema";
    private static final String TABLE = "test_table";
    private static final UserIdentity TEST_USER = new UserIdentity(
            "admin", Set.of("ADMIN"), Map.of(), Instant.now());
    private static final org.pangrova.coordinator.query.session.CoordinatorSessionContext TEST_SESSION =
            new org.pangrova.coordinator.query.session.CoordinatorSessionContext(
                    "branch-query", Map.of("catalog", CATALOG, "schema", SCHEMA), TEST_USER, "tenant-a");

    @Provide
    Arbitrary<String> tagNames() {
        return Arbitraries.strings().alpha().ofMinLength(1).ofMaxLength(20)
                .map(s -> "tag_" + s.toLowerCase());
    }

    /**
     * P4: Creating a tag with a name that already exists fails.
     * Tags are immutable — once created, the same name cannot be reused.
     */
    @Property(tries = 50)
    void duplicateTagCreationFails(@ForAll("tagNames") String tagName) {
        TagTrackingConnector connector = new TagTrackingConnector(SCHEMA, TABLE);
        BranchDdlExecutor executor = buildExecutor(connector);
        CreateTagStatement stmt = new CreateTagStatement(
                tagName, Identifier.of(CATALOG, SCHEMA, TABLE));

        // First creation succeeds
        executor.executeCreateTag(stmt, TEST_SESSION);
        assertThat(connector.tagExists(tagName)).isTrue();

        // Second creation with same name fails (tag is immutable)
        assertThatThrownBy(() -> executor.executeCreateTag(stmt, TEST_SESSION))
                .isInstanceOf(PangrovaException.class)
                .hasMessageContaining("already exists");

        // Tag remains in the store (not corrupted by failed second attempt)
        assertThat(connector.tagExists(tagName)).isTrue();
    }

    /**
     * P4: Different tag names can coexist on the same table.
     * Immutability applies per-name, not per-table.
     */
    @Property(tries = 50)
    void distinctTagNamesCanCoexist(
            @ForAll("tagNames") String tagA,
            @ForAll("tagNames") String tagB) {

        if (tagA.equals(tagB)) {
            return;
        }

        TagTrackingConnector connector = new TagTrackingConnector(SCHEMA, TABLE);
        BranchDdlExecutor executor = buildExecutor(connector);

        executor.executeCreateTag(
                new CreateTagStatement(tagA, Identifier.of(CATALOG, SCHEMA, TABLE)), TEST_SESSION);
        executor.executeCreateTag(
                new CreateTagStatement(tagB, Identifier.of(CATALOG, SCHEMA, TABLE)), TEST_SESSION);

        assertThat(connector.tagExists(tagA)).isTrue();
        assertThat(connector.tagExists(tagB)).isTrue();
    }

    /**
     * P4: After tag creation, the tag snapshot reference is permanently fixed.
     * Multiple overwrite attempts do not change the original snapshot.
     */
    @Property(tries = 50)
    void tagSnapshotRemainsFixedAfterCreation(@ForAll("tagNames") String tagName) {
        TagTrackingConnector connector = new TagTrackingConnector(SCHEMA, TABLE);
        BranchDdlExecutor executor = buildExecutor(connector);
        CreateTagStatement stmt = new CreateTagStatement(
                tagName, Identifier.of(CATALOG, SCHEMA, TABLE));

        // Create the tag — captures current snapshot
        executor.executeCreateTag(stmt, TEST_SESSION);
        long snapshotAtCreation = connector.getTagSnapshot(tagName);

        // Multiple attempts to recreate all fail
        for (int i = 0; i < 3; i++) {
            assertThatThrownBy(() -> executor.executeCreateTag(stmt, TEST_SESSION))
                    .isInstanceOf(PangrovaException.class)
                    .hasMessageContaining("already exists");
        }

        // Tag still points to the same snapshot (immutable)
        assertThat(connector.getTagSnapshot(tagName)).isEqualTo(snapshotAtCreation);
    }

    // --- Test infrastructure ---

    private static BranchDdlExecutor buildExecutor(TagTrackingConnector connector) {
        CatalogDescriptor descriptor = new CatalogDescriptor(
                CATALOG, CatalogType.CONNECTOR, "test-provider",
                CatalogState.ACTIVE, Map.of(), "", Set.of());

        CatalogRegistry registry = new CatalogRegistry(
                new InMemoryMetadataStore(),
                d -> new ConnectorCatalogHandle(d, connector),
                null,
                null);
        registry.register(descriptor);

        BranchDdlContext ctx = new BranchDdlContext(registry, null);
        return new BranchDdlExecutor(ctx);
    }

    /**
     * Connector that tracks tags in memory with snapshot association.
     * Enforces tag immutability: duplicate createTag throws IllegalArgumentException.
     *
     * <p>Only metadata operations are exercised in these tests. Split management
     * and page source operations are not part of the tag immutability contract.</p>
     */
    static final class TagTrackingConnector implements Connector {
        private final Map<String, Long> tagToSnapshot = new HashMap<>();
        private final String schema;
        private final String table;
        private long snapshotCounter = 1;

        TagTrackingConnector(String schema, String table) {
            this.schema = schema;
            this.table = table;
        }

        boolean tagExists(String tagName) {
            return tagToSnapshot.containsKey(tagName);
        }

        long getTagSnapshot(String tagName) {
            return tagToSnapshot.get(tagName);
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) {
            return new TagTrackingMetadata();
        }

        @Override
        public ConnectorSplitManager getSplitManager() {
            throw new UnsupportedOperationException("Tag immutability tests do not exercise split management");
        }

        @Override
        public ConnectorPageSourceProvider getPageSourceProvider() {
            throw new UnsupportedOperationException("Tag immutability tests do not exercise page source");
        }

        /**
         * ConnectorMetadata that delegates tag operations to the enclosing connector's tag store.
         */
        final class TagTrackingMetadata implements ConnectorMetadata {
            @Override
            public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
                if (tableName.schema().equals(schema) && tableName.table().equals(table)) {
                    return new TestTableHandle(schema, table);
                }
                return null;
            }

            @Override
            public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table) {
                return TableStatistics.UNKNOWN;
            }

            @Override
            public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
                return Map.of();
            }

            @Override
            public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
                return new ConnectorTableMetadata(new SchemaTableName(schema, TagTrackingConnector.this.table), List.of(), Map.of());
            }

            @Override
            public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName) {
                return List.of(new SchemaTableName(schema, TagTrackingConnector.this.table));
            }

            @Override
            public List<String> listSchemaNames(ConnectorSession session) {
                return List.of(schema);
            }

            @Override
            public void createTag(ConnectorSession session, ConnectorTableHandle tableHandle, String tagName) {
                if (tagToSnapshot.containsKey(tagName)) {
                    throw new IllegalArgumentException("Tag '" + tagName + "' already exists");
                }
                tagToSnapshot.put(tagName, snapshotCounter++);
            }
        }
    }

    record TestTableHandle(String schema, String table) implements ConnectorTableHandle {}

    /**
     * In-memory MetadataStore for CatalogRegistry construction.
     * Provides functional catalog descriptor persistence for the registry's
     * register/getCatalog lifecycle used in these property tests.
     */
    static final class InMemoryMetadataStore implements MetadataStore {
        private final Map<String, CatalogDescriptor> catalogs = new HashMap<>();

        @Override
        public List<CatalogDescriptor> listAllCatalogDescriptors() {
            return List.copyOf(catalogs.values());
        }

        @Override
        public void saveCatalogDescriptor(CatalogDescriptor descriptor) {
            catalogs.put(descriptor.name(), descriptor);
        }

        @Override
        public Optional<CatalogDescriptor> getCatalogDescriptor(String name) {
            return Optional.ofNullable(catalogs.get(name));
        }

        @Override
        public void deleteCatalogDescriptor(String name) {
            catalogs.remove(name);
        }

        @Override
        public void saveTable(TableMetadata metadata) {
            // Intentional no-op: tag immutability tests do not exercise table persistence
        }

        @Override
        public Optional<TableMetadata> loadTable(String catalog, String schema, String table) {
            return Optional.empty();
        }

        @Override
        public void deleteTable(String catalog, String schema, String table) {
            // Intentional no-op: tag immutability tests do not exercise table persistence
        }

        @Override
        public List<QualifiedTableRef> searchTables(String pattern) {
            return List.of();
        }

        @Override
        public Optional<Instant> getLastSyncTime(String catalogName) {
            return Optional.empty();
        }

        @Override
        public void updateSyncTime(String catalogName, Instant syncTime) {
            // Intentional no-op: tag immutability tests do not exercise sync tracking
        }

        @Override
        public void recordSyncHistory(SyncResult result) {
            // Intentional no-op: tag immutability tests do not exercise sync tracking
        }

        @Override
        public void saveSchemas(String catalogName, List<String> schemas) {
            // Intentional no-op: tag immutability tests do not exercise schema persistence
        }
    }
}
