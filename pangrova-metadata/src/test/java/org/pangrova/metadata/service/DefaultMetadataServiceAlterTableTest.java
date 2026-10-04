package org.pangrova.metadata.service;

import org.pangrova.metadata.cache.TieredMetadataCache;
import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.catalog.CatalogState;
import org.pangrova.metadata.catalog.CatalogType;
import org.pangrova.metadata.catalog.ConnectorCatalogHandle;
import org.pangrova.metadata.catalog.ConnectorHandleFactory;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.metadata.exception.MetadataException;
import org.pangrova.metadata.model.ColumnType;
import org.pangrova.metadata.model.QualifiedTableRef;
import org.pangrova.metadata.model.TableMetadata;
import org.pangrova.metadata.schema.SchemaChange;
import org.pangrova.metadata.schema.SchemaEvolutionValidator;
import org.pangrova.metadata.store.MetadataStore;
import org.pangrova.metadata.sync.SyncResult;
import org.pangrova.spi.connector.Connector;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Verifies that ALTER TABLE operations propagate schema changes to the
 * underlying connector metadata (Iceberg/Paimon), not just update caches.
 */
class DefaultMetadataServiceAlterTableTest {

    private RecordingConnectorMetadata connectorMetadata;
    private DefaultMetadataService service;
    private CatalogRegistry registry;
    private MetadataEventDispatcher dispatcher;

    @BeforeEach
    void setUp() {
        connectorMetadata = new RecordingConnectorMetadata();
        TestMetadataStore store = new TestMetadataStore();
        dispatcher = new MetadataEventDispatcher();
        ConnectorHandleFactory chf = desc ->
                new ConnectorCatalogHandle(desc, new TestConnector(connectorMetadata));
        registry = new CatalogRegistry(store, chf, null, dispatcher);
        CatalogDescriptor descriptor = new CatalogDescriptor(
                "test_catalog", CatalogType.CONNECTOR, "test",
                CatalogState.ACTIVE, Map.of(), "", Set.of());
        registry.register(descriptor);
        registry.initialize();

        service = new DefaultMetadataService(
                registry,
                new TieredMetadataCache(),
                dispatcher,
                new SchemaEvolutionValidator(),
                10_000L,
                Duration.ofMinutes(5));
    }

    @AfterEach
    void tearDown() {
        registry.shutdown();
        dispatcher.shutdown();
    }

    @Test
    void alterTableAddColumnPropagatesColumnToConnector() {
        QualifiedTableRef tableRef = new QualifiedTableRef("test_catalog", "public", "orders");
        MetadataSessionContext session = testSession();

        List<SchemaChange> changes = List.of(
                new SchemaChange.AddColumn("email", ColumnType.of("VARCHAR"), true));

        service.alterTable(session, tableRef, changes);

        assertThat(connectorMetadata.addColumnCalls).hasSize(1);
        RecordingConnectorMetadata.AddColumnCall call = connectorMetadata.addColumnCalls.get(0);
        assertThat(call.columnName()).isEqualTo("email");
        assertThat(call.columnType()).isEqualTo(PangrovaType.VARCHAR);
    }

    @Test
    void alterTableDropColumnPropagatesDropToConnector() {
        QualifiedTableRef tableRef = new QualifiedTableRef("test_catalog", "public", "orders");
        MetadataSessionContext session = testSession();

        List<SchemaChange> changes = List.of(
                new SchemaChange.DropColumn("name"));

        service.alterTable(session, tableRef, changes);

        assertThat(connectorMetadata.dropColumnCalls).hasSize(1);
        assertThat(connectorMetadata.dropColumnCalls.get(0)).isEqualTo("name");
    }

    @Test
    void alterTableRenameColumnPropagatesRenameToConnector() {
        QualifiedTableRef tableRef = new QualifiedTableRef("test_catalog", "public", "orders");
        MetadataSessionContext session = testSession();

        List<SchemaChange> changes = List.of(
                new SchemaChange.RenameColumn("name", "full_name"));

        service.alterTable(session, tableRef, changes);

        assertThat(connectorMetadata.renameColumnCalls).hasSize(1);
        RecordingConnectorMetadata.RenameColumnCall call = connectorMetadata.renameColumnCalls.get(0);
        assertThat(call.oldName()).isEqualTo("name");
        assertThat(call.newName()).isEqualTo("full_name");
    }

    @Test
    void alterTableMultipleChangesAllPropagateToConnector() {
        QualifiedTableRef tableRef = new QualifiedTableRef("test_catalog", "public", "orders");
        MetadataSessionContext session = testSession();

        List<SchemaChange> changes = List.of(
                new SchemaChange.AddColumn("email", ColumnType.of("VARCHAR"), true),
                new SchemaChange.DropColumn("name"),
                new SchemaChange.AddColumn("age", ColumnType.of("INT"), true));

        service.alterTable(session, tableRef, changes);

        assertThat(connectorMetadata.addColumnCalls).hasSize(2);
        assertThat(connectorMetadata.dropColumnCalls).hasSize(1);
        assertThat(connectorMetadata.addColumnCalls.get(0).columnName()).isEqualTo("email");
        assertThat(connectorMetadata.addColumnCalls.get(1).columnName()).isEqualTo("age");
        assertThat(connectorMetadata.dropColumnCalls.get(0)).isEqualTo("name");
    }

    @Test
    void alterTableRejectsUnsupportedNotNullBeforeApplyingOtherChanges() {
        QualifiedTableRef tableRef = new QualifiedTableRef("test_catalog", "public", "orders");
        MetadataSessionContext session = testSession();

        assertThatThrownBy(() -> service.alterTable(session, tableRef, List.of(
                new SchemaChange.DropColumn("name"),
                new SchemaChange.AddColumn("age", ColumnType.of("INT"), false))))
                .isInstanceOf(MetadataException.class)
                .hasMessageContaining("NOT NULL");

        assertThat(connectorMetadata.dropColumnCalls).isEmpty();
        assertThat(connectorMetadata.addColumnCalls).isEmpty();
    }

    @Test
    void alterTableRejectsNullabilityChangeBeforeApplyingOtherChanges() {
        QualifiedTableRef tableRef = new QualifiedTableRef("test_catalog", "public", "orders");
        MetadataSessionContext session = testSession();

        assertThatThrownBy(() -> service.alterTable(session, tableRef, List.of(
                new SchemaChange.DropColumn("name"),
                new SchemaChange.ChangeNullability("id", false))))
                .isInstanceOf(MetadataException.class)
                .hasMessageContaining("nullability");

        assertThat(connectorMetadata.dropColumnCalls).isEmpty();
    }

    private static MetadataSessionContext testSession() {
        return new MetadataSessionContext(
                "test-user", "test_catalog", "public", "UTC", "en",
                Map.of(), "default", "", "test-query-001");
    }

    // ── Test infrastructure ──

    static final class RecordingConnectorMetadata implements ConnectorMetadata {
        final List<AddColumnCall> addColumnCalls = new ArrayList<>();
        final List<String> dropColumnCalls = new ArrayList<>();
        final List<RenameColumnCall> renameColumnCalls = new ArrayList<>();
        final List<ChangeColumnTypeCall> changeColumnTypeCalls = new ArrayList<>();

        record AddColumnCall(String columnName, PangrovaType columnType) {}
        record RenameColumnCall(String oldName, String newName) {}
        record ChangeColumnTypeCall(String columnName, PangrovaType newType) {}

        @Override
        public void addColumn(ConnectorSession session, ConnectorTableHandle table,
                              String columnName, PangrovaType columnType) {
            addColumnCalls.add(new AddColumnCall(columnName, columnType));
        }

        @Override
        public void dropColumn(ConnectorSession session, ConnectorTableHandle table,
                               String columnName) {
            dropColumnCalls.add(columnName);
        }

        @Override
        public void renameColumn(ConnectorSession session, ConnectorTableHandle table,
                                 String oldName, String newName) {
            renameColumnCalls.add(new RenameColumnCall(oldName, newName));
        }

        @Override
        public void changeColumnType(ConnectorSession session, ConnectorTableHandle table,
                                     String columnName, PangrovaType newType) {
            changeColumnTypeCalls.add(new ChangeColumnTypeCall(columnName, newType));
        }

        @Override
        public void beginQuery(org.pangrova.spi.connector.ConnectorQueryAuthority authority) { }

        @Override
        public void cleanupQuery(org.pangrova.spi.connector.ConnectorQueryAuthority authority) { }

        @Override
        public List<String> listSchemaNames(ConnectorSession session) {
            return List.of("public");
        }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schema) {
            return List.of(new SchemaTableName("public", "orders"));
        }

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName name) {
            return new StubTableHandle(name.schema(), name.table());
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle h) {
            StubTableHandle handle = (StubTableHandle) h;
            SchemaTableName name = new SchemaTableName(handle.schema(), handle.table());
            return new ConnectorTableMetadata(
                    name,
                    List.of("id", "name"),
                    List.of(
                            new ConnectorTableMetadata.Column("id", PangrovaType.BIGINT),
                            new ConnectorTableMetadata.Column("name", PangrovaType.VARCHAR)),
                    Map.of());
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle h) {
            return Map.of();
        }

        @Override
        public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle h) {
            return TableStatistics.UNKNOWN;
        }
    }

    record StubTableHandle(String schema, String table) implements ConnectorTableHandle { }

    static final class TestConnector implements Connector {
        private final ConnectorMetadata metadata;

        TestConnector(ConnectorMetadata metadata) {
            this.metadata = metadata;
        }

        @Override
        public ConnectorMetadata getMetadata(ConnectorSession session) { return metadata; }

        @Override
        public ConnectorSplitManager getSplitManager() { throw new UnsupportedOperationException(); }

        @Override
        public ConnectorPageSourceProvider getPageSourceProvider() { throw new UnsupportedOperationException(); }

        @Override
        public void shutdown() { }
    }

    static final class TestMetadataStore implements MetadataStore {
        private final CopyOnWriteArrayList<CatalogDescriptor> catalogs = new CopyOnWriteArrayList<>();

        @Override public void saveTable(TableMetadata m) { }
        @Override public Optional<TableMetadata> loadTable(String c, String s, String t) { return Optional.empty(); }
        @Override public void deleteTable(String c, String s, String t) { }
        @Override public List<QualifiedTableRef> searchTables(String p) { return List.of(); }
        @Override public void saveCatalogDescriptor(CatalogDescriptor d) {
            catalogs.removeIf(x -> x.name().equals(d.name()));
            catalogs.add(d);
        }
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String n) {
            return catalogs.stream().filter(d -> d.name().equals(n)).findFirst();
        }
        @Override public void deleteCatalogDescriptor(String n) { catalogs.removeIf(d -> d.name().equals(n)); }
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.copyOf(catalogs); }
        @Override public Optional<Instant> getLastSyncTime(String n) { return Optional.empty(); }
        @Override public void updateSyncTime(String n, Instant t) { }
        @Override public void recordSyncHistory(SyncResult r) { }
        @Override public void saveSchemas(String n, List<String> s) { }
    }
}
