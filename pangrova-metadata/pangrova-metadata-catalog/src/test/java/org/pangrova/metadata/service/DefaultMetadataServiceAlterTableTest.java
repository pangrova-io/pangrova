package org.pangrova.metadata.service;

import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;

import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

import org.pangrova.metadata.api.session.MetadataSessionContext;

import org.pangrova.metadata.cache.TieredMetadataCache;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.api.catalog.ConnectorHandleFactory;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.metadata.api.exception.MetadataException;
import org.pangrova.metadata.api.object.ColumnType;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.schema.SchemaChange;
import org.pangrova.metadata.schema.SchemaEvolutionValidator;
import org.pangrova.persistence.catalog.api.catalog.CatalogDeploymentRepository;
import org.pangrova.connector.api.plugin.Connector;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.data.ConnectorPageSourceProvider;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.ConnectorSplitManager;
import org.pangrova.connector.api.metadata.table.ColumnHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.pangrova.types.enums.PangrovaType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
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
        TestCatalogDeployments store = new TestCatalogDeployments();
        dispatcher = new MetadataEventDispatcher();
        ConnectorHandleFactory chf = desc ->
                new ConnectorCatalogHandle(desc, new TestConnector(connectorMetadata));
        registry = new CatalogRegistry(memoryTableStores(), store, chf, null, dispatcher);
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
        public void beginQuery(org.pangrova.connector.api.query.ConnectorQueryAuthority authority) { }

        @Override
        public void cleanupQuery(org.pangrova.connector.api.query.ConnectorQueryAuthority authority) { }

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

    static final class TestCatalogDeployments implements CatalogDeploymentRepository {
        @Override
        public java.util.Optional<org.pangrova.metadata.api.catalog.CatalogDescriptor>
                findCatalogDescriptorRevision(String name, String incarnationId, long revision) {
            throw new UnsupportedOperationException("This fixture does not store deployment history");
        }

        private final CopyOnWriteArrayList<CatalogDescriptor> catalogs = new CopyOnWriteArrayList<>();

        @Override public void saveCatalogDescriptor(CatalogDescriptor d) {
            catalogs.removeIf(x -> x.name().equals(d.name()));
            catalogs.add(d);
        }
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String n) {
            return catalogs.stream().filter(d -> d.name().equals(n)).findFirst();
        }
        @Override public boolean deleteCatalogDescriptor(CatalogDeploymentIdentity expected) { return catalogs.removeIf(d -> d.identity().equals(expected)); }
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.copyOf(catalogs); }

    }
}
