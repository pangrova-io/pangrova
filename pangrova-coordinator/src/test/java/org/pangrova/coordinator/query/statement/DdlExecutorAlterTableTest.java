package org.pangrova.coordinator.query.statement;

import org.pangrova.metadata.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.CatalogRegistry;
import org.pangrova.metadata.catalog.CatalogState;
import org.pangrova.metadata.catalog.CatalogType;
import org.pangrova.metadata.catalog.ConnectorCatalogHandle;
import org.pangrova.metadata.catalog.ConnectorHandleFactory;
import org.pangrova.metadata.model.TableMetadata;
import org.pangrova.metadata.store.MetadataStore;
import org.pangrova.metadata.sync.SyncResult;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.SqlParser;
import org.pangrova.parser.ast.statement.AlterTableStatement;
import org.pangrova.parser.ast.type.DataTypeSpec;
import org.pangrova.spi.connector.Connector;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSinkProvider;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;
import org.pangrova.spi.connector.data.type.PangrovaType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.pangrova.coordinator.query.statement.StatementTestSessions.context;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies ALTER TABLE DDL operations propagate to ConnectorMetadata.
 *
 * <p>Ensures the coordinator DDL handler calls the connector SPI methods
 * (addColumn, dropColumn, renameColumn, changeColumnType) so schema changes
 * reach the underlying storage layer (Iceberg/Paimon).</p>
 */
class DdlExecutorAlterTableTest {

    private static org.pangrova.coordinator.query.session.CoordinatorSessionContext statementSession() {
        return context();
    }

    private RecordingMetadata recordingMetadata;
    private DdlExecutor ddlExecutor;

    @BeforeEach
    void setUp() {
        recordingMetadata = new RecordingMetadata();
        CatalogRegistry registry = buildRegistry(recordingMetadata);
        ddlExecutor = new DdlExecutor(registry);
    }

    @Test
    void addColumn_callsConnectorMetadataAddColumn() {
        AlterTableStatement stmt = new AlterTableStatement.AddColumn(
                Identifier.of("test_cat", "default", "users"),
                "email", DataTypeSpec.fromRawTypeString("VARCHAR"));

        ddlExecutor.executeAlterTable(stmt, statementSession());

        assertThat(recordingMetadata.operations).hasSize(1);
        assertThat(recordingMetadata.operations.get(0))
                .isEqualTo("addColumn(email, VARCHAR, null)");
        assertThat(recordingMetadata.columns).containsKey("email");
        assertThat(recordingMetadata.columns.get("email")).isEqualTo("VARCHAR");
    }

    @Test
    void parsedUuidDefaultReachesTheConnectorAsUuid() {
        UUID expected = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");
        AlterTableStatement stmt = (AlterTableStatement) new SqlParser().parse(
                "ALTER TABLE test_cat.default.users ADD COLUMN external_id UUID DEFAULT UUID '"
                        + expected + "'");

        ddlExecutor.executeAlterTable(stmt, statementSession());

        assertThat(recordingMetadata.columns).containsEntry("external_id", "UUID");
        assertThat(recordingMetadata.defaults).containsEntry("external_id", expected);
    }

    @Test
    void dropColumn_callsConnectorMetadataDropColumn() {
        AlterTableStatement stmt = new AlterTableStatement.DropColumn(
                Identifier.of("test_cat", "default", "users"), "name");

        ddlExecutor.executeAlterTable(stmt, statementSession());

        assertThat(recordingMetadata.operations).hasSize(1);
        assertThat(recordingMetadata.operations.get(0))
                .isEqualTo("dropColumn(name)");
        assertThat(recordingMetadata.columns).doesNotContainKey("name");
    }

    @Test
    void renameColumn_callsConnectorMetadataRenameColumn() {
        AlterTableStatement stmt = new AlterTableStatement.RenameColumn(
                Identifier.of("test_cat", "default", "users"), "name", "full_name");

        ddlExecutor.executeAlterTable(stmt, statementSession());

        assertThat(recordingMetadata.operations).hasSize(1);
        assertThat(recordingMetadata.operations.get(0))
                .isEqualTo("renameColumn(name, full_name)");
        assertThat(recordingMetadata.columns).doesNotContainKey("name");
        assertThat(recordingMetadata.columns).containsKey("full_name");
    }

    @Test
    void changeColumnType_callsConnectorMetadataChangeColumnType() {
        AlterTableStatement stmt = new AlterTableStatement.ChangeColumnType(
                Identifier.of("test_cat", "default", "users"), "id", DataTypeSpec.fromRawTypeString("BIGINT"));

        ddlExecutor.executeAlterTable(stmt, statementSession());

        assertThat(recordingMetadata.operations).hasSize(1);
        assertThat(recordingMetadata.operations.get(0))
                .isEqualTo("changeColumnType(id, BIGINT)");
        assertThat(recordingMetadata.columns.get("id")).isEqualTo("BIGINT");
    }

    // ── Test infrastructure ──

    private static CatalogRegistry buildRegistry(ConnectorMetadata metadata) {
        Connector connector = new TestConnector(metadata);
        ConnectorHandleFactory chf = desc -> new ConnectorCatalogHandle(desc, connector);
        CatalogRegistry reg = new CatalogRegistry(new EmptyMetadataStore(), chf, null, null);
        CatalogDescriptor descriptor = new CatalogDescriptor(
                "test_cat", CatalogType.CONNECTOR, "test",
                CatalogState.ACTIVE, Map.of(), "", Set.of());
        reg.register(descriptor);
        return reg;
    }

    private static final class TestConnector implements Connector {
        private final ConnectorMetadata metadata;

        TestConnector(ConnectorMetadata metadata) {
            this.metadata = metadata;
        }

        @Override public ConnectorMetadata getMetadata(ConnectorSession s) { return metadata; }
        @Override public ConnectorSplitManager getSplitManager() { return null; }
        @Override public ConnectorPageSourceProvider getPageSourceProvider() { return null; }
    }

    /**
     * ConnectorMetadata that records all ALTER TABLE operations and maintains
     * an in-memory column map to verify schema changes propagate correctly.
     */
    private static final class RecordingMetadata implements ConnectorMetadata {

        final List<String> operations = new CopyOnWriteArrayList<>();
        final Map<String, Object> defaults = new LinkedHashMap<>();
        final Map<String, String> columns = new LinkedHashMap<>(Map.of(
                "id", "INTEGER",
                "name", "VARCHAR"
        ));

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName name) {
            return new TestTableHandle(name.schema(), name.table());
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
            return new ConnectorTableMetadata(
                    new SchemaTableName("default", "users"),
                    new ArrayList<>(columns.keySet()), Map.of());
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle table) {
            Map<String, ColumnHandle> handles = new LinkedHashMap<>();
            for (String col : columns.keySet()) {
                handles.put(col, new TestColumnHandle(col));
            }
            return handles;
        }

        @Override
        public TableStatistics getTableStatistics(ConnectorSession session, ConnectorTableHandle table) {
            return TableStatistics.UNKNOWN;
        }

        @Override
        public List<String> listSchemaNames(ConnectorSession session) {
            return List.of("default");
        }

        @Override
        public List<SchemaTableName> listTables(ConnectorSession session, java.util.Optional<String> schemaName) {
            return List.of(new SchemaTableName("default", "users"));
        }

        @Override
        public void addColumn(ConnectorSession session, ConnectorTableHandle table,
                              String columnName, PangrovaType columnType, Object defaultValue) {
            operations.add("addColumn(" + columnName + ", " + columnType + ", " + defaultValue + ")");
            columns.put(columnName, columnType.name());
            defaults.put(columnName, defaultValue);
        }

        @Override
        public void dropColumn(ConnectorSession session, ConnectorTableHandle table,
                               String columnName) {
            operations.add("dropColumn(" + columnName + ")");
            columns.remove(columnName);
        }

        @Override
        public void renameColumn(ConnectorSession session, ConnectorTableHandle table,
                                 String oldName, String newName) {
            operations.add("renameColumn(" + oldName + ", " + newName + ")");
            String type = columns.remove(oldName);
            if (type != null) {
                columns.put(newName, type);
            }
        }

        @Override
        public void changeColumnType(ConnectorSession session, ConnectorTableHandle table,
                                     String columnName, PangrovaType newType) {
            operations.add("changeColumnType(" + columnName + ", " + newType + ")");
            columns.put(columnName, newType.name());
        }
    }

    private record TestTableHandle(String schema, String table) implements ConnectorTableHandle {}
    private record TestColumnHandle(String name) implements ColumnHandle {
        @Override public String columnName() { return name; }
    }

    private static final class EmptyMetadataStore implements MetadataStore {
        @Override public void saveTable(TableMetadata m) { }
        @Override public Optional<TableMetadata> loadTable(String c, String s, String t) { return Optional.empty(); }
        @Override public void deleteTable(String c, String s, String t) { }
        @Override public List<org.pangrova.metadata.model.QualifiedTableRef> searchTables(String p) { return List.of(); }
        @Override public void saveCatalogDescriptor(CatalogDescriptor d) { }
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String n) { return Optional.empty(); }
        @Override public void deleteCatalogDescriptor(String n) { }
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.of(); }
        @Override public Optional<Instant> getLastSyncTime(String c) { return Optional.empty(); }
        @Override public void updateSyncTime(String c, Instant t) { }
        @Override public void recordSyncHistory(SyncResult r) { }
        @Override public void saveSchemas(String c, List<String> s) { }
    }
}
