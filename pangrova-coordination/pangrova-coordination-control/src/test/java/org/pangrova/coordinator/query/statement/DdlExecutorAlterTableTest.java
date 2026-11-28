package org.pangrova.coordinator.query.statement;

import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;
import org.pangrova.indexing.api.evolution.SchemaChangeEvent;
import org.pangrova.indexing.api.enums.SchemaChangeType;
import org.pangrova.indexing.evolution.IndexSchemaEvolutionHandler;
import org.pangrova.persistence.indexing.api.metadata.IndexMetadataStore;

import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.api.catalog.ConnectorHandleFactory;
import org.pangrova.persistence.catalog.api.catalog.CatalogDeploymentRepository;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.parse.AntlrSqlParser;
import org.pangrova.parser.api.ast.statement.AlterTableStatement;
import org.pangrova.parser.api.ast.statement.DropTableStatement;
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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static org.pangrova.coordinator.query.statement.StatementTestSessions.context;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Verifies ALTER TABLE DDL operations propagate to ConnectorMetadata.
 *
 * <p>Ensures the coordinator DDL handler calls the connector SPI methods
 * (addColumn, dropColumn, renameColumn, changeColumnType) so schema changes
 * reach the underlying storage layer (Iceberg/Paimon).</p>
 */
class DdlExecutorAlterTableTest {

    private static org.pangrova.query.api.session.QuerySession statementSession() {
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
                "email", org.pangrova.parser.build.type.RawSqlTypeParser.parse("VARCHAR"));

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
        AlterTableStatement stmt = (AlterTableStatement) new AntlrSqlParser().parse(
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
    void dropColumn_fencesIndexesBeforeConnectorMutationAndCommitsAfterward() {
        var observed = new AtomicReference<SchemaChangeEvent>();
        ddlExecutor.setSchemaEvolutionHandler(schemaHandler(recordingMetadata.operations, observed, null));
        AlterTableStatement stmt = new AlterTableStatement.DropColumn(
                Identifier.of("test_cat", "default", "users"), "name");

        ddlExecutor.executeAlterTable(stmt, statementSession());

        assertThat(recordingMetadata.operations).containsExactly("prepare", "dropColumn(name)", "commit", "apply");
        assertThat(observed.get().type()).isEqualTo(SchemaChangeType.DROP_COLUMN);
        assertThat(observed.get().oldColumnName()).isEqualTo("name");
    }

    @Test
    void unknownOutcomeFailureIsSuppressedWithoutMaskingTheConnectorFailure() {
        var connectorFailure = new IllegalArgumentException("connector failed");
        recordingMetadata.dropFailure = connectorFailure;
        ddlExecutor.setSchemaEvolutionHandler(schemaHandler(
                recordingMetadata.operations, new AtomicReference<>(), new IllegalStateException("abort failed")));
        AlterTableStatement stmt = new AlterTableStatement.DropColumn(
                Identifier.of("test_cat", "default", "users"), "name");

        Throwable failure = catchThrowable(() -> ddlExecutor.executeAlterTable(stmt, statementSession()));

        assertThat(failure).isSameAs(connectorFailure);
        assertThat(failure.getSuppressed()).singleElement().satisfies(suppressed ->
                assertThat(suppressed).isInstanceOf(IllegalStateException.class).hasMessage("abort failed"));
    }

    @Test
    void connectorFailureRecordsUnknownRatherThanAssumingAnAbort() {
        recordingMetadata.dropFailure = new IllegalArgumentException("connector result unavailable");
        ddlExecutor.setSchemaEvolutionHandler(schemaHandler(
                recordingMetadata.operations, new AtomicReference<>(), null));
        var stmt = new AlterTableStatement.DropColumn(
                Identifier.of("test_cat", "default", "users"), "name");
        assertThat(catchThrowable(() -> ddlExecutor.executeAlterTable(stmt, statementSession())))
                .isSameAs(recordingMetadata.dropFailure);
        assertThat(recordingMetadata.operations).containsExactly("prepare", "dropColumn(name)", "unknown");
    }

    @Test
    void initiallyAbsentDropHonorsIfExistsWithoutStartingJournalOperation() {
        recordingMetadata.tablePresent = false;
        ddlExecutor.setSchemaEvolutionHandler(schemaHandler(
                recordingMetadata.operations, new AtomicReference<>(), null));
        assertThat(ddlExecutor.executeDropTable(
                new DropTableStatement(Identifier.of("test_cat", "default", "users"), true),
                statementSession())).isEmpty();
        assertThat(recordingMetadata.operations).isEmpty();
        var failure = catchThrowable(() -> ddlExecutor.executeDropTable(
                new DropTableStatement(Identifier.of("test_cat", "default", "users"), false), statementSession()));
        assertThat(failure).isInstanceOfSatisfying(org.pangrova.common.exception.PangrovaException.class,
                error -> assertThat(error.getErrorCode()).isEqualTo(
                        org.pangrova.common.enums.PangrovaErrorCode.METADATA_TABLE_NOT_FOUND));
        assertThat(recordingMetadata.operations).isEmpty();
    }

    @Test
    void unresolvedDropResultDoesNotBecomeSuccessfulIfExists() {
        recordingMetadata.dropFailure = new org.pangrova.connector.api.exception.ConnectorException(
                org.pangrova.common.enums.PangrovaErrorCode.CONNECTOR_DDL_FAILED,
                "Iceberg drop outcome could not be established");
        var observed = new AtomicReference<SchemaChangeEvent>();
        ddlExecutor.setSchemaEvolutionHandler(schemaHandler(recordingMetadata.operations, observed, null));
        assertThat(catchThrowable(() -> ddlExecutor.executeDropTable(
                new DropTableStatement(Identifier.of("test_cat", "default", "users"), true), statementSession())))
                .isSameAs(recordingMetadata.dropFailure);
        assertThat(recordingMetadata.operations).containsExactly("prepare", "dropTable", "unknown");
        assertThat(observed.get().type()).isEqualTo(SchemaChangeType.DROP_TABLE);
    }

    @Test
    void dropTableGenericNotFoundFailureIsStillUnknownEvenWithIfExists() {
        recordingMetadata.dropFailure = new org.pangrova.connector.api.exception.ConnectorException(
                org.pangrova.common.enums.PangrovaErrorCode.CONNECTOR_TABLE_NOT_FOUND, "generic connector failure");
        ddlExecutor.setSchemaEvolutionHandler(schemaHandler(
                recordingMetadata.operations, new AtomicReference<>(), null));
        assertThat(catchThrowable(() -> ddlExecutor.executeDropTable(
                new DropTableStatement(Identifier.of("test_cat", "default", "users"), true), statementSession())))
                .isSameAs(recordingMetadata.dropFailure);
        assertThat(recordingMetadata.operations).containsExactly("prepare", "dropTable", "unknown");
    }

    @Test
    void confirmedDropTableStillCommitsTheExactJournalOperation() {
        ddlExecutor.setSchemaEvolutionHandler(schemaHandler(
                recordingMetadata.operations, new AtomicReference<>(), null));
        assertThat(ddlExecutor.executeDropTable(
                new DropTableStatement(Identifier.of("test_cat", "default", "users"), false),
                statementSession())).isEmpty();
        assertThat(recordingMetadata.operations).containsExactly("prepare", "dropTable", "commit", "apply");
    }

    @Test
    void changedSourceIdentityAbortsBeforeConnectorMutation() {
        recordingMetadata.identities.add(new org.pangrova.connector.api.metadata.table.TableIdentity(
                "ICEBERG", "table-a", "warehouse/a", "schema-a"));
        recordingMetadata.identities.add(new org.pangrova.connector.api.metadata.table.TableIdentity(
                "ICEBERG", "table-b", "warehouse/b", "schema-b"));
        ddlExecutor.setSchemaEvolutionHandler(schemaHandler(
                recordingMetadata.operations, new AtomicReference<>(), null));
        var stmt = new AlterTableStatement.DropColumn(
                Identifier.of("test_cat", "default", "users"), "name");
        assertThat(catchThrowable(() -> ddlExecutor.executeAlterTable(stmt, statementSession())))
                .isInstanceOf(IllegalStateException.class).hasMessageContaining("source identity changed");
        assertThat(recordingMetadata.operations).containsExactly("prepare", "abort");
        assertThat(recordingMetadata.columns).containsKey("name");
    }

    private static IndexSchemaEvolutionHandler schemaHandler(
            List<String> operations, AtomicReference<SchemaChangeEvent> observed,
            RuntimeException abortFailure) {
        UUID operationId = UUID.randomUUID();
        var now = java.time.Instant.now();
        IndexMetadataStore store = (IndexMetadataStore) Proxy.newProxyInstance(
                DdlExecutorAlterTableTest.class.getClassLoader(), new Class<?>[] {IndexMetadataStore.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "prepareSchemaEvolution" -> {
                        operations.add("prepare");
                        observed.set((SchemaChangeEvent) arguments[0]);
                        yield operationId;
                    }
                    case "markSchemaEvolutionCommitted" -> {
                        assertThat(arguments[0]).isEqualTo(operationId);
                        operations.add("commit");
                        yield null;
                    }
                    case "findSchemaEvolution" -> {
                        assertThat(arguments[0]).isEqualTo(operationId);
                        assertThat(operations).endsWith("commit");
                        yield Optional.of(new org.pangrova.persistence.indexing.api.metadata.IndexSchemaEvolutionStore
                                .IndexSchemaEvolutionRecord(operationId, null, observed.get(),
                                        org.pangrova.persistence.indexing.api.enums.IndexSchemaEvolutionState.COMMITTED,
                                        now, now));
                    }
                    case "listSchemaEvolutionTargets" -> List.of();
                    case "applySchemaEvolution" -> {
                        assertThat(arguments[0]).isEqualTo(operationId);
                        operations.add("apply");
                        yield null;
                    }
                    case "markSchemaEvolutionAborted" -> {
                        operations.add("abort");
                        yield null;
                    }
                    case "markSchemaEvolutionUnknown" -> {
                        operations.add("unknown");
                        if (abortFailure != null) {
                            throw abortFailure;
                        }
                        yield null;
                    }
                    case "toString" -> "IndexMetadataStoreTestDouble";
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> proxy == arguments[0];
                    default -> throw new AssertionError(
                            "Unexpected IndexMetadataStore call: " + method.getName());
                });
        return new IndexSchemaEvolutionHandler(store, (descriptor, event) ->
                IndexSchemaEvolutionHandler.SchemaObservation.absent());
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
                Identifier.of("test_cat", "default", "users"), "id", org.pangrova.parser.build.type.RawSqlTypeParser.parse("BIGINT"));

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
        CatalogRegistry reg = new CatalogRegistry(memoryTableStores(), new EmptyCatalogDeployments(), chf, null, null);
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
        RuntimeException dropFailure;
        boolean tablePresent = true;
        final java.util.ArrayDeque<org.pangrova.connector.api.metadata.table.TableIdentity> identities =
                new java.util.ArrayDeque<>();
        final Map<String, String> columns = new LinkedHashMap<>(Map.of(
                "id", "INTEGER",
                "name", "VARCHAR"
        ));

        @Override
        public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName name) {
            return tablePresent ? new TestTableHandle(name.schema(), name.table()) : null;
        }

        @Override
        public Optional<org.pangrova.connector.api.metadata.table.TableIdentity> getTableIdentity(
                ConnectorSession session, ConnectorTableHandle table) {
            return Optional.ofNullable(identities.poll());
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
        public void dropTable(ConnectorSession session, ConnectorTableHandle table) {
            operations.add("dropTable");
            if (dropFailure != null) {
                throw dropFailure;
            }
        }

        @Override
        public void dropColumn(ConnectorSession session, ConnectorTableHandle table,
                               String columnName) {
            operations.add("dropColumn(" + columnName + ")");
            if (dropFailure != null) {
                throw dropFailure;
            }
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

    private static final class EmptyCatalogDeployments implements CatalogDeploymentRepository {
        @Override
        public java.util.Optional<org.pangrova.metadata.api.catalog.CatalogDescriptor>
                findCatalogDescriptorRevision(String name, String incarnationId, long revision) {
            throw new UnsupportedOperationException("This fixture does not store deployment history");
        }

        @Override public void saveCatalogDescriptor(CatalogDescriptor d) { }
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String n) { return Optional.empty(); }
        @Override public boolean deleteCatalogDescriptor(CatalogDeploymentIdentity expected) { throw new UnsupportedOperationException("Read-only catalog fixture"); }
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() { return List.of(); }

    }
}
