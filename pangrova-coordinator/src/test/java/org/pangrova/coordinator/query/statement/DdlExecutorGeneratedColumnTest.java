package org.pangrova.coordinator.query.statement;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.core.catalog.CatalogRegistry;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.core.catalog.ConnectorCatalogHandle;
import org.pangrova.metadata.core.catalog.ConnectorHandleFactory;
import org.pangrova.metadata.api.schema.TableMetadata;
import org.pangrova.metadata.core.store.MetadataStore;
import org.pangrova.metadata.core.store.SyncResult;
import org.pangrova.parser.ast.Identifier;
import org.pangrova.parser.ast.statement.AutoEmbeddingColumnSpec;
import org.pangrova.parser.ast.statement.AlterTableStatement;
import org.pangrova.parser.ast.statement.CreateTableStatement;
import org.pangrova.parser.ast.type.DataTypeSpec;
import org.pangrova.protocol.BuiltInCapabilityCatalog;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.protocol.GeneratedColumnMetadataCodec;
import org.pangrova.protocol.v2.ExecutionMode;
import org.pangrova.protocol.v2.GeneratedColumnSet;
import org.pangrova.spi.connector.Connector;
import org.pangrova.spi.connector.ConnectorSession;
import org.pangrova.spi.connector.data.ConnectorPageSourceProvider;
import org.pangrova.spi.connector.metadata.ConnectorMetadata;
import org.pangrova.spi.connector.metadata.ConnectorOutputTableHandle;
import org.pangrova.spi.connector.metadata.ConnectorSplitManager;
import org.pangrova.spi.connector.metadata.table.ColumnHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableHandle;
import org.pangrova.spi.connector.metadata.table.ConnectorTableMetadata;
import org.pangrova.spi.connector.metadata.table.SchemaTableName;
import org.pangrova.spi.connector.metadata.table.TableStatistics;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.pangrova.coordinator.query.statement.StatementTestSessions.context;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DdlExecutorGeneratedColumnTest {

    private static org.pangrova.coordinator.query.session.CoordinatorSessionContext statementSession() {
        return context();
    }

    private RecordingMetadata metadata;
    private DdlExecutor executor;

    @BeforeEach
    void setUp() {
        metadata = new RecordingMetadata();
        Connector connector = new TestConnector(metadata);
        ConnectorHandleFactory factory = descriptor ->
                new ConnectorCatalogHandle(descriptor, connector);
        CatalogRegistry registry = new CatalogRegistry(
                new EmptyMetadataStore(), factory, null, null);
        registry.register(new CatalogDescriptor(
                "test_cat", CatalogType.CONNECTOR, "test",
                CatalogState.ACTIVE, Map.of(), "", Set.of()));
        executor = new DdlExecutor(registry);
    }

    @Test
    void persistsChecksummedGenericGeneratedColumnDefinition() {
        executor.executeCreateTable(table(
                new CreateTableStatement.ColumnDefinition(
                        "embedding", DataTypeSpec.fromRawTypeString("VECTOR(3)"), true,
                        Optional.empty(), Optional.of(new AutoEmbeddingColumnSpec(
                                "embed-v1", List.of("title", "body"),
                                AutoEmbeddingColumnSpec.EmbeddingMode.ASYNC)))), statementSession());

        GeneratedColumnSet definitions = GeneratedColumnMetadataCodec.decode(
                metadata.created.properties());
        assertThat(definitions.getDefinitionsList()).singleElement().satisfies(definition -> {
            assertThat(definition.getTargetColumn()).isEqualTo("embedding");
            assertThat(definition.getLogicalModelName()).isEqualTo("embed-v1");
            assertThat(definition.getSourceColumnsList()).containsExactly("title", "body");
            assertThat(definition.getExecutionMode())
                    .isEqualTo(ExecutionMode.EXECUTION_MODE_ASYNC);
            assertThat(definition.getCapability())
                    .isEqualTo(BuiltInCapabilityCatalog.aiEmbeddingPlanRef());
            assertThat(definition.getOutputType()).isEqualTo(VectorLogicalType.create(3));
        });
    }

    @Test
    void rejectsMissingOrNonTextSourcesAndAsyncNotNullTarget() {
        assertThatThrownBy(() -> executor.executeCreateTable(table(
                generated("embedding", List.of("missing"), true)), statementSession()))
                .hasMessageContaining("does not exist");

        assertThatThrownBy(() -> executor.executeCreateTable(tableWithSource(
                new CreateTableStatement.ColumnDefinition(
                        "payload", DataTypeSpec.fromRawTypeString("VARBINARY")),
                generated("embedding", List.of("payload"), true)), statementSession()))
                .hasMessageContaining("must be VARCHAR, CHAR, or STRING");

        assertThatThrownBy(() -> executor.executeCreateTable(table(
                generated("embedding", List.of("title"), false)), statementSession()))
                .hasMessageContaining("must be nullable");
    }

    @Test
    void rejectsSyncDefinitionBeforeCreatingAnUnwritableTable() {
        var sync = new CreateTableStatement.ColumnDefinition(
                "embedding", DataTypeSpec.fromRawTypeString("VECTOR(3)"), false,
                Optional.empty(), Optional.of(new AutoEmbeddingColumnSpec(
                        "embed-v1", List.of("title"),
                        AutoEmbeddingColumnSpec.EmbeddingMode.SYNC)));

        assertThatThrownBy(() -> executor.executeCreateTable(table(sync), statementSession()))
                .hasMessageContaining("Native write relation")
                .hasMessageContaining("use ASYNC");
        assertThat(metadata.created).isNull();
    }

    @Test
    void rejectsUserSuppliedGeneratedColumnProtocolProperties() {
        CreateTableStatement statement = new CreateTableStatement(
                Identifier.of("test_cat", "default", "docs"),
                List.of(new CreateTableStatement.ColumnDefinition(
                        "title", DataTypeSpec.fromRawTypeString("VARCHAR"))),
                false, List.of(), List.of(), List.of(), false,
                Optional.empty(), Map.of(
                        GeneratedColumnMetadataCodec.PAYLOAD_PROPERTY, "spoofed"));

        assertThatThrownBy(() -> executor.executeCreateTable(statement, statementSession()))
                .hasMessageContaining("system-managed");
    }

    @Test
    void rejectsNonAtomicAlterOfGeneratedDefinitionBeforeConnectorMutation() {
        executor.executeCreateTable(table(
                generated("embedding", List.of("title"), true)), statementSession());

        assertThatThrownBy(() -> executor.executeAlterTable(
                new AlterTableStatement.DropColumn(
                        Identifier.of("test_cat", "default", "docs"), "title"),
                statementSession()))
                .hasMessageContaining("generated-column definition references it");
        assertThat(metadata.dropCalls).isZero();

        assertThatThrownBy(() -> executor.executeAlterTable(
                new AlterTableStatement.AddColumn(
                        Identifier.of("test_cat", "default", "docs"),
                        "embedding_2", DataTypeSpec.fromRawTypeString("VECTOR(3)"),
                        Optional.empty(), Optional.of(new AutoEmbeddingColumnSpec(
                                "embed-v1", List.of("body"),
                                AutoEmbeddingColumnSpec.EmbeddingMode.SYNC))),
                statementSession()))
                .hasMessageContaining("requires atomic schema");
    }

    private static CreateTableStatement table(
            CreateTableStatement.ColumnDefinition generated) {
        return tableWithSource(new CreateTableStatement.ColumnDefinition(
                "title", DataTypeSpec.fromRawTypeString("VARCHAR")), generated);
    }

    private static CreateTableStatement tableWithSource(
            CreateTableStatement.ColumnDefinition source,
            CreateTableStatement.ColumnDefinition generated) {
        return new CreateTableStatement(
                Identifier.of("test_cat", "default", "docs"),
                List.of(source,
                        new CreateTableStatement.ColumnDefinition(
                                "body", DataTypeSpec.fromRawTypeString("STRING")),
                        generated),
                false, List.of(), List.of(), List.of(), false,
                Optional.empty(), Map.of());
    }

    private static CreateTableStatement.ColumnDefinition generated(
            String target, List<String> sources, boolean nullable) {
        return new CreateTableStatement.ColumnDefinition(
                target, DataTypeSpec.fromRawTypeString("VECTOR(3)"), nullable,
                Optional.empty(), Optional.of(new AutoEmbeddingColumnSpec(
                        "embed-v1", sources,
                        AutoEmbeddingColumnSpec.EmbeddingMode.ASYNC)));
    }

    private static final class RecordingMetadata implements ConnectorMetadata {
        private ConnectorTableMetadata created;
        private int dropCalls;

        @Override
        public ConnectorOutputTableHandle createTable(
                ConnectorSession session, ConnectorTableMetadata tableMetadata) {
            created = tableMetadata;
            return new TestOutputTableHandle();
        }

        @Override
        public ConnectorTableHandle getTableHandle(
                ConnectorSession session, SchemaTableName tableName) {
            return created == null ? null : new TestTableHandle();
        }

        @Override
        public ConnectorTableMetadata getTableMetadata(
                ConnectorSession session, ConnectorTableHandle table) {
            return created;
        }

        @Override
        public Map<String, ColumnHandle> getColumnHandles(
                ConnectorSession session, ConnectorTableHandle table) {
            return Map.of();
        }

        @Override
        public TableStatistics getTableStatistics(
                ConnectorSession session, ConnectorTableHandle table) {
            return TableStatistics.UNKNOWN;
        }

        @Override
        public List<String> listSchemaNames(ConnectorSession session) {
            return List.of("default");
        }

        @Override
        public List<SchemaTableName> listTables(
                ConnectorSession session, Optional<String> schemaName) {
            return created == null ? List.of() : List.of(created.table());
        }

        @Override
        public void dropColumn(
                ConnectorSession session, ConnectorTableHandle table, String columnName) {
            dropCalls++;
        }
    }

    private record TestTableHandle() implements ConnectorTableHandle {}
    private record TestOutputTableHandle() implements ConnectorOutputTableHandle {}

    private record TestConnector(ConnectorMetadata metadata) implements Connector {
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

    private static final class EmptyMetadataStore implements MetadataStore {
        private final Map<String, CatalogDescriptor> descriptors = new java.util.LinkedHashMap<>();
        @Override public void saveTable(TableMetadata metadata) {}
        @Override public Optional<TableMetadata> loadTable(String c, String s, String t) {
            return Optional.empty();
        }
        @Override public void deleteTable(String c, String s, String t) {}
        @Override public List<org.pangrova.metadata.api.object.QualifiedTableRef> searchTables(String p) {
            return List.of();
        }
        @Override public void saveCatalogDescriptor(CatalogDescriptor descriptor) {
            descriptors.put(descriptor.name(), descriptor);
        }
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String name) {
            return Optional.ofNullable(descriptors.get(name));
        }
        @Override public void deleteCatalogDescriptor(String name) { descriptors.remove(name); }
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() {
            return List.of();
        }
        @Override public Optional<Instant> getLastSyncTime(String catalog) {
            return Optional.empty();
        }
        @Override public void updateSyncTime(String catalog, Instant time) {}
        @Override public void recordSyncHistory(SyncResult result) {}
        @Override public void saveSchemas(String catalog, List<String> schemas) {}
    }
}
