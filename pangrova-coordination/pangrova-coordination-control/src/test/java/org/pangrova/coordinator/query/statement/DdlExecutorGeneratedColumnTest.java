package org.pangrova.coordinator.query.statement;

import org.pangrova.parser.api.enums.EmbeddingMode;
import org.pangrova.metadata.api.catalog.CatalogDeploymentIdentity;

import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.connector.ConnectorCatalogHandle;
import org.pangrova.metadata.api.catalog.ConnectorHandleFactory;
import org.pangrova.persistence.catalog.api.catalog.CatalogDeploymentRepository;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.statement.AutoEmbeddingColumnSpec;
import org.pangrova.parser.api.ast.statement.AlterTableStatement;
import org.pangrova.parser.api.ast.statement.CreateTableStatement;
import org.pangrova.protocol.capability.BuiltInCapabilityCatalog;
import org.pangrova.protocol.type.VectorLogicalType;
import org.pangrova.generation.codec.column.GeneratedColumnMetadataCodec;
import org.pangrova.protocol.v2.ExecutionMode;
import org.pangrova.protocol.v2.GeneratedColumnSet;
import org.pangrova.connector.api.plugin.Connector;
import org.pangrova.connector.api.session.ConnectorSession;
import org.pangrova.connector.api.data.ConnectorPageSourceProvider;
import org.pangrova.connector.api.metadata.ConnectorMetadata;
import org.pangrova.connector.api.metadata.ConnectorOutputTableHandle;
import org.pangrova.connector.api.metadata.ConnectorSplitManager;
import org.pangrova.connector.api.metadata.table.ColumnHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableHandle;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.connector.api.metadata.table.TableStatistics;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.pangrova.coordinator.query.statement.StatementTestSessions.context;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DdlExecutorGeneratedColumnTest {

    private static org.pangrova.query.api.session.QuerySession statementSession() {
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
        CatalogRegistry registry = new CatalogRegistry(memoryTableStores(),
                new EmptyCatalogDeployments(), factory, null, null);
        registry.register(new CatalogDescriptor(
                "test_cat", CatalogType.CONNECTOR, "test",
                CatalogState.ACTIVE, Map.of(), "", Set.of()));
        executor = new DdlExecutor(registry);
    }

    @Test
    void persistsChecksummedGenericGeneratedColumnDefinition() {
        executor.executeCreateTable(table(
                new CreateTableStatement.ColumnDefinition(
                        "embedding", org.pangrova.parser.build.type.RawSqlTypeParser.parse("VECTOR(3)"), true,
                        Optional.empty(), Optional.of(new AutoEmbeddingColumnSpec(
                                "embed-v1", List.of("title", "body"),
                                EmbeddingMode.ASYNC)))), statementSession());

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
                        "payload", org.pangrova.parser.build.type.RawSqlTypeParser.parse("VARBINARY")),
                generated("embedding", List.of("payload"), true)), statementSession()))
                .hasMessageContaining("must be VARCHAR, CHAR, or STRING");

        assertThatThrownBy(() -> executor.executeCreateTable(table(
                generated("embedding", List.of("title"), false)), statementSession()))
                .hasMessageContaining("must be nullable");
    }

    @Test
    void rejectsSyncDefinitionBeforeCreatingAnUnwritableTable() {
        var sync = new CreateTableStatement.ColumnDefinition(
                "embedding", org.pangrova.parser.build.type.RawSqlTypeParser.parse("VECTOR(3)"), false,
                Optional.empty(), Optional.of(new AutoEmbeddingColumnSpec(
                        "embed-v1", List.of("title"),
                        EmbeddingMode.SYNC)));

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
                        "title", org.pangrova.parser.build.type.RawSqlTypeParser.parse("VARCHAR"))),
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
                        "embedding_2", org.pangrova.parser.build.type.RawSqlTypeParser.parse("VECTOR(3)"),
                        Optional.empty(), Optional.of(new AutoEmbeddingColumnSpec(
                                "embed-v1", List.of("body"),
                                EmbeddingMode.SYNC))),
                statementSession()))
                .hasMessageContaining("requires atomic schema");
    }

    private static CreateTableStatement table(
            CreateTableStatement.ColumnDefinition generated) {
        return tableWithSource(new CreateTableStatement.ColumnDefinition(
                "title", org.pangrova.parser.build.type.RawSqlTypeParser.parse("VARCHAR")), generated);
    }

    private static CreateTableStatement tableWithSource(
            CreateTableStatement.ColumnDefinition source,
            CreateTableStatement.ColumnDefinition generated) {
        return new CreateTableStatement(
                Identifier.of("test_cat", "default", "docs"),
                List.of(source,
                        new CreateTableStatement.ColumnDefinition(
                                "body", org.pangrova.parser.build.type.RawSqlTypeParser.parse("STRING")),
                        generated),
                false, List.of(), List.of(), List.of(), false,
                Optional.empty(), Map.of());
    }

    private static CreateTableStatement.ColumnDefinition generated(
            String target, List<String> sources, boolean nullable) {
        return new CreateTableStatement.ColumnDefinition(
                target, org.pangrova.parser.build.type.RawSqlTypeParser.parse("VECTOR(3)"), nullable,
                Optional.empty(), Optional.of(new AutoEmbeddingColumnSpec(
                        "embed-v1", sources,
                        EmbeddingMode.ASYNC)));
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

    private static final class EmptyCatalogDeployments implements CatalogDeploymentRepository {
        @Override
        public java.util.Optional<org.pangrova.metadata.api.catalog.CatalogDescriptor>
                findCatalogDescriptorRevision(String name, String incarnationId, long revision) {
            throw new UnsupportedOperationException("This fixture does not store deployment history");
        }

        private final Map<String, CatalogDescriptor> descriptors = new java.util.LinkedHashMap<>();

        @Override public void saveCatalogDescriptor(CatalogDescriptor descriptor) {
            descriptors.put(descriptor.name(), descriptor);
        }
        @Override public Optional<CatalogDescriptor> getCatalogDescriptor(String name) {
            return Optional.ofNullable(descriptors.get(name));
        }
        @Override public boolean deleteCatalogDescriptor(CatalogDeploymentIdentity expected) { return descriptors.entrySet().removeIf(entry -> entry.getValue().identity().equals(expected)); }
        @Override public List<CatalogDescriptor> listAllCatalogDescriptors() {
            return List.of();
        }

    }
}
