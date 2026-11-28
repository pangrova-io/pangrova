package org.pangrova.coordinator.metadata;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.pangrova.common.client.result.ResultEndpoint;
import org.pangrova.common.enums.ResultDisposition;
import org.pangrova.connector.api.metadata.table.ConnectorTableMetadata;
import org.pangrova.connector.api.metadata.table.SchemaTableName;
import org.pangrova.coordinator.query.execution.dispatch.MetadataExecutionFactory;
import org.pangrova.coordinator.query.execution.dispatch.MetadataTask;
import org.pangrova.coordinator.query.execution.dispatch.StatementDispatcher;
import org.pangrova.coordinator.query.execution.dispatch.StatementDispatcherContext;
import org.pangrova.coordinator.query.execution.dispatch.task.ShowTablesTask;
import org.pangrova.coordinator.query.execution.dispatch.task.catalog.ShowCatalogsTask;
import org.pangrova.coordinator.query.execution.dispatch.task.catalog.ShowSchemasTask;
import org.pangrova.coordinator.query.session.SessionContextConverter;
import org.pangrova.coordinator.query.statement.ShowDescribeExecutor;
import org.pangrova.coordinator.result.CoordinatorResultBuffer;
import org.pangrova.coordinator.result.CoordinatorResultService;
import org.pangrova.coordinator.result.CoordinatorResultWriter;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.observability.sdk.tracing.otel.OtelExecutionTraceContextFactory;
import org.pangrova.parser.api.ast.ShowSchemasStatement;
import org.pangrova.parser.api.ast.Statement;
import org.pangrova.parser.api.ast.statement.ShowCatalogsStatement;
import org.pangrova.parser.api.ast.statement.ShowTablesStatement;
import org.pangrova.query.api.enums.QueryState;
import org.pangrova.query.api.session.QuerySession;
import org.pangrova.query.lifecycle.QueryStateMachine;
import org.pangrova.security.api.identity.UserIdentity;
import org.pangrova.security.secret.resolution.SecretResolverRegistry;
import org.pangrova.types.enums.PangrovaType;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pangrova.coordinator.result.CoordinatorResultTestAccess.streamedRoot;
import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

/** Actual metadata tasks and the production Arrow IPC result service preserve empty tabular schemas. */
class MetadataShowResultSchemaTest {
    @TempDir
    Path warehouse;

    @Test
    void emptyAndPopulatedCatalogSchemaTableListingsHaveTheSameShape() throws Exception {
        var factory = new PluginConnectorHandleFactory(getClass().getClassLoader(),
                new SecretResolverRegistry(), descriptor -> Optional.empty());
        var registry = new CatalogRegistry(memoryTableStores(), new MetadataBootstrapTest.InMemoryCatalogDeployments(),
                factory, null, null);
        try (var results = new ResultHarness()) {
            var executor = new ShowDescribeExecutor(registry);
            var showCatalogs = new ShowCatalogsTask(executor);
            var showSchemas = new ShowSchemasTask(executor);
            var showTables = new ShowTablesTask(executor);
            assertThat(results.execute(new ShowCatalogsStatement(), showCatalogs, List.of("catalog_name"))).isEmpty();

            registry.register(new CatalogDescriptor("test_cat", CatalogType.CONNECTOR, "iceberg",
                    CatalogState.ACTIVE, Map.of("type", "hadoop", "warehouse", warehouse.toString()), "", Set.of()));
            assertThat(results.execute(new ShowCatalogsStatement(), showCatalogs, List.of("catalog_name")))
                    .containsExactly(List.of("test_cat"));
            assertThat(results.execute(new ShowSchemasStatement("test_cat"), showSchemas, List.of("schema_name")))
                    .isEmpty();

            var session = SessionContextConverter.toConnectorSession(session("schema-mutate"), "test_cat", "default");
            var metadata = registry.getCatalog("test_cat").getMetadata(session);
            metadata.createSchema(session, "default");
            assertThat(results.execute(new ShowSchemasStatement("test_cat"), showSchemas, List.of("schema_name")))
                    .containsExactly(List.of("default"));
            var tables = new ShowTablesStatement(List.of("test_cat", "default"));
            assertThat(results.execute(tables, showTables, List.of("table_name"))).isEmpty();

            metadata.createTable(session, new ConnectorTableMetadata(new SchemaTableName("default", "orders"),
                    List.of("id"), List.of(new ConnectorTableMetadata.Column("id", PangrovaType.BIGINT)), Map.of()));
            assertThat(results.execute(tables, showTables, List.of("table_name")))
                    .containsExactly(List.of("orders"));
        } finally {
            registry.shutdown();
            factory.shutdown();
        }
    }

    static QuerySession session(String id) {
        return new QuerySession(id, Map.of("catalog", "test_cat", "schema", "default"),
                new UserIdentity("metadata-reader", Set.of(), Map.of(), Instant.EPOCH), "tenant-a",
                Optional.empty(), new OtelExecutionTraceContextFactory().capture());
    }

    static final class ResultHarness implements AutoCloseable {
        private final RootAllocator allocator = new RootAllocator();
        private final java.util.concurrent.ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        private final CoordinatorResultBuffer buffer = new CoordinatorResultBuffer(scheduler, 60_000L);
        private final CoordinatorResultService service = new CoordinatorResultService(buffer,
                new CoordinatorResultWriter(allocator), new ResultEndpoint("localhost", 9091, "unused", "grpc"),
                ignored -> () -> { });

        List<List<String>> execute(Statement statement, MetadataTask<?> task, List<String> columns) throws Exception {
            return execute(statement, task, columns, Map.of());
        }

        List<List<String>> execute(Statement statement, MetadataTask<?> task, List<String> columns,
                                   Map<String, ArrowType> declaredTypes) throws Exception {
            String id = UUID.randomUUID().toString();
            var state = new QueryStateMachine();
            var factory = new MetadataExecutionFactory(Map.of(statement.getClass(), task));
            var dispatcher = new StatementDispatcher(new StatementDispatcherContext(service, List.of(factory), List.of(), null));
            var result = dispatcher.executeDirect(statement, state, id, session(id));
            assertThat(result).isNotNull();
            assertThat(state.status().state()).isEqualTo(QueryState.FINISHED);
            assertThat(state.status().disposition()).isEqualTo(ResultDisposition.STREAM);
            assertThat(state.status().endpoint()).isEqualTo(result.resultEndpoint());
            try (var reader = buffer.claim(id)) {
                assertThat(reader).isNotNull();
                var root = streamedRoot(reader::streamTo);
                assertThat(root.getSchema().getFields()).extracting(field -> field.getName()).containsExactlyElementsOf(columns);
                assertThat(root.getSchema().getFields()).allSatisfy(field ->
                        assertThat(field.getType()).as("Arrow type of %s", field.getName())
                                .isEqualTo(declaredTypes.getOrDefault(field.getName(), ArrowType.Utf8.INSTANCE)));
                List<List<String>> values = new ArrayList<>();
                for (int row = 0; row < root.getRowCount(); row++) {
                    List<String> cells = new ArrayList<>();
                    for (var vector : root.getFieldVectors()) {
                        Object value = vector.getObject(row);
                        cells.add(value == null ? null : value.toString());
                    }
                    values.add(cells);
                }
                return values;
            }
        }

        @Override
        public void close() throws InterruptedException {
            service.close();
            scheduler.shutdownNow();
            assertThat(scheduler.awaitTermination(5, TimeUnit.SECONDS)).isTrue();
            allocator.close();
        }
    }
}
