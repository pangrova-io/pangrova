package org.pangrova.coordinator.metadata;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;
import org.pangrova.coordinator.indexing.connector.ConnectorAccessorResolver;
import org.pangrova.coordinator.indexing.ddl.IndexBuildServices;
import org.pangrova.coordinator.indexing.ddl.IndexDdlContext;
import org.pangrova.coordinator.indexing.ddl.IndexDdlExecutor;
import org.pangrova.coordinator.query.execution.dispatch.task.ShowIndexesTask;
import org.pangrova.coordinator.query.execution.dispatch.task.stream.ShowStreamsTask;
import org.pangrova.coordinator.testinfra.SharedPostgresContainer;
import org.pangrova.indexing.api.descriptor.IndexDescriptor;
import org.pangrova.indexing.api.enums.IndexState;
import org.pangrova.indexing.api.enums.IndexType;
import org.pangrova.indexing.build.IndexBuildContext;
import org.pangrova.indexing.build.IndexBuildCoordinator;
import org.pangrova.indexing.config.IndexingConfig;
import org.pangrova.metadata.catalog.registry.CatalogRegistry;
import org.pangrova.metadata.testinfra.TableWriteAuthorityFixture;
import org.pangrova.parser.api.ast.Identifier;
import org.pangrova.parser.api.ast.statement.ShowIndexesStatement;
import org.pangrova.parser.api.ast.statement.stream.ShowStreamsStatement;
import org.pangrova.persistence.indexing.postgresql.metadata.JdbcIndexMetadataStore;
import org.pangrova.persistence.streaming.postgresql.JdbcStreamStateStore;
import org.pangrova.streaming.api.checkpoint.StreamState;
import org.pangrova.streaming.api.enums.StreamLifecycleState;
import org.pangrova.streaming.cdc.CdcRecordConverterRegistry;
import org.pangrova.streaming.coordinator.CdcSourceRegistry;
import org.pangrova.streaming.coordinator.StreamDdlContext;
import org.pangrova.streaming.coordinator.StreamDdlExecutor;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.constants.TestObservability;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.pangrova.metadata.testinfra.TableStoreFixtures.memoryTableStores;

/** Actual JDBC stores, metadata task execution and Arrow result publication. */
class MetadataStoreShowResultSchemaIntegrationTest extends ContainerTestBase {
    @Test
    void emptyAndMixedIndexListingUsesTheDeclaredSchema() throws Exception {
        verifyIndexSchemas(SharedPostgresContainer.migratedDataSource("show_index_schema"));
    }

    @Test
    void emptyAndPopulatedStreamListingUsesTheDeclaredSchema() throws Exception {
        verifyStreamSchemas(SharedPostgresContainer.migratedDataSource("show_stream_schema"));
    }

    static void verifyIndexSchemas(DataSource source) throws Exception {
        var store = new JdbcIndexMetadataStore(source, TableWriteAuthorityFixture.create()::requireCurrent);
        var registry = new CatalogRegistry(memoryTableStores(), null);
        try (var builder = new IndexBuildCoordinator(new IndexBuildContext(
                new IndexingConfig("show-index-shape", false, null, null), null, null, null), null);
             var ddl = new IndexDdlExecutor(new IndexDdlContext(store, new IndexBuildServices(builder, null),
                     registry, new ConnectorAccessorResolver(registry)), null, null);
             var results = new MetadataShowResultSchemaTest.ResultHarness()) {
            var task = new ShowIndexesTask(ddl);
            var stmt = new ShowIndexesStatement(Identifier.of("test_cat", "default", "orders"));
            List<String> columns = List.of("index_name", "index_type", "columns", "state",
                    "stale_reason", "created_at", "algorithm", "metric", "index_id", "generation",
                    "build_snapshot_id", "last_indexed_snapshot_id", "updated_at");
            Map<String, ArrowType> types = Map.of("generation", new ArrowType.Int(32, true));
            assertThat(results.execute(stmt, task, columns, types)).isEmpty();

            store.registerIndex(descriptor("scalar", IndexType.BLOOM_FILTER, Map.of()));
            assertThat(results.execute(stmt, task, columns, types)).hasSize(1)
                    .allSatisfy(row -> {
                        assertThat(row.get(0)).isEqualTo("scalar");
                        assertThat(row.get(6)).isEmpty();
                        assertThat(row.get(7)).isEmpty();
                        assertThat(row.subList(8, 13)).containsExactly(
                                "show-index-scalar", "0", "snap-1", "snap-1", Instant.EPOCH.toString());
                    });
            store.registerIndex(descriptor("vector", IndexType.VECTOR, Map.of("algorithm", "hnsw", "metric", "cosine")));
            store.registerIndex(descriptor("inverted", IndexType.INVERTED, Map.of("analyzer", "standard")));
            var rows = results.execute(stmt, task, columns, types);
            assertThat(rows).hasSize(3);
            assertThat(rows.stream().filter(row -> row.get(0).equals("vector")).findFirst().orElseThrow().subList(6, 8))
                    .containsExactly("hnsw", "cosine");
            assertThat(rows.stream().filter(row -> row.get(0).equals("scalar")).findFirst().orElseThrow().subList(6, 8))
                    .containsExactly("", "");
            assertThat(rows.stream().filter(row -> row.get(0).equals("inverted")).findFirst().orElseThrow().subList(6, 8))
                    .containsExactly("standard", "");
        } finally {
            registry.shutdown();
        }
    }

    private static IndexDescriptor descriptor(String name, IndexType type, Map<String, String> properties) {
        return new IndexDescriptor("show-index-" + name, name, "test_cat", "default", "orders", List.of("id"),
                type, properties, IndexState.READY, null, 0, "snap-1", "snap-1",
                Instant.EPOCH, Instant.EPOCH, "tenant-a");
    }

    static void verifyStreamSchemas(DataSource source) throws Exception {
        var states = new JdbcStreamStateStore(source);
        try (var results = new MetadataShowResultSchemaTest.ResultHarness()) {
            var task = new ShowStreamsTask(new StreamDdlExecutor(new StreamDdlContext(states,
                    new CdcRecordConverterRegistry(List.of()), 10,
                    new CdcSourceRegistry(List.of()))));
            var stmt = new ShowStreamsStatement();
            List<String> columns = List.of("stream_name", "tenant_id", "state", "target_table", "source_format", "created_at");
            assertThat(results.execute(stmt, task, columns)).isEmpty();

            assertThat(states.createState(StreamState.builder().streamName("orders-stream").tenantId("tenant-a")
                    .sourceProvider("kafka").sourceProperties(Map.of()).sourceFormat("debezium-json")
                    .targetTable("test_cat.default.orders").lifecycleState(StreamLifecycleState.SUSPENDED)
                    .streamProperties(Map.of()).createdAt(Instant.EPOCH).updatedAt(Instant.EPOCH).build())).isTrue();
            assertThat(results.execute(stmt, task, columns)).containsExactly(List.of(
                    "orders-stream", "tenant-a", "SUSPENDED", "test_cat.default.orders", "debezium-json",
                    Instant.EPOCH.toString()));
        }
    }
}
