package org.pangrova.coordinator.metadata;

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
import org.pangrova.persistence.streaming.postgresql.JdbcCdcCommitStateStore;
import org.pangrova.persistence.streaming.postgresql.JdbcStreamOffsetStore;
import org.pangrova.persistence.streaming.postgresql.JdbcStreamStateStore;
import org.pangrova.persistence.streaming.postgresql.JdbcWatermarkPositionStore;
import org.pangrova.streaming.api.checkpoint.StreamState;
import org.pangrova.streaming.api.enums.StreamLifecycleState;
import org.pangrova.streaming.cdc.CdcRecordConverterRegistry;
import org.pangrova.streaming.commit.TwoPhaseCommitCoordinator;
import org.pangrova.streaming.coordinator.CdcSourceRegistry;
import org.pangrova.streaming.coordinator.StreamConfig;
import org.pangrova.streaming.coordinator.StreamDdlContext;
import org.pangrova.streaming.coordinator.StreamDdlExecutor;
import org.pangrova.streaming.coordinator.StreamScheduler;
import org.pangrova.streaming.coordinator.StreamSchedulerContext;
import org.pangrova.streaming.maintenance.CompactionResult;
import org.pangrova.streaming.maintenance.MaintenanceConfig;
import org.pangrova.streaming.maintenance.MaintenanceScheduler;
import org.pangrova.streaming.maintenance.MaintenanceStatus;
import org.pangrova.streaming.maintenance.OrphanCleanupResult;
import org.pangrova.streaming.maintenance.SnapshotExpirationResult;
import org.pangrova.streaming.maintenance.TableMaintenance;
import org.pangrova.streaming.watermark.WatermarkTracker;
import org.pangrova.testing.testkit.ContainerTestBase;
import org.pangrova.testing.testkit.constants.TestObservability;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

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
                    "stale_reason", "created_at", "algorithm", "metric");
            assertThat(results.execute(stmt, task, columns)).isEmpty();

            store.registerIndex(descriptor("scalar", IndexType.BLOOM_FILTER, Map.of()));
            assertThat(results.execute(stmt, task, columns)).hasSize(1)
                    .allSatisfy(row -> {
                        assertThat(row.get(0)).isEqualTo("scalar");
                        assertThat(row.get(6)).isEmpty();
                        assertThat(row.get(7)).isEmpty();
                    });
            store.registerIndex(descriptor("vector", IndexType.VECTOR, Map.of("algorithm", "hnsw", "metric", "cosine")));
            store.registerIndex(descriptor("inverted", IndexType.INVERTED, Map.of("analyzer", "standard")));
            var rows = results.execute(stmt, task, columns);
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
        var offsets = new JdbcStreamOffsetStore(source);
        try (var maintenance = new MaintenanceScheduler(new UnusedMaintenance(), MaintenanceConfig.defaults());
             var scheduler = new StreamScheduler(new StreamSchedulerContext(TestObservability.METRICS,
                     states, offsets, new TwoPhaseCommitCoordinator(new JdbcCdcCommitStateStore(source)),
                     new CdcRecordConverterRegistry(List.of()), new WatermarkTracker(new JdbcWatermarkPositionStore(source)),
                     maintenance, StreamConfig.withDefaults(),
                     state -> { throw new AssertionError("SHOW must not open a source"); },
                     state -> { throw new AssertionError("SHOW must not open a sink"); }), "show-stream-shape");
             var results = new MetadataShowResultSchemaTest.ResultHarness()) {
            var task = new ShowStreamsTask(new StreamDdlExecutor(new StreamDdlContext(states, offsets,
                    scheduler, StreamConfig.withDefaults(), new CdcSourceRegistry(List.of()))));
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

    private static final class UnusedMaintenance implements TableMaintenance {
        @Override public CompletableFuture<CompactionResult> compactAsync(String name) {
            throw new AssertionError("SHOW must not compact");
        }
        @Override public CompletableFuture<OrphanCleanupResult> cleanOrphansAsync(String name, Duration olderThan) {
            throw new AssertionError("SHOW must not clean orphans");
        }
        @Override public CompletableFuture<SnapshotExpirationResult> expireSnapshotsAsync(String name, Duration olderThan) {
            throw new AssertionError("SHOW must not expire snapshots");
        }
        @Override public MaintenanceStatus getMaintenanceStatus(String name) {
            throw new AssertionError("SHOW must not query maintenance");
        }
    }
}
