package org.pangrova.metadata.outbox;

import org.pangrova.testing.testkit.metadata.CurrentCatalogProjectionFixture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.cache.TieredMetadataCache;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.metadata.api.object.ColumnMetadata;
import org.pangrova.metadata.api.object.ColumnType;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.persistence.catalog.postgresql.catalog.PostgresCatalogDeploymentRepository;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;
import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import org.pangrova.metadata.api.catalog.MetadataChange;
import org.pangrova.persistence.catalog.postgresql.projection.PostgresTableProjectionRepository;
import org.pangrova.persistence.workflow.postgresql.projection.PostgresMetadataProjectionTransactions;
import org.pangrova.persistence.catalog.postgresql.synchronization.PostgresMetadataSyncRepository;

/** Actual PostgreSQL inventory component evidence, not a production federation provider or Native E2E. */
class MetadataDriftSamplingIntegrationTest {
    private DataSource sink;
    private PostgresCatalogDeploymentRepository sinkDeployments;
    private PostgresTableProjectionRepository sinkTables;
    private PostgresMetadataProjectionTransactions sinkProjections;
    private CurrentCatalogProjectionFixture sinkFixture;
    private PostgresCatalogDeploymentRepository sourceDeployments;
    private PostgresMetadataProjectionTransactions sourceProjections;
    private CurrentCatalogProjectionFixture sourceFixture;
    private PostgresFederationInventory source;
    private TieredMetadataCache cache;
    private MetadataEventDispatcher dispatcher;
    private MetadataSyncService service;

    @BeforeEach
    void setUp() throws SQLException {
        var sourceDataSource = SharedPostgresContainer.migratedDataSource("drift_source");
        sink = SharedPostgresContainer.migratedDataSource("drift_sink");
        sourceDeployments = new PostgresCatalogDeploymentRepository(sourceDataSource);
        sourceProjections = new PostgresMetadataProjectionTransactions(sourceDataSource);
        sourceFixture = new CurrentCatalogProjectionFixture(sourceDeployments, sourceProjections);
        sinkDeployments = new PostgresCatalogDeploymentRepository(sink);
        sinkTables = new PostgresTableProjectionRepository(sink);
        sinkProjections = new PostgresMetadataProjectionTransactions(sink);
        sinkFixture = new CurrentCatalogProjectionFixture(sinkDeployments, sinkProjections);
        var descriptor = new CatalogDescriptor("cat", CatalogType.FEDERATION, "inventory",
                CatalogState.ACTIVE, Map.of(), "", Set.of());
        sourceDeployments.saveCatalogDescriptor(descriptor);
        sourceFixture.saveSchemas("cat", List.of("sch"));
        sinkDeployments.saveCatalogDescriptor(descriptor);
        sinkFixture.saveSchemas("cat", List.of("sch"));
        source = new PostgresFederationInventory(sourceDataSource, "cat");
        cache = new TieredMetadataCache();
        dispatcher = new MetadataEventDispatcher();
        service = new MetadataSyncService(sinkTables, sinkProjections, new PostgresMetadataSyncRepository(sink), cache, dispatcher);
    }

    @AfterEach
    void tearDown() {
        if (source != null) source.close();
        if (dispatcher != null) dispatcher.shutdown();
    }

    @Test
    void schemaDriftDetectedAndCorrected() throws SQLException {
        var original = table(false);
        sinkFixture.saveTable(original);
        cache.putTable("cat", "sch", "t1", original);
        sourceFixture.saveTable(table(true));
        var actual = source.getTable("sch", "t1");
        long before = tableEvents();

        service.validateSingleTable(sinkDeployments.getCatalogDescriptor("cat").orElseThrow().identity(), source, change(actual));

        assertThat(service.getSchemaDriftDetectedCount()).isEqualTo(1);
        assertThat(sinkTables.loadTable("cat", "sch", "t1")).contains(actual);
        assertThat(sinkTables.loadTable("cat", "sch", "t1").orElseThrow().columns()).hasSize(3);
        assertThat(cache.getTable("cat", "sch", "t1")).isEmpty();
        assertThat(tableEvents()).isEqualTo(before + 1);
        service.validateSingleTable(sinkDeployments.getCatalogDescriptor("cat").orElseThrow().identity(), source, change(actual));
        assertThat(service.getSchemaDriftDetectedCount()).isEqualTo(1);
        assertThat(tableEvents()).isEqualTo(before + 1);
    }

    @Test
    void noDriftWhenColumnsMatch() throws SQLException {
        sourceFixture.saveTable(table(false));
        var original = source.getTable("sch", "t1");
        sinkFixture.saveTable(original);
        cache.putTable("cat", "sch", "t1", original);
        long before = tableEvents();

        service.validateSingleTable(sinkDeployments.getCatalogDescriptor("cat").orElseThrow().identity(), source, change(original));
        service.validateSyncedSample(sinkDeployments.getCatalogDescriptor("cat").orElseThrow().identity(), source, List.of(change(original)));

        assertThat(service.getSchemaDriftDetectedCount()).isZero();
        assertThat(sinkTables.loadTable("cat", "sch", "t1")).contains(original);
        assertThat(cache.getTable("cat", "sch", "t1")).contains(original);
        assertThat(tableEvents()).isEqualTo(before);
    }

    @Test
    void schemaDriftSkipsDeletes() throws SQLException {
        var original = table(false);
        sinkFixture.saveTable(original);
        cache.putTable("cat", "sch", "t1", original);
        long before = tableEvents();
        // A closed real source would fail any read; DELETE must never consult it.
        source.close();

        service.validateSyncedSample(sinkDeployments.getCatalogDescriptor("cat").orElseThrow().identity(), source, List.of(new MetadataChange(MetadataChange.ChangeType.DELETE,
                new QualifiedTableRef("cat", "sch", "t1"), null)));

        assertThat(service.getSchemaDriftDetectedCount()).isZero();
        assertThat(sinkTables.loadTable("cat", "sch", "t1")).contains(original);
        assertThat(cache.getTable("cat", "sch", "t1")).contains(original);
        assertThat(tableEvents()).isEqualTo(before);
    }

    @Test
    void deniedRepairRequiresDurableRecoveryAndInvalidatesCacheOnlyAfterCommit() throws SQLException {
        var original = table(false);
        sinkFixture.saveTable(original);
        sourceFixture.saveTable(table(true));
        var actual = source.getTable("sch", "t1");
        var scope = sinkDeployments.getCatalogDescriptor("cat").orElseThrow().identity();
        long before = tableEvents();
        try (var poller = new org.pangrova.persistence.catalog.postgresql.invalidation.PostgresInvalidationPoller(sink, cache, 1)) {
            long initialId = lastEventId();
            poller.start();
            org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(10))
                    .until(() -> poller.getLastProcessedId() == initialId);
            cache.putTable("cat", "sch", "t1", original);
            try (var connection = sink.getConnection(); var statement = connection.createStatement()) {
                statement.execute("CREATE FUNCTION deny_drift_event() RETURNS trigger LANGUAGE plpgsql AS $$ "
                        + "BEGIN RAISE EXCEPTION 'drift event denied' USING ERRCODE='42501'; END $$");
                statement.execute("CREATE TRIGGER deny_drift_event BEFORE INSERT ON invalidation_log "
                        + "FOR EACH ROW EXECUTE FUNCTION deny_drift_event()");
            }
            try {
                assertThatThrownBy(() -> service.validateSingleTable(scope, source, change(actual)))
                        .isInstanceOf(org.pangrova.persistence.workflow.api.projection.MetadataProjectionPendingException.class)
                        .satisfies(failure -> assertThat(((SQLException) failure.getCause()).getSQLState()).isEqualTo("42501"));
                assertThat(service.getSchemaDriftDetectedCount()).isZero();
                assertThat(sinkTables.loadTable("cat", "sch", "t1")).contains(original);
                assertThat(cache.getTable("cat", "sch", "t1")).contains(original);
                assertThat(tableEvents()).isEqualTo(before);
            } finally {
                try (var connection = sink.getConnection(); var statement = connection.createStatement()) {
                    statement.execute("DROP TRIGGER deny_drift_event ON invalidation_log");
                    statement.execute("DROP FUNCTION deny_drift_event()");
                }
            }
            sinkProjections.recoverPending(scope);
            long recoveredId = lastEventId();
            org.awaitility.Awaitility.await().atMost(java.time.Duration.ofSeconds(10))
                    .until(() -> poller.getLastProcessedId() == recoveredId);
            assertThat(sinkTables.loadTable("cat", "sch", "t1")).contains(actual);
            assertThat(cache.getTable("cat", "sch", "t1")).isEmpty();
            assertThat(tableEvents()).isEqualTo(before + 1);
            service.validateSingleTable(scope, source, change(actual));
            // A recovered storage operation is not a newly completed drift-detection invocation.
            assertThat(service.getSchemaDriftDetectedCount()).isZero();
            assertThat(tableEvents()).isEqualTo(before + 1);
        }
    }

    private long lastEventId() throws SQLException {
        try (var connection = sink.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT COALESCE(MAX(id),0) FROM invalidation_log")) {
            rows.next();
            return rows.getLong(1);
        }
    }

    private long tableEvents() throws SQLException {
        try (var connection = sink.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT COUNT(*) FROM invalidation_log "
                        + "WHERE invalidation_type='TABLE'")) {
            rows.next();
            return rows.getLong(1);
        }
    }

    private static MetadataChange change(TableMetadata actual) {
        return new MetadataChange(MetadataChange.ChangeType.UPDATE, new QualifiedTableRef("cat", "sch", "t1"), actual);
    }

    private static TableMetadata table(boolean expanded) {
        var id = new ColumnMetadata("id", ColumnType.of("BIGINT"), true, "", Map.of());
        var name = new ColumnMetadata("name", ColumnType.of("VARCHAR"), true, "", Map.of());
        var email = new ColumnMetadata("email", ColumnType.of("VARCHAR"), true, "", Map.of());
        return new TableMetadata("cat", "sch", "t1", expanded ? List.of(id, name, email) : List.of(id, name),
                Map.of(), List.of(), Map.of(), Instant.EPOCH, Instant.EPOCH);
    }
}
