package org.pangrova.metadata.outbox;

import org.pangrova.testing.testkit.metadata.CurrentCatalogProjectionFixture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.cache.TieredMetadataCache;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.enums.CatalogState;
import org.pangrova.metadata.api.enums.CatalogType;
import org.pangrova.metadata.event.MetadataEventDispatcher;
import org.pangrova.metadata.api.object.ColumnMetadata;
import org.pangrova.metadata.api.object.ColumnType;
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
import org.pangrova.metadata.api.catalog.SyncResult;
import org.pangrova.persistence.catalog.postgresql.projection.PostgresTableProjectionRepository;
import org.pangrova.persistence.workflow.postgresql.projection.PostgresMetadataProjectionTransactions;
import org.pangrova.persistence.catalog.postgresql.synchronization.PostgresMetadataSyncRepository;

/** Real source/sink PostgreSQL component integration; not a federation-provider or Native E2E test. */
class MetadataSyncInvalidationIntegrationTest {
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
        DataSource sourceDataSource = SharedPostgresContainer.migratedDataSource("sync_inventory_source");
        sink = SharedPostgresContainer.migratedDataSource("sync_inventory_sink");
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
        source = new PostgresFederationInventory(sourceDataSource, "cat");
        cache = new TieredMetadataCache();
        dispatcher = new MetadataEventDispatcher();
        dispatcher.addListener(cache);
        service = new MetadataSyncService(sinkTables, sinkProjections, new PostgresMetadataSyncRepository(sink), cache, dispatcher);
    }

    @AfterEach
    void tearDown() {
        if (source != null) {
            source.close();
        }
        if (dispatcher != null) {
            dispatcher.shutdown();
        }
    }

    @Test
    void incrementalSyncWritesToInvalidationLog() throws SQLException {
        sinkFixture.saveSchemas("cat", List.of("sch"));
        TableMetadata original = table("t1", "old");
        sinkFixture.saveTable(original);
        cache.putTable("cat", "sch", "t1", sinkTables.loadTable("cat", "sch", "t1").orElseThrow());
        sourceFixture.saveTable(table("t1", "new"));
        TableMetadata persistedSource = source.getTable("sch", "t1");
        long previousEvents = tableEvents();

        SyncResult result = service.incrementalSync(sinkDeployments.getCatalogDescriptor("cat").orElseThrow(), source);

        assertThat(result.changesApplied()).isEqualTo(1);
        assertThat(result.errors()).isZero();
        assertThat(sinkTables.loadTable("cat", "sch", "t1")).contains(persistedSource);
        assertThat(cache.getTable("cat", "sch", "t1")).isEmpty();
        assertThat(tableEvents()).isEqualTo(previousEvents + 1);
        assertExactTableEvent("t1");
    }

    @Test
    void fullSyncWritesToInvalidationLogForEachTable() throws SQLException {
        sourceFixture.saveTable(table("t1", "one"));
        sourceFixture.saveTable(table("t2", "two"));

        SyncResult result = service.fullSync(sinkDeployments.getCatalogDescriptor("cat").orElseThrow(), source);

        assertThat(result.changesApplied()).isEqualTo(2);
        assertThat(result.errors()).isZero();
        assertThat(tableEvents()).isEqualTo(2);
        assertExactTableEvent("t1");
        assertExactTableEvent("t2");
        assertThat(sinkTables.loadTable("cat", "sch", "t1")).contains(source.getTable("sch", "t1"));
        assertThat(sinkTables.loadTable("cat", "sch", "t2")).contains(source.getTable("sch", "t2"));
    }

    @Test
    void deniedInvalidationRetainsOriginalProjectionAndLocalCacheUntilActualCommit() throws SQLException {
        sinkFixture.saveSchemas("cat", List.of("sch"));
        sinkFixture.saveTable(table("t1", "original"));
        TableMetadata original = sinkTables.loadTable("cat", "sch", "t1").orElseThrow();
        cache.putTable("cat", "sch", "t1", original);
        sourceFixture.saveTable(table("t1", "updated"));
        long previousEvents = tableEvents();
        try (var connection = sink.getConnection(); var statement = connection.createStatement()) {
            statement.execute("CREATE FUNCTION reject_sync_table_invalidation() RETURNS trigger LANGUAGE plpgsql AS $$ "
                    + "BEGIN IF NEW.invalidation_type='TABLE' THEN RAISE EXCEPTION 'table invalidation denied' "
                    + "USING ERRCODE='42501'; END IF; RETURN NEW; END $$");
            statement.execute("CREATE TRIGGER reject_sync_table_invalidation BEFORE INSERT ON invalidation_log "
                    + "FOR EACH ROW EXECUTE FUNCTION reject_sync_table_invalidation()");
        }

        Throwable rejected = org.assertj.core.api.Assertions.catchThrowable(
                () -> service.fullSync(sinkDeployments.getCatalogDescriptor("cat").orElseThrow(), source));
        assertThat(rejected).isInstanceOf(org.pangrova.persistence.workflow.api.exception.MetadataProjectionPendingException.class);
        assertThat(rejected.getCause()).isInstanceOf(SQLException.class);
        assertThat(((SQLException) rejected.getCause()).getSQLState()).isEqualTo("42501");
        var originalId = ((org.pangrova.persistence.workflow.api.exception.MetadataProjectionPendingException) rejected).operationId();
        assertThat(sinkTables.loadTable("cat", "sch", "t1")).contains(original);
        assertThat(cache.getTable("cat", "sch", "t1")).contains(original);
        assertThat(tableEvents()).isEqualTo(previousEvents);
        try (var connection = sink.getConnection(); var statement = connection.createStatement()) {
            statement.execute("DROP TRIGGER reject_sync_table_invalidation ON invalidation_log");
            statement.execute("DROP FUNCTION reject_sync_table_invalidation()");
        }

        SyncResult retried = service.fullSync(sinkDeployments.getCatalogDescriptor("cat").orElseThrow(), source);

        assertThat(retried.changesApplied()).isEqualTo(1);
        assertThat(retried.errors()).isZero();
        assertThat(sinkTables.loadTable("cat", "sch", "t1")).contains(source.getTable("sch", "t1"));
        assertThat(cache.getTable("cat", "sch", "t1")).isEmpty();
        assertThat(new PostgresMetadataProjectionTransactions(sink).findReceipt(originalId)).isPresent();
        // The original accepted operation is recovered once; the new full scan records its own observation.
        assertThat(tableEvents()).isEqualTo(previousEvents + 2);
    }

    private long tableEvents() throws SQLException {
        try (var connection = sink.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery(
                        "SELECT count(*) FROM invalidation_log WHERE invalidation_type='TABLE'")) {
            assertThat(rows.next()).isTrue();
            return rows.getLong(1);
        }
    }

    private void assertExactTableEvent(String table) throws SQLException {
        try (var connection = sink.getConnection();
                var statement = connection.prepareStatement("SELECT cache_key, invalidation_type "
                        + "FROM invalidation_log WHERE cache_key=? ORDER BY id DESC LIMIT 1")) {
            statement.setString(1, "cat\u001Fsch\u001F" + table);
            try (var rows = statement.executeQuery()) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString(1)).isEqualTo("cat\u001Fsch\u001F" + table);
                assertThat(rows.getString(2)).isEqualTo("TABLE");
                assertThat(rows.next()).isFalse();
            }
        }
    }

    private static TableMetadata table(String name, String version) {
        Instant timestamp = Instant.parse("2026-01-01T00:00:00Z");
        return new TableMetadata("cat", "sch", name,
                List.of(new ColumnMetadata("id", ColumnType.of("BIGINT"), false, "", Map.of())),
                Map.of(), List.of("id"), Map.of("version", version), timestamp, timestamp);
    }
}
