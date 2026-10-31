package org.pangrova.metadata.core.outbox;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.core.cache.TieredMetadataCache;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.core.event.MetadataEventDispatcher;
import org.pangrova.metadata.api.object.ColumnMetadata;
import org.pangrova.metadata.api.object.ColumnType;
import org.pangrova.metadata.api.object.TableMetadata;
import org.pangrova.metadata.store.jdbc.catalog.JdbcMetadataStore;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import org.pangrova.metadata.api.catalog.SyncResult;

/** Real source/sink PostgreSQL component integration; not a federation-provider or Native E2E test. */
class MetadataSyncInvalidationIntegrationTest {
    private DataSource sink;
    private JdbcMetadataStore sinkStore;
    private JdbcMetadataStore sourceStore;
    private PostgresFederationInventory source;
    private TieredMetadataCache cache;
    private MetadataEventDispatcher dispatcher;
    private MetadataSyncService service;

    @BeforeEach
    void setUp() throws SQLException {
        DataSource sourceDataSource = SharedPostgresContainer.migratedDataSource("sync_inventory_source");
        sink = SharedPostgresContainer.migratedDataSource("sync_inventory_sink");
        sourceStore = new JdbcMetadataStore(sourceDataSource);
        sinkStore = new JdbcMetadataStore(sink);
        var descriptor = new CatalogDescriptor("cat", CatalogType.FEDERATION, "inventory",
                CatalogState.ACTIVE, Map.of(), "", Set.of());
        sourceStore.saveCatalogDescriptor(descriptor);
        sourceStore.saveSchemas("cat", List.of("sch"));
        sinkStore.saveCatalogDescriptor(descriptor);
        source = new PostgresFederationInventory(sourceDataSource, "cat");
        cache = new TieredMetadataCache();
        dispatcher = new MetadataEventDispatcher();
        dispatcher.addListener(cache);
        service = new MetadataSyncService(sinkStore, cache, dispatcher);
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
        sinkStore.saveSchemas("cat", List.of("sch"));
        TableMetadata original = table("t1", "old");
        sinkStore.saveTable(original);
        cache.putTable("cat", "sch", "t1", sinkStore.loadTable("cat", "sch", "t1").orElseThrow());
        sourceStore.saveTable(table("t1", "new"));
        TableMetadata persistedSource = source.getTable("sch", "t1");
        long previousEvents = tableEvents();

        SyncResult result = service.incrementalSync("cat", source);

        assertThat(result.changesApplied()).isEqualTo(1);
        assertThat(result.errors()).isZero();
        assertThat(sinkStore.loadTable("cat", "sch", "t1")).contains(persistedSource);
        assertThat(cache.getTable("cat", "sch", "t1")).isEmpty();
        assertThat(tableEvents()).isEqualTo(previousEvents + 1);
        assertExactTableEvent("t1");
    }

    @Test
    void fullSyncWritesToInvalidationLogForEachTable() throws SQLException {
        sourceStore.saveTable(table("t1", "one"));
        sourceStore.saveTable(table("t2", "two"));

        SyncResult result = service.fullSync("cat", source);

        assertThat(result.changesApplied()).isEqualTo(2);
        assertThat(result.errors()).isZero();
        assertThat(tableEvents()).isEqualTo(2);
        assertExactTableEvent("t1");
        assertExactTableEvent("t2");
        assertThat(sinkStore.loadTable("cat", "sch", "t1")).contains(source.getTable("sch", "t1"));
        assertThat(sinkStore.loadTable("cat", "sch", "t2")).contains(source.getTable("sch", "t2"));
    }

    @Test
    void deniedInvalidationRetainsOriginalProjectionAndLocalCacheUntilActualCommit() throws SQLException {
        sinkStore.saveSchemas("cat", List.of("sch"));
        sinkStore.saveTable(table("t1", "original"));
        TableMetadata original = sinkStore.loadTable("cat", "sch", "t1").orElseThrow();
        cache.putTable("cat", "sch", "t1", original);
        sourceStore.saveTable(table("t1", "updated"));
        long previousEvents = tableEvents();
        try (var connection = sink.getConnection(); var statement = connection.createStatement()) {
            statement.execute("CREATE FUNCTION reject_sync_table_invalidation() RETURNS trigger LANGUAGE plpgsql AS $$ "
                    + "BEGIN IF NEW.invalidation_type='TABLE' THEN RAISE EXCEPTION 'table invalidation denied' "
                    + "USING ERRCODE='42501'; END IF; RETURN NEW; END $$");
            statement.execute("CREATE TRIGGER reject_sync_table_invalidation BEFORE INSERT ON invalidation_log "
                    + "FOR EACH ROW EXECUTE FUNCTION reject_sync_table_invalidation()");
        }

        SyncResult rejected = service.fullSync("cat", source);

        assertThat(rejected.changesApplied()).isZero();
        assertThat(rejected.errors()).isEqualTo(1);
        assertThat(rejected.errorMessages()).singleElement().asString().contains("sqlState=42501");
        assertThat(sinkStore.loadTable("cat", "sch", "t1")).contains(original);
        assertThat(cache.getTable("cat", "sch", "t1")).contains(original);
        assertThat(tableEvents()).isEqualTo(previousEvents);
        try (var connection = sink.getConnection(); var statement = connection.createStatement()) {
            statement.execute("DROP TRIGGER reject_sync_table_invalidation ON invalidation_log");
            statement.execute("DROP FUNCTION reject_sync_table_invalidation()");
        }

        SyncResult retried = service.fullSync("cat", source);

        assertThat(retried.changesApplied()).isEqualTo(1);
        assertThat(retried.errors()).isZero();
        assertThat(sinkStore.loadTable("cat", "sch", "t1")).contains(source.getTable("sch", "t1"));
        assertThat(cache.getTable("cat", "sch", "t1")).isEmpty();
        assertThat(tableEvents()).isEqualTo(previousEvents + 1);
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
