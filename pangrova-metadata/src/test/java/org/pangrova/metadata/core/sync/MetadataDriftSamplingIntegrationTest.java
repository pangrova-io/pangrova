package org.pangrova.metadata.core.sync;

import org.pangrova.metadata.core.federation.MetadataChange;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.core.cache.TieredMetadataCache;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.core.event.MetadataEventDispatcher;
import org.pangrova.metadata.api.schema.ColumnMetadata;
import org.pangrova.metadata.api.schema.ColumnType;
import org.pangrova.metadata.api.object.QualifiedTableRef;
import org.pangrova.metadata.api.schema.TableMetadata;
import org.pangrova.metadata.store.jdbc.JdbcMetadataStore;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Actual PostgreSQL inventory component evidence, not a production federation provider or Native E2E. */
class MetadataDriftSamplingIntegrationTest {
    private DataSource sink;
    private JdbcMetadataStore sinkStore;
    private JdbcMetadataStore sourceStore;
    private PostgresFederationInventory source;
    private TieredMetadataCache cache;
    private MetadataEventDispatcher dispatcher;
    private MetadataSyncService service;

    @BeforeEach
    void setUp() throws SQLException {
        var sourceDataSource = SharedPostgresContainer.migratedDataSource("drift_source");
        sink = SharedPostgresContainer.migratedDataSource("drift_sink");
        sourceStore = new JdbcMetadataStore(sourceDataSource);
        sinkStore = new JdbcMetadataStore(sink);
        var descriptor = new CatalogDescriptor("cat", CatalogType.FEDERATION, "inventory",
                CatalogState.ACTIVE, Map.of(), "", Set.of());
        sourceStore.saveCatalogDescriptor(descriptor);
        sourceStore.saveSchemas("cat", List.of("sch"));
        sinkStore.saveCatalogDescriptor(descriptor);
        sinkStore.saveSchemas("cat", List.of("sch"));
        source = new PostgresFederationInventory(sourceDataSource, "cat");
        cache = new TieredMetadataCache();
        dispatcher = new MetadataEventDispatcher();
        service = new MetadataSyncService(sinkStore, cache, dispatcher);
    }

    @AfterEach
    void tearDown() {
        if (source != null) source.close();
        if (dispatcher != null) dispatcher.shutdown();
    }

    @Test
    void schemaDriftDetectedAndCorrected() throws SQLException {
        var original = table(false);
        sinkStore.saveTable(original);
        cache.putTable("cat", "sch", "t1", original);
        sourceStore.saveTable(table(true));
        var actual = source.getTable("sch", "t1");
        long before = tableEvents();

        service.validateSingleTable("cat", source, change(actual));

        assertThat(service.getSchemaDriftDetectedCount()).isEqualTo(1);
        assertThat(sinkStore.loadTable("cat", "sch", "t1")).contains(actual);
        assertThat(sinkStore.loadTable("cat", "sch", "t1").orElseThrow().columns()).hasSize(3);
        assertThat(cache.getTable("cat", "sch", "t1")).isEmpty();
        assertThat(tableEvents()).isEqualTo(before + 1);
        service.validateSingleTable("cat", source, change(actual));
        assertThat(service.getSchemaDriftDetectedCount()).isEqualTo(1);
        assertThat(tableEvents()).isEqualTo(before + 1);
    }

    @Test
    void noDriftWhenColumnsMatch() throws SQLException {
        sourceStore.saveTable(table(false));
        var original = source.getTable("sch", "t1");
        sinkStore.saveTable(original);
        cache.putTable("cat", "sch", "t1", original);
        long before = tableEvents();

        service.validateSingleTable("cat", source, change(original));
        service.validateSyncedSample("cat", source, List.of(change(original)));

        assertThat(service.getSchemaDriftDetectedCount()).isZero();
        assertThat(sinkStore.loadTable("cat", "sch", "t1")).contains(original);
        assertThat(cache.getTable("cat", "sch", "t1")).contains(original);
        assertThat(tableEvents()).isEqualTo(before);
    }

    @Test
    void schemaDriftSkipsDeletes() throws SQLException {
        var original = table(false);
        sinkStore.saveTable(original);
        cache.putTable("cat", "sch", "t1", original);
        long before = tableEvents();
        // A closed real source would fail any read; DELETE must never consult it.
        source.close();

        service.validateSyncedSample("cat", source, List.of(new MetadataChange(MetadataChange.ChangeType.DELETE,
                new QualifiedTableRef("cat", "sch", "t1"), null)));

        assertThat(service.getSchemaDriftDetectedCount()).isZero();
        assertThat(sinkStore.loadTable("cat", "sch", "t1")).contains(original);
        assertThat(cache.getTable("cat", "sch", "t1")).contains(original);
        assertThat(tableEvents()).isEqualTo(before);
    }

    @Test
    void deniedRepairDoesNotIncrementCounterOrInvalidateOriginalCache() throws SQLException {
        var original = table(false);
        sinkStore.saveTable(original);
        cache.putTable("cat", "sch", "t1", original);
        sourceStore.saveTable(table(true));
        var actual = source.getTable("sch", "t1");
        long before = tableEvents();
        try (var connection = sink.getConnection(); var statement = connection.createStatement()) {
            statement.execute("CREATE FUNCTION deny_drift_event() RETURNS trigger LANGUAGE plpgsql AS $$ "
                    + "BEGIN RAISE EXCEPTION 'drift event denied' USING ERRCODE='42501'; END $$");
            statement.execute("CREATE TRIGGER deny_drift_event BEFORE INSERT ON invalidation_log "
                    + "FOR EACH ROW EXECUTE FUNCTION deny_drift_event()");
        }
        try {
            assertThatThrownBy(() -> sinkStore.saveTable(actual))
                    .hasMessageContaining("sqlState=42501").hasRootCauseInstanceOf(SQLException.class);
            service.validateSingleTable("cat", source, change(actual));
            assertThat(service.getSchemaDriftDetectedCount()).isZero();
            assertThat(sinkStore.loadTable("cat", "sch", "t1")).contains(original);
            assertThat(cache.getTable("cat", "sch", "t1")).contains(original);
            assertThat(tableEvents()).isEqualTo(before);
        } finally {
            try (var connection = sink.getConnection(); var statement = connection.createStatement()) {
                statement.execute("DROP TRIGGER deny_drift_event ON invalidation_log");
                statement.execute("DROP FUNCTION deny_drift_event()");
            }
        }
        service.validateSingleTable("cat", source, change(actual));
        assertThat(service.getSchemaDriftDetectedCount()).isEqualTo(1);
        assertThat(sinkStore.loadTable("cat", "sch", "t1")).contains(actual);
        assertThat(cache.getTable("cat", "sch", "t1")).isEmpty();
        assertThat(tableEvents()).isEqualTo(before + 1);
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
