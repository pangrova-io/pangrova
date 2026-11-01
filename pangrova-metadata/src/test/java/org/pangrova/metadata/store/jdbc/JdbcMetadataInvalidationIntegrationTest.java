package org.pangrova.metadata.store.jdbc;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.api.catalog.CatalogDescriptor;
import org.pangrova.metadata.api.catalog.CatalogState;
import org.pangrova.metadata.api.catalog.CatalogType;
import org.pangrova.metadata.api.error.MetastoreException;
import org.pangrova.metadata.api.schema.ColumnMetadata;
import org.pangrova.metadata.api.schema.ColumnType;
import org.pangrova.metadata.api.schema.TableMetadata;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.awaitility.Awaitility.await;

/** Real PostgreSQL projection and invalidation transaction tests, including the migrated commit-order gate. */
class JdbcMetadataInvalidationIntegrationTest {
    private DataSource dataSource;
    private JdbcMetadataStore store;

    @BeforeEach
    void setUp() throws SQLException {
        dataSource = SharedPostgresContainer.migratedDataSource("metadata_invalidation");
        store = new JdbcMetadataStore(dataSource);
    }

    @Test
    void tableSaveUpdateDeleteAndNoopPublishOnlyExactCommittedChanges() throws SQLException {
        seedParent();
        TableMetadata original = table("original");
        TableMetadata updated = table("updated");
        store.saveTable(original);
        assertThat(store.loadTable("cat", "sch", "tbl")).contains(original);
        store.saveTable(updated);
        assertThat(store.loadTable("cat", "sch", "tbl")).contains(updated);
        store.deleteTable("cat", "sch", "tbl");
        store.deleteTable("cat", "sch", "tbl");

        assertThat(store.loadTable("cat", "sch", "tbl")).isEmpty();
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT cache_key, invalidation_type FROM invalidation_log "
                        + "WHERE invalidation_type='TABLE' ORDER BY id")) {
            for (int index = 0; index < 3; index++) {
                assertThat(rows.next()).isTrue();
                assertThat(rows.getString(1)).isEqualTo("cat\u001Fsch\u001Ftbl");
                assertThat(rows.getString(2)).isEqualTo("TABLE");
            }
            assertThat(rows.next()).isFalse();
        }
    }

    @Test
    void catalogRevisionsSchemaBatchAndNoopsPublishOnceEach() throws SQLException {
        CatalogDescriptor first = descriptor("one");
        store.saveCatalogDescriptor(first);
        assertThat(eventCount()).isEqualTo(1);
        Throwable rejected = catchThrowable(() -> store.saveCatalogDescriptor(first));
        assertThat(rejected).isInstanceOf(MetastoreException.class).hasMessageContaining("revision fence");
        assertThat(eventCount()).isEqualTo(1);
        CatalogDescriptor second = nextRevision(first);
        store.saveCatalogDescriptor(second);
        store.saveSchemas("cat", List.of("s1", "s2"));
        store.saveSchemas("cat", List.of());
        assertThat(eventCount()).isEqualTo(3);
        store.deleteCatalogDescriptor("cat");
        store.deleteCatalogDescriptor("cat");

        assertThat(eventCount()).isEqualTo(4);
        assertThat(store.findCatalogDescriptorRevision("cat", first.incarnationId(), 1)).contains(first);
        assertThat(store.findCatalogDescriptorRevision("cat", first.incarnationId(), 2)).contains(second);
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT cache_key, invalidation_type FROM invalidation_log")) {
            while (rows.next()) {
                assertThat(rows.getString(1)).isEqualTo("cat");
                assertThat(rows.getString(2)).isEqualTo("CATALOG");
            }
        }
    }

    @Test
    void deniedTableSaveRollsBackProjectionAndRetriesAfterPermissionRecovery() throws SQLException {
        seedParent();
        TableMetadata original = table("original");
        store.saveTable(original);
        long events = eventCount();
        denyInvalidation();

        assertDenied(() -> store.saveTable(table("updated")));

        assertThat(store.loadTable("cat", "sch", "tbl")).contains(original);
        assertThat(eventCount()).isEqualTo(events);
        allowInvalidation();
        store.saveTable(table("updated"));
        assertThat(store.loadTable("cat", "sch", "tbl")).contains(table("updated"));
        assertThat(eventCount()).isEqualTo(events + 1);
    }

    @Test
    void deniedTableDeleteRetainsProjection() throws SQLException {
        seedParent();
        store.saveTable(table("original"));
        long events = eventCount();
        denyInvalidation();

        assertDenied(() -> store.deleteTable("cat", "sch", "tbl"));

        assertThat(store.loadTable("cat", "sch", "tbl")).contains(table("original"));
        assertThat(eventCount()).isEqualTo(events);
        allowInvalidation();
        store.deleteTable("cat", "sch", "tbl");
        assertThat(store.loadTable("cat", "sch", "tbl")).isEmpty();
        assertThat(eventCount()).isEqualTo(events + 1);
    }

    @Test
    void deniedCatalogRevisionRollsBackCurrentAndImmutableHistory() throws SQLException {
        CatalogDescriptor first = descriptor("one");
        CatalogDescriptor second = nextRevision(first);
        store.saveCatalogDescriptor(first);
        denyInvalidation();

        assertDenied(() -> store.saveCatalogDescriptor(second));

        assertThat(store.getCatalogDescriptor("cat")).contains(first);
        assertThat(store.findCatalogDescriptorRevision("cat", first.incarnationId(), 1)).contains(first);
        assertThat(store.findCatalogDescriptorRevision("cat", first.incarnationId(), 2)).isEmpty();
        assertThat(eventCount()).isEqualTo(1);
        allowInvalidation();
        store.saveCatalogDescriptor(second);
        assertThat(store.getCatalogDescriptor("cat")).contains(second);
        assertThat(store.findCatalogDescriptorRevision("cat", first.incarnationId(), 2)).contains(second);
        assertThat(eventCount()).isEqualTo(2);
    }

    @Test
    void deniedFirstCatalogInsertCannotLeaveCurrentOrHistory() throws SQLException {
        CatalogDescriptor first = descriptor("one");
        denyInvalidation();

        assertDenied(() -> store.saveCatalogDescriptor(first));

        assertThat(store.getCatalogDescriptor("cat")).isEmpty();
        assertThat(store.findCatalogDescriptorRevision("cat", first.incarnationId(), 1)).isEmpty();
        assertThat(eventCount()).isZero();
        allowInvalidation();
        store.saveCatalogDescriptor(first);
        assertThat(store.getCatalogDescriptor("cat")).contains(first);
        assertThat(store.findCatalogDescriptorRevision("cat", first.incarnationId(), 1)).contains(first);
        assertThat(eventCount()).isEqualTo(1);
    }

    @Test
    void deniedCatalogDeleteRollsBackAllCascadedProjections() throws SQLException {
        CatalogDescriptor first = seedParent();
        store.saveTable(table("original"));
        long events = eventCount();
        denyInvalidation();

        assertDenied(() -> store.deleteCatalogDescriptor("cat"));

        assertThat(store.getCatalogDescriptor("cat")).contains(first);
        assertThat(schemaCount()).isEqualTo(1);
        assertThat(store.loadTable("cat", "sch", "tbl")).contains(table("original"));
        assertThat(store.findCatalogDescriptorRevision("cat", first.incarnationId(), 1)).contains(first);
        assertThat(eventCount()).isEqualTo(events);
        allowInvalidation();
        store.deleteCatalogDescriptor("cat");
        assertThat(store.getCatalogDescriptor("cat")).isEmpty();
        assertThat(schemaCount()).isZero();
        assertThat(store.loadTable("cat", "sch", "tbl")).isEmpty();
        assertThat(store.findCatalogDescriptorRevision("cat", first.incarnationId(), 1)).contains(first);
        assertThat(eventCount()).isEqualTo(events + 1);
    }

    @Test
    void deniedSchemaBatchRollsBackNewRowsAndExistingTimestamp() throws SQLException {
        seedParent();
        Timestamp originalSync = schemaSyncTime();
        long events = eventCount();
        denyInvalidation();

        assertDenied(() -> store.saveSchemas("cat", List.of("sch", "s2", "s3")));

        assertThat(schemaCount()).isEqualTo(1);
        assertThat(schemaSyncTime()).isEqualTo(originalSync);
        assertThat(eventCount()).isEqualTo(events);
        allowInvalidation();
        store.saveSchemas("cat", List.of("sch", "s2", "s3"));
        assertThat(schemaCount()).isEqualTo(3);
        assertThat(eventCount()).isEqualTo(events + 1);
    }

    @Test
    void failedSchemaStatementCannotCommitEarlierBatchMembersOrEvent() throws SQLException {
        store.saveCatalogDescriptor(descriptor("one"));

        Throwable failure = catchThrowable(() -> store.saveSchemas("cat", java.util.Arrays.asList("s1", null)));

        assertThat(failure).isInstanceOf(MetastoreException.class);
        assertThat(failure.getCause()).isInstanceOf(SQLException.class);
        assertThat(((SQLException) failure.getCause()).getSQLState()).isEqualTo("23502");
        assertThat(schemaCount()).isZero();
        assertThat(eventCount()).isEqualTo(1);
    }

    @Test
    void projectionAndLogRemainInvisibleUntilActualCommitGateReleases() throws Exception {
        seedParent();
        long events = eventCount();
        var executor = Executors.newSingleThreadExecutor();
        try (var blocker = dataSource.getConnection()) {
            blocker.setAutoCommit(false);
            try (var statement = blocker.createStatement()) {
                statement.execute("LOCK TABLE invalidation_log IN SHARE MODE");
            }
            var writer = executor.submit(() -> store.saveTable(table("updated")));
            try {
                await().atMost(Duration.ofSeconds(10)).until(this::actualInvalidationInsertIsBlocked);
                assertThat(store.loadTable("cat", "sch", "tbl")).isEmpty();
                assertThat(eventCount()).isEqualTo(events);
            } finally {
                blocker.commit();
            }
            writer.get(10, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }

        assertThat(store.loadTable("cat", "sch", "tbl")).contains(table("updated"));
        assertThat(eventCount()).isEqualTo(events + 1);
    }

    @Test
    void migratedCommitOrderGateBlocksBothRealWritersBeforeAllocatingTheirIds() throws Exception {
        seedParent();
        long events = eventCount();
        var executor = Executors.newFixedThreadPool(2);
        long earlierId;
        try (var blocker = dataSource.getConnection()) {
            blocker.setAutoCommit(false);
            try (var statement = blocker.createStatement();
                    var rows = statement.executeQuery("INSERT INTO invalidation_log(cache_key, invalidation_type) "
                            + "VALUES ('cat','CATALOG') RETURNING id")) {
                assertThat(rows.next()).isTrue();
                earlierId = rows.getLong(1);
            }
            TableMetadata first = table("one");
            TableMetadata second = new TableMetadata(first.catalog(), first.schema(), "tbl2", first.columns(),
                    first.partitionSpec(), first.sortOrder(), first.properties(), first.createdAt(), first.updatedAt());
            var writerOne = executor.submit(() -> store.saveTable(first));
            var writerTwo = executor.submit(() -> store.saveTable(second));
            try {
                await().atMost(Duration.ofSeconds(10)).until(() -> waitingOnCanonicalCommitGate() == 2);
                assertThat(store.loadTable("cat", "sch", "tbl")).isEmpty();
                assertThat(store.loadTable("cat", "sch", "tbl2")).isEmpty();
                assertThat(eventCount()).isEqualTo(events);
            } finally {
                blocker.commit();
            }
            writerOne.get(10, TimeUnit.SECONDS);
            writerTwo.get(10, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
        }

        assertThat(eventCount()).isEqualTo(events + 3);
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery(
                        "SELECT id FROM invalidation_log WHERE invalidation_type='TABLE' ORDER BY id")) {
            assertThat(rows.next()).isTrue();
            long firstId = rows.getLong(1);
            assertThat(firstId).isGreaterThan(earlierId);
            assertThat(rows.next()).isTrue();
            assertThat(rows.getLong(1)).isGreaterThan(firstId);
            assertThat(rows.next()).isFalse();
        }
    }

    private long waitingOnCanonicalCommitGate() throws SQLException {
        String sql = "WITH gate AS (SELECT hashtextextended(current_database() || ':' || current_schema() "
                + "|| ':invalidation_log', 0) AS key) SELECT count(*) FROM pg_locks, gate "
                + "WHERE locktype='advisory' AND NOT granted AND objsubid=1 "
                + "AND classid::bigint=((gate.key >> 32) & 4294967295) "
                + "AND objid::bigint=(gate.key & 4294967295)";
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery(sql)) {
            assertThat(rows.next()).isTrue();
            return rows.getLong(1);
        }
    }

    private boolean actualInvalidationInsertIsBlocked() throws SQLException {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT EXISTS (SELECT 1 FROM pg_locks "
                        + "WHERE relation='invalidation_log'::regclass AND NOT granted)")) {
            assertThat(rows.next()).isTrue();
            return rows.getBoolean(1);
        }
    }

    private CatalogDescriptor seedParent() {
        CatalogDescriptor descriptor = descriptor("one");
        store.saveCatalogDescriptor(descriptor);
        store.saveSchemas("cat", List.of("sch"));
        return descriptor;
    }

    private void denyInvalidation() throws SQLException {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute("CREATE FUNCTION reject_metadata_invalidation() RETURNS trigger LANGUAGE plpgsql AS $$ "
                    + "BEGIN RAISE EXCEPTION 'metadata invalidation denied' USING ERRCODE='42501'; END $$");
            statement.execute("CREATE TRIGGER reject_metadata_invalidation BEFORE INSERT ON invalidation_log "
                    + "FOR EACH ROW EXECUTE FUNCTION reject_metadata_invalidation()");
        }
    }

    private void allowInvalidation() throws SQLException {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute("DROP TRIGGER reject_metadata_invalidation ON invalidation_log");
            statement.execute("DROP FUNCTION reject_metadata_invalidation()");
        }
    }

    private static void assertDenied(Runnable mutation) {
        Throwable failure = catchThrowable(mutation::run);
        assertThat(failure).isInstanceOf(MetastoreException.class).hasMessageContaining("sqlState=42501");
        assertThat(failure.getCause()).isInstanceOf(SQLException.class);
        assertThat(((SQLException) failure.getCause()).getSQLState()).isEqualTo("42501");
    }

    private long eventCount() throws SQLException {
        return count("invalidation_log");
    }

    private long schemaCount() throws SQLException {
        return count("schema_metadata");
    }

    private long count(String table) throws SQLException {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT count(*) FROM " + table)) {
            assertThat(rows.next()).isTrue();
            return rows.getLong(1);
        }
    }

    private Timestamp schemaSyncTime() throws SQLException {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT synced_at FROM schema_metadata WHERE schema_name='sch'")) {
            assertThat(rows.next()).isTrue();
            return rows.getTimestamp(1);
        }
    }

    private static CatalogDescriptor descriptor(String version) {
        return new CatalogDescriptor("cat", CatalogType.FEDERATION, "inventory", CatalogState.ACTIVE,
                Map.of("version", version), "", Set.of());
    }

    private static CatalogDescriptor nextRevision(CatalogDescriptor first) {
        return new CatalogDescriptor(first.name(), first.type(), first.provider(), first.state(),
                Map.of("version", "two"), "", Set.of(), first.incarnationId(), first.revision() + 1);
    }

    private static TableMetadata table(String version) {
        Instant timestamp = Instant.parse("2026-01-01T00:00:00Z");
        return new TableMetadata("cat", "sch", "tbl",
                List.of(new ColumnMetadata("id", ColumnType.of("BIGINT"), false, "", Map.of())),
                Map.of(), List.of("id"), Map.of("version", version), timestamp, timestamp);
    }
}
