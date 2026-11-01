package org.pangrova.table.store.jdbc.control;

import org.pangrova.table.api.control.OwnershipMode;
import org.pangrova.table.api.control.TableControlProvenance;
import org.pangrova.table.api.control.TableControlRecord;
import org.pangrova.table.api.control.TableControlRenameIntent;
import org.pangrova.table.api.control.TableControlState;
import org.pangrova.table.api.control.TableVersionObservation;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;
import org.pangrova.testing.testkit.ContainerTestBase;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@Tag("integration")
class JdbcTableControlStoreTest extends ContainerTestBase {

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

    private DataSource dataSource;
    private JdbcTableControlStore store;

    @BeforeEach
    void setUp() throws SQLException {
        dataSource = MigratedPostgres.create();
        store = new JdbcTableControlStore(dataSource);
    }

    @Test
    void recordSurvivesAStoreRestartAndKeepsItsIdentity() {
        TableControlRecord record = managed("orders");
        store.save(record);

        TableControlRecord reloaded = new JdbcTableControlStore(dataSource)
                .find("cat", "db", "orders").orElseThrow();

        assertThat(reloaded.tableId()).isEqualTo(record.tableId());
        assertThat(reloaded.formatTableUuid()).isEqualTo("uuid-orders");
        assertThat(reloaded.versionToken()).isEqualTo("ICEBERG:uuid-orders:7");
        assertThat(reloaded.metadataRevision()).isEqualTo(3L);
        assertThat(reloaded.properties()).containsEntry("owner", "alice");
        assertThat(reloaded.versionObservedAt()).isEqualTo(NOW);
        assertThat(reloaded.createdAt()).isEqualTo(NOW);
    }

    @Test
    void upsertChangesStateWithoutChangingIdentity() {
        TableControlRecord record = managed("orders");
        store.save(record);

        store.save(record.toBuilder()
                .state(TableControlState.QUARANTINED)
                .metadataRevision(4)
                .updatedAt(NOW.plusSeconds(5))
                .build());

        assertThat(store.find("cat", "db", "orders")).get()
                .satisfies(reloaded -> {
                    assertThat(reloaded.metadataRevision()).isEqualTo(4L);
                    assertThat(reloaded.state()).isEqualTo(TableControlState.QUARANTINED);
                });
    }

    @Test
    void droppedRecordDisappearsFromLiveLookupButRemainsById() {
        TableControlRecord record = managed("orders");
        store.save(record);
        store.save(record.toBuilder()
                .state(TableControlState.DROPPED)
                .metadataRevision(5)
                .build());

        assertThat(store.find("cat", "db", "orders")).isEmpty();
        assertThat(store.findById(record.tableId())).get()
                .extracting(TableControlRecord::state)
                .isEqualTo(TableControlState.DROPPED);

        store.save(managed("orders"));
        assertThat(store.find("cat", "db", "orders")).get()
                .extracting(TableControlRecord::tableId)
                .isNotEqualTo(record.tableId());
    }

    @Test
    void listsOnlyTheRequestedCatalog() {
        store.save(managed("orders", "cat"));
        store.save(managed("customers", "cat"));
        store.save(managed("events", "other"));

        assertThat(store.listByCatalog("cat"))
                .extracting(TableControlRecord::table)
                .containsExactly("customers", "orders");
        assertThat(store.listAll()).hasSize(3);
    }

    @Test
    void optionalFieldsRoundTripAsEmpty() {
        TableControlRecord record = TableControlRecord.builder()
                .tableId(UUID.randomUUID())
                .name("cat", "db", "minimal")
                .format("PAIMON")
                .ownershipMode(OwnershipMode.MANAGED)
                .state(TableControlState.ACTIVE)
                .provenance(TableControlProvenance.CREATED)
                .metadataRevision(1)
                .createdAt(NOW)
                .updatedAt(NOW)
                .build();
        store.save(record);

        TableControlRecord reloaded = store.find("cat", "db", "minimal").orElseThrow();
        assertThat(reloaded.formatTableUuid()).isNull();
        assertThat(reloaded.formatLocation()).isNull();
        assertThat(reloaded.versionToken()).isNull();
        assertThat(reloaded.versionObservedAt()).isNull();
        assertThat(reloaded.catalogIncarnationId()).isNull();
        assertThat(reloaded.catalogRevision()).isNull();
        assertThat(reloaded.lastSyncedAt()).isNull();
        assertThat(reloaded.lastMaintainedAt()).isNull();
        assertThat(reloaded.properties()).isEmpty();
    }

    @Test
    void versionObservationIsDurableAndOrderedNewestFirst() {
        TableControlRecord record = managed("orders");
        store.save(record);

        store.saveWithObservation(record.toBuilder().metadataRevision(4)
                .version("ICEBERG:uuid-orders:8", NOW.plusSeconds(30)).build(), new TableVersionObservation(
                record.tableId(), "ICEBERG:uuid-orders:8", "ICEBERG:uuid-orders:7",
                "PERIODIC", NOW.plusSeconds(30)));
        store.saveWithObservation(record.toBuilder().metadataRevision(5)
                .version("ICEBERG:uuid-orders:9", NOW.plusSeconds(60)).build(), new TableVersionObservation(
                record.tableId(), "ICEBERG:uuid-orders:9", "ICEBERG:uuid-orders:8",
                "PERIODIC", NOW.plusSeconds(60)));

        assertThat(store.listVersionObservations(record.tableId(), 10))
                .extracting(TableVersionObservation::versionToken)
                .containsExactly("ICEBERG:uuid-orders:9", "ICEBERG:uuid-orders:8");
        assertThat(store.listVersionObservations(record.tableId(), 1)).hasSize(1);
    }

    @Test
    void savingAControlRecordWritesATableInvalidationInTheSameUnitOfWork() throws SQLException {
        TableControlRecord record = managed("orders");
        long before = countTableInvalidations("cat", "db", "orders");

        store.save(record);

        assertThat(countTableInvalidations("cat", "db", "orders")).isEqualTo(before + 1);
    }

    @Test
    void observationFailureRollsBackRevisionAndInvalidation() throws SQLException {
        TableControlRecord record = managed("orders");
        store.save(record);
        long invalidations = countTableInvalidations("cat", "db", "orders");
        TableControlRecord updated = record.toBuilder().metadataRevision(4)
                .version("next", NOW.plusSeconds(30)).build();

        assertThatThrownBy(() -> store.saveWithObservation(updated,
                new TableVersionObservation(record.tableId(), "next", record.versionToken(),
                        "x".repeat(33), NOW.plusSeconds(30))))
                .isInstanceOf(org.pangrova.table.core.control.TableControlException.class);
        assertThat(store.findById(record.tableId())).contains(record);
        assertThat(countTableInvalidations("cat", "db", "orders")).isEqualTo(invalidations);
        assertThat(store.listVersionObservations(record.tableId(), 10)).isEmpty();
    }

    @Test
    void staleObservationCannotLeaveHistoryOrInvalidationsBehind() throws SQLException {
        TableControlRecord record = managed("orders");
        store.save(record);
        store.save(record.toBuilder().state(TableControlState.QUARANTINED)
                .metadataRevision(4).build());
        long invalidations = countTableInvalidations("cat", "db", "orders");

        assertThatThrownBy(() -> store.saveWithObservation(record.toBuilder()
                .metadataRevision(4).version("next", NOW).build(),
                new TableVersionObservation(record.tableId(), "next", record.versionToken(),
                        "PERIODIC", NOW)))
                .isInstanceOf(org.pangrova.table.core.control.TableControlException.class);
        assertThat(store.findById(record.tableId()).orElseThrow().state())
                .isEqualTo(TableControlState.QUARANTINED);
        assertThat(countTableInvalidations("cat", "db", "orders")).isEqualTo(invalidations);
        assertThat(store.listVersionObservations(record.tableId(), 10)).isEmpty();
    }

    @Test
    void pendingRenameReservesBothNamesAcrossRestartAndRejectsACompetingRegistration() {
        var original = managed("orders");
        store.save(original);
        var moved = original.toBuilder().name("cat", "db", "renamed")
                .state(TableControlState.RENAMING).properties(TableControlRenameIntent.begin(original))
                .metadataRevision(4).build();
        store.save(moved);
        var peer = new JdbcTableControlStore(dataSource);
        assertThat(peer.find("cat", "db", "orders")).contains(moved);
        assertThat(peer.find("cat", "db", "renamed")).contains(moved);
        assertThatThrownBy(() -> peer.save(managed("orders")))
                .isInstanceOf(org.pangrova.table.core.control.TableControlException.class);
        assertThat(peer.find("cat", "db", "orders")).contains(moved);
        peer.save(moved.toBuilder().state(TableControlState.ACTIVE)
                .properties(TableControlRenameIntent.complete(moved)).metadataRevision(5).build());
        assertThat(peer.find("cat", "db", "orders")).isEmpty();
        peer.save(managed("orders"));
        assertThat(peer.find("cat", "db", "orders").orElseThrow().tableId()).isNotEqualTo(original.tableId());
    }

    private long countTableInvalidations(String catalog, String schema, String table)
            throws SQLException {
        try (var conn = dataSource.getConnection();
             var ps = conn.prepareStatement("SELECT COUNT(*) FROM invalidation_log "
                     + "WHERE cache_key=? AND invalidation_type='TABLE'")) {
            ps.setString(1, catalog + '\u001F' + schema + '\u001F' + table);
            try (var rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private static TableControlRecord managed(String table) {
        return managed(table, "cat");
    }

    private static TableControlRecord managed(String table, String catalog) {
        return TableControlRecord.builder()
                .tableId(UUID.randomUUID())
                .name(catalog, "db", table)
                .format("ICEBERG")
                .formatTableUuid("uuid-" + table)
                .formatLocation("s3://bucket/" + table)
                .ownershipMode(OwnershipMode.MANAGED)
                .state(TableControlState.ACTIVE)
                .provenance(TableControlProvenance.CREATED)
                .metadataRevision(3)
                .version("ICEBERG:uuid-" + table + ":7", NOW)
                .ownerPrincipal("alice")
                .properties(java.util.Map.of("owner", "alice"))
                .sourceSignature("ICEBERG|uuid-" + table + "|s3://bucket/" + table)
                .createdAt(NOW)
                .updatedAt(NOW)
                .build();
    }
}
