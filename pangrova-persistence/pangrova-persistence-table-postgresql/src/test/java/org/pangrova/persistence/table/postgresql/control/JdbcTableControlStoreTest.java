package org.pangrova.persistence.table.postgresql.control;

import org.pangrova.table.api.enums.OwnershipMode;
import org.pangrova.table.api.enums.TableControlProvenance;
import org.pangrova.table.api.identity.CatalogDeploymentIdentity;
import org.pangrova.table.api.ownership.TableControlRecord;
import org.pangrova.table.api.rename.TableControlRenameIntent;
import org.pangrova.table.api.enums.TableControlState;
import org.pangrova.table.api.observation.TableVersionObservation;

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
    void observationHistoryFollowsAcceptedRevisionsDespiteClockRollbackOrTies() {
        var record = managed("orders");
        var first = new TableVersionObservation(record.tableId(), "v1", null, "SYNC", NOW);
        var second = new TableVersionObservation(record.tableId(), "v2", "v1", "SYNC", NOW.minusSeconds(30));
        var third = new TableVersionObservation(record.tableId(), "v3", "v2", "SYNC", NOW.minusSeconds(30));
        store.saveWithObservation(record.toBuilder().version("v1", NOW).build(), first);
        store.saveWithObservation(record.toBuilder().metadataRevision(record.metadataRevision() + 1)
                .version("v2", second.observedAt()).build(), second);
        store.saveWithObservation(record.toBuilder().metadataRevision(record.metadataRevision() + 2)
                .version("v3", third.observedAt()).build(), third);

        assertThat(store.listVersionObservations(record.tableId(), 10)).containsExactly(third, second, first);
        assertThat(store.listVersionObservations(record.tableId(), 2)).containsExactly(third, second);
        assertThat(store.listVersionObservations(record.tableId(), 0)).isEmpty();
        assertThat(store.listVersionObservations(record.tableId(), -1)).isEmpty();
        assertThat(store.listVersionObservations(UUID.randomUUID(), 10)).isEmpty();
        assertThat(store.findById(record.tableId()).orElseThrow().versionToken()).isEqualTo("v3");
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
    void readOwnerObservationPreservesTheRecordAcrossDeploymentChanges() throws SQLException {
        var owner = readOwner();
        var initial = store.observeReadOwner("cat", "db", "orders").orElseThrow();
        assertThat(initial.record()).isEqualTo(owner);
        assertThat(initial.deployment()).contains(new CatalogDeploymentIdentity(owner.catalogIncarnationId(), 7));

        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            assertThat(statement.executeUpdate("UPDATE catalog_definitions SET revision=8 WHERE name='cat'")).isOne();
            var changed = store.observeReadOwner("cat", "db", "orders").orElseThrow();
            assertThat(changed.record()).isEqualTo(owner);
            assertThat(changed.deployment()).contains(new CatalogDeploymentIdentity(owner.catalogIncarnationId(), 8));

            assertThat(statement.executeUpdate("DELETE FROM catalog_definitions WHERE name='cat'")).isOne();
        }
        var missing = store.observeReadOwner("cat", "db", "orders").orElseThrow();
        assertThat(missing.record()).isEqualTo(owner);
        assertThat(missing.deployment()).isEmpty();
        assertThat(store.observeReadOwner("cat", "db", "unregistered")).isEmpty();
    }

    @Test
    void readOwnerRequiresCurrentCatalogButAllowsDataVersionChanges() throws SQLException {
        var owner = readOwner();
        assertThat(store.matchesCurrentReadOwner(owner)).isTrue();

        store.save(owner.toBuilder().version("ICEBERG:uuid-orders:8", NOW.plusSeconds(1))
                .metadataRevision(owner.metadataRevision() + 1).build());
        assertThat(store.matchesCurrentReadOwner(owner)).isTrue();

        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            assertThat(statement.executeUpdate("UPDATE catalog_definitions SET revision=8 WHERE name='cat'")).isOne();
        }
        assertThat(store.matchesCurrentReadOwner(owner)).isFalse();
    }

    @Test
    void readOwnerRejectsDifferentPhysicalIdentityAndMissingNameReservation() throws SQLException {
        var owner = readOwner();
        assertThat(store.matchesCurrentReadOwner(owner.toBuilder().formatTableUuid("replacement").build())).isFalse();
        assertThat(store.matchesCurrentReadOwner(owner.toBuilder().formatLocation(null).build())).isFalse();

        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            assertThat(statement.executeUpdate("DELETE FROM table_control_name_reservation WHERE catalog_name='cat'"))
                    .isOne();
        }
        assertThat(store.findById(owner.tableId())).contains(owner);
        assertThat(store.matchesCurrentReadOwner(owner)).isFalse();
    }

    private TableControlRecord readOwner() throws SQLException {
        String incarnation = UUID.randomUUID().toString();
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement("""
                INSERT INTO catalog_definitions (name,type,provider,state,properties,incarnation_id,revision)
                VALUES ('cat','CONNECTOR','iceberg','ACTIVE','{}',?,7)
                """)) {
            statement.setString(1, incarnation);
            assertThat(statement.executeUpdate()).isOne();
        }
        var owner = managed("orders").toBuilder().catalogIncarnationId(incarnation).catalogRevision(7L).build();
        store.save(owner);
        return owner;
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
    void replacementFailureRestoresThePriorRegistrationNameAndInvalidations() throws SQLException {
        var current = managed("orders").toBuilder().createdAt(NOW.plusNanos(123)).build();
        var other = managed("elsewhere");
        store.save(current);
        store.save(other);
        var persisted = store.findById(current.tableId()).orElseThrow();
        assertThat(persisted.createdAt()).isNotEqualTo(current.createdAt());
        var conflicting = managed("orders").toBuilder().tableId(other.tableId()).metadataRevision(1).build();
        long before = countTableInvalidations("cat", "db", "orders");
        assertThatThrownBy(() -> store.replaceRegistration(current, conflicting, NOW))
                .isInstanceOf(org.pangrova.table.api.exception.TableControlException.class);
        assertThat(store.find("cat", "db", "orders")).contains(persisted);
        assertThat(store.find("cat", "db", "elsewhere")).contains(other);
        assertThat(countTableInvalidations("cat", "db", "orders")).isEqualTo(before);

        var replacement = managed("orders").toBuilder().metadataRevision(1).build();
        store.replaceRegistration(current, replacement, NOW);
        assertThat(store.findById(current.tableId())).contains(persisted.retiredAt(NOW));
        assertThat(store.find("cat", "db", "orders")).contains(replacement);
        var stale = managed("orders").toBuilder().metadataRevision(1).build();
        assertThatThrownBy(() -> store.replaceRegistration(current, stale, NOW))
                .isInstanceOf(org.pangrova.table.api.exception.TableControlException.class);
        assertThat(store.findById(stale.tableId())).isEmpty();
        assertThat(store.find("cat", "db", "orders")).contains(replacement);
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
                .isInstanceOf(org.pangrova.table.api.exception.TableControlException.class);
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
                .isInstanceOf(org.pangrova.table.api.exception.TableControlException.class);
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
                .isInstanceOf(org.pangrova.table.api.exception.TableControlException.class);
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
