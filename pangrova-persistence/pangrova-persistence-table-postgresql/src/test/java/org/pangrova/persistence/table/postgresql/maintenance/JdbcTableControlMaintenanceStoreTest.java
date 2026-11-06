package org.pangrova.persistence.table.postgresql.maintenance;

import org.pangrova.table.api.ownership.OwnershipMode;
import org.pangrova.table.api.ownership.TableControlProvenance;
import org.pangrova.table.api.ownership.TableControlRecord;
import org.pangrova.table.api.ownership.TableControlState;
import org.pangrova.table.api.maintenance.MaintenanceOperation;
import org.pangrova.table.api.maintenance.MaintenanceRunState;
import org.pangrova.table.api.maintenance.TableMaintenancePolicy;
import org.pangrova.table.api.maintenance.TableMaintenanceRun;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.pangrova.testing.testkit.postgres.MigratedPostgres;
import org.pangrova.testing.testkit.ContainerTestBase;

import javax.sql.DataSource;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("integration")
class JdbcTableControlMaintenanceStoreTest extends ContainerTestBase {

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

    private DataSource dataSource;
    private JdbcTableControlMaintenanceStore store;

    @BeforeEach
    void setUp() throws java.sql.SQLException {
        dataSource = MigratedPostgres.create();
        store = new JdbcTableControlMaintenanceStore(dataSource);
    }

    @Test
    void policySurvivesAStoreRestart() {
        UUID tableId = UUID.randomUUID();
        TableMaintenancePolicy policy = new TableMaintenancePolicy(tableId,
                Set.of(MaintenanceOperation.COMPACT, MaintenanceOperation.EXPIRE_SNAPSHOTS),
                5, Duration.ofHours(48), Duration.ofHours(72), true, "alice", NOW);
        store.savePolicy(policy);

        TableMaintenancePolicy reloaded = new JdbcTableControlMaintenanceStore(dataSource)
                .findPolicy(tableId).orElseThrow();

        assertThat(reloaded.enabledOperations()).containsExactlyInAnyOrder(
                MaintenanceOperation.COMPACT, MaintenanceOperation.EXPIRE_SNAPSHOTS);
        assertThat(reloaded.retainLast()).isEqualTo(5);
        assertThat(reloaded.orphanFileAge()).isEqualTo(Duration.ofHours(48));
        assertThat(reloaded.snapshotRetention()).isEqualTo(Duration.ofHours(72));
        assertThat(reloaded.requireStableExternalVersion()).isTrue();
        assertThat(reloaded.updatedBy()).isEqualTo("alice");
        assertThat(reloaded.updatedAt()).isEqualTo(NOW);
    }

    @Test
    void policyCanBeUpdatedInPlace() {
        UUID tableId = UUID.randomUUID();
        store.savePolicy(new TableMaintenancePolicy(tableId, Set.of(), 10,
                Duration.ofDays(3), Duration.ofDays(7), true, "alice", NOW));
        store.savePolicy(new TableMaintenancePolicy(tableId,
                Set.of(MaintenanceOperation.COMPACT), 20,
                Duration.ofDays(4), Duration.ofDays(9), false, "bob", NOW.plusSeconds(5)));

        TableMaintenancePolicy reloaded = store.findPolicy(tableId).orElseThrow();
        assertThat(reloaded.enabledOperations()).containsExactly(MaintenanceOperation.COMPACT);
        assertThat(reloaded.retainLast()).isEqualTo(20);
        assertThat(reloaded.updatedBy()).isEqualTo("bob");
    }

    @Test
    void emptyOperationSetRoundTrips() {
        UUID tableId = UUID.randomUUID();
        store.savePolicy(new TableMaintenancePolicy(tableId, java.util.Set.of(), 10,
                java.time.Duration.ofDays(3), java.time.Duration.ofDays(7), true, "", NOW));

        assertThat(store.findPolicy(tableId).orElseThrow().enabledOperations()).isEmpty();
    }

    @Test
    void runsAreOrderedNewestFirstAndFinishInPlace() {
        UUID tableId = UUID.randomUUID();
        TableControlRecord record = record(tableId, "orders");
        TableMaintenanceRun first = TableMaintenanceRun.running(record,
                MaintenanceOperation.COMPACT, "MANUAL", "v1", "alice", NOW);
        TableMaintenanceRun second = TableMaintenanceRun.running(record,
                MaintenanceOperation.VACUUM, "MANUAL", "v2", "bob", NOW.plusSeconds(60));
        store.saveRun(first);
        store.saveRun(second);

        store.saveRun(first.finished(MaintenanceRunState.SUCCEEDED, "v2",
                Map.of("files", "3"), NOW.plusSeconds(30)));

        var runs = store.listRuns(tableId, 10);
        assertThat(runs).extracting(TableMaintenanceRun::operation)
                .containsExactly(MaintenanceOperation.VACUUM, MaintenanceOperation.COMPACT);
        TableMaintenanceRun reloaded = runs.get(1);
        assertThat(reloaded.state()).isEqualTo(MaintenanceRunState.SUCCEEDED);
        assertThat(reloaded.versionAfter()).isEqualTo("v2");
        assertThat(reloaded.details()).containsEntry("files", "3");
        assertThat(reloaded.finishedAt()).isEqualTo(NOW.plusSeconds(30));
    }

    @Test
    void runsAreScopedToTheirTableId() {
        UUID firstTable = UUID.randomUUID();
        UUID secondTable = UUID.randomUUID();
        store.saveRun(TableMaintenanceRun.running(record(firstTable, "orders"),
                MaintenanceOperation.COMPACT, "MANUAL", "v1", "alice", NOW));
        store.saveRun(TableMaintenanceRun.running(record(secondTable, "customers"),
                MaintenanceOperation.COMPACT, "MANUAL", "v1", "alice", NOW));

        assertThat(store.listRuns(firstTable, 10)).hasSize(1);
        assertThat(store.listRuns(secondTable, 10)).hasSize(1);
        assertThat(store.listRuns(UUID.randomUUID(), 10)).isEmpty();
    }

    private static TableControlRecord record(UUID tableId, String table) {
        return TableControlRecord.builder()
                .tableId(tableId)
                .name("cat", "db", table)
                .format("ICEBERG")
                .formatTableUuid("uuid-" + table)
                .ownershipMode(OwnershipMode.MANAGED)
                .state(TableControlState.ACTIVE)
                .provenance(TableControlProvenance.CREATED)
                .metadataRevision(1)
                .createdAt(NOW)
                .updatedAt(NOW)
                .build();
    }
}
