package org.pangrova.metadata.core.control;

import org.pangrova.metadata.api.control.OwnershipMode;

import org.junit.jupiter.api.Test;
import org.pangrova.spi.security.AccessDeniedException;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TableControlMaintenanceServiceTest {

    private static final Instant NOW = Instant.parse("2026-05-01T00:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    private final InMemoryTableControlMaintenanceStore store =
            new InMemoryTableControlMaintenanceStore();
    private final TableControlMaintenanceService service =
            new TableControlMaintenanceService(store, CLOCK);

    @Test
    void linkedTablesStartWithoutMaintenanceRights() {
        TableMaintenancePolicy policy = service.policyFor(record(OwnershipMode.LINKED));

        assertThat(policy.enabledOperations()).isEmpty();
        assertThat(policy.requireStableExternalVersion()).isTrue();
        assertThat(policy.allows(MaintenanceOperation.COMPACT)).isFalse();
        assertThat(policy.orphanFileAge())
                .isGreaterThanOrEqualTo(TableMaintenancePolicy.MIN_ORPHAN_FILE_AGE);
    }

    @Test
    void managedTablesStartWithCompactionAndCleanup() {
        TableMaintenancePolicy policy = service.policyFor(record(OwnershipMode.MANAGED));

        assertThat(policy.allows(MaintenanceOperation.COMPACT)).isTrue();
        assertThat(policy.allows(MaintenanceOperation.OPTIMIZE)).isTrue();
        assertThat(policy.allows(MaintenanceOperation.VACUUM)).isTrue();
        assertThat(policy.requireStableExternalVersion()).isFalse();
        assertThat(policy.retainLast()).isGreaterThan(1);
    }

    @Test
    void adoptionPropertiesConfigureOperationsAndLimits() {
        TableControlRecord linked = record(OwnershipMode.LINKED);

        TableMaintenancePolicy policy = service.applyAdoptionProperties(linked,
                Map.of("maintenance_operations", "COMPACT, EXPIRE_SNAPSHOTS",
                        "retain_last", "5",
                        "orphan_file_age_hours", "48",
                        "snapshot_retention_hours", "72"),
                "alice");

        assertThat(policy.enabledOperations()).containsExactlyInAnyOrder(
                MaintenanceOperation.COMPACT, MaintenanceOperation.EXPIRE_SNAPSHOTS);
        assertThat(policy.retainLast()).isEqualTo(5);
        assertThat(policy.orphanFileAge()).isEqualTo(Duration.ofHours(48));
        assertThat(policy.snapshotRetention()).isEqualTo(Duration.ofHours(72));
        assertThat(policy.updatedBy()).isEqualTo("alice");
        assertThat(policy.updatedAt()).isEqualTo(NOW);
        assertThat(store.findPolicy(linked.tableId())).contains(policy);
    }

    @Test
    void adoptionRejectsUnknownPropertyInsteadOfIgnoringIt() {
        assertThatThrownBy(() -> service.applyAdoptionProperties(
                record(OwnershipMode.LINKED), Map.of("maintenance_op", "COMPACT"), "alice"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("unknown maintenance property");
    }

    @Test
    void adoptionRejectsUnknownOperation() {
        assertThatThrownBy(() -> service.applyAdoptionProperties(
                record(OwnershipMode.LINKED),
                Map.of("maintenance_operations", "COMPACT,DROP_TABLE"), "alice"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("unknown maintenance operation");
    }

    @Test
    void adoptionCannotDisableTheActiveWriterGuardForLinkedTables() {
        assertThatThrownBy(() -> service.applyAdoptionProperties(
                record(OwnershipMode.LINKED),
                Map.of("maintenance_operations", "COMPACT",
                        "active_writer_guard", "false"), "alice"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("active-writer guard");
    }

    @Test
    void unsafeRetentionIsRejectedBeforeItCanBeStored() {
        assertThatThrownBy(() -> new TableMaintenancePolicy(UUID.randomUUID(),
                Set.of(MaintenanceOperation.COMPACT), 10, Duration.ofHours(12),
                Duration.ofDays(7), true, "alice", NOW))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("orphanFileAge");
        assertThatThrownBy(() -> new TableMaintenancePolicy(UUID.randomUUID(),
                Set.of(MaintenanceOperation.COMPACT), 10, Duration.ofDays(3),
                Duration.ofHours(12), true, "alice", NOW))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("snapshotRetention");
    }

    @Test
    void runLifecyclePersistsTheTerminalState() {
        TableControlRecord record = record(OwnershipMode.LINKED);
        TableMaintenanceRun run = service.beginRun(record, MaintenanceOperation.COMPACT,
                "MANUAL", "v1", "alice");

        assertThat(run.state()).isEqualTo(MaintenanceRunState.RUNNING);
        service.finishRun(run, MaintenanceRunState.SKIPPED, "v1",
                Map.of("reason", "external writer"));

        TableMaintenanceRun reloaded = service.listRuns(record.tableId(), 10).get(0);
        assertThat(reloaded.runId()).isEqualTo(run.runId());
        assertThat(reloaded.state()).isEqualTo(MaintenanceRunState.SKIPPED);
        assertThat(reloaded.versionBefore()).isEqualTo("v1");
        assertThat(reloaded.details()).containsEntry("reason", "external writer");
        assertThat(reloaded.finishedAt()).isEqualTo(NOW);
    }

    private static TableControlRecord record(OwnershipMode mode) {
        return TableControlRecord.builder()
                .tableId(UUID.randomUUID())
                .name("cat", "db", "orders")
                .format("ICEBERG")
                .formatTableUuid("uuid-orders")
                .ownershipMode(mode)
                .state(TableControlState.ACTIVE)
                .provenance(mode == OwnershipMode.LINKED
                        ? TableControlProvenance.ADOPTED : TableControlProvenance.CREATED)
                .metadataRevision(1)
                .createdAt(NOW)
                .updatedAt(NOW)
                .build();
    }
}
