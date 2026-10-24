package org.pangrova.metadata.control;

import org.junit.jupiter.api.Test;
import org.pangrova.spi.connector.metadata.table.TableIdentity;
import org.pangrova.spi.connector.metadata.table.TableVersionToken;
import org.pangrova.spi.security.AccessDeniedException;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TableControlServiceTest {

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    private static TableControlService service(boolean enabled, boolean enforce, String... allowlist) {
        return new TableControlService(
                new InMemoryTableControlStore(),
                new TableControlSettings(enabled, enforce, Set.of(allowlist)),
                CLOCK);
    }

    private static TableIdentity identity(String uuid) {
        return new TableIdentity("ICEBERG", uuid, "s3://bucket/table", "schema");
    }

    @Test
    void observedTableIsNotPersistedAndRejectsWrites() {
        TableControlService control = service(true, true);

        assertThat(control.describe("cat", "db", "t").ownershipMode())
                .isEqualTo(OwnershipMode.OBSERVED);
        assertThat(control.find("cat", "db", "t")).isEmpty();
        assertThatThrownBy(() -> control.requireManagedForWrite("cat", "db", "t", "INSERT"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("OBSERVED");
        assertThatThrownBy(() -> control.requireManagedForDdl("cat", "db", "t", "DROP TABLE"))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void enforcementDisabledAllowsWritesWithoutPersisting() {
        TableControlService control = service(true, false);

        assertThatCode(() -> control.requireManagedForWrite("cat", "db", "t", "INSERT"))
                .doesNotThrowAnyException();
        assertThat(control.find("cat", "db", "t")).isEmpty();
    }

    @Test
    void createdTableIsManagedAndUsable() {
        TableControlService control = service(true, true);

        TableControlRecord record = control.registerCreated("cat", "db", "t", "alice",
                Optional.of(identity("uuid-1")),
                Optional.of(TableVersionToken.of("ICEBERG", "uuid-1", "1"))).orElseThrow();

        assertThat(record.ownershipMode()).isEqualTo(OwnershipMode.MANAGED);
        assertThat(record.state()).isEqualTo(TableControlState.ACTIVE);
        assertThat(record.formatTableUuid()).isEqualTo("uuid-1");
        assertThat(record.versionToken()).contains("uuid-1:1");
        assertThat(control.describe("cat", "db", "t").tableId()).contains(record.tableId());

        assertThatCode(() -> control.requireManagedForWrite("cat", "db", "t", "INSERT"))
                .doesNotThrowAnyException();
        assertThatCode(() -> control.requireManagedForDdl("cat", "db", "t", "DROP TABLE"))
                .doesNotThrowAnyException();
    }

    @Test
    void recreatingTheSameNameWithANewFormatUuidIsANewTable() {
        TableControlService control = service(true, true);
        TableControlRecord original = control.registerCreated("cat", "db", "t", "alice",
                Optional.of(identity("uuid-1")), Optional.empty()).orElseThrow();

        TableControlRecord replacement = control.registerCreated("cat", "db", "t", "alice",
                Optional.of(identity("uuid-2")), Optional.empty()).orElseThrow();

        assertThat(replacement.tableId()).isNotEqualTo(original.tableId());
        assertThat(control.find("cat", "db", "t").orElseThrow().formatTableUuid())
                .isEqualTo("uuid-2");
        assertThat(control.find("cat", "db", "t").orElseThrow().metadataRevision())
                .isEqualTo(1L);
    }

    @Test
    void linkedTableRejectsDmlAndDestructiveDdl() {
        InMemoryTableControlStore store = new InMemoryTableControlStore();
        TableControlService control = new TableControlService(store,
                new TableControlSettings(true, true, Set.of()), CLOCK);
        store.save(linkedRecord("cat", "db", "external"));

        assertThat(control.describe("cat", "db", "external").ownershipMode())
                .isEqualTo(OwnershipMode.LINKED);
        assertThatThrownBy(() ->
                control.requireManagedForWrite("cat", "db", "external", "INSERT"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("LINKED");
        assertThatThrownBy(() ->
                control.requireManagedForDdl("cat", "db", "external", "DROP TABLE"))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void writeAdmissionFencesTheRegisteredFormatIdentity() {
        TableControlService control = service(true, true);
        control.registerCreated("cat", "db", "t", "alice",
                Optional.of(identity("uuid-1")), Optional.empty());

        assertThatCode(() -> control.requireManagedForWrite(
                "cat", "db", "t", "INSERT", Optional.of(identity("uuid-1"))))
                .doesNotThrowAnyException();
        assertThatThrownBy(() -> control.requireManagedForWrite(
                "cat", "db", "t", "INSERT", Optional.of(identity("uuid-2"))))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("no longer matches");
        assertThat(control.find("cat", "db", "t")).get()
                .extracting(TableControlRecord::state)
                .isEqualTo(TableControlState.QUARANTINED);
    }

    @Test
    void missingIdentityEvidenceBlocksTheWriteWithoutQuarantining() {
        TableControlService control = service(true, true);
        control.registerCreated("cat", "db", "t", "alice",
                Optional.of(identity("uuid-1")), Optional.empty());

        assertThatThrownBy(() -> control.requireManagedForWrite(
                "cat", "db", "t", "INSERT", Optional.empty()))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("no format identity");
        assertThat(control.find("cat", "db", "t")).get()
                .extracting(TableControlRecord::state)
                .isEqualTo(TableControlState.ACTIVE);
    }

    @Test
    void legacyRecordWithoutFenceAdoptsTheFirstObservedIdentity() {
        TableControlService control = service(true, true);
        control.registerCreated("cat", "db", "t", "alice",
                Optional.empty(), Optional.empty());

        assertThatCode(() -> control.requireManagedForWrite(
                "cat", "db", "t", "INSERT", Optional.of(identity("uuid-1"))))
                .doesNotThrowAnyException();
        assertThat(control.find("cat", "db", "t")).get()
                .extracting(TableControlRecord::formatTableUuid)
                .isEqualTo("uuid-1");
    }

    @Test
    void orphanedManagedRecordIsDroppableButNotAlterable() {
        InMemoryTableControlStore store = new InMemoryTableControlStore();
        TableControlService control = new TableControlService(store,
                new TableControlSettings(true, true, Set.of()), CLOCK);
        TableControlRecord created = control.registerCreated("cat", "db", "t", "alice",
                Optional.of(identity("uuid-1")), Optional.empty()).orElseThrow();
        store.save(created.toBuilder()
                .state(TableControlState.ORPHANED_MANAGED)
                .metadataRevision(created.metadataRevision() + 1)
                .build());

        assertThatThrownBy(() -> control.requireManagedForDdl("cat", "db", "t", "ALTER TABLE"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("ORPHANED_MANAGED");
        assertThatCode(() -> control.requireDroppable("cat", "db", "t", "DROP TABLE"))
                .doesNotThrowAnyException();
    }

    @Test
    void nonActiveManagedRecordBlocksWritesUntilReconciled() {
        InMemoryTableControlStore store = new InMemoryTableControlStore();
        TableControlService control = new TableControlService(store,
                new TableControlSettings(true, true, Set.of()), CLOCK);
        TableControlRecord quarantined = control.registerCreated("cat", "db", "t", "alice",
                Optional.of(identity("uuid-1")), Optional.empty()).orElseThrow();
        store.save(quarantined.toBuilder()
                .state(TableControlState.QUARANTINED)
                .metadataRevision(quarantined.metadataRevision() + 1)
                .build());

        assertThatThrownBy(() -> control.requireManagedForWrite("cat", "db", "t", "INSERT"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("QUARANTINED");
    }

    @Test
    void allowlistedLegacyTableIsRegisteredAsManagedOnFirstUse() {
        TableControlService control = service(true, true, "cat.db.legacy");

        assertThatCode(() -> control.requireManagedForWrite("cat", "db", "legacy", "INSERT"))
                .doesNotThrowAnyException();
        assertThat(control.find("cat", "db", "legacy")).get()
                .satisfies(record -> {
                    assertThat(record.ownershipMode()).isEqualTo(OwnershipMode.MANAGED);
                    assertThat(record.properties())
                            .containsEntry("legacy-write-allowlist", "true");
                });
    }

    @Test
    void renameMovesOwnershipToTheNewName() {
        TableControlService control = service(true, true);
        TableControlRecord record = control.registerCreated("cat", "db", "old", "alice",
                Optional.of(identity("uuid-1")), Optional.empty()).orElseThrow();

        control.rename("cat", "db", "old", "db", "new");

        assertThat(control.find("cat", "db", "old")).isEmpty();
        TableControlRecord moved = control.find("cat", "db", "new").orElseThrow();
        assertThat(moved.tableId()).isEqualTo(record.tableId());
        assertThat(moved.metadataRevision()).isEqualTo(record.metadataRevision() + 1);
    }

    @Test
    void droppedRecordIsNoLongerLiveButRemainsAuditable() {
        TableControlService control = service(true, true);
        TableControlRecord record = control.registerCreated("cat", "db", "t", "alice",
                Optional.of(identity("uuid-1")), Optional.empty()).orElseThrow();

        control.markDropped("cat", "db", "t");

        assertThat(control.find("cat", "db", "t")).isEmpty();
        assertThat(control.describe("cat", "db", "t").ownershipMode())
                .isEqualTo(OwnershipMode.OBSERVED);
    }

    @Test
    void adoptCreatesLinkedRecordAndIsIdempotent() {
        TableControlService control = service(true, true);

        TableControlRecord adopted = control.adopt("cat", "db", "t", "alice",
                Optional.of(identity("uuid-1")),
                Optional.of(TableVersionToken.of("ICEBERG", "uuid-1", "1")));
        TableControlRecord again = control.adopt("cat", "db", "t", "bob",
                Optional.of(identity("uuid-1")),
                Optional.of(TableVersionToken.of("ICEBERG", "uuid-1", "2")));

        assertThat(adopted.ownershipMode()).isEqualTo(OwnershipMode.LINKED);
        assertThat(adopted.provenance()).isEqualTo(TableControlProvenance.ADOPTED);
        assertThat(adopted.formatTableUuid()).isEqualTo("uuid-1");
        assertThat(adopted.versionToken()).isEqualTo("uuid-1:1");
        assertThat(again.tableId()).isEqualTo(adopted.tableId());
        assertThat(again.metadataRevision()).isEqualTo(adopted.metadataRevision());
        assertThat(control.describe("cat", "db", "t").ownershipMode())
                .isEqualTo(OwnershipMode.LINKED);
    }

    @Test
    void adoptRejectsNameOnlyAndChangedIdentity() {
        TableControlService control = service(true, true);

        assertThatThrownBy(() -> control.adopt("cat", "db", "t", "alice",
                Optional.empty(), Optional.empty()))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("stable format identity");

        control.adopt("cat", "db", "t", "alice",
                Optional.of(identity("uuid-1")), Optional.empty());
        assertThatThrownBy(() -> control.adopt("cat", "db", "t", "alice",
                Optional.of(identity("uuid-2")), Optional.empty()))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("identity");
        assertThat(control.find("cat", "db", "t").orElseThrow().formatTableUuid())
                .isEqualTo("uuid-1");
    }

    @Test
    void adoptRejectsManagedTables() {
        TableControlService control = service(true, true);
        control.registerCreated("cat", "db", "t", "alice",
                Optional.of(identity("uuid-1")), Optional.empty());

        assertThatThrownBy(() -> control.adopt("cat", "db", "t", "alice",
                Optional.of(identity("uuid-1")), Optional.empty()))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("already MANAGED");
    }

    @Test
    void unlinkDropsControlStateButKeepsThePhysicalTable() {
        TableControlService control = service(true, true);
        control.adopt("cat", "db", "t", "alice",
                Optional.of(identity("uuid-1")), Optional.empty());

        control.unlink("cat", "db", "t");

        assertThat(control.find("cat", "db", "t")).isEmpty();
        assertThat(control.describe("cat", "db", "t").ownershipMode())
                .isEqualTo(OwnershipMode.OBSERVED);
    }

    @Test
    void unlinkRejectsObservedManagedAndDisabledPlane() {
        TableControlService control = service(true, true);
        assertThatThrownBy(() -> control.unlink("cat", "db", "missing"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("OBSERVED");

        control.registerCreated("cat", "db", "managed", "alice",
                Optional.of(identity("uuid-1")), Optional.empty());
        assertThatThrownBy(() -> control.unlink("cat", "db", "managed"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("MANAGED");

        TableControlService disabled = service(false, false);
        assertThatThrownBy(() -> disabled.unlink("cat", "db", "t"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("disabled");
    }

    @Test
    void observedVersionAdvancesRevisionAndRecordsHistory() {
        InMemoryTableControlStore store = new InMemoryTableControlStore();
        TableControlService control = new TableControlService(store,
                new TableControlSettings(true, true, Set.of()), CLOCK);
        TableControlRecord record = control.adopt("cat", "db", "t", "alice",
                Optional.of(identity("uuid-1")),
                Optional.of(TableVersionToken.of("ICEBERG", "uuid-1", "1")));

        ObservationOutcome outcome = control.observeVersion(record,
                Optional.of(identity("uuid-1")),
                Optional.of(TableVersionToken.of("ICEBERG", "uuid-1", "2")),
                "PERIODIC", NOW.plusSeconds(30));

        assertThat(outcome).isEqualTo(ObservationOutcome.VERSION_CHANGED);
        TableControlRecord updated = control.find("cat", "db", "t").orElseThrow();
        assertThat(updated.versionToken()).isEqualTo("uuid-1:2");
        assertThat(updated.metadataRevision()).isEqualTo(record.metadataRevision() + 1);
        assertThat(updated.versionObservedAt()).isEqualTo(NOW.plusSeconds(30));
        assertThat(store.listVersionObservations(updated.tableId(), 10))
                .singleElement()
                .satisfies(observation -> {
                    assertThat(observation.versionToken()).isEqualTo("uuid-1:2");
                    assertThat(observation.previousVersionToken()).isEqualTo("uuid-1:1");
                    assertThat(observation.source()).isEqualTo("PERIODIC");
                });
    }

    @Test
    void unchangedVersionDoesNotWriteOrAdvanceRevision() {
        InMemoryTableControlStore store = new InMemoryTableControlStore();
        TableControlService control = new TableControlService(store,
                new TableControlSettings(true, true, Set.of()), CLOCK);
        TableControlRecord record = control.adopt("cat", "db", "t", "alice",
                Optional.of(identity("uuid-1")),
                Optional.of(TableVersionToken.of("ICEBERG", "uuid-1", "1")));

        ObservationOutcome outcome = control.observeVersion(record,
                Optional.of(identity("uuid-1")),
                Optional.of(TableVersionToken.of("ICEBERG", "uuid-1", "1")),
                "PERIODIC", NOW.plusSeconds(30));

        assertThat(outcome).isEqualTo(ObservationOutcome.UNCHANGED);
        assertThat(control.find("cat", "db", "t").orElseThrow().metadataRevision())
                .isEqualTo(record.metadataRevision());
        assertThat(store.listVersionObservations(record.tableId(), 10)).isEmpty();
    }

    @Test
    void identityChangeQuarantinesTheRecordAndStopsFurtherObservation() {
        TableControlService control = service(true, true);
        TableControlRecord record = control.adopt("cat", "db", "t", "alice",
                Optional.of(identity("uuid-1")), Optional.empty());

        ObservationOutcome outcome = control.observeVersion(record,
                Optional.of(identity("uuid-2")), Optional.empty(),
                "PERIODIC", NOW.plusSeconds(30));

        assertThat(outcome).isEqualTo(ObservationOutcome.IDENTITY_CHANGED);
        assertThat(control.find("cat", "db", "t")).get()
                .satisfies(current -> {
                    assertThat(current.state()).isEqualTo(TableControlState.QUARANTINED);
                    assertThat(current.metadataRevision())
                            .isEqualTo(record.metadataRevision() + 1);
                });
        assertThat(control.observeVersion(record, Optional.of(identity("uuid-2")),
                Optional.empty(), "PERIODIC", NOW.plusSeconds(60)))
                .isEqualTo(ObservationOutcome.IGNORED);
    }

    @Test
    void connectorWithoutVersionEvidenceLeavesTheRecordActive() {
        TableControlService control = service(true, true);
        TableControlRecord record = control.adopt("cat", "db", "t", "alice",
                Optional.of(identity("uuid-1")), Optional.empty());

        assertThat(control.observeVersion(record, Optional.of(identity("uuid-1")),
                Optional.empty(), "PERIODIC", NOW.plusSeconds(30)))
                .isEqualTo(ObservationOutcome.UNSUPPORTED);
        assertThat(control.find("cat", "db", "t")).get()
                .extracting(TableControlRecord::state)
                .isEqualTo(TableControlState.ACTIVE);
    }

    @Test
    void unavailableTableBecomesStaleAndReportsOnce() {
        TableControlService control = service(true, true);
        control.adopt("cat", "db", "t", "alice",
                Optional.of(identity("uuid-1")), Optional.empty());

        assertThat(control.markUnavailable("cat", "db", "t", NOW.plusSeconds(30)))
                .isEqualTo(ObservationOutcome.TABLE_MISSING);
        assertThat(control.find("cat", "db", "t")).get()
                .extracting(TableControlRecord::state)
                .isEqualTo(TableControlState.STALE);
        assertThat(control.markUnavailable("cat", "db", "t", NOW.plusSeconds(60)))
                .isEqualTo(ObservationOutcome.UNCHANGED);
    }

    @Test
    void staleRecordReturnsToActiveWhenTheTableResolvesWithTheSameVersion() {
        TableControlService control = service(true, true);
        TableControlRecord record = control.adopt("cat", "db", "t", "alice",
                Optional.of(identity("uuid-1")),
                Optional.of(TableVersionToken.of("ICEBERG", "uuid-1", "1")));
        control.markUnavailable("cat", "db", "t", NOW.plusSeconds(30));

        ObservationOutcome outcome = control.observeVersion(record,
                Optional.of(identity("uuid-1")),
                Optional.of(TableVersionToken.of("ICEBERG", "uuid-1", "1")),
                "PERIODIC", NOW.plusSeconds(60));

        assertThat(outcome).isEqualTo(ObservationOutcome.UNCHANGED);
        assertThat(control.find("cat", "db", "t")).get()
                .satisfies(current -> {
                    assertThat(current.state()).isEqualTo(TableControlState.ACTIVE);
                    assertThat(current.metadataRevision())
                            .isEqualTo(record.metadataRevision() + 2);
                });
    }

    @Test
    void orphanedManagedRecordIsRecoveredWhenTheTableStillResolves() {
        TableControlService control = service(true, true);
        control.registerOrphanedManaged("cat", "db", "t", "alice",
                Optional.of(identity("uuid-1")));
        TableControlRecord orphaned = control.find("cat", "db", "t").orElseThrow();
        assertThat(orphaned.state()).isEqualTo(TableControlState.ORPHANED_MANAGED);

        ObservationOutcome outcome = control.observeVersion(orphaned,
                Optional.of(identity("uuid-1")), Optional.empty(),
                "PERIODIC", NOW.plusSeconds(30));

        assertThat(outcome).isEqualTo(ObservationOutcome.UNSUPPORTED);
        assertThat(control.find("cat", "db", "t")).get()
                .satisfies(current -> {
                    assertThat(current.state()).isEqualTo(TableControlState.ACTIVE);
                    assertThat(current.metadataRevision())
                            .isEqualTo(orphaned.metadataRevision() + 1);
                });
    }

    @Test
    void listLiveExcludesRetiredRecords() {
        TableControlService control = service(true, true);
        control.adopt("cat", "db", "linked", "alice",
                Optional.of(identity("uuid-1")), Optional.empty());
        control.adopt("cat", "db", "gone", "alice",
                Optional.of(identity("uuid-2")), Optional.empty());
        control.unlink("cat", "db", "gone");

        assertThat(control.listLive("cat"))
                .extracting(TableControlRecord::table)
                .containsExactly("linked");
    }

    private static TableControlRecord linkedRecord(String catalog, String schema, String table) {
        return TableControlRecord.builder()
                .tableId(UUID.randomUUID())
                .name(catalog, schema, table)
                .format("ICEBERG")
                .formatTableUuid("uuid-linked")
                .formatLocation("s3://bucket/linked")
                .ownershipMode(OwnershipMode.LINKED)
                .state(TableControlState.ACTIVE)
                .provenance(TableControlProvenance.ADOPTED)
                .metadataRevision(1)
                .createdAt(NOW)
                .updatedAt(NOW)
                .build();
    }
}
