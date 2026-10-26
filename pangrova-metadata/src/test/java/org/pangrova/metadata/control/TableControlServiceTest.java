package org.pangrova.metadata.control;

import org.junit.jupiter.api.Test;
import org.pangrova.spi.connector.metadata.table.TableIdentity;
import org.pangrova.spi.connector.metadata.table.TableVersionToken;
import org.pangrova.spi.security.AccessDeniedException;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
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
                new InMemoryTableControlStore(), name -> new CatalogDeploymentIdentity("test-deployment", 1),
                new TableControlSettings(enabled, enforce, Set.of(allowlist)),
                CLOCK);
    }

    private static TableIdentity identity(String uuid) {
        return new TableIdentity("ICEBERG", uuid, "s3://bucket/table", "schema");
    }

    private static TableIdentity paimonIdentity(String uuid) {
        return new TableIdentity("PAIMON", uuid, "s3://bucket/table", "schema");
    }

    @Test
    void icebergRenameCannotTransferAuthorityToAnotherPhysicalUuid() {
        for (String format : java.util.List.of("ICEBERG", "iceberg")) {
            var control = service(true, true);
            var original = control.registerCreated("cat", "db", "source", "alice",
                    Optional.of(new TableIdentity(format, "original", "s3://bucket/source", "schema")),
                    Optional.empty()).orElseThrow();
            var pending = control.rename(original, "db", "renamed");
            assertThatThrownBy(() -> control.completeRename(pending,
                    Optional.of(new TableIdentity(format, "replacement", "s3://bucket/renamed", "schema")),
                    Optional.empty())).isInstanceOf(AccessDeniedException.class)
                    .hasMessageContaining("different Iceberg table UUID");
            assertThat(control.findById(original.tableId())).contains(pending);
            assertThat(control.find("cat", "db", "source")).contains(pending);
            assertThat(control.find("cat", "db", "renamed")).contains(pending);
            assertThat(pending.state()).isEqualTo(TableControlState.RENAMING);
        }
    }

    @Test
    void cachedOwnerProofCannotQuarantineACompletedRenameWhosePhysicalIdentityChanged() {
        var control = service(true, true);
        var original = control.registerCreated("cat", "db", "t", "alice",
                Optional.of(paimonIdentity("uuid-1")), Optional.empty()).orElseThrow();
        var pending = control.rename(original, "db", "other");
        control.completeRename(pending, Optional.of(paimonIdentity("uuid-2")), Optional.empty());
        var returning = control.rename(control.findById(original.tableId()).orElseThrow(), "db", "t");
        control.completeRename(returning, Optional.of(paimonIdentity("uuid-3")), Optional.empty());
        assertThatThrownBy(() -> control.requireReadOwner(original, "cached read"))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("ownership or namespace changed");
        var current = control.findById(original.tableId()).orElseThrow();
        assertThat(current.state()).isEqualTo(TableControlState.ACTIVE);
        control.requireReadOwner(current, "current read");
    }

    @Test
    void readCatalogRevisionUsesItsValueOutsideTheBoxedLongCache() {
        var store = new InMemoryTableControlStore();
        var control = new TableControlService(store,
                name -> new CatalogDeploymentIdentity("test-deployment", 1000),
                new TableControlSettings(true, true, Set.of()), CLOCK);
        var original = control.registerCreated("cat", "db", "t", "alice",
                Optional.of(identity("uuid-1")), Optional.empty()).orElseThrow();
        store.save(original.toBuilder().catalogRevision(Long.parseLong("1000"))
                .metadataRevision(original.metadataRevision() + 1).build());
        control.requireReadIdentity(original, Optional.of(identity("uuid-1")), "read");
        store.save(original.toBuilder().catalogRevision(1001L)
                .metadataRevision(original.metadataRevision() + 2).build());
        assertThatThrownBy(() -> control.requireReadIdentity(original, Optional.of(identity("uuid-1")), "read"))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("namespace changed");
    }

    @Test
    void readIdentityAllowsOrdinaryVersionAdvanceButRejectsRetirementAndReplacement() {
        var control = service(true, true);
        var original = control.registerCreated("cat", "db", "t", "alice",
                Optional.of(identity("uuid-1")), Optional.of(TableVersionToken.of("ICEBERG", "uuid-1", "1")))
                .orElseThrow();
        control.observeVersion(original, Optional.of(identity("uuid-1")),
                Optional.of(TableVersionToken.of("ICEBERG", "uuid-1", "2")), "EXTERNAL", NOW.plusSeconds(1));
        control.requireReadIdentity(original, Optional.of(identity("uuid-1")), "read");
        control.markDropped(control.findById(original.tableId()).orElseThrow());
        var replacement = control.registerCreated("cat", "db", "t", "alice",
                Optional.of(identity("uuid-2")), Optional.empty()).orElseThrow();
        assertThatThrownBy(() -> control.requireReadIdentity(original, Optional.of(identity("uuid-2")), "read"))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("ownership");
        assertThat(control.find("cat", "db", "t")).contains(replacement);
    }

    @Test
    void readNamespaceComparisonDoesNotFlattenQuotedIdentifierComponents() {
        var control = service(true, true);
        var original = control.registerCreated("cat", "db", "t.x", "alice",
                Optional.of(identity("uuid-1")), Optional.empty()).orElseThrow();
        var pending = control.rename(original, "db.t", "x");
        control.completeRename(pending, Optional.of(identity("uuid-1")), Optional.empty());
        var renamed = control.findById(original.tableId()).orElseThrow();
        assertThat(renamed.qualifiedName()).isEqualTo(original.qualifiedName());
        assertThatThrownBy(() -> control.requireReadIdentity(original, Optional.of(identity("uuid-1")), "read"))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("namespace changed");
        assertThat(control.findById(original.tableId()).orElseThrow().state()).isEqualTo(TableControlState.ACTIVE);
    }

    @Test
    void readIdentityRejectsMissingEvidenceWithoutQuarantineAndQuarantinesAContradiction() {
        var control = service(true, true);
        var original = control.registerCreated("cat", "db", "t", "alice",
                Optional.of(identity("uuid-1")), Optional.empty()).orElseThrow();
        assertThatThrownBy(() -> control.requireReadIdentity(original, Optional.empty(), "read"))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("missing format identity");
        assertThat(control.findById(original.tableId()).orElseThrow().state()).isEqualTo(TableControlState.ACTIVE);
        assertThatThrownBy(() -> control.requireReadIdentity(original, Optional.of(identity("uuid-2")), "read"))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("no longer matches");
        assertThat(control.findById(original.tableId()).orElseThrow().state()).isEqualTo(TableControlState.QUARANTINED);
    }

    @Test
    void missingObservationForARetiredTableCannotStaleItsReplacement() {
        TableControlService control = service(true, true);
        TableControlRecord original = control.registerCreated("cat", "db", "t", "alice",
                Optional.of(identity("uuid-1")), Optional.empty()).orElseThrow();
        control.markDropped(control.find("cat", "db", "t").orElseThrow());
        TableControlRecord replacement = control.registerCreated("cat", "db", "t", "alice",
                Optional.of(identity("uuid-2")), Optional.empty()).orElseThrow();

        assertThat(control.markUnavailable(original, NOW.plusSeconds(30)))
                .isEqualTo(ObservationOutcome.IGNORED);
        assertThat(control.find("cat", "db", "t")).contains(replacement);
    }

    @Test
    void staleObservationCannotOverwriteANewerVersion() {
        TableControlService control = service(true, true);
        TableControlRecord initial = control.adopt("cat", "db", "t", "alice",
                Optional.of(identity("uuid-1")),
                Optional.of(TableVersionToken.of("ICEBERG", "uuid-1", "1")));
        control.observeVersion(initial, Optional.of(identity("uuid-1")),
                Optional.of(TableVersionToken.of("ICEBERG", "uuid-1", "3")),
                "PERIODIC", NOW.plusSeconds(30));

        assertThat(control.observeVersion(initial, Optional.of(identity("uuid-1")),
                Optional.of(TableVersionToken.of("ICEBERG", "uuid-1", "2")),
                "PERIODIC", NOW.plusSeconds(20))).isEqualTo(ObservationOutcome.IGNORED);
        assertThat(control.find("cat", "db", "t").orElseThrow().versionToken())
                .isEqualTo("uuid-1:3");
    }

    @Test
    void delayedDropCannotRetireAReplacementWithTheSameName() {
        TableControlService control = service(true, true);
        var original = control.registerCreated("cat", "db", "t", "alice",
                Optional.of(identity("original")), Optional.empty()).orElseThrow();
        control.markDropped(original);
        var replacement = control.registerCreated("cat", "db", "t", "bob",
                Optional.of(identity("replacement")), Optional.empty()).orElseThrow();

        assertThatThrownBy(() -> control.markDropped(original))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("newer table registration");
        assertThat(control.find("cat", "db", "t")).contains(replacement);
    }

    @Test
    void staleRecordCannotRecoverWithoutIdentityEvidence() {
        TableControlService control = service(true, true);
        control.adopt("cat", "db", "t", "alice",
                Optional.of(identity("uuid-1")), Optional.empty());
        control.markUnavailable(control.find("cat", "db", "t").orElseThrow(), NOW.plusSeconds(10));
        TableControlRecord stale = control.find("cat", "db", "t").orElseThrow();

        assertThat(control.observeVersion(stale, Optional.empty(),
                Optional.of(TableVersionToken.of("ICEBERG", "uuid-1", "2")),
                "PERIODIC", NOW.plusSeconds(30))).isEqualTo(ObservationOutcome.UNSUPPORTED);
        assertThat(control.find("cat", "db", "t").orElseThrow()).isEqualTo(stale);
    }

    @Test
    void orphanedRecordCannotRecoverWithoutIdentityOrVersionEvidence() {
        TableControlService control = service(true, true);
        control.registerOrphanedManaged("cat", "db", "t", "alice", Optional.empty());
        TableControlRecord orphaned = control.find("cat", "db", "t").orElseThrow();

        assertThat(control.observeVersion(orphaned, Optional.empty(), Optional.empty(),
                "PERIODIC", NOW.plusSeconds(30))).isEqualTo(ObservationOutcome.UNSUPPORTED);
        assertThat(control.find("cat", "db", "t").orElseThrow()).isEqualTo(orphaned);
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
        TableControlService control = new TableControlService(store, name -> new CatalogDeploymentIdentity("test-deployment", 1),
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
    void explicitlyAllowlistedLegacyRecordCanBindItsFirstIdentity() {
        TableControlService control = service(true, true, "cat.db.t");
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
    void unrecordedIdentityCannotBeInferredByMutationOrBackgroundObservation() {
        TableControlService control = service(true, true);
        var record = control.registerCreated("cat", "db", "t", "alice",
                Optional.empty(), Optional.empty()).orElseThrow();
        for (Optional<TableIdentity> evidence : List.of(Optional.<TableIdentity>empty(), Optional.of(identity("replacement")))) {
            assertThatThrownBy(() -> control.requireManagedForWrite("cat", "db", "t", "INSERT", evidence))
                    .isInstanceOf(AccessDeniedException.class).hasMessageContaining("no registered format identity");
            assertThatThrownBy(() -> control.requireDroppable("cat", "db", "t", "DROP", evidence))
                    .isInstanceOf(AccessDeniedException.class).hasMessageContaining("no registered format identity");
        }
        assertThat(control.observeVersion(record, Optional.of(identity("replacement")),
                Optional.of(TableVersionToken.of("ICEBERG", "replacement", "2")), "TEST", CLOCK.instant()))
                .isEqualTo(ObservationOutcome.UNSUPPORTED);
        assertThat(control.find("cat", "db", "t")).contains(record);
    }

    @Test
    void orphanedManagedRecordIsDroppableButNotAlterable() {
        InMemoryTableControlStore store = new InMemoryTableControlStore();
        TableControlService control = new TableControlService(store, name -> new CatalogDeploymentIdentity("test-deployment", 1),
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
        TableControlService control = new TableControlService(store, name -> new CatalogDeploymentIdentity("test-deployment", 1),
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

        assertThat(control.find("cat", "db", "old")).get()
                .extracting(TableControlRecord::state).isEqualTo(TableControlState.RENAMING);
        assertThatThrownBy(() -> control.requireCreatableName("cat", "db", "old"))
                .isInstanceOf(AccessDeniedException.class);
        TableControlRecord moved = control.find("cat", "db", "new").orElseThrow();
        assertThat(moved.tableId()).isEqualTo(record.tableId());
        assertThat(moved.metadataRevision()).isEqualTo(record.metadataRevision() + 1);
    }

    @Test
    void orphanRegistrationCannotOverwriteOrReleasePendingRenameReservations() {
        var control = service(true, true);
        var original = control.registerCreated("cat", "db", "old", "alice",
                Optional.of(identity("uuid-1")), Optional.empty()).orElseThrow();
        var moved = control.rename(original, "db", "new");
        for (String name : java.util.List.of("old", "new")) {
            assertThatThrownBy(() -> control.registerCreated("cat", "db", name,
                    "concurrent-create", Optional.of(identity("uuid-1")), Optional.empty()))
                    .isInstanceOf(AccessDeniedException.class).hasMessageContaining("reserved");
            assertThatThrownBy(() -> control.registerOrphanedManaged("cat", "db", name,
                    "concurrent-create", Optional.of(identity("replacement"))))
                    .isInstanceOf(AccessDeniedException.class).hasMessageContaining("reserved");
            assertThat(control.find("cat", "db", name)).contains(moved);
        }
    }

    @Test
    void observersCannotOverwriteAnInFlightRename() {
        TableControlService control = service(true, true);
        var original = control.registerCreated("cat", "db", "old", "alice",
                Optional.of(identity("uuid-1")), Optional.empty()).orElseThrow();
        var moved = control.rename(original, "db", "new");
        assertThat(moved.state()).isEqualTo(TableControlState.RENAMING);
        assertThat(control.markUnavailable(moved, Instant.now())).isEqualTo(ObservationOutcome.IGNORED);
        assertThat(control.observeVersion(moved, Optional.of(identity("intermediate")),
                Optional.empty(), "PERIODIC", Instant.now())).isEqualTo(ObservationOutcome.IGNORED);
        assertThat(control.find("cat", "db", "new")).contains(moved);
        assertThatThrownBy(() -> control.requireManagedForWrite("cat", "db", "new", "INSERT"))
                .isInstanceOf(AccessDeniedException.class);
    }

    @Test
    void completedIcebergRenameRetainsPhysicalIdentityAndClearsAnUnavailableVersion() {
        TableControlService control = service(true, true);
        var original = control.registerCreated("cat", "db", "old", "alice",
                Optional.of(identity("uuid-old")),
                Optional.of(TableVersionToken.of("ICEBERG", "uuid-old", "1"))).orElseThrow();
        var moved = control.rename(original, "db", "new");
        var physical = identity("uuid-old");
        control.completeRename(moved, Optional.of(physical), Optional.empty());
        var completed = control.find("cat", "db", "new").orElseThrow();
        assertThat(control.find("cat", "db", "old")).isEmpty();
        assertThat(completed.tableId()).isEqualTo(original.tableId());
        assertThat(completed.formatTableUuid()).isEqualTo(physical.formatTableUuid());
        assertThat(completed.metadataRevision()).isEqualTo(moved.metadataRevision() + 1);
        assertThat(completed.versionToken()).isNull();
        assertThatCode(() -> control.requireManagedForWrite("cat", "db", "new", "INSERT",
                Optional.of(physical))).doesNotThrowAnyException();
        assertThatThrownBy(() -> control.completeRename(moved,
                Optional.of(identity("unrelated")), Optional.empty()))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("changed control record");
        assertThat(control.find("cat", "db", "new")).contains(completed);
    }

    @Test
    void delayedRenameRollbackCannotMoveAReplacementRegistration() {
        TableControlService control = service(true, true);
        var original = control.registerCreated("cat", "db", "old", "alice",
                Optional.of(identity("uuid-1")), Optional.empty()).orElseThrow();
        var moved = control.rename(original, "db", "new");
        control.markDropped(moved);
        var replacement = control.registerCreated("cat", "db", "new", "bob",
                Optional.of(identity("uuid-2")), Optional.empty()).orElseThrow();

        assertThatThrownBy(() -> control.rename(moved, "db", "old"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("control record changed");
        assertThat(control.find("cat", "db", "old")).isEmpty();
        assertThat(control.find("cat", "db", "new")).contains(replacement);
    }

    @Test
    void droppedRecordIsNoLongerLiveButRemainsAuditable() {
        TableControlService control = service(true, true);
        TableControlRecord record = control.registerCreated("cat", "db", "t", "alice",
                Optional.of(identity("uuid-1")), Optional.empty()).orElseThrow();

        control.markDropped(control.find("cat", "db", "t").orElseThrow());

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
        TableControlService control = new TableControlService(store, name -> new CatalogDeploymentIdentity("test-deployment", 1),
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
        TableControlService control = new TableControlService(store, name -> new CatalogDeploymentIdentity("test-deployment", 1),
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

        assertThat(control.markUnavailable(control.find("cat", "db", "t").orElseThrow(), NOW.plusSeconds(30)))
                .isEqualTo(ObservationOutcome.TABLE_MISSING);
        assertThat(control.find("cat", "db", "t")).get()
                .extracting(TableControlRecord::state)
                .isEqualTo(TableControlState.STALE);
        assertThat(control.markUnavailable(control.find("cat", "db", "t").orElseThrow(), NOW.plusSeconds(60)))
                .isEqualTo(ObservationOutcome.UNCHANGED);
    }

    @Test
    void staleRecordReturnsToActiveWhenTheTableResolvesWithTheSameVersion() {
        TableControlService control = service(true, true);
        TableControlRecord record = control.adopt("cat", "db", "t", "alice",
                Optional.of(identity("uuid-1")),
                Optional.of(TableVersionToken.of("ICEBERG", "uuid-1", "1")));
        control.markUnavailable(control.find("cat", "db", "t").orElseThrow(), NOW.plusSeconds(30));

        ObservationOutcome outcome = control.observeVersion(
                control.find("cat", "db", "t").orElseThrow(),
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
                .catalogIncarnationId("test-deployment")
                .catalogRevision(1L)
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
