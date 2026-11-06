package org.pangrova.table.core.ownership;

import org.pangrova.table.core.config.TableControlSettings;
import org.pangrova.table.core.policybinding.TableControlPolicyBinder;

import org.pangrova.table.api.identity.CatalogDeploymentIdentity;
import org.pangrova.table.api.ownership.TableControlRecord;
import org.pangrova.table.api.ownership.TableControlState;
import org.pangrova.table.api.policybinding.GovernanceBindingType;
import org.pangrova.table.core.statistics.TableControlStatisticsService;
import org.pangrova.persistence.table.memory.governancebinding.InMemoryTableControlGovernanceBindingStore;
import org.pangrova.persistence.table.memory.statistics.InMemoryTableControlStatisticsStore;
import org.pangrova.persistence.table.memory.control.InMemoryTableControlStore;

import org.junit.jupiter.api.Test;
import org.pangrova.connector.api.metadata.table.TableIdentity;
import org.pangrova.connector.api.metadata.table.TableStatistics;
import org.pangrova.security.api.error.AccessDeniedException;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TableControlCatalogDeploymentTest {
    private static final TableIdentity IDENTITY =
            new TableIdentity("ICEBERG", "same-uuid", "s3://bucket/table", "schema");
    private final AtomicReference<CatalogDeploymentIdentity> catalog =
            new AtomicReference<>(descriptor("original", 1));
    private final InMemoryTableControlStore store = new InMemoryTableControlStore();
    private final TableControlService control = new TableControlService(store, name -> catalog.get(),
            new TableControlSettings(true, true, Set.of()));

    @Test
    void createAndAdoptCaptureTheCurrentCatalogDeployment() {
        TableControlRecord created = created();
        TableControlRecord adopted = control.adopt("cat", "db", "external", "alice",
                Optional.of(IDENTITY), Optional.empty());

        assertThat(created.catalogIncarnationId()).isEqualTo("original");
        assertThat(created.catalogRevision()).isEqualTo(1);
        assertThat(adopted.catalogIncarnationId()).isEqualTo("original");
        assertThat(adopted.catalogRevision()).isEqualTo(1);
    }

    @Test
    void recreatedCatalogCannotInheritAuthorityEvenWithTheSamePhysicalUuid() {
        TableControlRecord original = created();
        catalog.set(descriptor("replacement", 1));

        assertAllAdmissionRejected(original);
        assertThat(control.find("cat", "db", "t")).contains(original);
    }

    @Test
    void changedRevisionCannotInheritAuthority() {
        TableControlRecord original = created();
        catalog.set(descriptor("original", 2));

        assertAllAdmissionRejected(original);
    }

    @Test
    void createdReplacementGetsANewStableIdInsteadOfTheOldGovernanceIdentity() {
        TableControlRecord original = created();
        catalog.set(descriptor("replacement", 1));

        TableControlRecord replacement = created();

        assertThat(replacement.tableId()).isNotEqualTo(original.tableId());
        assertThat(replacement.catalogIncarnationId()).isEqualTo("replacement");
        assertThat(store.findById(original.tableId()).orElseThrow().state())
                .isEqualTo(TableControlState.DROPPED);
    }

    @Test
    void adoptDoesNotReviveBindingsFromAnEarlierDeployment() {
        control.adopt("cat", "db", "external", "alice", Optional.of(IDENTITY), Optional.empty());
        catalog.set(descriptor("replacement", 1));

        assertThatThrownBy(() -> control.adopt("cat", "db", "external", "bob",
                Optional.of(IDENTITY), Optional.empty()))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("catalog deployment");
        control.unlink("cat", "db", "external");
        assertThat(control.adopt("cat", "db", "external", "bob",
                Optional.of(IDENTITY), Optional.empty()).catalogIncarnationId()).isEqualTo("replacement");
    }

    @Test
    void legacyRecordDoesNotAcquireTheCurrentDeploymentByName() {
        TableControlRecord legacy = legacy(created());

        assertThatThrownBy(() -> control.observeVersion(legacy, Optional.of(IDENTITY), Optional.empty(),
                "PERIODIC", Instant.now()))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("never registered");
        assertAllAdmissionRejected(legacy);
        assertThat(store.findById(legacy.tableId())).contains(legacy);
    }

    @Test
    void allowlistedLegacyBackfillRequiresThePreviouslyRegisteredPhysicalIdentity() {
        TableControlRecord legacy = legacy(created());
        TableControlService allowlisted = new TableControlService(store, name -> catalog.get(),
                new TableControlSettings(true, true, Set.of("cat.db.t")));
        assertThatThrownBy(() -> allowlisted.observeVersion(legacy, Optional.empty(), Optional.empty(),
                "PERIODIC", Instant.now())).isInstanceOf(AccessDeniedException.class);
        TableIdentity replacement = new TableIdentity("ICEBERG", "different", IDENTITY.location(), "schema");
        assertThatThrownBy(() -> allowlisted.observeVersion(legacy, Optional.of(replacement), Optional.empty(),
                "PERIODIC", Instant.now())).isInstanceOf(AccessDeniedException.class);

        allowlisted.observeVersion(legacy, Optional.of(IDENTITY), Optional.empty(), "PERIODIC", Instant.now());

        TableControlRecord bound = store.findById(legacy.tableId()).orElseThrow();
        assertThat(bound.catalogIncarnationId()).isEqualTo("original");
        assertThat(bound.catalogRevision()).isEqualTo(1);
        assertThat(bound.metadataRevision()).isEqualTo(legacy.metadataRevision() + 1);
    }

    @Test
    void explicitLegacyAllowlistCannotRebindAnAlreadyCapturedDeployment() {
        TableControlRecord original = created();
        catalog.set(descriptor("replacement", 1));
        TableControlService allowlisted = new TableControlService(store, name -> catalog.get(),
                new TableControlSettings(true, true, Set.of("cat.db.t")));

        assertThatThrownBy(() -> allowlisted.observeVersion(original, Optional.of(IDENTITY), Optional.empty(),
                "PERIODIC", Instant.now())).isInstanceOf(AccessDeniedException.class);
        assertThat(store.findById(original.tableId())).contains(original);
    }

    @Test
    void partialLegacyBindingCannotBeCompletedAgainstAnUnprovenDeployment() {
        TableControlRecord original = created();
        TableControlRecord partial = original.toBuilder().catalogRevision(null)
                .metadataRevision(original.metadataRevision() + 1).build();
        store.save(partial);
        TableControlService allowlisted = new TableControlService(store, name -> catalog.get(),
                new TableControlSettings(true, true, Set.of("cat.db.t")));

        assertThatThrownBy(() -> allowlisted.observeVersion(partial, Optional.of(IDENTITY), Optional.empty(),
                "PERIODIC", Instant.now())).isInstanceOf(AccessDeniedException.class);
        assertThat(store.findById(partial.tableId())).contains(partial);
    }

    private void assertAllAdmissionRejected(TableControlRecord record) {
        assertThatThrownBy(() -> control.requireManagedForWrite("cat", "db", "t", "INSERT",
                Optional.of(IDENTITY))).isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("catalog deployment");
        assertThatThrownBy(() -> control.requireDroppable("cat", "db", "t", "DROP TABLE"))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("catalog deployment");
        assertThatThrownBy(() -> control.requireManagedForDdl("cat", "db", "t", "ALTER TABLE"))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("catalog deployment");
        assertThatThrownBy(() -> control.requireActiveIdentity(record, Optional.of(IDENTITY), "COMPACT"))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("catalog deployment");
        assertThatThrownBy(() -> control.describe("cat", "db", "t"))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("catalog deployment");
        assertThatThrownBy(() -> control.observeVersion(record, Optional.of(IDENTITY), Optional.empty(),
                "PERIODIC", Instant.now())).isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("catalog deployment");
        assertThatThrownBy(() -> control.rename(record, "db", "renamed"))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("catalog deployment");
        assertThatThrownBy(() -> control.markDropped(record))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("catalog deployment");
        var binder = new TableControlPolicyBinder(control, new InMemoryTableControlGovernanceBindingStore());
        assertThatThrownBy(() -> binder.bind("cat", "db", "t", GovernanceBindingType.COLUMN_MASK,
                "mask", Map.of(), "alice")).isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("catalog deployment");
        var statistics = new TableControlStatisticsService(control, new InMemoryTableControlStatisticsStore());
        assertThatThrownBy(() -> statistics.record(record, new TableStatistics(1, 10), "ANALYZE", "v1"))
                .isInstanceOf(AccessDeniedException.class).hasMessageContaining("catalog deployment");
    }

    private TableControlRecord created() {
        return control.registerCreated("cat", "db", "t", "alice", Optional.of(IDENTITY), Optional.empty())
                .orElseThrow();
    }

    private TableControlRecord legacy(TableControlRecord record) {
        TableControlRecord legacy = record.toBuilder().catalogIncarnationId(null).catalogRevision(null)
                .metadataRevision(record.metadataRevision() + 1).build();
        store.save(legacy);
        return legacy;
    }

    private static CatalogDeploymentIdentity descriptor(String incarnation, long revision) {
        return new CatalogDeploymentIdentity(incarnation, revision);
    }
}
