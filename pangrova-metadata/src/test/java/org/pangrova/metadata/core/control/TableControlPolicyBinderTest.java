package org.pangrova.metadata.core.control;

import org.junit.jupiter.api.Test;
import org.pangrova.spi.connector.metadata.table.TableIdentity;
import org.pangrova.spi.connector.metadata.table.TableVersionToken;
import org.pangrova.spi.security.AccessDeniedException;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TableControlPolicyBinderTest {

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");
    private static final Clock CLOCK = Clock.fixed(NOW, ZoneOffset.UTC);

    private final InMemoryTableControlStore controlStore = new InMemoryTableControlStore();
    private final InMemoryTableControlGovernanceBindingStore bindingStore =
            new InMemoryTableControlGovernanceBindingStore();
    private final TableControlService control = new TableControlService(controlStore, name -> new CatalogDeploymentIdentity("test-deployment", 1),
            new TableControlSettings(true, true, Set.of()), CLOCK);
    private final TableControlPolicyBinder binder =
            new TableControlPolicyBinder(control, bindingStore, CLOCK);

    @Test
    void observedTableCannotCarryGovernance() {
        assertThatThrownBy(() -> binder.bind("cat", "db", "external",
                GovernanceBindingType.COLUMN_TAG, "pii_type", Map.of("value", "EMAIL"), "alice"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("OBSERVED");
        assertThat(bindingStore.list(UUID.randomUUID())).isEmpty();
    }

    @Test
    void bindingIsStoredUnderTheStableTableId() {
        TableControlRecord record = adopt("cat", "db", "external", "uuid-1");

        GovernanceBinding binding = binder.bind("cat", "db", "external",
                GovernanceBindingType.COLUMN_TAG, "pii_type", Map.of("value", "EMAIL"), "alice");

        assertThat(binding.tableId()).isEqualTo(record.tableId());
        assertThat(binding.createdBy()).isEqualTo("alice");
        assertThat(binder.requireCurrentBinding("cat", "db", "external",
                GovernanceBindingType.COLUMN_TAG, "pii_type")).isEqualTo(binding);
        assertThat(binder.bindingsFor("cat", "db", "external")).containsExactly(binding);
    }

    @Test
    void rebindingUpdatesConfigAndKeepsTheBindingIdentity() {
        adopt("cat", "db", "external", "uuid-1");
        GovernanceBinding first = binder.bind("cat", "db", "external",
                GovernanceBindingType.QUALITY_RULE, "non_null_id", Map.of("max_null", "0"), "alice");

        GovernanceBinding second = binder.bind("cat", "db", "external",
                GovernanceBindingType.QUALITY_RULE, "non_null_id", Map.of("max_null", "5"), "bob");

        assertThat(second.bindingId()).isEqualTo(first.bindingId());
        assertThat(second.createdBy()).isEqualTo("alice");
        assertThat(second.config()).containsEntry("max_null", "5");
        assertThat(binder.bindingsFor("cat", "db", "external")).containsExactly(second);
    }

    @Test
    void bindingFromADroppedAndRecreatedTableIsRejected() {
        TableControlRecord original = adopt("cat", "db", "external", "uuid-1");
        binder.bind("cat", "db", "external",
                GovernanceBindingType.COLUMN_MASK, "email_mask", Map.of(), "alice");

        control.unlink("cat", "db", "external");
        adopt("cat", "db", "external", "uuid-2");

        assertThat(bindingStore.find(original.tableId(),
                GovernanceBindingType.COLUMN_MASK, "email_mask")).isPresent();
        assertThatThrownBy(() -> binder.requireCurrentBinding("cat", "db", "external",
                GovernanceBindingType.COLUMN_MASK, "email_mask"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("dropped/recreated");
    }

    @Test
    void purgeRemovesEveryBindingForTheRetiredTable() {
        TableControlRecord record = adopt("cat", "db", "external", "uuid-1");
        binder.bind("cat", "db", "external",
                GovernanceBindingType.COLUMN_TAG, "pii_type", Map.of(), "alice");
        binder.bind("cat", "db", "external",
                GovernanceBindingType.LINEAGE, "upstream_orders", Map.of(), "alice");

        binder.purge(record.tableId());

        assertThat(bindingStore.list(record.tableId())).isEmpty();
    }

    @Test
    void blankBindingReferenceIsRejected() {
        adopt("cat", "db", "external", "uuid-1");

        assertThatThrownBy(() -> binder.bind("cat", "db", "external",
                GovernanceBindingType.COLUMN_TAG, " ", Map.of(), "alice"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("must not be blank");
    }

    @Test
    void inactiveControlRecordRejectsBinding() {
        adopt("cat", "db", "external", "uuid-1");
        controlStore.save(controlStore.find("cat", "db", "external").orElseThrow().toBuilder()
                .state(TableControlState.QUARANTINED)
                .metadataRevision(20)
                .build());

        assertThatThrownBy(() -> binder.bind("cat", "db", "external",
                GovernanceBindingType.COLUMN_TAG, "pii_type", Map.of(), "alice"))
                .isInstanceOf(AccessDeniedException.class)
                .hasMessageContaining("QUARANTINED");
    }

    private TableControlRecord adopt(String catalog, String schema, String table, String formatUuid) {
        return control.adopt(catalog, schema, table, "alice",
                Optional.of(new TableIdentity("ICEBERG", formatUuid,
                        "s3://bucket/" + table, "schema")),
                Optional.of(TableVersionToken.of("ICEBERG", formatUuid, "7")));
    }
}
