package org.pangrova.metadata.core.control;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("integration")
class JdbcTableControlGovernanceBindingStoreTest {

    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

    private DataSource dataSource;
    private JdbcTableControlGovernanceBindingStore store;

    @BeforeEach
    void setUp() throws SQLException {
        dataSource = SharedPostgresContainer.migratedDataSource("governance_binding");
        store = new JdbcTableControlGovernanceBindingStore(dataSource);
    }

    @Test
    void bindingSurvivesARestartAndKeepsItsTableId() {
        UUID tableId = UUID.randomUUID();
        GovernanceBinding binding = binding(tableId,
                GovernanceBindingType.COLUMN_TAG, "pii_type", Map.of("value", "EMAIL"));

        store.save(binding);

        GovernanceBinding reloaded = new JdbcTableControlGovernanceBindingStore(dataSource)
                .find(tableId, GovernanceBindingType.COLUMN_TAG, "pii_type").orElseThrow();
        assertThat(reloaded.bindingId()).isEqualTo(binding.bindingId());
        assertThat(reloaded.tableId()).isEqualTo(tableId);
        assertThat(reloaded.config()).containsEntry("value", "EMAIL");
        assertThat(reloaded.createdBy()).isEqualTo("alice");
        assertThat(reloaded.createdAt()).isEqualTo(NOW);
    }

    @Test
    void upsertReplacesConfigWithoutChangingCreationMetadata() {
        UUID tableId = UUID.randomUUID();
        store.save(binding(tableId,
                GovernanceBindingType.QUALITY_RULE, "non_null_id", Map.of("max_null", "0")));

        store.save(new GovernanceBinding(UUID.randomUUID(), tableId,
                GovernanceBindingType.QUALITY_RULE, "non_null_id",
                Map.of("max_null", "5"), "bob", NOW, NOW.plusSeconds(30)));

        GovernanceBinding stored = store.find(tableId,
                GovernanceBindingType.QUALITY_RULE, "non_null_id").orElseThrow();
        assertThat(stored.config()).containsEntry("max_null", "5");
        assertThat(stored.createdBy()).isEqualTo("alice");
        assertThat(stored.createdAt()).isEqualTo(NOW);
        assertThat(stored.updatedAt()).isEqualTo(NOW.plusSeconds(30));
    }

    @Test
    void bindingsAreScopedByTableId() {
        UUID first = UUID.randomUUID();
        UUID second = UUID.randomUUID();
        store.save(binding(first, GovernanceBindingType.COLUMN_MASK, "email_mask", Map.of()));
        store.save(binding(second, GovernanceBindingType.COLUMN_MASK, "email_mask", Map.of()));

        assertThat(store.list(first)).extracting(GovernanceBinding::tableId).containsOnly(first);
        assertThat(store.list(second)).extracting(GovernanceBinding::tableId).containsOnly(second);
    }

    @Test
    void deleteRemovesOnlyTheNamedBinding() {
        UUID tableId = UUID.randomUUID();
        store.save(binding(tableId, GovernanceBindingType.COLUMN_TAG, "pii_type", Map.of()));
        store.save(binding(tableId, GovernanceBindingType.LINEAGE, "upstream", Map.of()));

        store.delete(tableId, GovernanceBindingType.COLUMN_TAG, "pii_type");

        assertThat(store.find(tableId, GovernanceBindingType.COLUMN_TAG, "pii_type")).isEmpty();
        assertThat(store.find(tableId, GovernanceBindingType.LINEAGE, "upstream")).isPresent();
    }

    @Test
    void deleteAllRetiresEveryBindingForTheTable() {
        UUID tableId = UUID.randomUUID();
        store.save(binding(tableId, GovernanceBindingType.COLUMN_TAG, "pii_type", Map.of()));
        store.save(binding(tableId, GovernanceBindingType.ROW_FILTER, "tenant_filter", Map.of()));
        store.save(binding(UUID.randomUUID(),
                GovernanceBindingType.COLUMN_TAG, "pii_type", Map.of()));

        store.deleteAll(tableId);

        assertThat(store.list(tableId)).isEmpty();
    }

    private static GovernanceBinding binding(UUID tableId, GovernanceBindingType type,
            String ref, Map<String, String> config) {
        return new GovernanceBinding(UUID.randomUUID(), tableId, type, ref, config,
                "alice", NOW, NOW);
    }
}
