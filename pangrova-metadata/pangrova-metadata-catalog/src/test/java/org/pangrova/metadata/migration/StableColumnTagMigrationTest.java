package org.pangrova.metadata.migration;

import org.pangrova.table.api.policy.GovernanceBinding;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.pangrova.table.api.enums.GovernanceBindingType;
import org.pangrova.persistence.table.postgresql.policy.PostgresTablePolicyRepository;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;

import javax.sql.DataSource;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Historical name-keyed data may migrate only with proof of its stable governance ownership. */
class StableColumnTagMigrationTest {
    @Test
    void provenTagMigratesWithoutChangingItsIdentityOrActor() throws Exception {
        var source = source();
        var tableId = historicalTable(source, false);
        var store = new PostgresTablePolicyRepository(source);
        var original = historicalBinding(source, tableId, "PII_EMAIL");
        insert(source, "cat|db|orders|email", "PII_EMAIL");

        migrate(source, "98");

        var binding = store.find(tableId, GovernanceBindingType.COLUMN_TAG, "5:emailpii_type").orElseThrow();
        assertThat(binding.bindingId()).isEqualTo(original);
        assertThat(binding.createdBy()).isEqualTo("alice");
        assertThat(binding.config()).containsAllEntriesOf(Map.of(
                "column", "email", "tagKey", "pii_type", "value", "PII_EMAIL"));
        assertThat(count(source, "SELECT count(*) FROM information_schema.tables "
                + "WHERE table_schema=current_schema() AND table_name='column_tags'")).isZero();
    }

    @Test
    void unprovenNameKeyedTagsBlockUpgradeAndRemainIntact() throws Exception {
        var source = source();
        insert(source, "cat|db|orders|email", "PII_EMAIL");

        assertThatThrownBy(() -> migrate(source, "97")).hasStackTraceContaining("ownership is unproven");

        assertThat(count(source, "SELECT count(*) FROM column_tags")).isEqualTo(1);
        assertThat(count(source, "SELECT count(*) FROM flyway_schema_history WHERE version='97' AND success"))
                .isZero();
    }

    @Test
    void replacedOrConflictingBindingsCannotClaimTheHistoricalTag() throws Exception {
        for (boolean dropped : new boolean[]{false, true}) {
            var source = source();
            var tableId = historicalTable(source, dropped);
            historicalBinding(source, tableId, dropped ? "PII_EMAIL" : "PII_PHONE");
            insert(source, "cat|db|orders|email", "PII_EMAIL");
            assertThatThrownBy(() -> migrate(source, "97")).hasStackTraceContaining("ownership is unproven");
            assertThat(count(source, "SELECT count(*) FROM column_tags")).isEqualTo(1);
        }
    }

    @Test
    void ambiguousFramingOrAnOrphanBindingBlocksMigration() throws Exception {
        var source = source();
        var tableId = historicalTable(source, false);
        historicalBinding(source, tableId, "PII_EMAIL");
        assertThatThrownBy(() -> migrate(source, "97")).hasStackTraceContaining("ownership is unproven");
        // A single old reference can represent two different column/tag pairs.
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.execute("UPDATE table_control_governance_binding SET binding_ref='a:b:c'");
            statement.execute("INSERT INTO column_tags VALUES ('cat|db|orders|a:b','c','PII_EMAIL'), "
                    + "('cat|db|orders|a','b:c','PII_EMAIL')");
        }
        assertThatThrownBy(() -> migrate(source, "97")).hasStackTraceContaining("ownership is unproven or ambiguous");
        assertThat(count(source, "SELECT count(*) FROM column_tags")).isEqualTo(2);
    }

    private static DataSource source() throws Exception {
        var source = SharedPostgresContainer.isolatedDataSource("stable_tag_upgrade");
        migrate(source, "96");
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.execute("CREATE TABLE column_tags (store_key VARCHAR(1024), tag_key VARCHAR(255), "
                    + "tag_value TEXT, PRIMARY KEY(store_key,tag_key))");
        }
        return source;
    }

    private static java.util.UUID historicalTable(DataSource source, boolean dropped) throws Exception {
        var id = java.util.UUID.randomUUID();
        try (var connection = source.getConnection(); var insert = connection.prepareStatement(
                "INSERT INTO table_control_registry (table_id,catalog_name,schema_name,table_name,format,"
                        + "ownership_mode,state,provenance,metadata_revision,created_at,updated_at) "
                        + "VALUES (?,'cat','db','orders','TEST','MANAGED',?,'CREATED',1,NOW(),NOW())")) {
            insert.setObject(1, id);
            insert.setString(2, dropped ? "DROPPED" : "ACTIVE");
            insert.executeUpdate();
        }
        return id;
    }

    private static java.util.UUID historicalBinding(DataSource source, java.util.UUID tableId, String value) {
        var id = java.util.UUID.randomUUID();
        new PostgresTablePolicyRepository(source).save(new org.pangrova.table.api.policy.GovernanceBinding(
                id, tableId, GovernanceBindingType.COLUMN_TAG, "email:pii_type", Map.of("value", value), "alice",
                java.time.Instant.now(), java.time.Instant.now()));
        return id;
    }

    private static void migrate(DataSource source, String version) {
        Flyway.configure().dataSource(source).locations("classpath:db/migration").target(version).load().migrate();
    }

    private static void insert(DataSource source, String key, String value) throws Exception {
        try (var connection = source.getConnection(); var insert = connection.prepareStatement(
                "INSERT INTO column_tags VALUES (?, 'pii_type', ?)")) {
            insert.setString(1, key);
            insert.setString(2, value);
            insert.executeUpdate();
        }
    }

    private static long count(DataSource source, String sql) throws Exception {
        try (var connection = source.getConnection(); var statement = connection.createStatement();
             var row = statement.executeQuery(sql)) {
            assertThat(row.next()).isTrue();
            return row.getLong(1);
        }
    }
}
