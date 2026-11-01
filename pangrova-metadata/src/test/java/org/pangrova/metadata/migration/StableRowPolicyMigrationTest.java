package org.pangrova.metadata.migration;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.spi.security.rowfilter.RowPolicy;
import org.pangrova.persistence.jdbc.security.rowfilter.JdbcRowPolicyStore;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Upgrade preserves historical policy facts without claiming name-only ownership. */
class StableRowPolicyMigrationTest {
    @Test
    void upgradePreservesUnboundPoliciesAndReplacesTheHistoricalNameConstraint() throws Exception {
        var source = SharedPostgresContainer.isolatedDataSource("row_policy_upgrade");
        Flyway.configure().dataSource(source).locations("classpath:db/migration").target("99").load().migrate();
        new JdbcRowPolicyStore(source).initSchema();
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.execute("DROP INDEX row_policy_scoped_identity");
            statement.execute("DROP INDEX row_policy_unbound_name");
            statement.execute("ALTER TABLE row_policy DROP COLUMN table_id");
            statement.execute("ALTER TABLE row_policy ADD CONSTRAINT legacy_policy_name "
                    + "UNIQUE(catalog_name,schema_name,table_name,policy_name)");
            statement.execute("INSERT INTO row_policy (policy_name,catalog_name,schema_name,table_name,"
                    + "filter_expression,apply_to_roles,enabled,priority) "
                    + "VALUES ('rows','cat','db','orders','id > 0','reader',true,4)");
        }
        long revisionBefore;
        try (var connection = source.getConnection(); var statement = connection.createStatement();
                var row = statement.executeQuery("SELECT revision FROM pangrova_authorization_revision")) {
            assertThat(row.next()).isTrue();
            revisionBefore = row.getLong(1);
        }
        Flyway.configure().dataSource(source).locations("classpath:db/migration").load().migrate();
        var store = new JdbcRowPolicyStore(source);
        assertThat(store.getPolicies("cat", "db", "orders")).singleElement().satisfies(policy -> {
            assertThat(policy.tableId()).isNull();
            assertThat(policy.filterExpression()).isEqualTo("id > 0");
            assertThat(policy.priority()).isEqualTo(4);
        });
        try (var connection = source.getConnection(); var statement = connection.createStatement();
                var row = statement.executeQuery("SELECT revision FROM pangrova_authorization_revision")) {
            assertThat(row.next()).isTrue();
            assertThat(row.getLong(1)).isEqualTo(revisionBefore);
        }
        UUID first = UUID.randomUUID();
        UUID second = UUID.randomUUID();
        for (UUID tableId : List.of(first, second)) {
            store.savePolicy(new RowPolicy(0, "rows", "cat", "db", "orders", "id > 10",
                    List.of("reader"), true, 4, tableId));
            assertThat(store.getPolicies(tableId)).hasSize(1);
        }
        assertThat(store.getPolicies("cat", "db", "orders")).hasSize(3);
    }
}
