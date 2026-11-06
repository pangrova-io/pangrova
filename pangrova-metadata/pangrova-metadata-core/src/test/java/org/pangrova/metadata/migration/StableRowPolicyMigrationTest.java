package org.pangrova.metadata.migration;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.security.api.rowfilter.RowPolicy;
import org.pangrova.persistence.security.postgresql.rowfilter.JdbcRowPolicyStore;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/** Upgrade preserves historical policy facts without claiming name-only ownership. */
class StableRowPolicyMigrationTest {
    @Test
    void upgradePreservesUnboundPoliciesAndReplacesTheHistoricalNameConstraint() throws Exception {
        var source = SharedPostgresContainer.isolatedDataSource("row_policy_upgrade");
        Flyway.configure().dataSource(source).locations("classpath:db/migration").target("99").load().migrate();
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            // Explicit historical fixture; production stores never create schemas.
            statement.execute("CREATE TABLE row_policy (policy_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY, "
                    + "policy_name VARCHAR(255) NOT NULL, catalog_name VARCHAR(255) NOT NULL, "
                    + "schema_name VARCHAR(255) NOT NULL, table_name VARCHAR(255) NOT NULL, "
                    + "filter_expression TEXT NOT NULL, apply_to_roles TEXT NOT NULL, "
                    + "enabled BOOLEAN NOT NULL DEFAULT TRUE, priority INT NOT NULL DEFAULT 0, "
                    + "CONSTRAINT legacy_policy_name UNIQUE(catalog_name,schema_name,table_name,policy_name))");
            statement.execute("CREATE TRIGGER pangrova_authority_gate BEFORE INSERT OR UPDATE OR DELETE "
                    + "OR TRUNCATE ON row_policy FOR EACH STATEMENT "
                    + "EXECUTE FUNCTION pangrova_authority_mutation_gate()");
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
