package org.pangrova.metadata.migration;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.pangrova.governance.store.jdbc.JdbcQualityRuleStore;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;
import org.pangrova.persistence.jdbc.security.policy.JdbcGrantStore;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;

/** Historical name-only rules and audit results are preserved without invented ownership. */
class StableQualityRuleMigrationTest {
    @Test
    void populatedUpgradeKeepsLegacyFactsAndDoesNotAdvanceUnchangedAuthorization() throws Exception {
        var source = SharedPostgresContainer.isolatedDataSource("quality_table_upgrade");
        Flyway.configure().dataSource(source).locations("classpath:db/migration").target("100").load().migrate();
        new JdbcGrantStore(source).initSchema();
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.execute("INSERT INTO quality_rules (rule_id,rule_name,catalog_name,schema_name,table_name,"
                    + "check_expression,severity,enabled,created_at,updated_at,execution_principal,execution_tenant) "
                    + "VALUES (1,'positive','cat','db','orders','id > 0','ERROR',true,now(),now(),'alice','tenant-a')");
            statement.execute("INSERT INTO quality_check_results (rule_id,rule_name,table_name,severity,check_time,"
                    + "passed,total_rows,failed_rows,execution_time_ms,error_message) "
                    + "VALUES (1,'positive','orders','ERROR',now(),false,2,1,10,'original failure')");
        }
        long revision = revision(source);
        Flyway.configure().dataSource(source).locations("classpath:db/migration").load().migrate();
        assertThat(new JdbcQualityRuleStore(source).getRule(1)).hasValueSatisfying(rule -> {
            assertThat(rule.tableId()).isNull();
            assertThat(rule.enabled()).isTrue();
            assertThat(rule.checkExpression()).isEqualTo("id > 0");
            assertThat(rule.executionIdentity().principal()).isEqualTo("alice");
        });
        try (var connection = source.getConnection(); var query = connection.prepareStatement(
                "SELECT table_id,total_rows,failed_rows,error_message FROM quality_check_results WHERE rule_id=1");
                var row = query.executeQuery()) {
            assertThat(row.next()).isTrue();
            assertThat(row.getObject(1)).isNull();
            assertThat(row.getLong(2)).isEqualTo(2);
            assertThat(row.getLong(3)).isEqualTo(1);
            assertThat(row.getString(4)).isEqualTo("original failure");
            assertThat(row.next()).isFalse();
        }
        assertThat(revision(source)).isEqualTo(revision);
    }

    private static long revision(DataSource source) throws Exception {
        try (var connection = source.getConnection(); var query = connection.createStatement();
                var row = query.executeQuery("SELECT revision FROM pangrova_authorization_revision")) {
            assertThat(row.next()).isTrue();
            return row.getLong(1);
        }
    }
}
