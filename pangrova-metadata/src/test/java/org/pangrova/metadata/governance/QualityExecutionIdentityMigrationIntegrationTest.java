package org.pangrova.metadata.governance;

import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.Test;
import org.pangrova.governance.quality.alert.Severity;
import org.pangrova.governance.quality.rule.DataQualityRule;
import org.pangrova.governance.quality.rule.QualityExecutionIdentity;
import org.pangrova.metadata.testinfra.SharedPostgresContainer;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Real upgrade from the last schema without quality execution ownership. */
class QualityExecutionIdentityMigrationIntegrationTest {
    @Test
    void unownedLegacyRuleRemainsVisibleButCannotRunUntilExplicitlyAssigned() throws Exception {
        var source = SharedPostgresContainer.isolatedDataSource("quality_identity_upgrade");
        Flyway.configure().dataSource(source).locations("classpath:db/migration")
                .target("73").load().migrate();
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            statement.execute("INSERT INTO quality_rules (rule_id,rule_name,catalog_name,schema_name,table_name,"
                    + "check_expression,severity,enabled,schedule,created_at,updated_at) VALUES "
                    + "(1,'legacy','catalog','schema','orders','amount > 0','ERROR',true,'0 * * * *',now(),now())");
        }
        Flyway.configure().dataSource(source).locations("classpath:db/migration").load().migrate();
        var store = new JdbcQualityRuleStore(source);
        var legacy = store.getRule(1).orElseThrow();
        assertThat(legacy.enabled()).isFalse();
        assertThat(legacy.executionIdentity()).isNull();
        assertThat(legacy.checkExpression()).isEqualTo("amount > 0");
        assertThat(store.getAllEnabledRules()).isEmpty();
        try (var connection = source.getConnection(); var statement = connection.createStatement()) {
            assertThatThrownBy(() -> statement.execute("UPDATE quality_rules SET enabled=true WHERE rule_id=1"))
                    .isInstanceOf(java.sql.SQLException.class).hasMessageContaining("quality_rule_execution_identity");
            assertThatThrownBy(() -> statement.execute("UPDATE quality_rules SET execution_principal='owner' WHERE rule_id=1"))
                    .isInstanceOf(java.sql.SQLException.class).hasMessageContaining("quality_rule_execution_identity");
        }
        var identity = new QualityExecutionIdentity("quality-owner", "tenant-a");
        store.saveRule(new DataQualityRule(legacy.ruleId(), legacy.ruleName(), legacy.catalogName(),
                legacy.schemaName(), legacy.tableName(), legacy.checkExpression(), legacy.severity(),
                legacy.message(), true, legacy.schedule(), legacy.createdAt(), Instant.now(), identity, null));
        var restarted = new JdbcQualityRuleStore(source);
        assertThat(restarted.getAllEnabledRules()).singleElement()
                .satisfies(rule -> assertThat(rule.executionIdentity()).isEqualTo(identity));
    }

    @Test
    void definitionUpdatePersistsReplacementIdentityAcrossStoreRestart() throws Exception {
        var source = SharedPostgresContainer.isolatedDataSource("quality_identity_store");
        var store = new JdbcQualityRuleStore(source);
        store.initSchema();
        Instant created = Instant.parse("2026-01-01T00:00:00Z");
        store.saveRule(rule(created, new QualityExecutionIdentity("owner-a", "tenant-a")));
        var replacement = new QualityExecutionIdentity("owner-b", "tenant-b");
        store.saveRule(rule(created, replacement));
        assertThat(new JdbcQualityRuleStore(source).getRule(1).orElseThrow().executionIdentity())
                .isEqualTo(replacement);
    }

    private static DataQualityRule rule(Instant created, QualityExecutionIdentity identity) {
        return new DataQualityRule(1, "rule", "catalog", "schema", "orders", "amount > 0",
                Severity.ERROR, "positive", true, "0 * * * *", created, created, identity, null);
    }
}
