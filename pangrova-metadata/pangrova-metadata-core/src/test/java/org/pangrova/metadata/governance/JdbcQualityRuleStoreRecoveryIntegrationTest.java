package org.pangrova.metadata.governance;

import org.pangrova.persistence.governance.postgresql.JdbcQualityRuleStore;
import org.pangrova.governance.api.quality.QualityExecutionIdentity;
import org.pangrova.governance.api.quality.DataQualityRule;
import org.pangrova.governance.api.quality.QualityRuleStoreException;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.pangrova.governance.api.quality.QualityCheckResult;
import org.pangrova.governance.api.quality.Severity;
import org.postgresql.ds.PGSimpleDataSource;

import javax.sql.DataSource;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises actual PostgreSQL persistence, failure visibility and deployment pool ownership. */
class JdbcQualityRuleStoreRecoveryIntegrationTest {
    private static DataSource dataSource;

    @BeforeAll
    static void startDatabase() throws Exception {
        dataSource = SharedPostgresContainer.migratedDataSource("quality_recovery");
    }

    private static JdbcQualityRuleStore initializedStore(javax.sql.DataSource database) {
        return new JdbcQualityRuleStore(database);
    }

    @Test
    void upsertPreservesCreationAndFreshStoreReadsActualCommittedState() throws Exception {
        var store = initializedStore(dataSource);
        Instant created = Instant.parse("2026-01-01T00:00:00Z");
        Instant updated = created.plusSeconds(1);
        store.saveRule(new DataQualityRule(1, "initial", "c", "s", "t", "x IS NOT NULL",
                Severity.WARNING, "initial", true, null, created, created,
                new QualityExecutionIdentity("quality-test-user", "quality-test-tenant"), null));
        store.saveRule(new DataQualityRule(1, "revised", "c", "s", "t", "x > 0",
                Severity.ERROR, "revised", false, null, updated, updated,
                new QualityExecutionIdentity("quality-test-user", "quality-test-tenant"), null));
        var fresh = initializedStore(dataSource);
        var loaded = fresh.getRule(1).orElseThrow();
        assertThat(loaded.ruleName()).isEqualTo("revised");
        assertThat(loaded.createdAt()).isEqualTo(created);
        assertThat(loaded.updatedAt()).isEqualTo(updated);
        assertThat(fresh.getRulesForTable("c", "s", "t")).containsExactly(loaded);
        assertThat(fresh.getAllEnabledRules()).doesNotContain(loaded);
        store.saveResult(new QualityCheckResult(1, 1, "revised", "t", Severity.ERROR,
                updated, false, 10, 2, 1, "actual failure", null));
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
             var rows = statement.executeQuery("SELECT failed_rows FROM quality_check_results WHERE result_id=1")) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getLong(1)).isEqualTo(2);
        }
    }

    @Test
    void databaseReadAndWriteFailuresCannotBecomeEmptyRulesOrLostResults() throws Exception {
        DataSource failing = SharedPostgresContainer.migratedDataSource("quality_failure");
        var store = initializedStore(failing);
        try (var connection = failing.getConnection(); var statement = connection.createStatement()) {
            statement.execute("DROP TABLE quality_rules");
            statement.execute("DROP TABLE quality_check_results");
        }
        assertThatThrownBy(() -> store.getRule(1)).isInstanceOf(QualityRuleStoreException.class);
        assertThatThrownBy(() -> store.getRulesForTable("c", "s", "t"))
                .isInstanceOf(QualityRuleStoreException.class);
        assertThatThrownBy(store::getAllEnabledRules).isInstanceOf(QualityRuleStoreException.class);
        assertThatThrownBy(() -> store.saveResult(new QualityCheckResult(2, 1, "r", "t",
                Severity.ERROR, Instant.now(), false, 1, 1, 1, "failure", null)))
                .isInstanceOf(QualityRuleStoreException.class);
    }

    @Test
    void missingCanonicalSchemaFailsClosedWithoutPublishingStore() throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String role = "quality_readonly_" + suffix;
        String schema = "denied_quality_" + suffix;
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute("CREATE ROLE " + role + " LOGIN PASSWORD 'quality_readonly'");
            statement.execute("CREATE SCHEMA " + schema);
            statement.execute("GRANT USAGE ON SCHEMA " + schema + " TO " + role);
        }
        var restricted = new PGSimpleDataSource();
        try (var connection = dataSource.getConnection()) {
            restricted.setURL(connection.getMetaData().getURL());
        }
        restricted.setUser(role);
        restricted.setPassword("quality_readonly");
        restricted.setCurrentSchema(schema);
        var store = initializedStore(restricted);
        assertThatThrownBy(store::getAllEnabledRules)
                .isInstanceOf(QualityRuleStoreException.class)
                .hasRootCauseInstanceOf(java.sql.SQLException.class);
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute("DROP SCHEMA " + schema + " CASCADE");
            statement.execute("DROP ROLE " + role);
        }
    }
}
