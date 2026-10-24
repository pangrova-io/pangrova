package org.pangrova.metadata.governance;

import org.pangrova.governance.quality.rule.QualityExecutionIdentity;
import org.pangrova.governance.quality.rule.DataQualityRule;
import org.pangrova.metadata.exception.MetastoreException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.pangrova.governance.quality.QualityCheckResult;
import org.pangrova.governance.quality.alert.Severity;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Exercises actual PostgreSQL persistence, failure visibility and deployment pool ownership. */
class JdbcQualityRuleStoreRecoveryIntegrationTest {
    private static final GenericContainer<?> DATABASE = new GenericContainer<>("postgres:16-alpine")
            .withExposedPorts(5432).withEnv("POSTGRES_DB", "quality")
            .withEnv("POSTGRES_USER", "quality").withEnv("POSTGRES_PASSWORD", "quality")
            .waitingFor(Wait.forLogMessage(".*database system is ready to accept connections.*\\n", 2));
    private static PGSimpleDataSource dataSource;

    @BeforeAll
    static void startDatabase() {
        DATABASE.start();
        dataSource = new PGSimpleDataSource();
        dataSource.setURL("jdbc:postgresql://" + DATABASE.getHost() + ":"
                + DATABASE.getMappedPort(5432) + "/quality");
        dataSource.setUser("quality");
        dataSource.setPassword("quality");
    }

    @AfterAll
    static void closeDatabase() {
        DATABASE.close();
    }

    private static JdbcQualityRuleStore initializedStore(javax.sql.DataSource database) {
        var store = new JdbcQualityRuleStore(database);
        store.initSchema();
        return store;
    }

    @Test
    void upsertPreservesCreationAndFreshStoreReadsActualCommittedState() throws Exception {
        var store = initializedStore(dataSource);
        Instant created = Instant.parse("2026-01-01T00:00:00Z");
        Instant updated = created.plusSeconds(1);
        store.saveRule(new DataQualityRule(1, "initial", "c", "s", "t", "x IS NOT NULL",
                Severity.WARNING, "initial", true, null, created, created,
                new QualityExecutionIdentity("quality-test-user", "quality-test-tenant")));
        store.saveRule(new DataQualityRule(1, "revised", "c", "s", "t", "x > 0",
                Severity.ERROR, "revised", false, null, updated, updated,
                new QualityExecutionIdentity("quality-test-user", "quality-test-tenant")));
        var fresh = initializedStore(dataSource);
        var loaded = fresh.getRule(1).orElseThrow();
        assertThat(loaded.ruleName()).isEqualTo("revised");
        assertThat(loaded.createdAt()).isEqualTo(created);
        assertThat(loaded.updatedAt()).isEqualTo(updated);
        assertThat(fresh.getRulesForTable("c", "s", "t")).containsExactly(loaded);
        assertThat(fresh.getAllEnabledRules()).doesNotContain(loaded);
        store.saveResult(new QualityCheckResult(1, 1, "revised", "t", Severity.ERROR,
                updated, false, 10, 2, 1, "actual failure"));
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
             var rows = statement.executeQuery("SELECT failed_rows FROM quality_check_results WHERE result_id=1")) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getLong(1)).isEqualTo(2);
        }
    }

    @Test
    void databaseReadAndWriteFailuresCannotBecomeEmptyRulesOrLostResults() throws Exception {
        var store = initializedStore(dataSource);
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute("DROP TABLE quality_rules");
            statement.execute("DROP TABLE quality_check_results");
        }
        try {
            assertThatThrownBy(() -> store.getRule(1)).isInstanceOf(MetastoreException.class);
            assertThatThrownBy(() -> store.getRulesForTable("c", "s", "t"))
                    .isInstanceOf(MetastoreException.class);
            assertThatThrownBy(store::getAllEnabledRules).isInstanceOf(MetastoreException.class);
            assertThatThrownBy(() -> store.saveResult(new QualityCheckResult(2, 1, "r", "t",
                    Severity.ERROR, Instant.now(), false, 1, 1, 1, "failure")))
                    .isInstanceOf(MetastoreException.class);
        } finally {
            initializedStore(dataSource);
        }
    }

    @Test
    void deniedSchemaInitializationFailsBeforePublishingStore() throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute("CREATE ROLE quality_readonly LOGIN PASSWORD 'quality_readonly'");
            statement.execute("CREATE SCHEMA denied_quality");
            statement.execute("GRANT USAGE ON SCHEMA denied_quality TO quality_readonly");
        }
        var restricted = new PGSimpleDataSource();
        restricted.setURL(dataSource.getURL());
        restricted.setUser("quality_readonly");
        restricted.setPassword("quality_readonly");
        restricted.setCurrentSchema("denied_quality");
        assertThatThrownBy(() -> initializedStore(restricted))
                .isInstanceOf(MetastoreException.class)
                .hasMessageContaining("initialize quality rule schema")
                .hasRootCauseInstanceOf(java.sql.SQLException.class);
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
             var rows = statement.executeQuery("SELECT 1")) {
            assertThat(rows.next()).isTrue();
        }
    }
}
