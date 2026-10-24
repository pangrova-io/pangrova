package org.pangrova.metadata.governance;

import org.pangrova.governance.quality.rule.QualityExecutionIdentity;
import org.pangrova.governance.quality.rule.DataQualityRule;
import org.pangrova.governance.quality.rule.DefaultQualityRuleExecutor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.common.exception.PangrovaErrorCode;
import org.pangrova.common.exception.PangrovaException;
import org.pangrova.governance.quality.QualityCheckResult;
import org.pangrova.governance.quality.alert.Severity;
import org.pangrova.governance.quality.gate.RuleBasedWriteQualityGate;
import org.pangrova.testing.ContainerTestBase;
import org.postgresql.ds.PGSimpleDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class JdbcQualityRuleStoreIntegrationTest extends ContainerTestBase {

    private String schema;
    private PGSimpleDataSource dataSource;

    private static JdbcQualityRuleStore initializedStore(PGSimpleDataSource source) {
        var store = new JdbcQualityRuleStore(source);
        store.initSchema();
        return store;
    }

    @BeforeEach
    void createIsolatedSchema() throws SQLException {
        schema = "quality_" + UUID.randomUUID().toString().replace("-", "");
        try (Connection connection = adminDataSource().getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + schema);
        }
        dataSource = dataSource(schema);
    }

    @AfterEach
    void dropIsolatedSchema() throws SQLException {
        if (schema != null) {
            try (Connection connection = adminDataSource().getConnection();
                 Statement statement = connection.createStatement()) {
                statement.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
            }
        }
    }

    @Test
    void persistsRulesAndResultsAcrossStoreRestart() throws SQLException {
        JdbcQualityRuleStore first = initializedStore(dataSource);
        Instant createdAt = Instant.parse("2026-01-01T00:00:00Z");
        Instant updatedAt = Instant.parse("2026-01-02T00:00:00Z");
        DataQualityRule original = rule(101, "initial", true, createdAt, updatedAt);
        first.saveRule(original);

        assertThat(first.getRule(101)).contains(original);
        assertThat(first.getRule(999)).isEmpty();
        assertThat(first.getRulesForTable("catalog", "schema", "orders")).containsExactly(original);
        assertThat(first.getAllEnabledRules()).containsExactly(original);

        DataQualityRule replacement = rule(101, "updated", false, createdAt.plusSeconds(60),
                updatedAt.plusSeconds(60));
        first.saveRule(replacement);

        JdbcQualityRuleStore restarted = initializedStore(dataSource);
        DataQualityRule persisted = restarted.getRule(101).orElseThrow();
        assertThat(persisted.ruleName()).isEqualTo("updated");
        assertThat(persisted.enabled()).isFalse();
        assertThat(persisted.createdAt()).isEqualTo(createdAt);
        assertThat(persisted.updatedAt()).isEqualTo(updatedAt.plusSeconds(60));
        assertThat(restarted.getAllEnabledRules()).isEmpty();
        assertThat(restarted.getRulesForTable("catalog", "schema", "orders")).containsExactly(persisted);

        QualityCheckResult result = result(501);
        restarted.saveResult(result);
        assertSqlFailure("23505", () -> restarted.saveResult(new QualityCheckResult(
                501, 101, "duplicate", "orders", Severity.ERROR,
                Instant.parse("2026-01-04T00:00:00Z"), true, 1, 0, 1, "must not replace")));
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery("SELECT * FROM quality_check_results WHERE result_id = 501")) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getLong("rule_id")).isEqualTo(101);
            assertThat(rows.getString("rule_name")).isEqualTo("updated");
            assertThat(rows.getString("severity")).isEqualTo("ERROR");
            assertThat(rows.getBoolean("passed")).isFalse();
            assertThat(rows.getLong("failed_rows")).isEqualTo(2);
            assertThat(rows.getString("error_message")).isEqualTo("bad rows");
            assertThat(rows.next()).isFalse();
        }
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery("SELECT COUNT(*) FROM quality_rules")) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getLong(1)).isEqualTo(1);
        }
    }

    @Test
    void initializationFailureIsStructuredAndPreservesSqlCause() {
        PGSimpleDataSource missingSchema = dataSource("schema_that_does_not_exist");
        assertSqlFailure("3F000", () -> initializedStore(missingSchema));
    }

    @Test
    void readAndWriteFailuresAreNeverReportedAsEmptyOrSuccess() throws SQLException {
        JdbcQualityRuleStore store = initializedStore(dataSource);
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE quality_rules");
            statement.execute("DROP TABLE quality_check_results");
        }

        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        assertSqlFailure("42P01", () -> store.saveRule(rule(101, "initial", true, now, now)));
        assertSqlFailure("42P01", () -> store.getRule(101));
        assertSqlFailure("42P01", () -> store.getRulesForTable("catalog", "schema", "orders"));
        assertSqlFailure("42P01", store::getAllEnabledRules);
        assertSqlFailure("42P01", () -> store.saveResult(result(501)));
    }

    @Test
    void writeGateFailsClosedWhenRuleReadFails() throws SQLException {
        JdbcQualityRuleStore store = initializedStore(dataSource);
        RuleBasedWriteQualityGate gate = new RuleBasedWriteQualityGate(store, new DefaultQualityRuleExecutor());

        var noRules = gate.validate("catalog", "schema", "orders");
        assertThat(noRules.allowed()).isTrue();
        assertThat(noRules.ruleResults()).isEmpty();

        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE quality_rules");
        }

        assertSqlFailure("42P01", () -> gate.validate("catalog", "schema", "orders"));
    }

    private static void assertSqlFailure(String sqlState,
                                         org.assertj.core.api.ThrowableAssert.ThrowingCallable operation) {
        assertThatThrownBy(operation)
                .isInstanceOf(PangrovaException.class)
                .satisfies(throwable -> {
                    PangrovaException failure = (PangrovaException) throwable;
                    assertThat(failure.getErrorCode()).isEqualTo(PangrovaErrorCode.METADATA_METASTORE_ERROR);
                    assertThat(failure.getCause()).isInstanceOf(SQLException.class);
                    assertThat(((SQLException) failure.getCause()).getSQLState()).isEqualTo(sqlState);
                });
    }

    private static DataQualityRule rule(long id, String name, boolean enabled,
                                        Instant createdAt, Instant updatedAt) {
        return new DataQualityRule(id, name, "catalog", "schema", "orders", "amount > 0",
                Severity.ERROR, "bad rows", enabled, null, createdAt, updatedAt,
                new QualityExecutionIdentity("quality-test-user", "quality-test-tenant"));
    }

    private static QualityCheckResult result(long id) {
        return new QualityCheckResult(id, 101, "updated", "orders", Severity.ERROR,
                Instant.parse("2026-01-03T00:00:00Z"), false, 10, 2, 17, "bad rows");
    }

    private static PGSimpleDataSource adminDataSource() {
        return dataSource(null);
    }

    private static PGSimpleDataSource dataSource(String currentSchema) {
        PGSimpleDataSource source = new PGSimpleDataSource();
        source.setURL(org.pangrova.metadata.testinfra.SharedPostgresContainer.jdbcUrl());
        source.setUser("pangrova");
        source.setPassword("pangrova");
        if (currentSchema != null) {
            source.setCurrentSchema(currentSchema);
        }
        return source;
    }
}
