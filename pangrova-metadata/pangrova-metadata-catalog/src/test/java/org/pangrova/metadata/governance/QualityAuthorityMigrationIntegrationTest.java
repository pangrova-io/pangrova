package org.pangrova.metadata.governance;

import org.pangrova.persistence.governance.postgresql.JdbcQualityRuleStore;
import org.flywaydb.core.Flyway;
import org.pangrova.testing.testkit.postgres.SharedPostgresContainer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Runs the production migration against genuine legacy and canonical PostgreSQL facts. */
class QualityAuthorityMigrationIntegrationTest {
    private DataSource dataSource;

    @BeforeEach
    void createActualPreMigrationDeployment() throws Exception {
        dataSource = SharedPostgresContainer.isolatedDataSource("quality_authority");
        Flyway.configure().dataSource(dataSource).locations("classpath:db/migration")
                .target("52").load().migrate();
        try (var source = getClass().getResourceAsStream("/quality/legacy-governance-quality-schema.sql")) {
            assertThat(source).isNotNull();
            sql(new String(source.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    @Test
    void legacyRulesAndResultsConvergeWithoutChangingCommittedValues() throws Exception {
        insertLegacyRule();
        insertLegacyResult();
        migrate();
        var rule = new JdbcQualityRuleStore(dataSource).getRule(1).orElseThrow();
        assertThat(rule.ruleName()).isEqualTo("legacy_rule");
        assertThat(rule.checkExpression()).isEqualTo("amount > 0");
        assertThat(rule.message()).isNull();
        assertThat(rule.schedule()).isEqualTo("0 0 * * * ?");
        assertThat(rule.createdAt().toString()).isEqualTo("2026-01-01T00:00:00Z");
        assertThat(rule.updatedAt().toString()).isEqualTo("2026-01-02T00:00:00Z");
        assertThat(count("quality_rules")).isEqualTo(1);
        assertThat(count("quality_check_results")).isEqualTo(1);
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT total_rows,failed_rows,error_message "
                        + "FROM quality_check_results WHERE result_id=11")) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getLong(1)).isEqualTo(10);
            assertThat(rows.getLong(2)).isEqualTo(2);
            assertThat(rows.getString(3)).isEqualTo("actual failure");
            assertThat(rows.next()).isFalse();
        }
        assertRetiredTablesAbsent();
        assertThat(migrate()).isZero();
    }

    @Test
    void exactPreexistingCanonicalFactsAreReconciledOnce() throws Exception {
        insertLegacyRule();
        insertLegacyResult();
        sql("INSERT INTO quality_rules SELECT * FROM cdc_quality_rules");
        sql("INSERT INTO quality_check_results SELECT * FROM cdc_quality_results");
        migrate();
        assertThat(count("quality_rules")).isEqualTo(1);
        assertThat(count("quality_check_results")).isEqualTo(1);
        assertRetiredTablesAbsent();
    }

    @Test
    void conflictingRuleIdentityRejectsWholeMigrationAndPreservesBothAuthorities() throws Exception {
        insertLegacyRule();
        insertLegacyResult();
        sql("INSERT INTO quality_rules SELECT * FROM cdc_quality_rules");
        sql("UPDATE quality_rules SET check_expression='amount >= 0' WHERE rule_id=1");
        assertThatThrownBy(this::migrate).hasStackTraceContaining("Quality rule identities conflict");
        assertThat(count("cdc_quality_rules")).isEqualTo(1);
        assertThat(count("cdc_quality_results")).isEqualTo(1);
        assertThat(count("quality_rules")).isEqualTo(1);
        assertThat(count("quality_check_results")).isZero();
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT check_expression FROM quality_rules WHERE rule_id=1")) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getString(1)).isEqualTo("amount >= 0");
        }
    }

    @Test
    void conflictingResultIdentityRollsBackEarlierRuleCopyAndRetainsLegacyRows() throws Exception {
        insertLegacyRule();
        insertLegacyResult();
        sql("INSERT INTO quality_check_results SELECT * FROM cdc_quality_results");
        sql("UPDATE quality_check_results SET failed_rows=3 WHERE result_id=11");
        assertThatThrownBy(this::migrate).hasStackTraceContaining("Quality result identities conflict");
        assertThat(count("quality_rules")).isZero();
        assertThat(count("quality_check_results")).isEqualTo(1);
        assertThat(count("cdc_quality_rules")).isEqualTo(1);
        assertThat(count("cdc_quality_results")).isEqualTo(1);
    }

    @Test
    void invalidLegacyNullableResultCannotBeInventedOrSilentlyDiscarded() throws Exception {
        insertLegacyRule();
        insertLegacyResult();
        sql("UPDATE cdc_quality_results SET rule_name=NULL WHERE result_id=11");
        assertThatThrownBy(this::migrate).hasRootCauseInstanceOf(java.sql.SQLException.class);
        assertThat(count("quality_rules")).isZero();
        assertThat(count("quality_check_results")).isZero();
        assertThat(count("cdc_quality_rules")).isEqualTo(1);
        assertThat(count("cdc_quality_results")).isEqualTo(1);
    }

    @Test
    void deploymentWithoutLegacyTablesRemainsOnTheExistingCanonicalStore() throws Exception {
        sql("DROP TABLE cdc_quality_results");
        sql("DROP TABLE cdc_quality_rules");
        migrate();
        assertThat(count("quality_rules")).isZero();
        assertThat(count("quality_check_results")).isZero();
        assertRetiredTablesAbsent();
    }

    private int migrate() {
        return Flyway.configure().dataSource(dataSource).locations("classpath:db/migration")
                .load().migrate().migrationsExecuted;
    }

    private void insertLegacyRule() throws Exception {
        // Use the retired producer's actual Timestamp.from(Instant) bindings rather
        // than inventing a timezone interpretation for a legacy TIMESTAMP literal.
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                "INSERT INTO cdc_quality_rules VALUES "
                        + "(1,'legacy_rule','catalog','schema','orders','amount > 0','ERROR',NULL,true,"
                        + "'0 0 * * * ?',?,?)")) {
            statement.setTimestamp(1, java.sql.Timestamp.from(java.time.Instant.parse("2026-01-01T00:00:00Z")));
            statement.setTimestamp(2, java.sql.Timestamp.from(java.time.Instant.parse("2026-01-02T00:00:00Z")));
            statement.executeUpdate();
        }
    }

    private void insertLegacyResult() throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.prepareStatement(
                "INSERT INTO cdc_quality_results VALUES "
                        + "(11,1,'legacy_rule','orders','ERROR',?,false,10,2,4,'actual failure')")) {
            statement.setTimestamp(1, java.sql.Timestamp.from(java.time.Instant.parse("2026-01-02T00:00:00Z")));
            statement.executeUpdate();
        }
    }

    private void assertRetiredTablesAbsent() throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT to_regclass('cdc_quality_rules'),"
                        + "to_regclass('cdc_quality_results')")) {
            assertThat(rows.next()).isTrue();
            assertThat(rows.getObject(1)).isNull();
            assertThat(rows.getObject(2)).isNull();
        }
    }

    private long count(String table) throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement();
                var rows = statement.executeQuery("SELECT count(*) FROM " + table)) {
            assertThat(rows.next()).isTrue();
            return rows.getLong(1);
        }
    }

    private void sql(String sql) throws Exception {
        try (var connection = dataSource.getConnection(); var statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}
