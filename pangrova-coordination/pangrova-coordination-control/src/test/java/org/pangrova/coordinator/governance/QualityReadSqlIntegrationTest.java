package org.pangrova.coordinator.governance;

import org.pangrova.governance.api.enums.QualityQueryType;
import org.pangrova.governance.api.quality.DataQualityRule;
import org.pangrova.governance.api.exception.GovernanceQueryException;
import org.pangrova.governance.api.quality.GovernanceQueryRunner;
import org.pangrova.governance.api.quality.QualityExecutionIdentity;
import org.pangrova.governance.api.quality.QualityQueryResult;
import org.pangrova.governance.api.quality.QualityReadRequest;
import org.pangrova.governance.api.enums.Severity;
import org.pangrova.governance.quality.executor.sql.SqlQualityRuleExecutor;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.testing.testkit.ContainerTestBase;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Actual PostgreSQL SQL oracle for the rule predicate and result contract.
 * This test does not substitute PostgreSQL for the production Pangrova query pipeline.
 */
class QualityReadSqlIntegrationTest extends ContainerTestBase {
    private static final java.util.UUID TABLE_ID = java.util.UUID.fromString("00000000-0000-0000-0000-000000000001");

    private final PostgresSqlOracle runner = new PostgresSqlOracle();
    private final SqlQualityRuleExecutor executor = new SqlQualityRuleExecutor(runner);

    @BeforeAll
    static void startDatabase() throws SQLException {
        executeSql("CREATE SCHEMA schema");
    }

    @BeforeEach
    void createActualTable() throws SQLException {
        executeSql("DROP TABLE IF EXISTS schema.test_table");
        executeSql("CREATE TABLE schema.test_table "
                + "(column_x INTEGER, col INTEGER, amount INTEGER, status TEXT, label TEXT)");
    }

    @Test
    void ruleColumnXGreaterThanZero_violatingRow_reportsFailure() throws SQLException {
        executeSql("INSERT INTO schema.test_table(column_x,label) "
                + "VALUES (-1,'row_a'),(0,'row_b'),(1,'row_c'),(2,'row_d'),(3,'row_e')");
        var result = executor.execute(rule(1, "positive_check", "column_x > 0"));
        assertThat(result.passed()).isFalse();
        assertThat(result.totalRows()).isEqualTo(5);
        assertThat(result.failedRows()).isEqualTo(2);
        assertThat(result.errorMessage()).contains("column_x > 0");
        assertThat(result.errorMessage()).contains("2 of 5");
        assertThat(result.ruleName()).isEqualTo("positive_check");
        assertThat(runner.results.get(1).failingSamples()).hasSize(2);
        assertThat(runner.results.get(1).failingSamples()).extracting(row -> row.get(0))
                .containsExactlyInAnyOrder("-1", "0");
    }

    @Test
    void ruleWithAllRowsPassing_reportsSuccess() throws SQLException {
        executeSql("INSERT INTO schema.test_table(col) SELECT generate_series(1,10)");
        var result = executor.execute(rule(2, "not_null_check", "col IS NOT NULL"));
        assertThat(result.passed()).isTrue();
        assertThat(result.totalRows()).isEqualTo(10);
        assertThat(result.failedRows()).isEqualTo(0);
        assertThat(result.errorMessage()).isNull();
    }

    @Test
    void blankExpression_throwsGovernanceQueryException() {
        assertThatThrownBy(() -> executor.execute(rule(3, "blank_rule", "   ")))
                .isInstanceOf(GovernanceQueryException.class)
                .hasMessageContaining("blank check expression");
        assertThat(runner.queries).isEmpty();
    }

    @Test
    void queryRunnerFailure_returnsFailedResult() throws SQLException {
        executeSql("DROP TABLE schema.test_table");
        var result = executor.execute(rule(4, "failing_query", "col > 0"));
        assertThat(result.passed()).isFalse();
        assertThat((Throwable) runner.lastFailure).isNotNull();
        assertThat(runner.lastFailure.getSQLState()).isEqualTo("42P01");
        assertThat(result.errorMessage()).isEqualTo(runner.lastFailure.getMessage());
        assertThat(result.totalRows()).isZero();
        assertThat(result.failedRows()).isZero();
        assertThat(runner.queries).hasSize(1);
    }

    @Test
    void buildCountSql_constructsCorrectQuery() throws SQLException {
        executeSql("INSERT INTO schema.test_table(amount) VALUES (101),(100),(NULL)");
        var request = request("amount > 100", QualityQueryType.COUNTS);
        String sql = QualityReadSql.render(request);
        assertThat(sql).contains("COUNT(*)");
        assertThat(sql).contains("IS NOT TRUE", "failed_rows");
        assertThat(sql).contains("\"catalog\".\"schema\".\"test_table\"");
        var result = runner.execute(request,
                new QualityExecutionIdentity("quality-test-user", "quality-test-tenant"));
        assertThat(result.totalRows()).isEqualTo(3);
        assertThat(result.failedRows()).isEqualTo(2);
    }

    @Test
    void buildSampleSql_constructsCorrectQuery() throws SQLException {
        executeSql("INSERT INTO schema.test_table(status) VALUES ('active'),('inactive'),(NULL)");
        var request = request("status = 'active'", QualityQueryType.SAMPLES);
        String sql = QualityReadSql.render(request);
        assertThat(sql).contains("SELECT *", "FROM \"catalog\".\"schema\".\"test_table\"");
        assertThat(sql).contains("IS NOT TRUE", "'active'");
        assertThat(sql).contains("LIMIT " + QualityQueryResult.MAX_SAMPLES);
        var result = runner.execute(request,
                new QualityExecutionIdentity("quality-test-user", "quality-test-tenant"));
        assertThat(result.failingSamples()).hasSize(2);
        assertThat(result.failingSamples()).extracting(row -> row.get(3))
                .containsExactlyInAnyOrder("inactive", null);
    }

    @Test
    void noFailingRows_doesNotRunSampleQuery() throws SQLException {
        executeSql("INSERT INTO schema.test_table(col) SELECT generate_series(1,10)");
        var result = executor.execute(rule(7, "no_sample", "col > 0"));
        assertThat(result.passed()).isTrue();
        assertThat(runner.queries).hasSize(1);
        assertThat(runner.queries.get(0)).startsWith("SELECT COUNT(*)");
    }

    @Test
    void unknownRowsFailAndActualSampleRemainsBounded() throws SQLException {
        executeSql("INSERT INTO schema.test_table(column_x) SELECT NULL FROM generate_series(1,20)");
        executeSql("INSERT INTO schema.test_table(column_x) VALUES (1),(0)");
        var result = executor.execute(rule(8, "unknown_check", "column_x > 0"));
        assertThat(result.passed()).isFalse();
        assertThat(result.totalRows()).isEqualTo(22);
        assertThat(result.failedRows()).isEqualTo(21);
        assertThat(result.errorMessage()).contains("21 of 22", "Sample violations: 10 rows");
        assertThat(runner.results.get(1).failingSamples()).hasSize(QualityQueryResult.MAX_SAMPLES);
        assertThat(runner.results.get(1).failingSamples()).allSatisfy(row ->
                assertThat(row.get(0) == null || row.get(0).equals("0")).isTrue());
    }

    @Test
    void emptyActualTablePassesWithoutSampling() {
        var result = executor.execute(rule(9, "empty_check", "column_x > 0"));
        assertThat(result.passed()).isTrue();
        assertThat(result.totalRows()).isZero();
        assertThat(result.failedRows()).isZero();
        assertThat(runner.queries).hasSize(1);
    }

    @Test
    void quotedKeywordsAndCommentCharactersInLiteralsAreOrdinaryData() throws SQLException {
        executeSql("INSERT INTO schema.test_table(label) VALUES ('drop; -- /* union'),('other'),(NULL)");
        var result = executor.execute(rule(10, "literal_check", "label = 'drop; -- /* union'"));
        assertThat(result.totalRows()).isEqualTo(3);
        assertThat(result.failedRows()).isEqualTo(2);
        assertThat(runner.results.get(1).failingSamples()).hasSize(2);
    }

    @Test
    void statementEscapeAndUnboundParametersAreRejectedBeforeSqlExecution() {
        for (String expression : List.of("col > 0; DROP TABLE schema.test_table", "col = ?",
                "col > 0) THEN 1 END) FROM other --", "col > 0) FROM schema.test_table --",
                "col > 0), TRUE --", "col > 0) AS alias --", "col > 0) UNION SELECT TRUE --",
                "col > 0) WHERE TRUE --", "col > 0) LIMIT 1 --")) {
            var result = executor.execute(rule(11, "malformed_check", expression));
            assertThat(result.passed()).isFalse();
            assertThat(runner.queries).isEmpty();
        }
    }

    @Test
    void legitimateReadSubqueryRemainsInsideThePredicate() throws SQLException {
        executeSql("CREATE TABLE schema.reference_values (value INTEGER)");
        executeSql("INSERT INTO schema.reference_values VALUES (2)");
        executeSql("INSERT INTO schema.test_table(col) VALUES (1),(2),(NULL)");
        var result = executor.execute(rule(12, "reference_check",
                "col IN (SELECT value FROM schema.reference_values)"));
        assertThat(result.totalRows()).isEqualTo(3);
        assertThat(result.failedRows()).isEqualTo(2);
        assertThat(runner.results.get(1).failingSamples()).hasSize(2);
    }

    @Test
    void QuotedIdentifiersDoNotBecomeSqlFragments() throws SQLException {
        executeSql("CREATE TABLE schema.\"odd\"\"table\" (\"drop\" TEXT)");
        executeSql("INSERT INTO schema.\"odd\"\"table\" VALUES ('delete'),('other'),(NULL)");
        var now = Instant.now();
        var rule = new DataQualityRule(13, "quoted_rule", "catalog", "schema", "odd\"table", "\"drop\" = 'delete'",
                Severity.WARNING, "message", true, null, now, now,
                new QualityExecutionIdentity("quality-test-user", "quality-test-tenant"), TABLE_ID);
        var result = executor.execute(rule);
        assertThat(result.totalRows()).isEqualTo(3);
        assertThat(result.failedRows()).isEqualTo(2);
    }

    private static QualityReadRequest request(String expression, QualityQueryType type) {
        return new QualityReadRequest("catalog", "schema", "test_table", TABLE_ID, expression, type);
    }

    private static void executeSql(String sql) throws SQLException {
        try (var connection = DriverManager.getConnection(
                QualityPostgres.jdbcUrl(), "pangrova", "pangrova_test");
             var statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static DataQualityRule rule(long id, String name, String expression) {
        return new DataQualityRule(id, name, "catalog", "schema", "test_table",
                expression, Severity.WARNING, "msg", true, null, Instant.now(), Instant.now(),
                new QualityExecutionIdentity("quality-test-user", "quality-test-tenant"), TABLE_ID);
    }

    /** Test-only independent engine; every returned count and row is read from the actual SQL result. */
    private static final class PostgresSqlOracle implements GovernanceQueryRunner {
        private final List<String> queries = new ArrayList<>();
        private final List<QualityQueryResult> results = new ArrayList<>();
        private SQLException lastFailure;

        @Override
        public QualityQueryResult execute(QualityReadRequest request, QualityExecutionIdentity identity) {
            String sql = QualityReadSql.render(request);
            assertThat(new org.pangrova.parser.syntax.AntlrSqlParser().parse(sql))
                    .as("generated SQL must use the actual production grammar").isNotNull();
            QualityQueryType type = request.type();
            assertThat(identity).isEqualTo(new QualityExecutionIdentity("quality-test-user", "quality-test-tenant"));
            queries.add(sql);
            try (var connection = DriverManager.getConnection(
                    QualityPostgres.jdbcUrl(), "pangrova", "pangrova_test");
                 var statement = connection.createStatement();
                 var rows = statement.executeQuery(sql)) {
                var metadata = rows.getMetaData();
                QualityQueryResult result;
                if (type == QualityQueryType.COUNTS) {
                    if (!rows.next()) {
                        throw new IllegalStateException("Actual aggregate returned no result");
                    }
                    result = new QualityQueryResult(rows.getLong("total_rows"), rows.getLong("failed_rows"), List.of());
                    assertThat(rows.next()).isFalse();
                } else {
                    List<List<String>> samples = new ArrayList<>();
                    while (rows.next()) {
                        List<String> row = new ArrayList<>();
                        for (int column = 1; column <= metadata.getColumnCount(); column++) {
                            row.add(rows.getString(column));
                        }
                        samples.add(row);
                    }
                    result = new QualityQueryResult(samples.size(), samples.size(), samples);
                }
                results.add(result);
                return result;
            } catch (SQLException failure) {
                lastFailure = failure;
                throw new GovernanceQueryException(sql, failure.getMessage(), failure);
            }
        }
    }
}
