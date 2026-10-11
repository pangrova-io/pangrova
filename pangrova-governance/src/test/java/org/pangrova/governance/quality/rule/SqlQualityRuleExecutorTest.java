package org.pangrova.governance.quality.rule;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pangrova.governance.quality.alert.Severity;
import org.pangrova.testing.ContainerTestBase;
import org.pangrova.governance.testinfra.SharedPostgresContainer;

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
class SqlQualityRuleExecutorTest extends ContainerTestBase {
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
        String sql = executor.buildCountSql(rule(5, "count_test", "amount > 100"));
        assertThat(sql).contains("COUNT(*)");
        assertThat(sql).contains("COUNT(CASE WHEN (amount > 100) IS NOT TRUE THEN 1 END)");
        assertThat(sql).contains("\"catalog\".\"schema\".\"test_table\"");
        var result = runner.execute(sql);
        assertThat(result.totalRows()).isEqualTo(3);
        assertThat(result.failedRows()).isEqualTo(2);
    }

    @Test
    void buildSampleSql_constructsCorrectQuery() throws SQLException {
        executeSql("INSERT INTO schema.test_table(status) VALUES ('active'),('inactive'),(NULL)");
        String sql = executor.buildSampleSql(rule(6, "sample_test", "status = 'active'"));
        assertThat(sql).contains("SELECT * FROM \"catalog\".\"schema\".\"test_table\"");
        assertThat(sql).contains("WHERE (status = 'active') IS NOT TRUE");
        assertThat(sql).contains("LIMIT " + QualityQueryResult.MAX_SAMPLES);
        var result = runner.execute(sql);
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

    private static void executeSql(String sql) throws SQLException {
        try (var connection = DriverManager.getConnection(
                SharedPostgresContainer.jdbcUrl(), "pangrova", "pangrova_test");
             var statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static DataQualityRule rule(long id, String name, String expression) {
        return new DataQualityRule(id, name, "catalog", "schema", "test_table",
                expression, Severity.WARNING, "msg", true, null, Instant.now(), Instant.now());
    }

    /** Test-only independent engine; every returned count and row is read from the actual SQL result. */
    private static final class PostgresSqlOracle implements GovernanceQueryRunner {
        private final List<String> queries = new ArrayList<>();
        private final List<QualityQueryResult> results = new ArrayList<>();
        private SQLException lastFailure;

        @Override
        public QualityQueryResult execute(String sql) {
            queries.add(sql);
            try (var connection = DriverManager.getConnection(
                    SharedPostgresContainer.jdbcUrl(), "pangrova", "pangrova_test");
                 var statement = connection.createStatement();
                 var rows = statement.executeQuery(sql)) {
                var metadata = rows.getMetaData();
                QualityQueryResult result;
                if (metadata.getColumnCount() == 2 && metadata.getColumnLabel(1).equals("total_rows")) {
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
