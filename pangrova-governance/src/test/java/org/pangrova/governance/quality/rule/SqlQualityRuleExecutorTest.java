package org.pangrova.governance.quality.rule;

import org.pangrova.governance.quality.QualityCheckResult;
import org.pangrova.governance.quality.alert.Severity;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that SqlQualityRuleExecutor actually executes quality rule SQL
 * against data and reports pass/fail results correctly.
 */
class SqlQualityRuleExecutorTest {

    @Test
    void ruleColumnXGreaterThanZero_violatingRow_reportsFailure() {
        // Simulate: table has 5 rows, 2 violate "column_x > 0"
        GovernanceQueryRunner runner = sql -> {
            if (sql.contains("COUNT(*)")) {
                return new QualityQueryResult(5, 2, List.of());
            }
            // Sample query returns violating rows
            return new QualityQueryResult(2, 2, List.of(
                    List.of("-1", "row_a"),
                    List.of("0", "row_b")
            ));
        };

        SqlQualityRuleExecutor executor = new SqlQualityRuleExecutor(runner);
        DataQualityRule rule = rule(1, "positive_check", "column_x > 0");

        QualityCheckResult result = executor.execute(rule);

        assertThat(result.passed()).isFalse();
        assertThat(result.totalRows()).isEqualTo(5);
        assertThat(result.failedRows()).isEqualTo(2);
        assertThat(result.errorMessage()).contains("column_x > 0");
        assertThat(result.errorMessage()).contains("2 of 5");
        assertThat(result.ruleName()).isEqualTo("positive_check");
    }

    @Test
    void ruleWithAllRowsPassing_reportsSuccess() {
        GovernanceQueryRunner runner = sql ->
                new QualityQueryResult(10, 0, List.of());

        SqlQualityRuleExecutor executor = new SqlQualityRuleExecutor(runner);
        DataQualityRule rule = rule(2, "not_null_check", "col IS NOT NULL");

        QualityCheckResult result = executor.execute(rule);

        assertThat(result.passed()).isTrue();
        assertThat(result.totalRows()).isEqualTo(10);
        assertThat(result.failedRows()).isEqualTo(0);
        assertThat(result.errorMessage()).isNull();
    }

    @Test
    void blankExpression_throwsGovernanceQueryException() {
        GovernanceQueryRunner runner = sql ->
                new QualityQueryResult(0, 0, List.of());

        SqlQualityRuleExecutor executor = new SqlQualityRuleExecutor(runner);
        DataQualityRule rule = rule(3, "blank_rule", "   ");

        assertThatThrownBy(() -> executor.execute(rule))
                .isInstanceOf(GovernanceQueryException.class)
                .hasMessageContaining("blank check expression");
    }

    @Test
    void queryRunnerFailure_returnsFailedResult() {
        GovernanceQueryRunner runner = sql -> {
            throw new GovernanceQueryException(sql, "Connection refused");
        };

        SqlQualityRuleExecutor executor = new SqlQualityRuleExecutor(runner);
        DataQualityRule rule = rule(4, "failing_query", "col > 0");

        QualityCheckResult result = executor.execute(rule);

        assertThat(result.passed()).isFalse();
        assertThat(result.errorMessage()).isEqualTo("Connection refused");
    }

    @Test
    void buildCountSql_constructsCorrectQuery() {
        GovernanceQueryRunner runner = sql ->
                new QualityQueryResult(0, 0, List.of());

        SqlQualityRuleExecutor executor = new SqlQualityRuleExecutor(runner);
        DataQualityRule rule = rule(5, "count_test", "amount > 100");

        String sql = executor.buildCountSql(rule);

        assertThat(sql).contains("COUNT(*)");
        assertThat(sql).contains("COUNT(CASE WHEN NOT (amount > 100) THEN 1 END)");
        assertThat(sql).contains("\"catalog\".\"schema\".\"test_table\"");
    }

    @Test
    void buildSampleSql_constructsCorrectQuery() {
        GovernanceQueryRunner runner = sql ->
                new QualityQueryResult(0, 0, List.of());

        SqlQualityRuleExecutor executor = new SqlQualityRuleExecutor(runner);
        DataQualityRule rule = rule(6, "sample_test", "status = 'active'");

        String sql = executor.buildSampleSql(rule);

        assertThat(sql).contains("SELECT * FROM \"catalog\".\"schema\".\"test_table\"");
        assertThat(sql).contains("WHERE NOT (status = 'active')");
        assertThat(sql).contains("LIMIT " + QualityQueryResult.MAX_SAMPLES);
    }

    @Test
    void noFailingRows_doesNotRunSampleQuery() {
        // Track whether sample query is called
        boolean[] sampleCalled = {false};
        GovernanceQueryRunner runner = sql -> {
            if (sql.contains("COUNT(*)")) {
                return new QualityQueryResult(10, 0, List.of());
            }
            sampleCalled[0] = true;
            return new QualityQueryResult(0, 0, List.of());
        };

        SqlQualityRuleExecutor executor = new SqlQualityRuleExecutor(runner);
        DataQualityRule rule = rule(7, "no_sample", "col > 0");

        executor.execute(rule);

        assertThat(sampleCalled[0]).isFalse();
    }

    private static DataQualityRule rule(long id, String name, String expression) {
        return new DataQualityRule(id, name, "catalog", "schema", "test_table",
                expression, Severity.WARNING, "msg", true, null,
                Instant.now(), Instant.now());
    }
}
