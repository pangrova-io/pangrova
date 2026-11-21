package org.pangrova.governance.runtime.quality;

import org.pangrova.governance.api.quality.QualityExecutionIdentity;
import org.pangrova.governance.api.enums.Severity;
import org.pangrova.governance.api.quality.DataQualityRule;
import org.pangrova.governance.api.quality.QualityRuleExecutor;
import org.pangrova.governance.api.quality.QualityCheckResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests on-demand quality check API: table-level and single-rule execution.
 */
class QualityRuleEvaluationTest {

    private InMemoryQualityRuleStore ruleStore;
    private DataQualityAlerter alerter;

    @BeforeEach
    void setUp() {
        ruleStore = new InMemoryQualityRuleStore();
        alerter = new DataQualityAlerter(new AlertChannelRegistry());
    }

    @Test
    void checkTable_executesAllEnabledRulesAndStoresResults() {
        ruleStore.saveRule(rule(1, "rule_a", "col > 0"));
        ruleStore.saveRule(rule(2, "rule_b", "col IS NOT NULL"));

        QualityRuleExecutor executor = r -> new QualityCheckResult(
                0, r.ruleId(), r.ruleName(), r.tableName(),
                r.severity(), Instant.now(), true, 50, 0, 5, null, null);

        var service = new GovernanceStatementHandler(ruleStore, executor, alerter);

        List<QualityCheckResult> results = service.handleRunCheck("catalog", "schema", "orders");

        assertThat(results).hasSize(2);
        assertThat(results).allMatch(QualityCheckResult::passed);
        assertThat(ruleStore.getRecentResults()).hasSize(2);
    }

    @Test
    void checkTable_returnsFailuresWithMetrics() {
        ruleStore.saveRule(rule(1, "positive_amount", "amount > 0"));

        QualityRuleExecutor executor = r -> new QualityCheckResult(
                0, r.ruleId(), r.ruleName(), r.tableName(),
                r.severity(), Instant.now(), false, 1000, 42, 15,
                "42 rows have non-positive amount", null);

        var service = new GovernanceStatementHandler(ruleStore, executor, alerter);

        List<QualityCheckResult> results = service.handleRunCheck("catalog", "schema", "orders");

        assertThat(results).hasSize(1);
        QualityCheckResult result = results.get(0);
        assertThat(result.passed()).isFalse();
        assertThat(result.totalRows()).isEqualTo(1000);
        assertThat(result.failedRows()).isEqualTo(42);
        assertThat(result.errorMessage()).contains("non-positive amount");
    }

    @Test
    void checkRule_executesSingleRuleById() {
        ruleStore.saveRule(rule(42, "specific_rule", "status IN ('A','B','C')"));

        QualityRuleExecutor executor = r -> new QualityCheckResult(
                0, r.ruleId(), r.ruleName(), r.tableName(),
                r.severity(), Instant.now(), true, 200, 0, 3, null, null);

        var service = new GovernanceStatementHandler(ruleStore, executor, alerter);

        QualityCheckResult result = service.handleRunRule(42).orElseThrow();

        assertThat(result.ruleName()).isEqualTo("specific_rule");
        assertThat(result.passed()).isTrue();
        assertThat(ruleStore.getRecentResults()).hasSize(1);
    }

    @Test
    void missingRuleIsSkippedWithoutEvaluatingAnotherRule() {
        QualityRuleExecutor executor = r -> { throw new AssertionError("should not be called"); };
        var service = new GovernanceStatementHandler(ruleStore, executor, alerter);

        assertThat(service.handleRunRule(999)).isEmpty();
    }

    @Test
    void checkTable_skipsDisabledRules() {
        ruleStore.saveRule(rule(1, "enabled_rule", "col > 0"));
        ruleStore.saveRule(new DataQualityRule(
                2, "disabled_rule", "catalog", "schema", "orders",
                "col IS NOT NULL", Severity.WARNING, "msg", false, null,
                Instant.now(), Instant.now(),
                new QualityExecutionIdentity("quality-test-user", "quality-test-tenant"), null));

        QualityRuleExecutor executor = r -> new QualityCheckResult(
                0, r.ruleId(), r.ruleName(), r.tableName(),
                r.severity(), Instant.now(), true, 10, 0, 1, null, null);

        var service = new GovernanceStatementHandler(ruleStore, executor, alerter);

        List<QualityCheckResult> results = service.handleRunCheck("catalog", "schema", "orders");

        assertThat(results).hasSize(1);
        assertThat(results.get(0).ruleName()).isEqualTo("enabled_rule");
    }

    @Test
    void checkTable_noRules_returnsEmptyList() {
        QualityRuleExecutor executor = r -> { throw new AssertionError("should not be called"); };
        var service = new GovernanceStatementHandler(ruleStore, executor, alerter);

        List<QualityCheckResult> results = service.handleRunCheck("catalog", "schema", "empty_table");

        assertThat(results).isEmpty();
    }

    private static DataQualityRule rule(long id, String name, String expression) {
        return new DataQualityRule(id, name, "catalog", "schema", "orders",
                expression, Severity.WARNING, "msg", true, null,
                Instant.now(), Instant.now(),
                new QualityExecutionIdentity("quality-test-user", "quality-test-tenant"), null);
    }
}
