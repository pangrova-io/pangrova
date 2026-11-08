package org.pangrova.governance.core.quality;

import org.pangrova.governance.api.quality.QualityExecutionIdentity;
import org.pangrova.governance.api.quality.QualityCheckResult;
import org.pangrova.governance.api.quality.Severity;
import org.pangrova.governance.api.quality.DataQualityRule;
import org.pangrova.governance.api.quality.QualityRuleExecutor;
import org.pangrova.governance.api.quality.WriteValidationResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that the pre-commit write quality gate blocks writes when
 * ERROR-severity rules fail, and allows writes when all rules pass
 * or only WARNING/INFO rules fail.
 */
class WriteQualityGateTest {

    private InMemoryQualityRuleStore ruleStore;

    @BeforeEach
    void setUp() {
        ruleStore = new InMemoryQualityRuleStore();
    }

    @Test
    void unconfiguredEvaluationBlocksErrorRulesAndPersistsTheCapturedOwner() {
        var tableId = java.util.UUID.randomUUID();
        ruleStore.saveRule(new DataQualityRule(1, "required_positive", "catalog", "schema", "test_table",
                "amount > 0", Severity.ERROR, "msg", true, null, Instant.now(), Instant.now(),
                new QualityExecutionIdentity("quality-test-user", "quality-test-tenant"), tableId));
        var gate = new RuleBasedWriteQualityGate(ruleStore,
                new org.pangrova.governance.core.quality.TestQualityRuleExecutor());

        var result = gate.validate("catalog", "schema", "test_table");

        assertThat(result.allowed()).isFalse();
        assertThat(result.blockReason()).contains("was not evaluated");
        assertThat(ruleStore.getRecentResults()).singleElement().satisfies(audit -> {
            assertThat(audit.passed()).isFalse();
            assertThat(audit.tableId()).isEqualTo(tableId);
            assertThat(audit.totalRows()).isZero();
            assertThat(audit.failedRows()).isZero();
        });
    }

    @Test
    void violatingErrorRule_blocksWrite() {
        DataQualityRule errorRule = rule(1, "not_null_check", "col IS NOT NULL", Severity.ERROR);
        ruleStore.saveRule(errorRule);

        QualityRuleExecutor failingExecutor = rule -> new QualityCheckResult(
                0, rule.ruleId(), rule.ruleName(), rule.tableName(),
                rule.severity(), Instant.now(), false, 100, 5, 10,
                "5 rows have NULL values in col", null);

        RuleBasedWriteQualityGate gate = new RuleBasedWriteQualityGate(ruleStore, failingExecutor);

        WriteValidationResult result = gate.validate("catalog", "schema", "test_table");

        assertThat(result.allowed()).isFalse();
        assertThat(result.blockReason()).contains("not_null_check");
        assertThat(result.ruleResults()).hasSize(1);
        assertThat(result.ruleResults().get(0).passed()).isFalse();
    }

    @Test
    void allRulesPassing_allowsWrite() {
        DataQualityRule rule = rule(1, "positive_check", "amount > 0", Severity.ERROR);
        ruleStore.saveRule(rule);

        QualityRuleExecutor passingExecutor = r -> new QualityCheckResult(
                0, r.ruleId(), r.ruleName(), r.tableName(),
                r.severity(), Instant.now(), true, 100, 0, 5, null, null);

        RuleBasedWriteQualityGate gate = new RuleBasedWriteQualityGate(ruleStore, passingExecutor);

        WriteValidationResult result = gate.validate("catalog", "schema", "test_table");

        assertThat(result.allowed()).isTrue();
        assertThat(result.blockReason()).isNull();
        assertThat(result.ruleResults()).hasSize(1);
        assertThat(result.ruleResults().get(0).passed()).isTrue();
    }

    @Test
    void warningRuleFailing_doesNotBlockWrite() {
        DataQualityRule warningRule = rule(1, "format_check", "email LIKE '%@%'", Severity.WARNING);
        ruleStore.saveRule(warningRule);

        QualityRuleExecutor failingExecutor = r -> new QualityCheckResult(
                0, r.ruleId(), r.ruleName(), r.tableName(),
                r.severity(), Instant.now(), false, 50, 3, 8,
                "3 rows have invalid email format", null);

        RuleBasedWriteQualityGate gate = new RuleBasedWriteQualityGate(ruleStore, failingExecutor);

        WriteValidationResult result = gate.validate("catalog", "schema", "test_table");

        assertThat(result.allowed()).isTrue();
        assertThat(result.blockReason()).isNull();
        assertThat(result.ruleResults()).hasSize(1);
        assertThat(result.ruleResults().get(0).passed()).isFalse();
    }

    @Test
    void noRulesForTable_allowsWrite() {
        RuleBasedWriteQualityGate gate = new RuleBasedWriteQualityGate(
                ruleStore, r -> { throw new AssertionError("should not be called"); });

        WriteValidationResult result = gate.validate("catalog", "schema", "no_rules_table");

        assertThat(result.allowed()).isTrue();
        assertThat(result.ruleResults()).isEmpty();
    }

    @Test
    void disabledRules_areSkipped() {
        DataQualityRule disabled = new DataQualityRule(
                1, "disabled_rule", "catalog", "schema", "test_table",
                "col > 0", Severity.ERROR, "msg", false, null,
                Instant.now(), Instant.now(),
                new QualityExecutionIdentity("quality-test-user", "quality-test-tenant"), null);
        ruleStore.saveRule(disabled);

        RuleBasedWriteQualityGate gate = new RuleBasedWriteQualityGate(
                ruleStore, r -> { throw new AssertionError("should not be called"); });

        WriteValidationResult result = gate.validate("catalog", "schema", "test_table");

        assertThat(result.allowed()).isTrue();
        assertThat(result.ruleResults()).isEmpty();
    }

    @Test
    void multipleRules_oneErrorFails_blocksWrite() {
        ruleStore.saveRule(rule(1, "null_check", "col IS NOT NULL", Severity.ERROR));
        ruleStore.saveRule(rule(2, "range_check", "val BETWEEN 0 AND 100", Severity.WARNING));

        QualityRuleExecutor executor = r -> {
            boolean passed = r.ruleId() != 1;
            return new QualityCheckResult(
                    0, r.ruleId(), r.ruleName(), r.tableName(),
                    r.severity(), Instant.now(), passed, 100,
                    passed ? 0 : 10, 5,
                    passed ? null : "10 violations found", null);
        };

        RuleBasedWriteQualityGate gate = new RuleBasedWriteQualityGate(ruleStore, executor);

        WriteValidationResult result = gate.validate("catalog", "schema", "test_table");

        assertThat(result.allowed()).isFalse();
        assertThat(result.blockReason()).contains("null_check");
        assertThat(result.ruleResults()).hasSize(2);
    }

    @Test
    void resultsArePersistedToStore() {
        ruleStore.saveRule(rule(1, "persist_check", "col > 0", Severity.ERROR));

        QualityRuleExecutor executor = r -> new QualityCheckResult(
                0, r.ruleId(), r.ruleName(), r.tableName(),
                r.severity(), Instant.now(), true, 50, 0, 3, null, null);

        RuleBasedWriteQualityGate gate = new RuleBasedWriteQualityGate(ruleStore, executor);
        gate.validate("catalog", "schema", "test_table");

        List<QualityCheckResult> stored = ruleStore.getRecentResults();
        assertThat(stored).hasSize(1);
        assertThat(stored.get(0).ruleName()).isEqualTo("persist_check");
    }

    @Test
    void executorException_treatedAsFailure() {
        ruleStore.saveRule(rule(1, "crash_rule", "col > 0", Severity.ERROR));

        QualityRuleExecutor crashingExecutor = r -> {
            throw new RuntimeException("executor crashed");
        };

        RuleBasedWriteQualityGate gate = new RuleBasedWriteQualityGate(ruleStore, crashingExecutor);

        WriteValidationResult result = gate.validate("catalog", "schema", "test_table");

        assertThat(result.allowed()).isFalse();
        assertThat(result.blockReason()).contains("crash_rule");
        assertThat(result.ruleResults().get(0).errorMessage()).contains("executor crashed");
    }

    private static DataQualityRule rule(long id, String name, String expression, Severity severity) {
        return new DataQualityRule(id, name, "catalog", "schema", "test_table",
                expression, severity, "msg", true, null,
                Instant.now(), Instant.now(),
                new QualityExecutionIdentity("quality-test-user", "quality-test-tenant"), null);
    }
}
