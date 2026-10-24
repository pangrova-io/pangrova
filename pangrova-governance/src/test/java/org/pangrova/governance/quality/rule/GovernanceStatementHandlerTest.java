package org.pangrova.governance.quality.rule;

import org.pangrova.governance.quality.QualityCheckResult;
import org.pangrova.governance.quality.alert.AlertChannelRegistry;
import org.pangrova.governance.quality.alert.DataQualityAlerter;
import org.pangrova.governance.quality.alert.Severity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests GovernanceStatementHandler uses QualityRuleExecutor for actual
 * quality check execution instead of hardcoded passed=true.
 */
class GovernanceStatementHandlerTest {

    private InMemoryQualityRuleStore ruleStore;
    private GovernanceStatementHandler handler;

    @BeforeEach
    void setUp() {
        ruleStore = new InMemoryQualityRuleStore();
        AlertChannelRegistry channelRegistry = new AlertChannelRegistry();
        DataQualityAlerter alerter = new DataQualityAlerter(channelRegistry);
        handler = new GovernanceStatementHandler(
                ruleStore, new DefaultQualityRuleExecutor(), alerter);
    }

    @Test
    void handleRunCheckUsesExecutorNotHardcoded() {
        DataQualityRule rule = rule(1, "null_check", "col IS NOT NULL");
        ruleStore.saveRule(rule);

        List<QualityCheckResult> results = handler.handleRunCheck(
                "catalog", "schema", "table");

        assertEquals(1, results.size());
        QualityCheckResult result = results.get(0);
        assertEquals("null_check", result.ruleName());
        // With DefaultQualityRuleExecutor, a rule with valid expression passes
        assertTrue(result.passed());
    }

    @Test
    void handleRunCheckReportsFailureForBlankExpression() {
        // Rule with blank checkExpression should fail validation
        FailingRuleExecutor failingExecutor = new FailingRuleExecutor();
        GovernanceStatementHandler failHandler = new GovernanceStatementHandler(
                ruleStore, failingExecutor,
                new DataQualityAlerter(new AlertChannelRegistry()));

        DataQualityRule rule = rule(2, "bad_rule", "col > 0");
        ruleStore.saveRule(rule);

        List<QualityCheckResult> results = failHandler.handleRunCheck(
                "catalog", "schema", "table");

        assertEquals(1, results.size());
        assertFalse(results.get(0).passed());
        assertEquals("simulated failure", results.get(0).errorMessage());
    }

    @Test
    void handleRunCheckSkipsDisabledRules() {
        DataQualityRule enabled = rule(1, "enabled_rule", "col > 0");
        DataQualityRule disabled = new DataQualityRule(
                2, "disabled_rule", "catalog", "schema", "table",
                "col > 0", Severity.WARNING, "msg", false, null,
                Instant.now(), Instant.now(),
                new QualityExecutionIdentity("quality-test-user", "quality-test-tenant"));
        ruleStore.saveRule(enabled);
        ruleStore.saveRule(disabled);

        List<QualityCheckResult> results = handler.handleRunCheck(
                "catalog", "schema", "table");

        assertEquals(1, results.size());
        assertEquals("enabled_rule", results.get(0).ruleName());
    }

    @Test
    void handleRunCheckHandlesExecutorException() {
        GovernanceStatementHandler exHandler = new GovernanceStatementHandler(
                ruleStore,
                rule -> { throw new RuntimeException("executor crash"); },
                new DataQualityAlerter(new AlertChannelRegistry()));

        ruleStore.saveRule(rule(1, "crash_rule", "col > 0"));

        List<QualityCheckResult> results = exHandler.handleRunCheck(
                "catalog", "schema", "table");

        assertEquals(1, results.size());
        assertFalse(results.get(0).passed());
        assertEquals("executor crash", results.get(0).errorMessage());
    }

    @Test
    void handleCreateRulePersistsToStore() {
        DataQualityRule rule = rule(10, "new_rule", "col IS NOT NULL");
        handler.handleCreateRule(rule);

        assertTrue(ruleStore.getRule(10).isPresent());
        assertEquals("new_rule", ruleStore.getRule(10).get().ruleName());
    }

    @Test
    void handleShowRulesReturnsMatchingRules() {
        ruleStore.saveRule(rule(1, "rule_a", "col > 0"));
        ruleStore.saveRule(rule(2, "rule_b", "col IS NOT NULL"));

        List<DataQualityRule> rules = handler.handleShowRules(
                "catalog", "schema", "table");

        assertEquals(2, rules.size());
    }

    private static DataQualityRule rule(long id, String name, String expression) {
        return new DataQualityRule(id, name, "catalog", "schema", "table",
                expression, Severity.WARNING, "msg", true, null,
                Instant.now(), Instant.now(),
                new QualityExecutionIdentity("quality-test-user", "quality-test-tenant"));
    }

    /** Executor that always returns a failed result. */
    static class FailingRuleExecutor implements QualityRuleExecutor {
        @Override
        public QualityCheckResult execute(DataQualityRule rule) {
            return new QualityCheckResult(
                    0, rule.ruleId(), rule.ruleName(), rule.tableName(),
                    rule.severity(), Instant.now(), false, 100, 50, 10,
                    "simulated failure");
        }
    }
}
