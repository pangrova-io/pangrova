package org.pangrova.governance.quality.gate;

import org.pangrova.governance.quality.QualityCheckResult;
import org.pangrova.governance.quality.alert.Severity;
import org.pangrova.governance.quality.rule.DataQualityRule;
import org.pangrova.governance.quality.rule.QualityRuleExecutor;
import org.pangrova.governance.quality.rule.QualityRuleStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.pangrova.common.util.Preconditions.checkNotNull;

/**
 * Evaluates all enabled quality rules for a target table before write commit.
 *
 * <p>Rules with {@link Severity#ERROR} that fail will block the write.
 * Rules with {@link Severity#WARNING} or {@link Severity#INFO} are evaluated
 * and results stored, but do not block the write.</p>
 *
 * <p>Each rule result is persisted to the {@link QualityRuleStore} regardless
 * of pass/fail status, providing a complete audit trail of pre-commit checks.</p>
 */
public final class RuleBasedWriteQualityGate implements WriteQualityGate {

    private static final Logger LOG = LoggerFactory.getLogger(RuleBasedWriteQualityGate.class);

    private final QualityRuleStore ruleStore;
    private final QualityRuleExecutor ruleExecutor;

    public RuleBasedWriteQualityGate(QualityRuleStore ruleStore, QualityRuleExecutor ruleExecutor) {
        this.ruleStore = checkNotNull(ruleStore, "ruleStore must not be null");
        this.ruleExecutor = checkNotNull(ruleExecutor, "ruleExecutor must not be null");
    }

    @Override
    public WriteValidationResult validate(String catalogName, String schemaName, String tableName) {
        checkNotNull(catalogName, "catalogName must not be null");
        checkNotNull(schemaName, "schemaName must not be null");
        checkNotNull(tableName, "tableName must not be null");

        List<DataQualityRule> rules = ruleStore.getRulesForTable(catalogName, schemaName, tableName)
                .stream()
                .filter(DataQualityRule::enabled)
                .toList();

        if (rules.isEmpty()) {
            return WriteValidationResult.allowed(List.of());
        }

        List<QualityCheckResult> results = new ArrayList<>(rules.size());
        for (DataQualityRule rule : rules) {
            QualityCheckResult result = executeRule(rule);
            results.add(result);
            ruleStore.saveResult(result);
        }

        List<QualityCheckResult> blockingFailures = results.stream()
                .filter(r -> !r.passed() && r.severity() == Severity.ERROR)
                .toList();

        if (blockingFailures.isEmpty()) {
            LOG.info("event=write_quality_gate_passed table={}.{}.{} rulesEvaluated={}",
                    catalogName, schemaName, tableName, rules.size());
            return WriteValidationResult.allowed(results);
        }

        String reason = buildBlockReason(blockingFailures);
        LOG.warn("event=write_quality_gate_blocked table={}.{}.{} failedRules={}",
                catalogName, schemaName, tableName, blockingFailures.size());
        return WriteValidationResult.blocked(results, reason);
    }

    private QualityCheckResult executeRule(DataQualityRule rule) {
        try {
            return ruleExecutor.execute(rule);
        } catch (Exception e) {
            LOG.error("event=write_gate_rule_execution_failed ruleId={} ruleName={} error={}",
                    rule.ruleId(), rule.ruleName(), e.getMessage());
            return new QualityCheckResult(
                    0, rule.ruleId(), rule.ruleName(), rule.tableName(),
                    rule.severity(), Instant.now(), false, 0, 0, 0,
                    "Rule execution failed: " + e.getMessage());
        }
    }

    private String buildBlockReason(List<QualityCheckResult> failures) {
        return failures.stream()
                .map(f -> "'" + f.ruleName() + "': " + f.errorMessage())
                .collect(Collectors.joining("; ", "Write blocked by quality rules: ", ""));
    }
}
