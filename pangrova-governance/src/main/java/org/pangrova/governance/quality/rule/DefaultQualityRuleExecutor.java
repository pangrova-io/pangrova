package org.pangrova.governance.quality.rule;

import org.pangrova.governance.quality.QualityCheckResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

/**
 * Default {@link QualityRuleExecutor} that evaluates rules by checking
 * the rule definition validity. Returns a meaningful result based on
 * whether the rule's check expression is well-formed.
 *
 * <p>Production deployments should use {@link SqlQualityRuleExecutor}
 * which evaluates {@link DataQualityRule#checkExpression()} against actual
 * table data via the query engine. This default is retained for tests and
 * environments where no {@link GovernanceQueryRunner} is available.</p>
 */
public final class DefaultQualityRuleExecutor implements QualityRuleExecutor {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultQualityRuleExecutor.class);

    /** Nanoseconds per millisecond, for elapsed-time conversion. */
    private static final long NANOS_PER_MILLI = 1_000_000;

    @Override
    public QualityCheckResult execute(DataQualityRule rule) {
        long startNanos = System.nanoTime();
        boolean passed = validateRule(rule);
        long elapsedMs = (System.nanoTime() - startNanos) / NANOS_PER_MILLI;
        String errorMessage = passed ? null
                : "Rule check expression is blank or rule is misconfigured";
        return new QualityCheckResult(
                0, rule.ruleId(), rule.ruleName(), rule.tableName(),
                rule.severity(), Instant.now(), passed, 0, 0, elapsedMs,
                errorMessage);
    }

    private boolean validateRule(DataQualityRule rule) {
        if (rule.checkExpression() == null || rule.checkExpression().isBlank()) {
            LOG.warn("event=quality_rule_invalid ruleId={} ruleName={} reason=blank_expression",
                    rule.ruleId(), rule.ruleName());
            return false;
        }
        return true;
    }
}
