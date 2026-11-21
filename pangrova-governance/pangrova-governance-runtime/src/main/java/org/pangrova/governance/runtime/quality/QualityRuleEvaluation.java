package org.pangrova.governance.runtime.quality;

import org.pangrova.governance.api.quality.DataQualityRule;
import org.pangrova.governance.api.quality.QualityCheckResult;
import org.pangrova.governance.api.quality.QualityRuleExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Objects;

/** Evaluates a captured rule and binds its outcome to the rule that granted the operation. */
final class QualityRuleEvaluation {
    private static final Logger LOG = LoggerFactory.getLogger(QualityRuleEvaluation.class);

    private QualityRuleEvaluation() {}

    static QualityCheckResult execute(QualityRuleExecutor executor, DataQualityRule rule) {
        long start = System.nanoTime();
        try {
            QualityCheckResult result = Objects.requireNonNull(executor.execute(rule), "Missing quality result");
            if (result.ruleId() != rule.ruleId() || !Objects.equals(result.tableId(), rule.tableId())
                    || !Objects.equals(result.ruleName(), rule.ruleName())
                    || !Objects.equals(result.tableName(), rule.tableName()) || result.severity() != rule.severity()) {
                throw new IllegalStateException("Quality result does not match the evaluated rule");
            }
            if (result.failedRows() < 0 || result.totalRows() < result.failedRows()
                    || (result.passed() && result.failedRows() != 0)) {
                throw new IllegalStateException("Inconsistent quality result counts");
            }
            return result;
        } catch (RuntimeException failure) {
            long elapsedMs = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            LOG.warn("event=quality_rule_evaluation_failed ruleId={} tableId={}", rule.ruleId(), rule.tableId(),
                    failure);
            return new QualityCheckResult(0, rule.ruleId(), rule.ruleName(), rule.tableName(), rule.severity(),
                    Instant.now(), false, 0, 0, elapsedMs, failure.getMessage(), rule.tableId());
        }
    }
}
