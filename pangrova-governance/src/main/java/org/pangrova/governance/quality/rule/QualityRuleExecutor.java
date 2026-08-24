package org.pangrova.governance.quality.rule;

import org.pangrova.governance.quality.QualityCheckResult;

/**
 * SPI for executing data quality rules against actual table data.
 *
 * <p>Implementations are responsible for evaluating the rule's
 * {@link DataQualityRule#checkExpression()} against the target table
 * and returning a {@link QualityCheckResult} with accurate metrics.</p>
 */
public interface QualityRuleExecutor {

    /**
     * Execute a quality rule and return the check result.
     *
     * @param rule the quality rule to execute
     * @return result containing pass/fail status and row-level metrics
     */
    QualityCheckResult execute(DataQualityRule rule);
}
