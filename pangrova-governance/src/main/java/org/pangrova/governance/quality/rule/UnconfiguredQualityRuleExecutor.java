package org.pangrova.governance.quality.rule;

import org.pangrova.governance.quality.QualityCheckResult;

import java.util.Objects;

/** Rejects evaluation when bootstrap has no authorized data-plane query runner. */
public final class UnconfiguredQualityRuleExecutor implements QualityRuleExecutor {
    @Override
    public QualityCheckResult execute(DataQualityRule rule) {
        Objects.requireNonNull(rule, "rule");
        throw new GovernanceQueryException("", "Quality rule '" + rule.ruleName()
                + "' was not evaluated: no authorized data-plane quality query runner is configured");
    }
}
